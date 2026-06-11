import os
import sys
import platform
import urllib.request
import urllib.error
import zipfile
import shutil
import stat
from pathlib import Path

DIST_DIR = Path("dist")
VALIDATION_FILES = ["properties.json"]

PLATFORM_CONFIGS = {
    # ── Windows (unofficial ─────────────────────────────────────────────────
    "windows-x86_64": {
        "owner": "SystemCallW",
        "repo": "camoufox",
        "tag": "v150.0.2-beta.25",
        "asset_version": "150.0.2-beta.25",
        "os_slug": "win",
        "arch": "x86_64",
    },

    # ── Linux (official) ────────────────────────────────────────────────────
    "linux-x86_64": {
        "owner": "daijro",
        "repo": "camoufox",
        "tag": "v150.0.2-beta.25",
        "asset_version": "150.0.2-alpha.26",
        "os_slug": "lin",
        "arch": "x86_64",
    },
    "linux-arm64": {
        "owner": "daijro",
        "repo": "camoufox",
        "tag": "v150.0.2-beta.25",
        "asset_version": "150.0.2-alpha.25",
        "os_slug": "lin",
        "arch": "arm64",
    },

    # ── macOS (official) ────────────────────────────────────────────────────
    "darwin-x86_64": {
        "owner": "daijro",
        "repo": "camoufox",
        "tag": "v150.0.2-beta.25",
        "asset_version": "150.0.2-alpha.25",
        "os_slug": "macos",
        "arch": "x86_64",
    },
    "darwin-arm64": {
        "owner": "daijro",
        "repo": "camoufox",
        "tag": "v150.0.2-beta.25",
        "asset_version": "150.0.2-alpha.25",
        "os_slug": "macos",
        "arch": "arm64",
    },
}

DEFAULT_TEMPLATE = "camoufox-{asset_version}-{os_slug}.{arch}.zip"

def get_platform_key() -> str:
    """Return a key like 'windows-x86_64', 'linux-arm64', 'darwin-x86_64'."""
    system = platform.system().lower()
    machine = platform.machine().lower()

    if machine in ("amd64", "x86_64", "x64"):
        arch = "x86_64"
    elif machine in ("arm64", "aarch64"):
        arch = "arm64"
    elif machine in ("i386", "i686", "x86"):
        arch = "i686"
    else:
        arch = machine

    return f"{system}-{arch}"

def get_platform_config() -> dict:
    """Resolve the configuration for the current platform."""
    key = get_platform_key()
    cfg = PLATFORM_CONFIGS.get(key)
    if cfg is None:
        system = platform.system().lower()
        for k, v in PLATFORM_CONFIGS.items():
            if k.startswith(f"{system}-"):
                print(f"[setup] Warning: exact platform '{key}' not found. Using closest match '{k}'.")
                return v
        raise RuntimeError(
            f"No PLATFORM_CONFIGS entry for platform '{key}'.\n"
            f"Available keys: {list(PLATFORM_CONFIGS.keys())}"
        )
    return cfg

def get_asset_name(cfg: dict) -> str:
    """Build the asset filename from config, using exact override or template."""
    if "asset_name" in cfg:
        return cfg["asset_name"]
    template = cfg.get("asset_template", DEFAULT_TEMPLATE)
    return template.format(
        asset_version=cfg["asset_version"],
        os_slug=cfg["os_slug"],
        arch=cfg["arch"],
    )

def get_download_url(cfg: dict) -> str:
    """Build the full GitHub release asset download URL."""
    owner = cfg["owner"]
    repo = cfg["repo"]
    tag = cfg["tag"]
    asset = get_asset_name(cfg)
    return f"https://github.com/{owner}/{repo}/releases/download/{tag}/{asset}"

def _get_expected_exe_name() -> str:
    """Return the expected executable name for the current OS."""
    return "camoufox.exe" if platform.system().lower() == "windows" else "camoufox"

def is_browser_installed(dist_path: Path) -> bool:
    """
    Check whether the Camoufox browser bundle is present and valid.

    Validates by checking for at least one marker file (properties.json)
    AND the presence of the executable (camoufox / camoufox.exe).
    The official releases do not include version.json, so we do not require it.
    """
    if not dist_path.exists():
        return False

    # Must have at least one marker file
    has_marker = any((dist_path / marker).exists() for marker in VALIDATION_FILES)
    if not has_marker:
        return False

    # Must also have the executable somewhere in the bundle
    if find_executable(dist_path) is None:
        return False

    return True

def find_executable(dist_path: Path) -> Path | None:
    """Locate the Camoufox executable inside the dist folder."""
    exe_name = _get_expected_exe_name()

    candidate = dist_path / exe_name
    if candidate.exists():
        return candidate

    for sub in dist_path.iterdir():
        if sub.is_dir():
            candidate = sub / exe_name
            if candidate.exists():
                return candidate
    return None

def download_file(url: str, dest: Path, chunk_size: int = 8192) -> None:
    """Download a file with a simple progress indicator."""
    print(f"[setup] Downloading:\n  {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        with urllib.request.urlopen(url, timeout=120) as response:
            total = int(response.headers.get("Content-Length", 0))
            downloaded = 0
            with open(dest, "wb") as f:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = downloaded * 100 // total
                        sys.stdout.write(f"\r[setup] {pct}% ({downloaded:,} / {total:,} bytes)")
                        sys.stdout.flush()
            print("\n[setup] Download complete.")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise RuntimeError(
                f"Asset not found (404) at:\n  {url}\n"
                f"Verify the release tag and asset name are correct."
            ) from e
        raise

def extract_zip(archive_path: Path, dest_dir: Path) -> None:
    """Extract a .zip archive into the destination directory."""
    print(f"[setup] Extracting {archive_path.name} ...")
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(dest_dir)
    print(f"[setup] Extracted to {dest_dir}")

def make_executable(file_path: Path) -> None:
    """Ensure the binary has executable permissions on Unix."""
    if platform.system() != "Windows" and file_path.exists():
        st = os.stat(file_path)
        os.chmod(file_path, st.st_mode | stat.S_IEXEC)

def hoist_nested_bundle(dist_path: Path) -> None:
    """
    If the archive extracted into a single sub-folder (e.g. dist/camoufox-xxx/),
    move its contents up to dist/ so the paths match what Camoufox expects.
    """
    subdirs = [d for d in dist_path.iterdir() if d.is_dir()]
    if len(subdirs) == 1 and not is_browser_installed(dist_path):
        nested = subdirs[0]
        print(f"[setup] Hoisting nested bundle: {nested.name}/ -> dist/")
        for item in nested.iterdir():
            dest = dist_path / item.name
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest)
                else:
                    dest.unlink()
            shutil.move(str(item), str(dest))
        nested.rmdir()

def fetch_browser(dist_path: Path | None = None) -> Path:
    """
    Ensure the browser bundle is present in dist/. Download if missing.
    Returns the path to the dist/ directory.
    """
    if dist_path is None:
        dist_path = DIST_DIR.resolve()
    else:
        dist_path = Path(dist_path).resolve()

    cfg = get_platform_config()
    platform_key = get_platform_key()
    print(f"[setup] Platform detected: {platform_key}")
    print(f"[setup] Release: {cfg['owner']}/{cfg['repo']} @ {cfg['tag']}")
    print(f"[setup] Asset:   {get_asset_name(cfg)}")

    if is_browser_installed(dist_path):
        print(f"[setup] Camoufox browser already present in {dist_path}")
        exe = find_executable(dist_path)
        if exe:
            print(f"[setup] Executable found: {exe}")
        return dist_path

    print(f"[setup] Browser not found in {dist_path}. Fetching from GitHub...")

    url = get_download_url(cfg)
    asset_name = get_asset_name(cfg)
    archive_path = dist_path / asset_name

    download_file(url, archive_path)
    extract_zip(archive_path, dist_path)
    archive_path.unlink(missing_ok=True)

    hoist_nested_bundle(dist_path)

    if not is_browser_installed(dist_path):
        raise RuntimeError(
            "Download succeeded but the extracted bundle does not look like a valid Camoufox install.\n"
            f"Expected to find at least {VALIDATION_FILES} and the executable in {dist_path}"
        )

    exe = find_executable(dist_path)
    if exe:
        make_executable(exe)
        print(f"[setup] Ready. Executable: {exe}")

    return dist_path

def get_dist_path(dist_path: Path | None = None) -> Path:
    """
    Return the resolved dist/ directory path.
    Does NOT download. Use this if you only need the folder path.
    """
    if dist_path is None:
        return DIST_DIR.resolve()
    return Path(dist_path).resolve()

def get_browser_path(
    dist_path: Path | None = None,
    auto_fetch: bool = True,
) -> Path:
    """
    Return the absolute path to the Camoufox executable.

    Parameters
    ----------
    dist_path : Path | None
        Custom dist directory. Defaults to the DIST_DIR config.
    auto_fetch : bool
        If True (default), downloads the browser if it is not already present.
        If False, raises FileNotFoundError when the bundle is missing.

    Returns
    -------
    Path
        Absolute path to the camoufox executable (e.g. .../dist/camoufox.exe).

    Raises
    ------
    FileNotFoundError
        If auto_fetch=False and the browser is not installed.
    RuntimeError
        If the bundle is missing required validation files or the executable
        cannot be located after extraction.
    """
    resolved_dist = get_dist_path(dist_path)

    if not is_browser_installed(resolved_dist):
        if auto_fetch:
            fetch_browser(resolved_dist)
        else:
            raise FileNotFoundError(
                f"Camoufox browser not found in {resolved_dist}. "
                f"Run with auto_fetch=True or pre-install the bundle."
            )

    exe = find_executable(resolved_dist)
    if exe is None:
        raise RuntimeError(
            f"Camoufox bundle exists in {resolved_dist} but no executable was found. "
            f"Expected '{_get_expected_exe_name()}' inside the bundle."
        )

    return exe.resolve()

if __name__ == "__main__":
    from setuptools import setup, find_packages
    from setuptools.command.install import install
    from setuptools.command.develop import develop

    class InstallCommand(install):
        """Runs the browser fetch before the normal install."""
        def run(self):
            fetch_browser()
            super().run()

    class DevelopCommand(develop):
        """Runs the browser fetch before the normal develop (editable install)."""
        def run(self):
            fetch_browser()
            super().run()

    def _cli_main():
        import argparse
        parser = argparse.ArgumentParser(description="Camoufox browser setup helper")
        parser.add_argument(
            "--dist-dir", "-d",
            default=str(DIST_DIR),
            help=f"Directory to install the browser (default: {DIST_DIR})"
        )
        args = parser.parse_args()

        dist = fetch_browser(Path(args.dist_dir))
        print(f"\n[setup] Camoufox is ready at: {dist}")

        exe = find_executable(dist)
        if exe:
            print(f"""
# Usage in Python:
from camoufox.sync_api import Camoufox

with Camoufox(executable_path=r"{exe}") as browser:
    page = browser.new_page()
    page.goto("https://github.com/SystemCallW/Soylock")
""")

    # If invoked with arguments that look like a fetch command, run CLI.
    # Otherwise let setuptools handle it (pip install, etc.).
    if len(sys.argv) > 1 and sys.argv[1] in ("fetch", "--dist-dir", "-d"):
        _cli_main()
        sys.exit(0)

    setup(
        name="soylock",
        version="0.18.0",
        description="",
        author="SystemCallW",
        packages=find_packages(),
        python_requires=">=3.9",
        install_requires=[
            "bs4",
            "pandas",
            "colorama",
            "requests",
            "tls_client",
            "playwright==1.57.0",
            "cloverlabs-camoufox==0.6.0",
        ],
        cmdclass={
            "install": InstallCommand,
            "develop": DevelopCommand,
        },
        entry_points={
            "console_scripts": [
                "camoufox-fetch=setup:main",
            ],
        },
    )
