import sys
from pathlib import Path
import camoufox.pkgman

DIST_DIR = Path(__file__).parent / "../dist"

camoufox.pkgman.INSTALL_DIR = DIST_DIR.resolve()

if hasattr(camoufox.pkgman, 'LAUNCH_FILE') and camoufox.pkgman.OS_NAME == 'lin':
    camoufox.pkgman.LAUNCH_FILE['lin'] = 'camoufox'

_orig_camoufox_path = camoufox.pkgman.camoufox_path

def _patched_camoufox_path(download_if_missing=True):
    """Return the local dist folder, bypassing internal registry entirely."""
    return camoufox.pkgman.INSTALL_DIR

camoufox.pkgman.camoufox_path = _patched_camoufox_path

_orig_installed_verstr = camoufox.pkgman.installed_verstr

def _patched_installed_verstr():
    """Return a fake version string that satisfies launch_options()."""
    return "150.0.2-alpha.26"

camoufox.pkgman.installed_verstr = _patched_installed_verstr
_orig_launch_path = camoufox.pkgman.launch_path

def _patched_launch_path(browser_path=None):
    """Return the camoufox executable path directly. No existence checks."""
    if browser_path is None:
        browser_path = camoufox.pkgman.INSTALL_DIR

    os_name = camoufox.pkgman.OS_NAME
    exe_name = camoufox.pkgman.LAUNCH_FILE.get(os_name, 'camoufox')

    # macOS executable is nested inside the .app bundle
    if os_name == 'mac':
        return str(
            browser_path / 'Camoufox.app' / 'Contents' / 'Resources' / exe_name
        )

    return str(browser_path / exe_name)

camoufox.pkgman.launch_path = _patched_launch_path

for mod_name in list(sys.modules.keys()):
    if not mod_name.startswith('camoufox.'):
        continue
    mod = sys.modules[mod_name]
    if hasattr(mod, 'camoufox_path') and mod.camoufox_path is _orig_camoufox_path:
        mod.camoufox_path = _patched_camoufox_path
    if hasattr(mod, 'installed_verstr') and mod.installed_verstr is _orig_installed_verstr:
        mod.installed_verstr = _patched_installed_verstr
    if hasattr(mod, 'launch_path') and mod.launch_path is _orig_launch_path:
        mod.launch_path = _patched_launch_path
