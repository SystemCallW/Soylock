import asyncio
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, Dict, Any

from browser import patch

from camoufox import AsyncCamoufox
from playwright.async_api import Page, Error as PlaywrightError
from setup import get_browser_path

from browser.helper import (
    SHADOW_INTERCEPT_SCRIPT,
    is_challenge_present,
    click_cf_checkbox,
)

@dataclass
class BrowserResponse:
    """Drop-in replacement for requests.Response in the Soylock flow."""
    status_code: int
    text: str
    elapsed: timedelta = field(default_factory=lambda: timedelta(seconds=0))
    encoding: str = "utf-8"
    url: str = ""
    headers: Dict[str, str] = field(default_factory=dict)


class BrowserEngine:
    """
    Pool-based tab engine using context-level page events.
    
    Anchor starts on a real origin to reliably spawn tabs, then navigates
    to about:blank. All pool tabs are empty (about:blank) and ready for use.
    
    Captcha behaviour:
        - A single asyncio.Lock ensures only one tab solves a challenge at a time.
        - When a tab sees a challenge it acquires the lock, solves it, then releases it.
        - If the pool is exhausted and another tab is already solving, new requests
          simply wait on the semaphore as usual.
    """

    def __init__(self, max_workers: int = 20, headless: bool = True, proxy: Optional[str] = None):
        self.max_workers = max_workers
        self.headless = headless
        self.proxy = proxy
        self._camoufox = None
        self.browser = None
        self.context = None
        self.anchor = None
        self._page_pool = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(max_workers)
        self._captcha_lock = asyncio.Lock()   # ← global captcha serializer
        self._started = False

    async def start(self):
        if self._started:
            return

        camoufox_config = {
            'headless': self.headless,
            'humanize': True,
            'main_world_eval': True,
            'i_know_what_im_doing': True,
            'config': {'forceScopeAccess': True},
            'disable_coop': True,
            'executable_path': get_browser_path(auto_fetch=True),
            'ff_version': 150,
            'firefox_user_prefs': {
                'browser.link.open_newwindow': 3,
                'browser.link.open_newwindow.restriction': 0,
                'browser.tabs.loadInBackground': True,
                'dom.disable_open_during_load': False,
                'privacy.popups.disable_popup_from_plugins': False,
            }
        }
        if self.proxy:
            camoufox_config['proxy'] = {'server': self.proxy}

        self._camoufox = AsyncCamoufox(**camoufox_config)
        self.browser = await self._camoufox.__aenter__()
        self.context = await self.browser.new_context()

        # Anchor page: start on real origin for reliable tab spawning
        self.anchor = await self.context.new_page()
        await self.anchor.goto("about:blank", wait_until="domcontentloaded")
        await asyncio.sleep(0.2)
        await self.anchor.add_init_script(SHADOW_INTERCEPT_SCRIPT)

        # Pre-allocate empty tabs via injected <a> click
        for i in range(self.max_workers):
            page = await self._create_empty_tab()
            if page is None:
                print(f"Failed to create tab {i+1}, pool size will be smaller")
                break
            await self._page_pool.put(page)

        # NOW navigate anchor to about:blank — tabs are already created
        try:
            await self.anchor.goto("about:blank", wait_until="domcontentloaded", timeout=5000)
        except Exception:
            pass

        actual_size = self._page_pool.qsize()
        if actual_size == 0:
            raise RuntimeError("BrowserEngine failed to create any tabs")

        self._started = True

    async def _create_empty_tab(self) -> Optional[Page]:
        """Create a new empty tab via injected <a> click from anchor."""
        known_before = set(self.context.pages)

        try:
            await self.anchor.evaluate("""() => {
                const a = document.createElement('a');
                a.href = 'about:blank';
                a.target = '_blank';
                a.rel = 'noopener';
                document.body.appendChild(a);
                a.click();
                a.remove();
                return null;
            }""")
        except Exception as e:
            print(f"Link injection failed: {e}")
            return None

        # Wait for new page via context event
        try:
            new_page = await asyncio.wait_for(
                self.context.wait_for_event('page'),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            await asyncio.sleep(0.3)
            current = set(self.context.pages)
            newcomers = current - known_before
            if not newcomers:
                print("New tab was not detected by Playwright")
                return None
            new_page = newcomers.pop()

        # about:blank loads synchronously — just verify readyState
        try:
            for _ in range(20):
                ready = await new_page.evaluate("() => document.readyState")
                if ready in ("complete", "interactive"):
                    break
                await asyncio.sleep(0.05)
        except Exception:
            pass

        try:
            await new_page.add_init_script(SHADOW_INTERCEPT_SCRIPT)
        except Exception as e:
            print(f"add_init_script failed: {e}")

        return new_page

    async def close(self):
        while not self._page_pool.empty():
            try:
                page = self._page_pool.get_nowait()
                if not page.is_closed():
                    await page.close()
            except Exception:
                pass
        if self.anchor and not self.anchor.is_closed():
            try:
                await self.anchor.close()
            except Exception:
                pass
        if self._camoufox:
            await self._camoufox.__aexit__(None, None, None)
        self._started = False

    async def _safe_reset(self, page: Page):
        """Navigate back to blank, swallowing any errors."""
        try:
            await page.goto("about:blank", wait_until="domcontentloaded", timeout=5000)
        except Exception:
            pass

    async def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Any] = None,
        timeout: int = 60,
        allow_redirects: bool = True,
        **kwargs
    ) -> BrowserResponse:
        """
        Execute a request using a pooled tab.
        GET with allow_redirects=True uses full navigation.
        All other methods use Playwright's APIRequestContext.
        
        Captcha solving is globally serialized via _captcha_lock so that
        only one tab attempts to solve a challenge at any given moment.
        """
        async with self._semaphore:
            page = await self._page_pool.get()
            start_time = time.monotonic()

            filtered_headers = {
                k: v for k, v in (headers or {}).items()
                if k.lower() != 'user-agent'
            }

            try:
                # ── GET with full navigation (for WAF / captcha solving) ──
                if method == "GET" and allow_redirects:
                    await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=timeout * 1000,
                    )

                    # ── Captcha gate: globally serialized ──
                    if await is_challenge_present(page):
                        async with self._captcha_lock:
                            solved = await click_cf_checkbox(page)
                            if not solved:
                                print(f"Warning: Could not solve challenge for {url}")
                            await asyncio.sleep(1)

                    perf_status = await page.evaluate(
                        '() => { const nav = performance.getEntriesByType("navigation")[0]; return nav ? nav.responseStatus : 200; }'
                    )
                    status = perf_status or 200
                    final_url = page.url
                    text = await page.content()
                    elapsed = timedelta(seconds=time.monotonic() - start_time)

                    return BrowserResponse(
                        status_code=status,
                        text=text,
                        elapsed=elapsed,
                        url=final_url,
                    )

                # ── GET without redirects, HEAD, POST, PUT ──
                else:
                    req_kwargs = {
                        "headers": filtered_headers,
                        "timeout": timeout * 1000,
                    }
                    if not allow_redirects:
                        req_kwargs["max_redirects"] = 0

                    request_context = page.request
                    if method == "GET":
                        response = await request_context.get(url, **req_kwargs)
                    elif method == "HEAD":
                        response = await request_context.head(url, **req_kwargs)
                    elif method == "POST":
                        if json is not None:
                            req_kwargs["json"] = json
                        response = await request_context.post(url, **req_kwargs)
                    elif method == "PUT":
                        if json is not None:
                            req_kwargs["json"] = json
                        response = await request_context.put(url, **req_kwargs)
                    else:
                        raise RuntimeError(f"Unsupported HTTP method for browser engine: {method}")

                    elapsed = timedelta(seconds=time.monotonic() - start_time)
                    text = await response.text()
                    resp_url = response.url

                    return BrowserResponse(
                        status_code=response.status,
                        text=text,
                        elapsed=elapsed,
                        url=resp_url,
                    )

            except PlaywrightError as e:
                elapsed = timedelta(seconds=time.monotonic() - start_time)
                return BrowserResponse(status_code=0, text=str(e), elapsed=elapsed, url=url)
            except Exception as e:
                elapsed = timedelta(seconds=time.monotonic() - start_time)
                return BrowserResponse(status_code=0, text=str(e), elapsed=elapsed, url=url)
            finally:
                if not page.is_closed():
                    await self._safe_reset(page)
                    await self._page_pool.put(page)

class BrowserSession:
    """
    Thin wrapper that mimics requests.Session / curl_cffi.AsyncSession.
    """

    def __init__(self, engine: BrowserEngine):
        self.engine = engine

    def get(self, url, **kwargs):
        return asyncio.ensure_future(self.engine.request("GET", url, **kwargs))

    def post(self, url, **kwargs):
        return asyncio.ensure_future(self.engine.request("POST", url, **kwargs))

    def head(self, url, **kwargs):
        return asyncio.ensure_future(self.engine.request("HEAD", url, **kwargs))

    def put(self, url, **kwargs):
        return asyncio.ensure_future(self.engine.request("PUT", url, **kwargs))
