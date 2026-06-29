import asyncio
import random
from typing import Optional

from playwright.async_api import Page, ElementHandle

# ── 1. Shadow root interceptor (must be injected BEFORE page load) ──
SHADOW_INTERCEPT_SCRIPT = """
(() => {
    if (window.__shadowRoots) return;
    window.__shadowRoots = [];
    const original = Element.prototype.attachShadow;
    Element.prototype.attachShadow = function(init) {
        const shadow = original.call(this, init);
        window.__shadowRoots.push(shadow);
        return shadow;
    };
})();
"""

# ── 2. Shadow-piercing JS to run INSIDE the iframe ──
PIERCE_CHECKBOX_JS = """
() => {
    function pierceShadow(root, selector) {
        let el = root.querySelector(selector);
        if (el) return el;
        for (const elem of root.querySelectorAll('*')) {
            if (elem.shadowRoot) {
                const found = pierceShadow(elem.shadowRoot, selector);
                if (found) return found;
            }
        }
        return null;
    }

    function findCheckbox(root) {
        let el = pierceShadow(root, 'input[type="checkbox"]');
        if (el) return el;
        const walker = document.createTreeWalker(
            root, NodeFilter.SHOW_ELEMENT, null, false
        );
        let node;
        while (node = walker.nextNode()) {
            if (node.shadowRoot) {
                const found = findCheckbox(node.shadowRoot);
                if (found) return found;
            }
            if (node.tagName === 'INPUT' && node.type === 'checkbox') return node;
            if (node.getAttribute('role') === 'checkbox') return node;
            if (node.classList && (
                node.classList.contains('cb-lb') ||
                node.classList.contains('cb-c')
            )) return node;
        }
        return null;
    }

    const cb = findCheckbox(document);
    if (!cb) return null;
    const rect = cb.getBoundingClientRect();
    return {
        x: rect.left + rect.width / 2,
        y: rect.top + rect.height / 2,
        width: rect.width,
        height: rect.height,
        found: true
    };
}
"""

def pierce_and_return_iframe_js():
    """
    Returns an IIFE that pierces intercepted closed shadow roots on the
    parent page and returns the actual <iframe> HTMLIFrameElement so
    Playwright can wrap it in an ElementHandle.
    """
    return """
    () => {
        function findInRoot(root) {
            const iframes = root.querySelectorAll('iframe');
            for (const iframe of iframes) {
                const src = iframe.src || iframe.getAttribute('src') || '';
                if (src.includes('challenges.cloudflare.com') || src.includes('turnstile.cloudflare.com')) {
                    return iframe;
                }
            }
            if (window.__shadowRoots) {
                for (const shadow of window.__shadowRoots) {
                    try {
                        const found = findInRoot(shadow);
                        if (found) return found;
                    } catch (e) {}
                }
            }
            return null;
        }

        function findInDoc(doc) {
            const result = findInRoot(doc);
            if (result) return result;
            const iframes = doc.querySelectorAll('iframe');
            for (const iframe of iframes) {
                try {
                    const childDoc = iframe.contentDocument;
                    if (childDoc) {
                        const found = findInDoc(childDoc);
                        if (found) return found;
                    }
                } catch (e) {}
            }
            return null;
        }

        return findInDoc(document);
    }
    """


async def is_challenge_present(page: Page) -> bool:
    """More robust challenge detection."""
    try:
        title = await page.title()
        content = await page.content()
        return (
            'Just a moment...' in title
            or 'NG Guard' in title
            or 'Reddit - Please wait for verification' in title
            or 'Enable JavaScript and cookies to continue' in content
            or 'This requires JavaScript. Enable JavaScript and then reload the page.' in content
            or 'This may be caused by certain browser extensions, such as ad blockers, or by connecting through a VPN or proxy.' in content
            or '<div id="loading-error" role="alert" aria-live="polite">' in content
            or 'Please wait while your request is being verified...' in content
            or '<img class="loading-img" src="/pre-loading.png">' in content
            or 'https://assets.guns.lol/wasm/gpp_gunslol.js' in content
            or 'Please enable JavaScript to continue.' in content
            or 'Verifying your browser, please wait' in content
            or 'Checking your browser' in content
            or 'cf-challenge' in content
        )
    except Exception:
        return False


async def get_cf_iframe_handle(page: Page) -> Optional[ElementHandle]:
    """Return the iframe ElementHandle on the parent page (not the Frame object)."""
    try:
        handle = await page.evaluate_handle(pierce_and_return_iframe_js())
        if handle and await handle.evaluate("el => !!el"):
            return handle
    except Exception as e:
        print(f"Shadow piercing failed: {e}")
    return None


async def click_cf_checkbox(page: Page, max_attempts: int = 60) -> bool:
    """
    Clicks the Cloudflare Turnstile checkbox using viewport coordinates.
    This bypasses cross-origin and closed Shadow DOM barriers.
    """
    for attempt in range(max_attempts):
        # ── Check if already solved ──
        if not await is_challenge_present(page):
            return True

        # ── Locate iframe element handle ──
        iframe_handle = await get_cf_iframe_handle(page)
        if not iframe_handle:
            await asyncio.sleep(0.5)
            if attempt == 30:
                await page.reload(wait_until="domcontentloaded", timeout=5000)
            continue

        # ── Get iframe viewport bounding box ──
        box = await iframe_handle.bounding_box()
        if not box:
            await asyncio.sleep(0.5)
            continue

        # ── Strategy A: Coordinate-based click ──
        candidates = [
            (box['x'] + 20, box['y'] + box['height'] / 2),
            (box['x'] + box['width'] * 0.12, box['y'] + box['height'] / 2),
            (box['x'] + 30, box['y'] + 30),
        ]

        clicked = False
        for cx, cy in candidates:
            try:
                steps = random.randint(10, 20)
                await page.mouse.move(cx, cy, steps=steps)
                await asyncio.sleep(random.uniform(0.08, 0.25))

                jitter_x = random.uniform(-2, 2)
                jitter_y = random.uniform(-2, 2)
                await page.mouse.move(cx + jitter_x, cy + jitter_y, steps=3)
                await asyncio.sleep(random.uniform(0.05, 0.15))

                await page.mouse.click(cx + jitter_x, cy + jitter_y)
                clicked = True
                break
            except Exception as e:
                continue

        # ── Strategy B: Shadow-piercing JS inside iframe + coordinate click ──
        if not clicked:
            try:
                frame = await iframe_handle.content_frame()
                if frame:
                    coords = await frame.evaluate(PIERCE_CHECKBOX_JS)
                    if coords and coords.get('found'):
                        abs_x = box['x'] + coords['x']
                        abs_y = box['y'] + coords['y']
                        await page.mouse.move(abs_x, abs_y, steps=12)
                        await asyncio.sleep(0.1)
                        await page.mouse.click(abs_x, abs_y)
                        clicked = True
            except Exception as e:
                print(f"Shadow-pierce fallback failed: {e}")

        if not clicked:
            await asyncio.sleep(0.5)
            continue

        # ── Wait for challenge clearance ──
        for _ in range(12):
            await asyncio.sleep(1)
            if not await is_challenge_present(page):
                return True

    return False
