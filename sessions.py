from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from logging import getLogger
from pickle import PickleError, dumps

from curl_cffi import requests
from curl_cffi.requests import AsyncSession
from typing import Any, Awaitable, Callable, Optional, Set
import asyncio

logger = getLogger(__name__)

BackgroundCallback = Callable[['AsyncFuturesSession', Any], Any]

class FuturesSession(AsyncSession):
    def __init__(
        self,
        *,
        max_concurrency: Optional[int] = None,
        session: Optional[AsyncSession] = None,
        adapter_kwargs: Optional[dict] = None,
        **kwargs,
    ):
        """
        :param max_concurrency: Optional maximum number of concurrent requests.
                                If None, concurrency is unlimited.
        :param session: reserved for API compatibility (not used here).
        :param adapter_kwargs: reserved for compatibility.
        :param kwargs: passed to AsyncSession constructor.
        """
        super().__init__(**kwargs)
        self._semaphore = (
            asyncio.Semaphore(max_concurrency) if max_concurrency and max_concurrency > 0 else None
        )
        self.session = session
        self._tasks: Set[asyncio.Task] = set()
        self._closed = False

    async def _run_request(self, method: str, url: str, **kwargs):
        """
        Internal coroutine that actually performs the async request, honoring
        the optional semaphore.
        """
        sem = self._semaphore
        if sem is None:
            resp = await super().request(method, url, **kwargs)
            return resp

        async with sem:
            resp = await super().request(method, url, **kwargs)
            return resp

    def _schedule_task(self, coro: Awaitable, *, track: bool = True) -> asyncio.Task:
        """Create a task and optionally track it for shutdown/cleanup."""
        task = asyncio.create_task(coro)
        if track:
            self._tasks.add(task)

            def _on_done(_t: asyncio.Task):
                self._tasks.discard(_t)

            task.add_done_callback(_on_done)
        return task

    def request(
        self,
        method: str,
        url: str,
        *,
        background_callback: Optional[BackgroundCallback] = None,
        create_task: bool = True,
        **kwargs,
    ) -> asyncio.Task | Awaitable:
        """
        Start an async request.

        :param method: HTTP method string, e.g. 'GET'
        :param url: target URL
        :param background_callback: optional callback(self, resp) executed after response arrives.
                                    If coroutine function, it will be awaited; otherwise will be run
                                    in the default thread executor.
        :param create_task: if True (default) returns an asyncio.Task started immediately.
                            If False, returns the coroutine (awaitable) which the caller can
                            await or schedule.
        :param kwargs: passed to the underlying AsyncSession.request
        :return: asyncio.Task (if create_task) or coroutine (awaitable)
        """
        if self._closed:
            raise RuntimeError("Session is closed")

        coro = self._run_request(method, url, **kwargs)

        if background_callback:
            async def _with_callback():
                resp = await coro
                try:
                    if asyncio.iscoroutinefunction(background_callback):
                        await background_callback(self, resp)
                    else:
                        loop = asyncio.get_running_loop()

                        await loop.run_in_executor(None, partial(background_callback, self, resp))
                except Exception:
                    logger.exception("Error in background_callback")
                return resp

            wrapped = _with_callback()
            return self._schedule_task(wrapped) if create_task else wrapped

        return self._schedule_task(coro) if create_task else coro

    async def close(self):
        if self._closed:
            return
        self._closed = True

        if cancel_pending:
            # Cancel outstanding tasks
            tasks = list(self._tasks)
            for t in tasks:
                t.cancel()
            # wait for them to finish canceling (best effort)
            await asyncio.gather(*tasks, return_exceptions=True)

    def get(self, url, **kwargs):
        r"""
        Sends a GET request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype : concurrent.futures.Future
        """
        return super(FuturesSession, self).get(url, **kwargs)

    def options(self, url, **kwargs):
        r"""Sends a OPTIONS request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype : concurrent.futures.Future
        """
        return super(FuturesSession, self).options(url, **kwargs)

    def head(self, url, **kwargs):
        r"""Sends a HEAD request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype : concurrent.futures.Future
        """
        return super(FuturesSession, self).head(url, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        r"""Sends a POST request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param json: (optional) json to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype : concurrent.futures.Future
        """
        return super(FuturesSession, self).post(
            url, data=data, json=json, **kwargs
        )

    def put(self, url, data=None, **kwargs):
        r"""Sends a PUT request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype : concurrent.futures.Future
        """
        return super(FuturesSession, self).put(url, data=data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        r"""Sends a PATCH request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype : concurrent.futures.Future
        """
        return super(FuturesSession, self).patch(url, data=data, **kwargs)

    def delete(self, url, **kwargs):
        r"""Sends a DELETE request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype : concurrent.futures.Future
        """
        return super(FuturesSession, self).delete(url, **kwargs)
