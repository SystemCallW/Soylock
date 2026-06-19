#! /usr/bin/env python3

"""
Soylock: Find Usernames Across Social Networks Module

This module contains the main logic to search for usernames at social
networks.
"""

import sys

import csv
import signal
import json
import pandas as pd
import os
import re
import tls_client
import asyncio
import requests
import urllib.parse
import codecs
import collections
from parser import Parser
from datetime import datetime
from dataclasses import dataclass
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from json import loads as json_loads
from typing import Any, Optional, Union

from browser.client import BrowserEngine, BrowserSession

from __init__ import (
    __longname__,
    __shortname__,
    __version__,
    forge_api_latest_release,
)

from result import QueryStatus
from result import QueryResult
from notify import QueryNotify
from notify import QueryNotifyPrint
from sites import SitesInformation
from colorama import init
from argparse import ArgumentTypeError

async def _request_in_thread(
    method: str,
    url: str,
    headers: dict,
    proxy: Optional[str],
    allow_redirects: bool,
    timeout: int,
    json_payload=None,
):
    """Execute a synchronous tls-client request in an asyncio thread pool."""

    def _do_request():
        session = tls_client.Session(
            client_identifier="chrome_138",
            random_tls_extension_order=True
        )
        kwargs = {
            "headers": headers,
            "allow_redirects": allow_redirects,
            "timeout_seconds": timeout
        }
        if proxy:
            kwargs["proxy"] = proxy
        if json_payload is not None:
            kwargs["json"] = json_payload

        if method == "GET":
            return session.get(url, **kwargs)
        elif method == "HEAD":
            return session.head(url, **kwargs)
        elif method == "POST":
            return session.post(url, **kwargs)
        elif method == "PUT":
            return session.put(url, **kwargs)
        else:
            raise RuntimeError(f"Unsupported request_method for {url}")

    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(None, _do_request),
        timeout=timeout + 15,
    )

async def get_response(request_future, error_type, social_network):
    # Default for Response object if some failure occurs.
    response = None

    error_context = "General Unknown Error"
    exception_text = None
    try:
        response = await request_future

        if response is not None and response.status_code is not None:
            # Status code exists in response object
            error_context = None
    except asyncio.TimeoutError as errt:
        error_context = "Timeout Error"
        exception_text = str(errt)
    except Exception as err:
        err_str = str(err).lower()
        if "timeout" in err_str:
            error_context = "Timeout Error"
        elif "proxy" in err_str:
            error_context = "Proxy Error"
        elif "connection" in err_str:
            error_context = "Error Connecting"
        elif "http" in err_str:
            error_context = "HTTP Error"
        else:
            error_context = "Unknown Error"
        exception_text = str(err)

    return response, error_context, exception_text

def interpolate_string(input_object, username):
    if isinstance(input_object, str):
        return input_object.replace("{}", username)
    elif isinstance(input_object, dict):
        return {k: interpolate_string(v, username) for k, v in input_object.items()}
    elif isinstance(input_object, list):
        return [interpolate_string(i, username) for i in input_object]
    return input_object

def check_for_parameter(username):
    """checks if {?} exists in the username
    if exist it means that soylock is looking for more multiple username"""
    return "{?}" in username

checksymbols = ["_", "-", "."]

def multiple_usernames(username):
    """replace the parameter with with symbols and return a list of usernames"""
    allUsernames = []
    for i in checksymbols:
        allUsernames.append(username.replace("{?}", i))
    return allUsernames

async def soylock(
    username: str,
    site_data: dict,
    query_notify: QueryNotify,
    session: Optional[BrowserSession] = None,
    dump_response: bool = False,
    proxy: Optional[str] = None,
    timeout: int = 60,
    disable_archive: bool = False,
):
    """Run Soylock Analysis.

    Checks for existence of username on various social media sites.

    * HTTP is the default probe for every site.
    * If ``session`` is provided (browser mode):
        – Sites tagged ``browserOnly`` go straight to BrowserSession.
        – Any site that returns WAF/BLOCKED on the HTTP path is retried
          through BrowserSession.
    * All sites are processed concurrently; a result is recorded as soon
      as its individual probe finishes.
    """
    query_notify.start(username)

    notify_lock = asyncio.Lock()
    results_total = {}

    WAFHitMsgs = [
        r'.loading-spinner{visibility:hidden}body.no-js .challenge-running{display:none}body.dark{background-color:#222;color:#d9d9d9}body.dark a{color:#fff}body.dark a:hover{color:#ee730a;text-decoration:underline}body.dark .lds-ring div{border-color:#999 transparent transparent}body.dark .font-red{color:#b20f03}body.dark',  # 2024-05-13 Cloudflare
        r'<span id="challenge-error-text">',  # 2024-11-11 Cloudflare error page
        r'AwsWafIntegration.forceRefreshToken',  # 2024-11-11 Cloudfront (AWS)
        r'{return l.onPageView}}),Object.defineProperty(r,"perimeterxIdentifiers",{enumerable:',  # 2024-04-09 PerimeterX / Human Security
        'We’re committed to safety and security. Unless you’re a bot. Complete the challenge below and let us know you’re',  # 2025-11-07 Reddit
        'Please wait while your request is being verified...',  # 2025-11-11 OurDJTalk
        'Your request has been blocked due to a network policy.', # 2026-06-08 Reddit
        '<noscript><p><b>JavaScript is required to access this page.</b></p></noscript>', # 2026-06-08 MusicBrainz
        '<head><title>415 Unsupported Media Type</title></head>',  # 2026-06-08 OurDJTalk
        'https://assets.guns.lol/wasm/gpp_gunslol.js', # 2026-06-09 guns.lol
        '<title>Reddit - Please wait for verification</title>', # 2026-06-10 Reddit
        'This website is using a security service to protect itself from online attacks. The action you just performed triggered the security solution. There are several actions that could trigger this block including submitting a certain word or phrase, a SQL command or malformed data.', # 2026-06-14 Cloudflare
        'You\'ve been blocked by network security.' # 2026-06-14 Reddit
    ]

    RegulationHitMsgs = [
        '<link rel="stylesheet" href="/dist/age-wall.min.',  # 2025-11-11 Pornhub / YouPorn / RedTube
        'Although this platform is, and has always been, for adults only, as it appears you are accessing the platform from',  # 2025-11-11 ChaturBate
        'Visitors from United Kingdom must verify their age to access this site.',  # 2025-11-11 BongaCams
        'To continue, we are required to verify that you are 18 or older, in line with the UK Online Safety Act.',  # 2025-11-11 LushStories / Pornhub (A) / YouPorn (A) / RedTube (A)
        'We\'ve had to temporarily block access to the APClips preview area from your state.', # 2026-06-08 APClips
        'Youporn is not currently accepting new account registrations in your region' # 2026-06-08 Youporn
    ]

    class ProbeResult:
        __slots__ = (
            "query_status",
            "http_status",
            "response_text",
            "response_time",
            "error_context",
            "text_for_check",
        )

        def __init__(
            self,
            query_status,
            http_status="?",
            response_text="",
            response_time=None,
            error_context=None,
            text_for_check="",
        ):
            self.query_status = query_status
            self.http_status = http_status
            self.response_text = response_text
            self.response_time = response_time
            self.error_context = error_context
            self.text_for_check = text_for_check

    def _decode_text(value) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value or ""

    def _safe_attr(obj, attr: str, default=None):
        try:
            return getattr(obj, attr)
        except Exception:
            return default

    def _default_headers():
        return {
            "accept-language": "en-US,en;q=0.9,ar;q=0.8",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "pragma": "no-cache",
            "sec-ch-ua": "\"Google Chrome\";v=\"138\", \"Chromium\";v=\"138\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/138.0.0.0 Safari/537.36"
            ),
        }

    def _eval_status(
        text_for_check,
        http_status,
        url_for_check,
        error_type,
        net_info,
        error_context,
        social_network,
    ):
        """Determine QueryStatus from response text/status."""
        text_for_check = _decode_text(text_for_check)
        url_for_check = _decode_text(url_for_check)

        if error_context is not None:
            return QueryStatus.UNKNOWN, error_context

        if any(hit_msg in text_for_check for hit_msg in WAFHitMsgs):
            return QueryStatus.WAF, None

        if any(hit_msg in text_for_check for hit_msg in RegulationHitMsgs):
            return QueryStatus.BLOCKED, None

        if "Page.goto: NS_ERROR_NET_EMPTY_RESPONSE" in text_for_check:
            return QueryStatus.UNKNOWN, error_context

        if error_type == "message":
            try:
                status_code_val = int(http_status) if http_status not in (None, "?") else None
            except Exception:
                status_code_val = None

            error_flag = True
            errors = net_info.get("errorMsg")

            if isinstance(errors, str):
                if errors in text_for_check:
                    error_flag = False
            elif errors is not None:
                for error in errors:
                    if error in text_for_check:
                        error_flag = False
                        break

            if not error_flag:
                if status_code_val in (403, 429, 503):
                    return QueryStatus.WAF, None
                if status_code_val in (0, 500):
                    return QueryStatus.UNKNOWN, None

            if error_flag:
                return QueryStatus.CLAIMED, None

            return QueryStatus.AVAILABLE, None

        if error_type == "status_code":
            error_codes = net_info.get("errorCode")

            if isinstance(error_codes, int):
                error_codes = [error_codes]

            if error_codes is not None and http_status in error_codes:
                return QueryStatus.AVAILABLE, None
            if http_status in (403, 429, 503):
                return QueryStatus.WAF, None
            if http_status in (0, 500):
                return QueryStatus.UNKNOWN, None
            if isinstance(http_status, int) and (http_status >= 300 or http_status < 200):
                return QueryStatus.AVAILABLE, None
            if http_status in (None, "?"):
                return QueryStatus.UNKNOWN, None

            return QueryStatus.CLAIMED, None

        if error_type == "response_url":
            error_flag = True
            error = net_info.get("errorUrl")

            if isinstance(error, str) and error in url_for_check:
                error_flag = False

            if error_flag:
                return QueryStatus.CLAIMED, None

            return QueryStatus.AVAILABLE, None

        raise ValueError(
            f"Unknown Error Type '{error_type}' for site '{social_network}'"
        )

    async def _browser_probe(
        url_probe,
        request_method,
        headers,
        request_payload,
        error_type,
        social_network,
        net_info,
    ):
        """Execute a single probe through BrowserSession."""
        if session is None:
            return ProbeResult(
                query_status=QueryStatus.UNKNOWN,
                error_context="Browser session not available",
            )

        http_status = "?"
        response_text = b""
        text_for_check = ""
        response_time = None
        error_context = None
        url_for_check = ""

        try:
            method = (request_method or "GET").upper()

            request_fn = {
                "GET": session.get,
                "HEAD": session.head,
                "POST": session.post,
                "PUT": session.put,
            }.get(method, session.get)

            request_kwargs = {
                "url": url_probe,
                "headers": headers,
                "allow_redirects": True,
                "timeout": timeout,
            }

            if method in ("GET", "POST", "PUT"):
                request_kwargs["json"] = request_payload

            r = await request_fn(**request_kwargs)

            response_time = _safe_attr(r, "elapsed")
            http_status = _safe_attr(r, "status_code", "?")

            raw_text = _safe_attr(r, "text", b"")
            text_for_check = _decode_text(raw_text)

            if isinstance(raw_text, bytes):
                response_text = raw_text
            else:
                response_text = raw_text.encode("utf-8") if raw_text else b""

            url_for_check = _decode_text(_safe_attr(r, "url", ""))

        except asyncio.TimeoutError:
            error_context = "Timeout Error"
            text_for_check = ""
        except Exception:
            error_context = "Unknown Error"
            text_for_check = ""

        query_status, _ = _eval_status(
            text_for_check,
            http_status,
            url_for_check,
            error_type,
            net_info,
            error_context,
            social_network,
        )

        return ProbeResult(
            query_status=query_status,
            http_status=http_status,
            response_text=response_text,
            response_time=response_time,
            error_context=error_context,
            text_for_check=text_for_check,
        )

    async def _http_probe(
        url_probe,
        request_method,
        headers,
        request_payload,
        error_type,
        social_network,
        net_info,
    ):
        """Execute a single probe through the normal HTTP request path."""
        future = _request_in_thread(
            method=request_method,
            url=url_probe,
            headers=headers,
            proxy=proxy,
            allow_redirects=True,
            timeout=timeout,
            json_payload=request_payload,
        )

        r, error_text, exception_text = await get_response(
            request_future=future,
            error_type=error_type,
            social_network=social_network,
        )

        response_time = _safe_attr(r, "elapsed")
        http_status = _safe_attr(r, "status_code", "?")
        response_text = _safe_attr(r, "text", "")
        url_for_check = _safe_attr(r, "url", "")

        error_context = error_text if error_text is not None else None

        query_status, _ = _eval_status(
            response_text,
            http_status,
            url_for_check,
            error_type,
            net_info,
            error_context,
            social_network,
        )

        return ProbeResult(
            query_status=query_status,
            http_status=http_status,
            response_text=response_text,
            response_time=response_time,
            error_context=error_context,
            text_for_check=_decode_text(response_text),
        )

    async def _run_probe(
        browser_only,
        url_probe,
        request_method,
        headers,
        request_payload,
        error_type,
        social_network,
        net_info,
    ):
        """Choose HTTP/browser path, including WAF/BLOCKED browser retry."""
        if browser_only:
            return await _browser_probe(
                url_probe,
                request_method,
                headers,
                request_payload,
                error_type,
                social_network,
                net_info,
            )

        result = await _http_probe(
            url_probe,
            request_method,
            headers,
            request_payload,
            error_type,
            social_network,
            net_info,
        )

        if session is not None and result.query_status in (
            QueryStatus.WAF,
            QueryStatus.BLOCKED,
        ):
            return await _browser_probe(
                url_probe,
                request_method,
                headers,
                request_payload,
                error_type,
                social_network,
                net_info,
            )

        return result

    def _dump_probe_result(result, social_network, url, error_type, net_info):
        print("+++++++++++++++++++++")
        print(f"TARGET NAME   : {social_network}")
        print(f"USERNAME      : {username}")
        print(f"TARGET URL    : {url}")
        print(f"TEST METHOD   : {error_type}")

        if "errorCode" in net_info:
            print(f"STATUS CODES  : {net_info['errorCode']}")

        print("Results...")
        print(f"RESPONSE CODE : {result.http_status}")

        if "errorMsg" in net_info:
            print(f"ERROR TEXT    : {net_info['errorMsg']}")

        print(">>>>> BEGIN RESPONSE TEXT")
        try:
            print(result.text_for_check or _decode_text(result.response_text))
        except Exception:
            pass
        print("<<<<< END RESPONSE TEXT")

        if session is not None:
            print("BROWSER_MODE  : TRUE")

        print("VERDICT       : " + str(result.query_status))
        print("+++++++++++++++++++++")

    def _parser_for_result(result, net_info):
        if net_info.get("fields") is None and not net_info.get("dumpUrls"):
            return None

        return Parser(_decode_text(result.response_text))

    def _normalize_dump_urls(dump_urls):
        if dump_urls is None:
            return []
        if isinstance(dump_urls, str):
            return [dump_urls]
        return list(dump_urls)

    def _normalize_found_url(candidate_url, url_filter):
        if ("https://www.youtube.com/redirect?" in candidate_url or "https://steamcommunity.com/linkfilter" in candidate_url):
            return urllib.parse.unquote(candidate_url.split("=")[-1])
        if "sf16-va.tiktokcdn.com" in candidate_url:
            return None
        if url_filter == "url":
            return None
        if "http" in candidate_url:
            return codecs.decode(urllib.parse.unquote(candidate_url), "unicode_escape")
        if "/cdn-cgi/l/email-protection" in candidate_url:
            return "Found javascript protected email (you can copy it from browser)"

        return None

    def _update_fields(dictionary, element):
        for name, value in element.items():
            if isinstance(value, collections.abc.Mapping):
                dictionary[name] = _update_fields(dictionary.get(name, {}), value)
            else:
                dictionary[name] = value
        return dictionary

    def _as_list(value):
        if isinstance(value, (list, tuple, set)):
            return list(value)
        return [value]

    def _extract_found_fields(result, net_info, url):
        if result.query_status != QueryStatus.CLAIMED:
            return {}

        if net_info.get("fields") is None and not net_info.get("dumpUrls"):
            return None

        parser = Parser(_decode_text(result.response_text))
        if parser is None:
            return {}

        found_fields = {}

        for field, path in (net_info.get("fields") or {}).items():
            if isinstance(path, dict):
                found = parser.find(field, as_dict=True)

                if found is None:
                    continue

                sub_parser = Parser(_decode_text(str(found or "")))

                sub_paths = []
                for possible_paths in path.values():
                    sub_paths.extend(_as_list(possible_paths))

                found_rows = sub_parser.find_all(sub_paths)

                for index, row in enumerate(found_rows):
                    update_values = {}

                    for output_field, possible_paths in path.items():
                        if output_field in update_values:
                            continue

                        for sub_path in _as_list(possible_paths):
                            value = row.get(sub_path)

                            if value is None or value == "":
                                continue

                            if (isinstance(value, int) or (isinstance(value, str) and value.isdigit())) and "date" in output_field.lower():
                                value = datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")

                            update_values[output_field] = value
                            break
                    if update_values:
                        update = {
                            str(index): update_values
                        }
                        _update_fields(found_fields, update)
            else:
                found = parser.find(path)
                if found is None or found == "":
                    continue
                if isinstance(found, int) and "date" in field.lower():
                    found = datetime.fromtimestamp(found).strftime("%Y-%m-%d %H:%M:%S")

                found_fields[field] = found

        url_filters = _normalize_dump_urls(net_info.get("dumpUrls"))
        if url_filters:
            url_attr = net_info.get("urlAttr")
            urls = []

            for url_filter in url_filters:
                if url_attr:
                    urls.extend(parser.find_all(url_filter, strategy="html", html_attr=url_attr))
                else:
                    urls.extend(parser.find_all(url_filter))

            found_urls = []
            for candidate_url in urls:
                if candidate_url is None or url in candidate_url:
                    continue

                for url_filter in url_filters:
                    normalized_url = ""
                    if ("https://www.youtube.com/redirect?" in candidate_url or "https://steamcommunity.com/linkfilter" in candidate_url):
                        normalized_url = urllib.parse.unquote(candidate_url.split("=")[-1])
                    if "sf16-va.tiktokcdn.com" in candidate_url:
                        continue
                    if url_filter == "url":
                        continue
                    if "http" in candidate_url:
                        normalized_url = codecs.decode(urllib.parse.unquote(candidate_url), "unicode_escape")
                    if "/cdn-cgi/l/email-protection" in candidate_url:
                        normalized_url = "Found javascript protected email (you can copy it from browser)"
                    if normalized_url:
                        found_urls.append(normalized_url)

            if found_urls:
                unique_urls = list(dict.fromkeys(found_urls))
                found_fields["Urls"] = "\n".join(unique_urls) + " (a chunk of the links may be stripped)"

        return found_fields

    async def _emit_result(results_site, result, http_status="", response_text=""):
        async with notify_lock:
            query_notify.update(result)

        results_site["status"] = result
        results_site["http_status"] = http_status
        results_site["response_text"] = response_text

    async def _check_one(social_network, net_info):
        """End-to-end check for a single site. Emits result as soon as done."""
        results_site = {"url_main": net_info.get("urlMain")}

        headers = _default_headers()
        headers.update(net_info.get("headers", {}))

        url = interpolate_string(net_info["url"], username.replace(" ", "%20"))

        regex_check = net_info.get("regexCheck")
        if regex_check and re.search(regex_check, username) is None:
            results_site["url_user"] = ""

            result = QueryResult(
                username,
                social_network,
                url,
                QueryStatus.ILLEGAL,
            )

            await _emit_result(results_site, result)
            return social_network, results_site

        browser_only = bool(net_info.get("browserOnly"))

        if browser_only and session is None:
            results_site["url_user"] = ""

            result = QueryResult(
                username,
                social_network,
                url,
                QueryStatus.UNIMPLEMENTED,
            )

            await _emit_result(results_site, result)
            return social_network, results_site

        results_site["url_user"] = url

        url_probe = net_info.get("urlProbe")
        if url_probe is None:
            url_probe = url
        else:
            url_probe = interpolate_string(url_probe, username)

        request_method = net_info.get("request_method") or "GET"
        request_payload = net_info.get("request_payload")

        if request_payload is not None:
            request_payload = interpolate_string(request_payload, username)

        error_type = net_info["errorType"]

        probe_result = await _run_probe(
            browser_only,
            url_probe,
            request_method,
            headers,
            request_payload,
            error_type,
            social_network,
            net_info,
        )

        if dump_response:
            _dump_probe_result(
                probe_result,
                social_network,
                url,
                error_type,
                net_info,
            )

        found_fields = _extract_found_fields(
            probe_result,
            net_info,
            url,
        )

        result = QueryResult(
            username=username,
            site_name=social_network,
            site_url_user=url,
            status=probe_result.query_status,
            query_time=probe_result.response_time,
            context=probe_result.error_context,
            fields=found_fields,
        )

        await _emit_result(
            results_site,
            result,
            http_status=probe_result.http_status,
            response_text=probe_result.response_text,
        )

        return social_network, results_site

    tasks = [
        asyncio.create_task(_check_one(social_network, net_info))
        for social_network, net_info in site_data.items()
    ]

    for completed in asyncio.as_completed(tasks):
        social_network, results_site = await completed
        results_total[social_network] = results_site

    session = tls_client.Session(
        client_identifier="chrome_138",
        random_tls_extension_order=True
    )

    if not disable_archive:
        query_notify.start(username, " Archive", True)
        blocked = False
        for social_network, net_info in site_data.items():
            if blocked:
                break
            if net_info.get("archiveUrls"):
                archive_urls = net_info.get("archiveUrls")
                url_list = []
                if isinstance(archive_urls, str):
                    url_list.append(archive_urls)
                else:
                    url_list = archive_urls

                for archive_url in url_list:
                    url_probe = interpolate_string(archive_url, username)
                    response = session.get(f"https://archive.org/wayback/available?url={url_probe}/")
                    response_text = _safe_attr(response, "text", "")
                    response_time = _safe_attr(response, "elapsed")
                    response_headers = _safe_attr(response, "headers", {})

                    if "429 Too Many Requests" in response_text:
                        query_notify.blocked("Archive.org", "Too Many Requests")
                        blocked = True
                        break
                    if "archived_snapshots\": {}" not in response_text:
                        result = QueryResult(
                            username=username,
                            site_name=social_network,
                            site_url_user=interpolate_string(f"https://web.archive.org/web/*/{archive_url}/", username),
                            status=QueryStatus.CLAIMED,
                            query_time=response_time,
                        )

                        async with notify_lock:
                            query_notify.update(result)
                        break

    return results_total

def timeout_check(value):
    """Check Timeout Argument.

    Checks timeout for validity.

    Keyword Arguments:
    value                  -- Time in seconds to wait before timing out request.

    Return Value:
    Floating point number representing the time (in seconds) that should be
    used for the timeout.

    NOTE:  Will raise an exception if the timeout is invalid.
    """

    float_value = float(value)

    if float_value <= 0:
        raise ArgumentTypeError(
            f"Invalid timeout value: {value}. Timeout must be a positive number."
        )

    return float_value

def tabs_check(value):
    """Check tabs Argument.

    Checks tabs for validity.

    Keyword Arguments:
    value                  -- Tab amount to be handled by chrome.

    Return Value:
    Integer representing the amount that should be
    used for tabs.

    NOTE:  Will raise an exception if the tabs is invalid.
    """


    int_value = int(value)
    
    if int_value <= 0:
        raise ArgumentTypeError(
            f"Invalid tabs value: {value}. Tabs must be a positive number."
        )
    elif int_value > 20:
        raise ArgumentTypeError(
            f"Invalid tabs value: {value}. Tabs shouldn't be more than 10."
        )

    return int_value


def handler(signal_received, frame):
    """Exit gracefully without throwing errors

    Source: https://www.devdungeon.com/content/python-catch-sigint-ctrl-c
    """
    sys.exit(0)

async def main():
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description=f"{__longname__} (Version {__version__})",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"{__shortname__} v{__version__}",
        help="Display version information and dependencies.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        "-d",
        "--debug",
        action="store_true",
        dest="verbose",
        default=False,
        help="Display extra debugging information and metrics.",
    )
    parser.add_argument(
        "--folderoutput",
        "-fo",
        dest="folderoutput",
        help="If using multiple usernames, the output of the results will be saved to this folder.",
    )
    parser.add_argument(
        "--output",
        "-o",
        dest="output",
        help="If using single username, the output of the result will be saved to this file.",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        dest="csv",
        default=False,
        help="Create Comma-Separated Values (CSV) File.",
    )
    parser.add_argument(
        "--xlsx",
        action="store_true",
        dest="xlsx",
        default=False,
        help="Create the standard file for the modern Microsoft Excel spreadsheet (xlsx).",
    )
    parser.add_argument(
        "--site",
        action="append",
        metavar="SITE_NAME",
        dest="site_list",
        default=[],
        help="Limit analysis to just the listed sites. Add multiple options to specify more than one site.",
    )
    parser.add_argument(
        "--proxy",
        "-p",
        metavar="PROXY_URL",
        action="store",
        dest="proxy",
        default=None,
        help="Make requests over a proxy. e.g. socks5://127.0.0.1:1080",
    )
    parser.add_argument(
        "--dump-response",
        action="store_true",
        dest="dump_response",
        default=False,
        help="Dump the HTTP response to stdout for targeted debugging.",
    )
    parser.add_argument(
        "--json",
        "-j",
        metavar="JSON_FILE",
        dest="json_file",
        default=None,
        help="Load data from a JSON file or an online, valid, JSON file. Upstream PR numbers also accepted.",
    )
    parser.add_argument(
        "--timeout",
        action="store",
        metavar="TIMEOUT",
        dest="timeout",
        type=timeout_check,
        default=60,
        help="Time (in seconds) to wait for response to requests (Default: 60)",
    )
    parser.add_argument(
        "--print-all",
        action="store_true",
        dest="print_all",
        default=False,
        help="Output sites where the username was not found.",
    )
    parser.add_argument(
        "--print-found",
        action="store_true",
        dest="print_found",
        default=True,
        help="Output sites where the username was found (also if exported as file).",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        dest="no_color",
        default=False,
        help="Don't color terminal output",
    )
    parser.add_argument(
        "username",
        nargs="+",
        metavar="USERNAMES",
        action="store",
        help="One or more usernames to check with social networks. Check similar usernames using {?} (replace to '_', '-', '.').",
    )
    parser.add_argument(
        "--browse",
        "-b",
        action="store_true",
        dest="browse",
        default=False,
        help="Browse to all results on default browser.",
    )

    parser.add_argument(
        "--local",
        "-l",
        action="store_true",
        default=False,
        help="Force the use of the local data.json file.",
    )

    parser.add_argument(
        "--nsfw",
        action="store_true",
        default=True,
        help="Include checking of NSFW sites from default list.",
    )

    parser.add_argument(
        "--disable-browser",
        action="store_true",
        dest="disable_browser",
        default=False,
        help="Disable the chromium based cloudflare interstitial captcha solver.",
    )

    parser.add_argument(
        "--disable-archive",
        action="store_true",
        dest="disable_archive",
        default=False,
        help="Disable the archive.org search.",
    )

    parser.add_argument(
        "--tabs",
        action="store",
        type=tabs_check,
        default=5,
        help="Parallel tabs handled by the browser.",
    )

    parser.add_argument(
        "--txt",
        action="store_true",
        dest="output_txt",
        default=False,
        help="Enable creation of a txt file",

    )

    args = parser.parse_args()

    # If the user presses CTRL-C, exit gracefully without throwing errors
    signal.signal(signal.SIGINT, handler)

    # Make prompts
    if args.proxy is not None:
        print("Using the proxy: " + args.proxy)

    if args.no_color:
        # Disable color output.
        init(strip=True, convert=False)
    else:
        # Enable color output.
        init(autoreset=True)

    # Check if both output methods are entered as input.
    if args.output is not None and args.folderoutput is not None:
        print("You can only use one of the output methods.")
        sys.exit(1)

    # Check validity for single username output.
    if args.output is not None and len(args.username) != 1:
        print("You can only use --output with a single username")
        sys.exit(1)

    # Create object with all information about sites we are aware of.
    try:
         if args.local:
            sites = SitesInformation(os.path.join(os.path.dirname(__file__), "resources/data.json"))
         else:
            json_file_location = args.json_file
            if args.json_file:
                # If --json parameter is a number, interpret it as a pull request number
                if args.json_file.isnumeric():
                    pull_number = args.json_file
                    pull_url = f"https://api.github.com/repos/SystemCallW/Soylock/pulls/{pull_number}"

                    try:
                        pr_response = requests.get(pull_url, timeout=30)
                        pull_request_raw = pr_response.text
                        pull_request_json = json_loads(pull_request_raw)

                        # Check if it's a valid pull request
                        if "message" in pull_request_json:
                            print(f"ERROR: Pull request #{pull_number} not found.")
                            sys.exit(1)

                        head_commit_sha = pull_request_json["head"]["sha"]
                        json_file_location = f"https://raw.githubusercontent.com/SystemCallW/Soylock/{head_commit_sha}/resources/data.json"
                    except Exception:
                        print("Failed to fetch PR info from GitHub.")
                        sys.exit(1)

            try:
                sites = SitesInformation(data_file_path=json_file_location)
            except:
                print("Failed to download data from github. Fallback to local data.")
                sites = SitesInformation(os.path.join(os.path.dirname(__file__), "resources/data.json"))
    except Exception as error:
        print(f"ERROR:  {error}")
        sys.exit(1)

    if not args.nsfw:
        sites.remove_nsfw_sites(do_not_remove=args.site_list)

    # Create original dictionary from SitesInformation() object.
    # Eventually, the rest of the code will be updated to use the new object
    # directly, but this will glue the two pieces together.
    site_data_all = {site.name: site.information for site in sites}
    if args.site_list == []:
        # Not desired to look at a sub-set of sites
        site_data = site_data_all
    else:
        # User desires to selectively run queries on a sub-set of the site list.
        # Make sure that the sites are supported & build up pruned site database.
        site_data = {}
        site_missing = []
        for site in args.site_list:
            counter = 0
            for existing_site in site_data_all:
                if site.lower() == existing_site.lower():
                    site_data[existing_site] = site_data_all[existing_site]
                    counter += 1
            if counter == 0:
                # Build up list of sites not supported for future error message.
                site_missing.append(f"'{site}'")

        if site_missing:
            print(f"Error: Desired sites not found: {', '.join(site_missing)}.")

        if not site_data:
            sys.exit(1)

    # Create notify object for query results.
    query_notify = QueryNotifyPrint(
        result=None, verbose=args.verbose, print_all=args.print_all, browse=args.browse
    )

    query_notify.splash()

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }
    try:
        r = requests.get(forge_api_latest_release, headers=headers, timeout=30)
        if r.status_code == 200:
            data = r.json()
            tag = data.get("tag_name")
            if tag != "v" +__version__:
                query_notify.versionAlert(tag)
    except Exception:
        pass  # Silently fail version check if offline

    # ── Initialize browser engine if browser-mode is enabled ──
    engine = None
    session = None
    if not args.disable_browser:
        max_workers = args.tabs if args.tabs else (5 if len(site_data) >= 20 else len(site_data))
        engine = BrowserEngine(
            max_workers=max_workers,
            headless=True,
            proxy=args.proxy,
        )
        await engine.start()
        session = BrowserSession(engine)

    # Run report on all specified users.
    all_usernames = []
    for username in args.username:
        if check_for_parameter(username):
            for name in multiple_usernames(username):
                all_usernames.append(name)
        else:
            all_usernames.append(username)

    try:
        for username in all_usernames:
            results = await soylock(
                username,
                site_data,
                query_notify,
                session=session,
                dump_response=args.dump_response,
                proxy=args.proxy,
                timeout=args.timeout,
                disable_archive=args.disable_archive
            )

            if args.output:
                result_file = args.output
            elif args.folderoutput:
                # The usernames results should be stored in a targeted folder.
                # If the folder doesn't exist, create it first
                os.makedirs(args.folderoutput, exist_ok=True)
                result_file = os.path.join(args.folderoutput, f"{username}.txt")
            else:
                result_file = f"{username}.txt"

            if args.output_txt:
                with open(result_file, "w", encoding="utf-8") as file:
                    exists_counter = 0
                    for website_name in results:
                        dictionary = results[website_name]
                        if dictionary.get("status").status == QueryStatus.CLAIMED:
                            exists_counter += 1
                            file.write(dictionary["url_user"] + "\n")
                    file.write(f"Total Websites Username Detected On : {exists_counter}\n")

            if args.csv:
                result_file = f"{username}.csv"
                if args.folderoutput:
                    # The usernames results should be stored in a targeted folder.
                    # If the folder doesn't exist, create it first
                    os.makedirs(args.folderoutput, exist_ok=True)
                    result_file = os.path.join(args.folderoutput, result_file)

                with open(result_file, "w", newline="", encoding="utf-8") as csv_report:
                    writer = csv.writer(csv_report)
                    writer.writerow(
                        [
                            "username",
                            "name",
                            "url_main",
                            "url_user",
                            "exists",
                            "http_status",
                            "response_time_s",
                        ]
                    )
                    for site in results:
                        if (
                            args.print_found
                            and not args.print_all
                            and results[site]["status"].status != QueryStatus.CLAIMED
                        ):
                            continue

                        response_time_s = results[site]["status"].query_time
                        if response_time_s is None:
                            response_time_s = ""
                        writer.writerow(
                            [
                                username,
                                site,
                                results[site]["url_main"],
                                results[site]["url_user"],
                                str(results[site]["status"].status),
                                results[site]["http_status"],
                                response_time_s,
                            ]
                        )
            if args.xlsx:
                usernames = []
                names = []
                url_main = []
                url_user = []
                exists = []
                http_status = []
                response_time_s = []

                for site in results:
                    if (
                        args.print_found
                        and not args.print_all
                        and results[site]["status"].status != QueryStatus.CLAIMED
                    ):
                        continue

                    if response_time_s is None:
                        response_time_s.append("")
                    else:
                        response_time_s.append(results[site]["status"].query_time)
                    usernames.append(username)
                    names.append(site)
                    url_main.append(results[site]["url_main"])
                    url_user.append(results[site]["url_user"])
                    exists.append(str(results[site]["status"].status))
                    http_status.append(results[site]["http_status"])

                DataFrame = pd.DataFrame(
                    {
                        "username": usernames,
                        "name": names,
                        "url_main": [f'=HYPERLINK(\"{u}\")' for u in url_main],
                        "url_user": [f'=HYPERLINK(\"{u}\")' for u in url_user],
                        "exists": exists,
                        "http_status": http_status,
                        "response_time_s": response_time_s,
                    }
                )
                DataFrame.to_excel(f"{username}.xlsx", sheet_name="sheet1", index=False)

            print()
    finally:
        if engine is not None:
            await engine.close()

    query_notify.finish()

if __name__ == "__main__":
    asyncio.run(main())
