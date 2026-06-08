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
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from json import loads as json_loads
from typing import Optional

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
    ]

    RegulationHitMsgs = [
        '<link rel="stylesheet" href="/dist/age-wall.min.',  # 2025-11-11 Pornhub / YouPorn / RedTube
        'Although this platform is, and has always been, for adults only, as it appears you are accessing the platform from',  # 2025-11-11 ChaturBate
        'We comply with laws across 19 states that mandate content controls and age verification measures.',  # 2025-11-11 RocketTube
        'Broke Straight Boys is the original Gay For Pay site. Watch over 2743 exclusive scenes of real straight boys doing whatever it takes to pay the bills - Highest Rated - Page 1',  # 2025-11-11 RocketTube alternative
        'Visitors from United Kingdom must verify their age to access this site.',  # 2025-11-11 BongaCams
        'To continue, we are required to verify that you are 18 or older, in line with the UK Online Safety Act.'  # 2025-11-11 LushStories / Pornhub (A) / YouPorn (A) / RedTube (A)
    ]

    def _eval_status(text_for_check, http_status, url_for_check, error_type, net_info, error_context, social_network):
        """Determine QueryStatus from response text/status."""
        if error_context is not None:
            return QueryStatus.UNKNOWN, error_context

        if any(hitMsg in text_for_check for hitMsg in WAFHitMsgs):
            return QueryStatus.WAF, None

        if any(hitMsg in text_for_check for hitMsg in RegulationHitMsgs):
            return QueryStatus.BLOCKED, None

        if error_type == "message":
            try:
                status_code_val = int(http_status) if http_status not in (None, "?") else None
            except Exception:
                status_code_val = None

            #if status_code_val in (403, 429, 503):
            #    return QueryStatus.WAF, None

            error_flag = True
            errors = net_info.get("errorMsg")
            if isinstance(errors, str):
                if errors in text_for_check:
                    error_flag = False
            else:
                for error in errors:
                    if error in text_for_check:
                        error_flag = False
                        break
            if error_flag:
                return QueryStatus.CLAIMED, None
            else:
                return QueryStatus.AVAILABLE, None

        elif error_type == "status_code":
            error_codes = net_info.get("errorCode")
            if isinstance(error_codes, int):
                error_codes = [error_codes]

            if error_codes is not None and http_status in error_codes:
                return QueryStatus.AVAILABLE, None
            elif http_status in (403, 429, 503):
                return QueryStatus.WAF, None
            elif isinstance(http_status, int) and (http_status >= 300 or http_status < 200):
                return QueryStatus.AVAILABLE, None
            elif http_status in (None, "?"):
                return QueryStatus.UNKNOWN, None
            else:
                return QueryStatus.CLAIMED, None

        elif error_type == "response_url":
            error_flag = True
            error = net_info.get("errorUrl")
            if isinstance(error, str):
                if error in url_for_check:
                    error_flag = False

            if error_flag:
                return QueryStatus.CLAIMED, None
            else:
                return QueryStatus.AVAILABLE, None

            if http_status in (403, 429, 503):
                return QueryStatus.WAF, None
            elif isinstance(http_status, int) and 200 <= http_status < 300:
                return QueryStatus.CLAIMED, None
            elif isinstance(http_status, int) and 300 <= http_status < 400:
                return QueryStatus.AVAILABLE, None
            else:
                return QueryStatus.AVAILABLE, None

        else:
            raise ValueError(
                f"Unknown Error Type '{error_type}' for " f"site '{social_network}'"
            )

    async def _browser_probe(url_probe, request_method, headers, request_payload, error_type, social_network, net_info):
        """Execute a single probe through BrowserSession. Returns status tuple."""
        http_status = "?"
        response_text = b""
        text_for_check = ""
        response_time = None
        error_context = None

        if session is None:
            error_context = "Browser session not available"
            return QueryStatus.UNKNOWN, http_status, response_text, response_time, error_context, text_for_check

        try:
            if request_method == "GET" or request_method is None:
                future = session.get(
                    url=url_probe,
                    headers=headers,
                    allow_redirects=True,
                    timeout=timeout,
                    json=request_payload,
                )
            elif request_method == "HEAD":
                future = session.head(
                    url=url_probe,
                    headers=headers,
                    allow_redirects=True,
                    timeout=timeout,
                )
            elif request_method == "POST":
                future = session.post(
                    url=url_probe,
                    headers=headers,
                    allow_redirects=True,
                    timeout=timeout,
                    json=request_payload,
                )
            elif request_method == "PUT":
                future = session.put(
                    url=url_probe,
                    headers=headers,
                    allow_redirects=True,
                    timeout=timeout,
                    json=request_payload,
                )
            else:
                future = session.get(
                    url=url_probe,
                    headers=headers,
                    allow_redirects=True,
                    timeout=timeout,
                )

            r = await future

            # Get response time for response of our request.
            try:
                response_time = r.elapsed
            except Exception:
                response_time = None

            # Attempt to get request information
            try:
                http_status = r.status_code
            except Exception:
                http_status = "?"

            try:
                if isinstance(r.text, bytes):
                    text_for_check = r.text.decode("utf-8", errors="replace")
                else:
                    text_for_check = r.text
            except Exception:
                text_for_check = ""

            try:
                if isinstance(r.text, bytes):
                    response_text = r.text
                else:
                    response_text = r.text.encode("utf-8") if r.text else b""
            except Exception:
                response_text = b""

            try:
                if isinstance(r.text, bytes):
                    url_for_check = r.url.decode("utf-8", errors="replace")
                else:
                    url_for_check = r.url
            except Exception:
                url_for_check = ""

        except asyncio.TimeoutError:
            error_context = "Timeout Error"
            text_for_check = ""
        except Exception as err:
            error_context = "Unknown Error"
            text_for_check = ""

        query_status, _ = _eval_status(
            text_for_check, http_status, url_for_check, error_type, net_info, error_context, social_network
        )

        return query_status, http_status, response_text, response_time, error_context, text_for_check

    async def _check_one(social_network, net_info):
        """End-to-end check for a single site. Emits result as soon as done."""
        results_site = {"url_main": net_info.get("urlMain")}

        # Record URL of main site

        # A user agent is needed because some sites don't return the correct
        # information since they think that we are bots (Which we actually are...)
        headers = {
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
            "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        }

        if "headers" in net_info:
            # Override/append any extra headers required by a given site.
            headers.update(net_info["headers"])

        # URL of user on site (if it exists)
        url = interpolate_string(net_info["url"], username.replace(' ', '%20'))

        # Don't make request if username is invalid for the site
        regex_check = net_info.get("regexCheck")
        if regex_check and re.search(regex_check, username) is None:
            results_site["status"] = QueryResult(
                username, social_network, url, QueryStatus.ILLEGAL
            )
            results_site["url_user"] = ""
            results_site["http_status"] = ""
            results_site["response_text"] = ""
            async with notify_lock:
                query_notify.update(results_site["status"])
            return social_network, results_site

        if net_info.get("browserOnly") and session is None:
            results_site["status"] = QueryResult(
                username, social_network, url, QueryStatus.UNIMPLEMENTED
            )
            results_site["url_user"] = ""
            results_site["http_status"] = ""
            results_site["response_text"] = ""
            async with notify_lock:
                query_notify.update(results_site["status"])
            return social_network, results_site

        results_site["url_user"] = url
        url_probe = net_info.get("urlProbe")
        request_method = net_info.get("request_method")
        request_payload = net_info.get("request_payload")

        if request_payload is not None:
            request_payload = interpolate_string(request_payload, username)

        if url_probe is None:
            url_probe = url
        else:
            url_probe = interpolate_string(url_probe, username)

        if request_method is None:
            request_method = "GET"

        error_type = net_info["errorType"]

        if session is not None and net_info.get("browserOnly"):
            query_status, http_status, response_text, response_time, error_context, text_for_check = await _browser_probe(
                url_probe, request_method, headers, request_payload, error_type, social_network, net_info
            )

            if dump_response:
                print("+++++++++++++++++++++")
                print(f"TARGET NAME   : {social_network}")
                print(f"USERNAME      : {username}")
                print(f"TARGET URL    : {url}")
                print(f"TEST METHOD   : {error_type}")
                try:
                    print(f"STATUS CODES  : {net_info['errorCode']}")
                except KeyError:
                    pass
                print("Results...")
                print(f"RESPONSE CODE : {http_status}")
                try:
                    print(f"ERROR TEXT    : {net_info['errorMsg']}")
                except KeyError:
                    pass
                print(">>>>> BEGIN RESPONSE TEXT")
                try:
                    print(text_for_check)
                except Exception:
                    pass
                print("<<<<< END RESPONSE TEXT")
                if session is not None:
                    print("BROWSER_MODE  : TRUE")
                print("VERDICT       : " + str(query_status))
                print("+++++++++++++++++++++")

            result = QueryResult(
                username=username,
                site_name=social_network,
                site_url_user=url,
                status=query_status,
                query_time=response_time,
                context=error_context,
            )
            async with notify_lock:
                query_notify.update(result)

            results_site["status"] = result
            results_site["http_status"] = http_status
            results_site["response_text"] = response_text
            return social_network, results_site

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
            request_future=future, error_type=error_type, social_network=social_network
        )

        # Get response time for response of our request.
        try:
            response_time = r.elapsed
        except AttributeError:
            response_time = None

        # Attempt to get request information
        try:
            http_status = r.status_code
        except Exception:
            http_status = "?"
        try:
            response_text = r.text
        except Exception:
            response_text = ""

        try:
            url_for_check = r.url
        except Exception:
            url_for_check = ""

        error_context = None
        if error_text is not None:
            error_context = error_text

        query_status, _ = _eval_status(
            response_text, http_status, url_for_check, error_type, net_info, error_context, social_network
        )

        if session is not None and query_status in (QueryStatus.WAF, QueryStatus.BLOCKED):
            query_status, http_status, response_text, response_time, error_context, text_for_check = await _browser_probe(
                url_probe, request_method, headers, request_payload, error_type, social_network, net_info
            )

        if dump_response:
            print("+++++++++++++++++++++")
            print(f"TARGET NAME   : {social_network}")
            print(f"USERNAME      : {username}")
            print(f"TARGET URL    : {url}")
            print(f"TEST METHOD   : {error_type}")
            try:
                print(f"STATUS CODES  : {net_info['errorCode']}")
            except KeyError:
                pass
            print("Results...")
            try:
                print(f"RESPONSE CODE : {http_status}")
            except Exception:
                pass
            try:
                print(f"ERROR TEXT    : {net_info['errorMsg']}")
            except KeyError:
                pass
            print(">>>>> BEGIN RESPONSE TEXT")
            try:
                if isinstance(response_text, bytes):
                    print(response_text.decode('utf-8', errors='replace'))
                else:
                    print(response_text)
            except Exception:
                pass
            print("<<<<< END RESPONSE TEXT")
            if session is not None:
                print("BROWSER_MODE  : TRUE")
            print("VERDICT       : " + str(query_status))
            print("+++++++++++++++++++++")

        result = QueryResult(
            username=username,
            site_name=social_network,
            site_url_user=url,
            status=query_status,
            query_time=response_time,
            context=error_context,
        )
        async with notify_lock:
            query_notify.update(result)

        results_site["status"] = result
        results_site["http_status"] = http_status
        results_site["response_text"] = response_text
        return social_network, results_site

    try:
        tasks = [
            asyncio.create_task(_check_one(sn, ni))
            for sn, ni in site_data.items()
        ]

        for completed in asyncio.as_completed(tasks):
            social_network, results_site = await completed
            results_total[social_network] = results_site

    finally:
        pass

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
        "--browser-mode",
        action="store_true",
        dest="browser_mode",
        default=False,
        help="Uses chromium to solve cloudflare turnstile.",
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

    if not args.browser_mode:
        print("For more and accurate results use --browser-mode")

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
    if args.browser_mode:
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
