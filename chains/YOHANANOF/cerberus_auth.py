#!/usr/bin/env python3
"""Cerberus login/session helpers for YOHANANOF downloads."""

from __future__ import annotations

import http.cookiejar
import ssl
import time
from html.parser import HTMLParser
from typing import Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import HTTPCookieProcessor, HTTPSHandler, OpenerDirector, Request, build_opener

LOGIN_URL = "https://url.publishedprices.co.il/login"
FILES_URL = "https://url.publishedprices.co.il/file"
USERNAME = "yohananof"
USER_AGENT = "PricyAPI/0.1 (+https://github.com/)"


class LoginPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.form_action: Optional[str] = None
        self.csrf_token: Optional[str] = None
        self.hidden_fields: Dict[str, str] = {}
        self.username_field: Optional[str] = None
        self.password_field: Optional[str] = None

        self._in_form = False

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        attrs_dict = dict(attrs)

        if tag == "meta":
            meta_name = (attrs_dict.get("name") or "").strip().lower()
            if meta_name == "csrftoken":
                token = (attrs_dict.get("content") or "").strip()
                if token:
                    self.csrf_token = token

        if tag == "form" and not self._in_form:
            self._in_form = True
            self.form_action = attrs_dict.get("action")
            return

        if tag != "input" or not self._in_form:
            return

        name = (attrs_dict.get("name") or "").strip()
        input_type = (attrs_dict.get("type") or "text").strip().lower()
        value = attrs_dict.get("value") or ""

        if not name:
            return

        if input_type == "hidden":
            self.hidden_fields[name] = value
            return

        lowered = name.lower()
        if input_type in {"text", "email"} and self.username_field is None:
            self.username_field = name
        if input_type == "password" and self.password_field is None:
            self.password_field = name

        if self.password_field is None and "pass" in lowered:
            self.password_field = name

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if tag == "form" and self._in_form:
            self._in_form = False


def build_cerberus_opener(insecure: bool = False) -> OpenerDirector:
    jar = http.cookiejar.CookieJar()
    handlers = [HTTPCookieProcessor(jar)]
    if insecure:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        handlers.append(HTTPSHandler(context=context))
    return build_opener(*handlers)


def fetch_text(opener: OpenerDirector, url: str, timeout: int, retries: int) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        request = Request(url=url, headers={"User-Agent": USER_AGENT})
        try:
            with opener.open(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            last_error = error
            if attempt < retries:
                time.sleep(min(1.5 * attempt, 5.0))
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def post_form(opener: OpenerDirector, url: str, payload: Dict[str, str], timeout: int, retries: int) -> str:
    encoded = urlencode(payload).encode("utf-8")
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        request = Request(
            url=url,
            data=encoded,
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        try:
            with opener.open(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            last_error = error
            if attempt < retries:
                time.sleep(min(1.5 * attempt, 5.0))
    raise RuntimeError(f"Failed login POST to {url}: {last_error}")


def create_logged_in_opener(timeout: int, retries: int, insecure: bool = False) -> OpenerDirector:
    opener = build_cerberus_opener(insecure=insecure)

    login_html = fetch_text(opener, LOGIN_URL, timeout=timeout, retries=retries)
    parser = LoginPageParser()
    parser.feed(login_html)

    user_field = parser.username_field or "username"
    pass_field = parser.password_field or "password"

    payload = dict(parser.hidden_fields)
    if parser.csrf_token:
        payload["csrftoken"] = parser.csrf_token
    payload[user_field] = USERNAME
    payload[pass_field] = ""

    action_url = urljoin(LOGIN_URL, parser.form_action or LOGIN_URL)
    post_form(opener, action_url, payload, timeout=timeout, retries=retries)

    probe = fetch_text(opener, FILES_URL, timeout=timeout, retries=retries)
    if "id=\"login-form\"" in probe or "action=\"/login/user\"" in probe:
        raise RuntimeError("Login did not complete successfully (received login page after auth POST)")

    return opener
