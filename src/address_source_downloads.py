#!/usr/bin/env python3
"""Shared download helpers for address sources."""
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Optional

from address_source_common import DOWNLOAD_USER_AGENT


def open_url(url: str, timeout: int = 120):
    request = urllib.request.Request(url, headers={"User-Agent": DOWNLOAD_USER_AGENT})
    return urllib.request.urlopen(request, timeout=timeout)


def read_json_url(url: str, params: Optional[Dict[str, object]] = None, timeout: int = 120) -> Dict[str, object]:
    query_url = url
    if params:
        query_url = f"{url}?{urllib.parse.urlencode(params)}"
    with open_url(query_url, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    if isinstance(payload, dict) and "error" in payload:
        raise RuntimeError(f"ArcGIS request failed for {url}: {payload['error']}")
    return payload


def download_file(url: str, target: Path, timeout: int = 120) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.stat().st_size > 0:
        return target
    temporary_target = target.with_suffix(target.suffix + ".part")
    with open_url(url, timeout=timeout) as response, temporary_target.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
    temporary_target.replace(target)
    return target
