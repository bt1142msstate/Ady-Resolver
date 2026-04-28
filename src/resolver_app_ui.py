#!/usr/bin/env python3
"""Static browser UI assets for the local resolver app."""
from __future__ import annotations

from pathlib import Path


STATIC_DIR = Path(__file__).resolve().with_name("static")
INDEX_HTML_PATH = STATIC_DIR / "index.html"


def load_index_html() -> str:
    return INDEX_HTML_PATH.read_text(encoding="utf-8")


HTML = load_index_html()
