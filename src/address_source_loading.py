#!/usr/bin/env python3
"""Compatibility facade for real address source parsing, downloading, and cache helpers."""
from __future__ import annotations

import address_maris as _maris
import address_openaddresses as _openaddresses

from address_maris import *  # noqa: F401,F403
from address_openaddresses import *  # noqa: F401,F403
from address_source_audit import *  # noqa: F401,F403
from address_source_common import *  # noqa: F401,F403
from address_source_downloads import *  # noqa: F401,F403
from address_source_manifest import *  # noqa: F401,F403
from address_source_parsers import *  # noqa: F401,F403


def _sync_download_hooks() -> None:
    """Keep legacy monkeypatches on this facade visible to moved download helpers."""
    _openaddresses.open_url = open_url
    _openaddresses.read_json_url = read_json_url
    _maris.open_url = open_url
    _maris.download_file = download_file
    _maris.read_json_url = read_json_url


def download_openaddresses_ms(*args, **kwargs):
    _sync_download_hooks()
    return _openaddresses.download_openaddresses_ms(*args, **kwargs)


def download_openaddresses_ms_source_configs(*args, **kwargs):
    _sync_download_hooks()
    return _openaddresses.download_openaddresses_ms_source_configs(*args, **kwargs)


def read_arcgis_features_for_object_ids(*args, **kwargs):
    _sync_download_hooks()
    return _openaddresses.read_arcgis_features_for_object_ids(*args, **kwargs)


def download_openaddresses_ms_direct(*args, **kwargs):
    _sync_download_hooks()
    return _openaddresses.download_openaddresses_ms_direct(*args, **kwargs)


def download_maris_point_addresses(*args, **kwargs):
    _sync_download_hooks()
    return _maris.download_maris_point_addresses(*args, **kwargs)


def maris_parcel_layers(*args, **kwargs):
    _sync_download_hooks()
    return _maris.maris_parcel_layers(*args, **kwargs)


def download_maris_parcels(*args, **kwargs):
    _sync_download_hooks()
    return _maris.download_maris_parcels(*args, **kwargs)
