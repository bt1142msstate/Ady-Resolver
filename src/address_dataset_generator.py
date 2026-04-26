#!/usr/bin/env python3
"""Generate a labeled address-matching dataset for a midterm project.

The script creates:
1. A canonical reference database sampled from real address sources.
2. Positive query addresses produced by corrupting those real reference addresses.
3. Negative ("no match") query addresses produced from real holdout addresses that
   are *not* in the reference database.
4. Train/validation/test splits assigned at the base-address level so variants of
   the same address stay in the same split.

Outputs are written as CSV files plus a JSON metadata summary.

Design goals:
- Fully self-contained: standard library only.
- Reproducible with a random seed.
- Realistic enough for record-linkage / entity-resolution experiments.
- Can sample from MARIS/MS811, NAD, or OpenAddresses-style real address sources.
- Includes built-in validation and informative console summaries.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import random
import re
import string
import hashlib
import struct
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Reference vocabularies
# ---------------------------------------------------------------------------

STATE_ABBREV_TO_NAME: Dict[str, str] = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AR": "ARKANSAS",
    "AZ": "ARIZONA",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DC": "DISTRICT OF COLUMBIA",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "IA": "IOWA",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "KS": "KANSAS",
    "LA": "LOUISIANA",
    "MA": "MASSACHUSETTS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MO": "MISSOURI",
    "MS": "MISSISSIPPI",
    "MT": "MONTANA",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "NE": "NEBRASKA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NV": "NEVADA",
    "NY": "NEW YORK",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VA": "VIRGINIA",
    "VT": "VERMONT",
    "WA": "WASHINGTON",
    "WI": "WISCONSIN",
    "WV": "WEST VIRGINIA",
    "WY": "WYOMING",
}
STATE_NAME_TO_ABBREV: Dict[str, str] = {name: abbrev for abbrev, name in STATE_ABBREV_TO_NAME.items()}
STATE_NAME_TOKEN_TO_ABBREV: Dict[str, str] = {
    re.sub(r"[^A-Z0-9]+", "", name): abbrev
    for abbrev, name in STATE_ABBREV_TO_NAME.items()
}

STREET_TYPE_TO_FULL: Dict[str, str] = {
    "ALY": "ALLEY",
    "AVE": "AVENUE",
    "BLVD": "BOULEVARD",
    "CIR": "CIRCLE",
    "CT": "COURT",
    "CV": "COVE",
    "DR": "DRIVE",
    "HWY": "HIGHWAY",
    "LN": "LANE",
    "LOOP": "LOOP",
    "PKWY": "PARKWAY",
    "PL": "PLACE",
    "RD": "ROAD",
    "SQ": "SQUARE",
    "ST": "STREET",
    "TER": "TERRACE",
    "TRL": "TRAIL",
    "WAY": "WAY",
}

DIRECTION_TO_FULL: Dict[str, str] = {
    "N": "NORTH",
    "S": "SOUTH",
    "E": "EAST",
    "W": "WEST",
    "NE": "NORTHEAST",
    "NW": "NORTHWEST",
    "SE": "SOUTHEAST",
    "SW": "SOUTHWEST",
}

UNIT_TYPE_TO_FULL: Dict[str, str] = {
    "APT": "APARTMENT",
    "STE": "SUITE",
    "UNIT": "UNIT",
    "FL": "FLOOR",
    "BLDG": "BUILDING",
    "RM": "ROOM",
}

OPENADDRESSES_RESULTS_URL = "https://results.openaddresses.io/index.html"
OPENADDRESSES_MS_ZIP_RE = re.compile(r'https://data\.openaddresses\.io/runs/\d+/us/ms/[^"]+\.zip')
OPENADDRESSES_MS_SOURCES_API_URL = "https://api.github.com/repos/openaddresses/openaddresses/contents/sources/us/ms?ref=master"
OPENADDRESSES_DIRECT_MANIFEST_FILENAME = "_openaddresses_ms_direct_manifest.json"
OPENADDRESSES_DIRECT_FIELDNAMES = ("NUMBER", "STREET", "UNIT", "CITY", "REGION", "POSTCODE", "SOURCE", "SOURCE_ID")
OPENADDRESSES_DIRECT_SKIP_SOURCE_NAMES = {
    # These are older statewide parcel layers. The app already uses the newer
    # MARIS April 2024 parcel service directly, so including these by default
    # mostly adds duplicate parcel-derived rows.
    "statewide-east",
    "statewide-west",
    "statewide-partial",
}
OPENADDRESSES_DIRECT_FULL_ADDRESS_FALLBACK_FIELDS = (
    "FULL_ADDR",
    "FullAddr",
    "FULLADDR",
    "FULL_ADDRESS",
    "ADDRESS",
    "Address",
    "ADDR",
    "addr",
    "SITEADD",
    "SITUS_ADDR",
)
GENERIC_FULL_ADDRESS_FIELDS = (
    "full_address",
    "fulladdr",
    "full_addr",
    "street_address",
    "address",
    "address1",
    "address_line_1",
    "line1",
    "addr",
    "situs_address",
    "site_address",
    "property_address",
)
GENERIC_CITY_FIELDS = ("city", "locality", "municipality", "post_city", "postal_city")
GENERIC_STATE_FIELDS = ("state", "region", "province")
GENERIC_ZIP_FIELDS = ("zip_code", "zipcode", "zip", "postcode", "postal_code")
MARIS_POINT_ADDRESSES_URL = "https://maris.mississippi.edu/HTML/DATA/data_Cadastral/PointAddressesCounty.html"
MARIS_POINT_ADDRESS_ZIP_RE = re.compile(
    r'https://maris\.mississippi\.edu/MARISdata/CadastralPLSS/MS_PointAddressing_Shared/[^"]+PointAd{1,2}resses?[^"]+\.zip',
    re.IGNORECASE,
)
NAD_TEXT_ZIP_URL = "https://data.transportation.gov/download/fc2s-wawr/application/x-zip-compressed"
MARIS_PARCELS_SERVICE_URL = "https://gis.mississippi.edu/server/rest/services/Cadastral/MS_Parcels_Aprl2024/MapServer"
MARIS_PARCEL_OUT_FIELDS = ("FID", "SITEADD", "SCITY", "SSTATE", "SZIP", "CNTYNAME")
MARIS_PARCEL_MANIFEST_FILENAME = "_maris_parcels_manifest.json"
MARIS_PARCEL_EXPECTED_LAYER_COUNT = 81
DOWNLOAD_USER_AGENT = "AdyResolverAddressDataset/1.0 (+https://openai.com)"
MISSING_REAL_VALUE_MARKERS = {
    "",
    "N/A",
    "NA",
    "NONE",
    "NULL",
    "NOT STATED",
    "UNKNOWN",
    "<NULL>",
}
PLACE_NAME_PLACEHOLDERS = {
    "COUNTY",
    "RURAL",
    "UNINC",
    "UNINCORPORATED",
    "OUTSIDE",
    "OTHER",
    "MISSISSIPPI",
    "MS",
}
KNOWN_STREET_NAME_CORRECTIONS = {
    ("MS", "39345", "NEWTON", "CLARKE", "AVE"): "Clark",
}
SIDE_OF_ROAD_PREFIX_RE = re.compile(r"^\s*[NSEW]\s*/\s*S\.?\s+", re.IGNORECASE)
PARCEL_DESCRIPTOR_START_TOKENS = {
    "ACROSS",
    "ADJ",
    "ADJACENT",
    "BEHIND",
    "BETWEEN",
    "NEAR",
    "OFF",
    "OF",
    "ON",
    "REAR",
}
PARCEL_DESCRIPTOR_DATE_RE = re.compile(
    r"\bDOD\b\s*(?:\d{6,8}|\d{1,2}(?:\s*[/-]\s*|\s+)\d{1,2})",
    re.IGNORECASE,
)
STATE_ZIP_PREFIXES: Dict[str, Tuple[str, ...]] = {
    "MS": tuple(str(prefix) for prefix in range(386, 398)),
}
MISSISSIPPI_COUNTIES: Sequence[str] = (
    "adams", "alcorn", "amite", "attala", "benton", "bolivar", "calhoun", "carroll",
    "chickasaw", "choctaw", "claiborne", "clarke", "clay", "coahoma", "copiah",
    "covington", "desoto", "forrest", "franklin", "george", "greene", "grenada",
    "hancock", "harrison", "hinds", "holmes", "humphreys", "issaquena", "itawamba",
    "jackson", "jasper", "jefferson", "jeffersondavis", "jones", "kemper", "lafayette",
    "lamar", "lauderdale", "lawrence", "leake", "lee", "leflore", "lincoln", "lowndes",
    "madison", "marion", "marshall", "monroe", "montgomery", "neshoba", "newton",
    "noxubee", "oktibbeha", "panola", "pearlriver", "perry", "pike", "pontotoc",
    "prentiss", "quitman", "rankin", "scott", "sharkey", "simpson", "smith", "stone",
    "sunflower", "tallahatchie", "tate", "tippah", "tishomingo", "tunica", "union",
    "walthall", "warren", "washington", "wayne", "webster", "wilkinson", "winston",
    "yalobusha", "yazoo",
)
COUNTY_NAME_ALIASES = {
    "jeffdavis": "jeffersondavis",
    "jeffersondavis": "jeffersondavis",
    "pearlriver": "pearlriver",
    "tallawalth": "tallahatchiewalthall",
}

DIRECTION_FULL_TO_CODE: Dict[str, str] = {full: code for code, full in DIRECTION_TO_FULL.items()}
DIRECTION_FULL_TO_CODE.update({code: code for code in DIRECTION_TO_FULL})

UNIT_FULL_TO_TYPE: Dict[str, str] = {full: code for code, full in UNIT_TYPE_TO_FULL.items()}
UNIT_FULL_TO_TYPE.update({code: code for code in UNIT_TYPE_TO_FULL})

STREET_TYPE_ALIASES: Dict[str, str] = {
    **{code: code for code in STREET_TYPE_TO_FULL},
    **{full: code for code, full in STREET_TYPE_TO_FULL.items()},
    "AV": "AVE",
    "AVEN": "AVE",
    "AVN": "AVE",
    "AVNUE": "AVE",
    "BOUL": "BLVD",
    "BOULV": "BLVD",
    "CIRC": "CIR",
    "CRCL": "CIR",
    "COURTS": "CT",
    "DRIV": "DR",
    "DRV": "DR",
    "HIGHWY": "HWY",
    "HIWAY": "HWY",
    "HIWY": "HWY",
    "HWAY": "HWY",
    "LANES": "LN",
    "PKWAY": "PKWY",
    "PKY": "PKWY",
    "PARKWY": "PKWY",
    "PARKWAYS": "PKWY",
    "PLACE": "PL",
    "ROAD": "RD",
    "ROADS": "RD",
    "STREET": "ST",
    "STRT": "ST",
    "STR": "ST",
    "TERR": "TER",
    "TR": "TRL",
    "TRAILS": "TRL",
    "ALLEY": "ALY",
    "ANNEX": "ANX",
    "ARCADE": "ARC",
    "BEND": "BND",
    "BRANCH": "BR",
    "BRIDGE": "BRG",
    "BROOK": "BRK",
    "BYPASS": "BYP",
    "CAUSEWAY": "CSWY",
    "CENTER": "CTR",
    "COMMONS": "CMNS",
    "CRESCENT": "CRES",
    "CROSSING": "XING",
    "DALE": "DL",
    "DIVIDE": "DV",
    "EXPRESSWAY": "EXPY",
    "EXTENSION": "EXT",
    "FERRY": "FRY",
    "FIELD": "FLD",
    "FLAT": "FLT",
    "FORD": "FRD",
    "FOREST": "FRST",
    "FORGE": "FRG",
    "FORK": "FRK",
    "FREEWAY": "FWY",
    "GARDEN": "GDN",
    "GATEWAY": "GTWY",
    "GLEN": "GLN",
    "GREEN": "GRN",
    "GROVE": "GRV",
    "HARBOR": "HBR",
    "HAVEN": "HVN",
    "HEIGHTS": "HTS",
    "HILL": "HL",
    "HOLLOW": "HOLW",
    "ISLAND": "IS",
    "JUNCTION": "JCT",
    "KEY": "KY",
    "KNOLL": "KNL",
    "LANDING": "LNDG",
    "LIGHT": "LGT",
    "LOCK": "LCK",
    "LODGE": "LDG",
    "MALL": "MALL",
    "MANOR": "MNR",
    "MEADOW": "MDW",
    "MILL": "ML",
    "MOTORWAY": "MTWY",
    "MOUNT": "MT",
    "MOUNTAIN": "MTN",
    "ORCHARD": "ORCH",
    "PARK": "PARK",
    "PASS": "PASS",
    "PATH": "PATH",
    "PIKE": "PIKE",
    "PLAIN": "PLN",
    "PLAZA": "PLZ",
    "POINT": "PT",
    "PORT": "PRT",
    "PRAIRIE": "PR",
    "RADIAL": "RADL",
    "RAMP": "RAMP",
    "RANCH": "RNCH",
    "RAPID": "RPD",
    "REST": "RST",
    "RIDGE": "RDG",
    "RIVER": "RIV",
    "ROUTE": "RTE",
    "ROW": "ROW",
    "RUE": "RUE",
    "RUN": "RUN",
    "SHOAL": "SHL",
    "SHORE": "SHR",
    "SKYWAY": "SKWY",
    "SPRING": "SPG",
    "STATION": "STA",
    "STRAVENUE": "STRA",
    "STREAM": "STRM",
    "SUMMIT": "SMT",
    "TRACE": "TRCE",
    "TRACK": "TRAK",
    "TRAFFICWAY": "TRFY",
    "TUNNEL": "TUNL",
    "TURNPIKE": "TPKE",
    "UNDERPASS": "UPAS",
    "UNION": "UN",
    "VALLEY": "VLY",
    "VIADUCT": "VIA",
    "VIEW": "VW",
    "VILLAGE": "VLG",
    "VILLE": "VL",
    "VISTA": "VIS",
    "WALK": "WALK",
    "WELL": "WL",
}

# Characters with rough keyboard-neighbor substitutions for realistic typos.
KEYBOARD_NEIGHBORS: Dict[str, str] = {
    "a": "sqwz", "b": "vghn", "c": "xdfv", "d": "serfcx", "e": "wsdr", "f": "drtgvc",
    "g": "ftyhbv", "h": "gyujnb", "i": "ujko", "j": "huikmn", "k": "jiolm", "l": "kop",
    "m": "njk", "n": "bhjm", "o": "iklp", "p": "ol", "q": "wa", "r": "edft",
    "s": "awedxz", "t": "rfgy", "u": "yhji", "v": "cfgb", "w": "qase", "x": "zsdc",
    "y": "tghu", "z": "asx",
}

OCR_ALPHA_CONFUSIONS: Dict[str, str] = {
    "a": "o",
    "b": "8",
    "c": "e",
    "e": "c",
    "g": "9q",
    "i": "1l",
    "l": "1i",
    "m": "nn",
    "n": "m",
    "o": "0",
    "q": "9",
    "r": "n",
    "s": "5",
    "t": "1",
    "u": "v",
    "v": "u",
    "z": "2",
}

OCR_DIGIT_CONFUSIONS: Dict[str, str] = {
    "0": "O",
    "1": "I",
    "2": "Z",
    "5": "S",
    "6": "8",
    "8": "B",
}

PHONETIC_REPLACEMENTS: Sequence[Tuple[str, str]] = (
    ("PH", "F"),
    ("CK", "K"),
    ("QU", "KW"),
    ("X", "KS"),
    ("Z", "S"),
    ("C", "K"),
    ("K", "C"),
    ("V", "B"),
    ("B", "V"),
    ("DG", "J"),
    ("TION", "SHUN"),
    ("GH", "G"),
    ("Y", "I"),
)

STREET_TYPE_CONFUSIONS: Dict[str, Sequence[str]] = {
    "ALY": ("LN", "RD"),
    "AVE": ("RD", "ST", "DR"),
    "BLVD": ("AVE", "RD"),
    "CIR": ("CT", "LOOP", "CV"),
    "CT": ("CIR", "PL"),
    "CV": ("CT", "CIR"),
    "DR": ("RD", "AVE", "WAY"),
    "HWY": ("RD", "BLVD"),
    "LN": ("RD", "DR", "ALY"),
    "LOOP": ("CIR", "TRL"),
    "PKWY": ("HWY", "RD", "DR"),
    "PL": ("CT", "TER"),
    "RD": ("DR", "ST", "AVE"),
    "SQ": ("PL", "CIR"),
    "ST": ("RD", "AVE", "DR"),
    "TER": ("PL", "TRL"),
    "TRL": ("TER", "LOOP"),
    "WAY": ("DR", "RD"),
}

STREET_TYPE_KEYBOARD_TYPOS: Dict[str, Sequence[str]] = {
    "AVE": ("ABE", "SVE", "AVW"),
    "BLVD": ("BKVD", "BLBD", "BLVF"),
    "CIR": ("CUR", "CJR", "CIE"),
    "CT": ("CR", "CY", "XT"),
    "DR": ("FR", "SR", "DE"),
    "LN": ("KN", "LM", "LB"),
    "RD": ("RF", "RS", "ED"),
    "ST": ("SR", "SY", "DT", "SE"),
    "TER": ("TRR", "TWR", "TED"),
    "TRL": ("TRK", "TRP", "TRLN"),
}


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class AddressRecord:
    address_id: str
    house_number: str
    predir: str
    street_name: str
    street_type: str
    suffixdir: str
    unit_type: str
    unit_value: str
    city: str
    state: str
    zip_code: str
    source_quality: float = 0.5


@dataclass
class RenderStyle:
    uppercase: bool = False
    lowercase: bool = False
    use_full_directional: bool = False
    use_full_street_type: bool = False
    use_full_unit_type: bool = False
    include_unit: bool = True
    include_zip: bool = True
    include_commas: bool = True
    unit_first: bool = False
    house_after_street_name: bool = False
    slash_unit: bool = False
    extra_spaces: bool = False


@dataclass
class QueryExample:
    query_id: str
    split: str
    label: int
    true_match_id: str
    base_address_id: str
    source: str
    difficulty: str
    noise_tags: str
    query_address: str
    canonical_address: str


@dataclass
class RealAddressLoadResult:
    records: List[AddressRecord]
    input_paths: List[str]
    source_format: str
    state: str
    rows_seen: int
    rows_loaded: int
    rows_skipped: int


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def titleish(text: str) -> str:
    """Title-case while preserving common address abbreviations and alphanumeric tokens."""
    cooked = []
    for word in text.split():
        if any(ch.isdigit() for ch in word):
            cooked.append(word.upper())
        elif word.isupper() and len(word) <= 5:
            cooked.append(word)
        else:
            cooked.append(word.capitalize())
    return " ".join(cooked)


def normalize_spaces(text: str) -> str:
    return " ".join(text.replace(",", " , ").split()).replace(" ,", ",")


def query_text_key(text: str) -> str:
    return normalize_spaces(text).replace(",", "").upper().strip()


def clean_real_value(value: object) -> str:
    if value is None:
        return ""
    cleaned = normalize_spaces(str(value).replace("\ufeff", "").strip().strip("'\""))
    if cleaned.upper() in MISSING_REAL_VALUE_MARKERS:
        return ""
    return cleaned


def clean_real_token(value: object) -> str:
    return re.sub(r"[^A-Z0-9]+", "", clean_real_value(value).upper())


def canonical_direction(value: object) -> str:
    token = clean_real_token(value)
    return DIRECTION_FULL_TO_CODE.get(token, "")


def canonical_street_type(value: object) -> str:
    token = clean_real_token(value)
    return STREET_TYPE_ALIASES.get(token, "")


def dict_get_canonical_street_type(row: Dict[str, str], *names: str) -> str:
    for name in names:
        value = dict_get(row, name)
        street_type = canonical_street_type(value)
        if street_type:
            return street_type
    return ""


def dict_get_place_name(row: Dict[str, str], *names: str) -> str:
    for name in names:
        value = dict_get(row, name)
        place_name = clean_city_candidate(value)
        if place_name:
            return place_name
    return ""


def apply_street_type_fallbacks(
    predir: str,
    street_name: str,
    street_type: str,
    suffixdir: str,
    *candidates: object,
) -> Tuple[str, str, str, str]:
    if street_type:
        return predir, street_name, street_type, suffixdir

    for candidate in candidates:
        candidate_name = clean_street_name(candidate)
        if not candidate_name:
            continue
        parsed_predir, parsed_name, parsed_type, parsed_suffixdir = parse_street_line(candidate_name)
        if parsed_type not in STREET_TYPE_TO_FULL:
            continue
        if not parsed_name or clean_real_token(parsed_name) == clean_real_token(candidate_name):
            continue
        predir = predir or parsed_predir
        street_type = parsed_type
        suffixdir = suffixdir or parsed_suffixdir
        if not street_name or clean_real_token(street_name) == clean_real_token(candidate_name):
            street_name = parsed_name
        break

    return predir, street_name, street_type, suffixdir


def canonical_unit_type(value: object) -> str:
    token = clean_real_token(value)
    return UNIT_FULL_TO_TYPE.get(token, "")


def canonical_state(value: object) -> str:
    token = clean_real_token(value)
    if len(token) == 2 and token in STATE_ABBREV_TO_NAME:
        return token
    return STATE_NAME_TO_ABBREV.get(token, STATE_NAME_TOKEN_TO_ABBREV.get(token, ""))


def normalize_county_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "", value.lower())
    for alias, canonical in COUNTY_NAME_ALIASES.items():
        normalized = normalized.replace(alias, canonical)
    return normalized


def mississippi_counties_in_paths(paths: Sequence[Path]) -> List[str]:
    found = set()
    counties_by_length = sorted((normalize_county_name(county) for county in MISSISSIPPI_COUNTIES), key=len, reverse=True)
    for path in paths:
        normalized_name = normalize_county_name(path.name)
        path_matches: List[str] = []
        for county in counties_by_length:
            if county in normalized_name and not any(county in matched for matched in path_matches):
                path_matches.append(county)
        found.update(path_matches)
    return sorted(found)


def clean_house_number(value: object) -> str:
    cleaned = clean_real_value(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s+", "", cleaned.upper())
    digits = re.sub(r"\D+", "", cleaned)
    if not digits or not any(digit != "0" for digit in digits):
        return ""
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?(?:E[+-]?\d+)?", cleaned):
        try:
            number = Decimal(cleaned)
            if number == 0:
                return ""
            if number == number.to_integral_value():
                return str(int(number))
        except InvalidOperation:
            pass
    cleaned = cleaned.strip(",;")
    return cleaned[:24]


def clean_zip_code(value: object) -> str:
    cleaned = clean_real_value(value)
    match = re.search(r"\d{5}", cleaned)
    return match.group(0) if match else ""


def zip_code_matches_state(zip_code: str, state: str) -> bool:
    if not zip_code:
        return True
    allowed_prefixes = STATE_ZIP_PREFIXES.get(state)
    if not allowed_prefixes:
        return True
    return zip_code.startswith(allowed_prefixes)


def clean_place_name(value: object) -> str:
    cleaned = clean_real_value(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s+", " ", cleaned.replace("_", " ")).strip(" ,")
    cleaned = re.sub(r"^(city|town|village)\s+of\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+(city|town|village|county)$", "", cleaned, flags=re.IGNORECASE)
    return title_real_text(cleaned)


def clean_city_candidate(value: object) -> str:
    cleaned = clean_place_name(value)
    if clean_real_token(cleaned) in PLACE_NAME_PLACEHOLDERS:
        return ""
    return cleaned


def clean_street_name(value: object) -> str:
    cleaned = clean_real_value(value)
    if not cleaned:
        return ""
    cleaned = cleaned.replace("/", " ")
    cleaned = re.sub(r"[,\t]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    return title_real_text(cleaned)


def strip_side_of_road_prefix(value: object) -> str:
    return SIDE_OF_ROAD_PREFIX_RE.sub("", clean_real_value(value), count=1)


def is_parcel_location_descriptor(value: object) -> bool:
    cleaned = clean_street_name(strip_side_of_road_prefix(value)).upper()
    if not cleaned:
        return True
    tokens = cleaned.split()
    if not tokens:
        return True
    if tokens[0] in PARCEL_DESCRIPTOR_START_TOKENS:
        return True
    if len(tokens) >= 2 and tokens[0] in DIRECTION_FULL_TO_CODE and tokens[1] == "OF":
        return True
    if len(tokens) >= 2 and tokens[0] == "CORNER" and tokens[1] == "OF":
        return True
    if len(tokens) >= 2 and tokens[0] in {"LAND", "LOT", "PART", "PT"} and tokens[1] in {
        "AT",
        "BEHIND",
        "NEAR",
        "OF",
        "ON",
        "SOUTH",
        "NORTH",
        "EAST",
        "WEST",
    }:
        return True
    if PARCEL_DESCRIPTOR_DATE_RE.search(cleaned):
        return True
    return False


def strip_duplicate_terminal_street_type(street_name: str, street_type: str) -> str:
    if not street_name or not street_type:
        return street_name
    tokens = street_name.split()
    if len(tokens) <= 1:
        return street_name
    if canonical_street_type(tokens[-1]) == street_type:
        return clean_street_name(" ".join(tokens[:-1]))
    return street_name


def title_real_text(text: str) -> str:
    abbreviations = {"US", "MS", "SR", "CR", "FM", "I", "II", "III", "IV", "MLK"}
    cooked: List[str] = []
    for word in text.split():
        token = clean_real_token(word)
        if any(ch.isdigit() for ch in word) or token in abbreviations:
            cooked.append(word.upper())
        else:
            cooked.append(word.capitalize())
    return " ".join(cooked)


def parse_unit(value: object) -> Tuple[str, str]:
    cleaned = clean_real_value(value)
    if not cleaned:
        return "", ""
    parts = cleaned.replace("#", " # ").split()
    if parts:
        unit_type = canonical_unit_type(parts[0])
        if unit_type:
            unit_value = " ".join(parts[1:]).strip() or cleaned
            return unit_type, unit_value.upper()[:24]
    return "UNIT", cleaned.upper()[:24]


def split_house_number_from_street(street: str) -> Tuple[str, str]:
    match = re.match(r"^\s*([0-9]+[A-Z0-9\-\/]*)\s+(.+)$", street, flags=re.IGNORECASE)
    if not match:
        return "", street
    return clean_house_number(match.group(1)), match.group(2)


def parse_street_line(street: object) -> Tuple[str, str, str, str]:
    cleaned = clean_street_name(strip_side_of_road_prefix(street))
    if not cleaned:
        return "", "", "", ""
    tokens = cleaned.split()
    predir = ""
    suffixdir = ""
    street_type = ""

    if tokens:
        maybe_predir = canonical_direction(tokens[0])
        if maybe_predir:
            predir = maybe_predir
            tokens = tokens[1:]

    if tokens:
        maybe_suffixdir = canonical_direction(tokens[-1])
        if maybe_suffixdir:
            suffixdir = maybe_suffixdir
            tokens = tokens[:-1]

    if tokens:
        maybe_type = canonical_street_type(tokens[-1])
        if maybe_type:
            street_type = maybe_type
            tokens = tokens[:-1]

    if not tokens and predir and street_type:
        street_name = clean_street_name(DIRECTION_TO_FULL.get(predir, predir))
        predir = ""
    else:
        street_name = clean_street_name(" ".join(tokens))
    if not street_name and cleaned:
        street_name = cleaned
    street_name = strip_duplicate_terminal_street_type(street_name, street_type)
    return predir, street_name, street_type or canonical_street_type(""), suffixdir


def strip_trailing_zip_and_state(text: str, state: str, zip_code: str) -> Tuple[str, str, str]:
    remainder = clean_real_value(text).strip(" ,")
    zip_match = re.search(r"(?:^|[\s,])(\d{5})(?:-\d{4})?\s*$", remainder)
    if zip_match:
        zip_code = zip_code or zip_match.group(1)
        remainder = remainder[: zip_match.start()].strip(" ,")

    tokens = remainder.split()
    for width in range(min(3, len(tokens)), 0, -1):
        candidate = " ".join(tokens[-width:])
        candidate_state = canonical_state(candidate)
        if not candidate_state:
            continue
        state = state or candidate_state
        remainder = " ".join(tokens[:-width]).strip(" ,")
        break

    return remainder, state, zip_code


def split_city_from_uncommaed_address(text: str) -> Tuple[str, str]:
    house_number, street_line = split_house_number_from_street(text)
    if not house_number:
        return text, ""

    tokens = street_line.split()
    for idx in range(len(tokens) - 1, -1, -1):
        if not canonical_street_type(tokens[idx]):
            continue
        city_tokens = tokens[idx + 1:]
        if not city_tokens:
            return text, ""
        street_tokens = tokens[: idx + 1]
        return f"{house_number} {' '.join(street_tokens)}", clean_city_candidate(" ".join(city_tokens))
    return text, ""


def parse_generic_full_address(
    full_address: str,
    city: str,
    state: str,
    zip_code: str,
) -> Tuple[str, str, str, str, str, str, str]:
    parts = [part.strip() for part in clean_real_value(full_address).split(",") if part.strip()]
    if not parts:
        return "", "", "", "", city, state, zip_code

    street_line = parts[0]
    locality_parts = parts[1:]
    if locality_parts:
        if len(locality_parts) >= 2 and not city:
            city = clean_city_candidate(locality_parts[-2])
        tail = locality_parts[-1]
        tail, state, zip_code = strip_trailing_zip_and_state(tail, state, zip_code)
        if tail and not city:
            city = clean_city_candidate(tail)
    else:
        stripped, state, zip_code = strip_trailing_zip_and_state(street_line, state, zip_code)
        if stripped != street_line:
            street_line = stripped
        if not city:
            street_line, city = split_city_from_uncommaed_address(street_line)

    if city:
        city_tokens = clean_city_candidate(city).split()
        street_tokens = street_line.split()
        if city_tokens and len(street_tokens) > len(city_tokens):
            if [token.upper() for token in street_tokens[-len(city_tokens):]] == [token.upper() for token in city_tokens]:
                street_line = " ".join(street_tokens[:-len(city_tokens)])

    house_number, street_value = split_house_number_from_street(street_line)
    predir, street_name, street_type, suffixdir = parse_street_line(street_value)
    return house_number, predir, street_name, street_type, suffixdir, city, state, zip_code


def shift_house_number(value: str, rng: random.Random) -> str:
    match = re.search(r"\d+", value)
    if not match:
        return mutate_numeric_token(value, rng) if value else str(rng.randint(1, 9999))
    current = int(match.group(0))
    step = rng.choice([2, 4, 6, 8, 10, 12])
    sign = -1 if rng.random() < 0.35 else 1
    shifted = str(max(1, current + sign * step))
    return f"{value[:match.start()]}{shifted}{value[match.end():]}"


def choose_weighted(rng: random.Random, items: Sequence[Tuple[str, float]]) -> str:
    total = sum(weight for _, weight in items)
    point = rng.random() * total
    upto = 0.0
    for item, weight in items:
        upto += weight
        if point <= upto:
            return item
    return items[-1][0]


def mutate_alpha_token(token: str, rng: random.Random) -> str:
    letters = [i for i, ch in enumerate(token) if ch.isalpha()]
    if not letters:
        return token
    mode = choose_weighted(rng, [("substitute", 0.5), ("delete", 0.25), ("transpose", 0.25)])
    idx = rng.choice(letters)
    chars = list(token)

    if mode == "substitute":
        lower = chars[idx].lower()
        replacement_pool = KEYBOARD_NEIGHBORS.get(lower, string.ascii_lowercase)
        replacement = rng.choice(replacement_pool)
        chars[idx] = replacement.upper() if chars[idx].isupper() else replacement
        return "".join(chars)

    if mode == "delete" and len(token) > 4:
        del chars[idx]
        return "".join(chars)

    if mode == "transpose" and len(token) > 3:
        swap_idx = idx + 1 if idx < len(token) - 1 else idx - 1
        if 0 <= swap_idx < len(token):
            chars[idx], chars[swap_idx] = chars[swap_idx], chars[idx]
            return "".join(chars)

    # fallback if transpose/delete are not feasible
    chars[idx] = rng.choice(string.ascii_lowercase)
    return "".join(chars)


def mutate_extra_or_missing_letter(token: str, rng: random.Random) -> str:
    letters = [idx for idx, ch in enumerate(token) if ch.isalpha()]
    if not letters:
        return token
    idx = rng.choice(letters)
    if len(token) > 4 and rng.random() < 0.55:
        return token[:idx] + token[idx + 1:]
    return token[:idx] + token[idx] + token[idx:]


def mutate_street_type_token(token: str, rng: random.Random) -> str:
    token = token.upper()
    choices = [choice for choice in STREET_TYPE_KEYBOARD_TYPOS.get(token, ()) if choice != token]
    if choices and rng.random() < 0.85:
        return rng.choice(choices)
    mutated = mutate_alpha_token(token, rng).upper()
    return mutated if mutated != token else (choices[0] if choices else token)


def mutate_numeric_token(token: str, rng: random.Random) -> str:
    digits = [i for i, ch in enumerate(token) if ch.isdigit()]
    if not digits:
        return token
    idx = rng.choice(digits)
    chars = list(token)
    replacement_choices = [d for d in string.digits if d != chars[idx]]
    chars[idx] = rng.choice(replacement_choices)
    return "".join(chars)


def mutate_ocr_token(token: str, rng: random.Random) -> str:
    if not token:
        return token

    positions = [idx for idx, ch in enumerate(token) if ch.isalnum()]
    if not positions:
        return token

    idx = rng.choice(positions)
    chars = list(token)
    current = chars[idx]
    if current.isdigit():
        replacement = OCR_DIGIT_CONFUSIONS.get(current)
        if not replacement:
            return token
        chars[idx] = replacement
        return "".join(chars)

    replacement = OCR_ALPHA_CONFUSIONS.get(current.lower())
    if not replacement:
        return token
    if len(replacement) == 1:
        chars[idx] = replacement.upper() if current.isupper() else replacement
        return "".join(chars)

    rendered = replacement.upper() if current.isupper() else replacement
    return token[:idx] + rendered + token[idx + 1:]


def mutate_locality_token(token: str, rng: random.Random) -> str:
    """Produce more human-looking misspellings for cities/states than a single typo."""
    if len(token) < 4:
        return token

    mutated = token
    edit_count = 1 if len(token) <= 6 else rng.choice([1, 2, 2, 3])
    for _ in range(edit_count):
        mode = choose_weighted(
            rng,
            [
                ("alpha", 0.34),
                ("duplicate_vowel", 0.16),
                ("drop_vowel", 0.16),
                ("swap_pair", 0.12),
                ("soft_replace", 0.12),
                ("ocr", 0.10),
            ],
        )

        if mode == "alpha":
            mutated = mutate_alpha_token(mutated, rng)
            continue

        if mode == "ocr":
            mutated = mutate_ocr_token(mutated, rng)
            continue

        letters = [i for i, ch in enumerate(mutated) if ch.isalpha()]
        if not letters:
            continue

        if mode == "duplicate_vowel":
            vowels = [i for i in letters if mutated[i].lower() in "aeiou"]
            if vowels:
                idx = rng.choice(vowels)
                mutated = mutated[:idx + 1] + mutated[idx] + mutated[idx + 1:]
                continue

        if mode == "drop_vowel" and len(mutated) > 5:
            vowels = [i for i in letters if mutated[i].lower() in "aeiou"]
            if vowels:
                idx = rng.choice(vowels)
                mutated = mutated[:idx] + mutated[idx + 1:]
                continue

        if mode == "swap_pair" and len(mutated) > 4:
            idx = rng.choice(letters[:-1]) if len(letters) > 1 else letters[0]
            swap_idx = idx + 1
            if swap_idx < len(mutated):
                chars = list(mutated)
                chars[idx], chars[swap_idx] = chars[swap_idx], chars[idx]
                mutated = "".join(chars)
                continue

        if mode == "soft_replace":
            idx = rng.choice(letters)
            ch = mutated[idx].lower()
            replacements = {
                "i": "e",
                "e": "i",
                "o": "u",
                "u": "o",
                "s": "ss",
                "c": "s",
                "p": "pp",
                "l": "ll",
            }.get(ch)
            if replacements:
                rendered = replacements.upper() if mutated[idx].isupper() else replacements
                mutated = mutated[:idx] + rendered + mutated[idx + 1:]
                continue

        mutated = mutate_alpha_token(mutated, rng)

    return mutated


def mutate_phonetic_token(token: str, rng: random.Random) -> str:
    if len(token) < 4:
        return token

    upper = token.upper()
    candidates = [(src, dst) for src, dst in PHONETIC_REPLACEMENTS if src in upper]
    if candidates:
        src, dst = rng.choice(candidates)
        idx = upper.index(src)
        rendered = dst if token.isupper() else dst.lower()
        return token[:idx] + rendered + token[idx + len(src):]

    # fallback: vowel drift tends to create phonetic-looking misspellings
    for src, dst in (("A", "E"), ("E", "I"), ("I", "Y"), ("O", "U"), ("U", "O")):
        if src in upper:
            idx = upper.index(src)
            rendered = dst if token.isupper() else dst.lower()
            return token[:idx] + rendered + token[idx + 1:]
    return token


def stable_split_for_key(key: str) -> str:
    """Deterministic split using a cryptographic hash for better distribution."""
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, "big") / float(2 ** (8 * len(digest)))
    if value < 0.70:
        return "train"
    if value < 0.85:
        return "validation"
    return "test"


# ---------------------------------------------------------------------------
# Real address ingestion
# ---------------------------------------------------------------------------

def discover_input_files(paths: Sequence[Path]) -> List[Path]:
    discovered: List[Path] = []
    for path in paths:
        if path.is_dir():
            discovered.extend(
                candidate
                for candidate in sorted(path.rglob("*"))
                if candidate.suffix.lower() in {".csv", ".txt", ".zip", ".dbf"}
            )
        elif path.exists():
            discovered.append(path)
        else:
            raise FileNotFoundError(f"Real address input does not exist: {path}")
    return discovered


def iter_text_streams(path: Path) -> Iterator[Tuple[str, io.TextIOBase]]:
    suffix = path.suffix.lower()
    if suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            for member in archive.namelist():
                member_suffix = Path(member).suffix.lower()
                if member_suffix not in {".csv", ".txt"}:
                    continue
                with archive.open(member) as raw:
                    yield f"{path}:{member}", io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        return

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        yield str(path), handle


def row_format(fieldnames: Sequence[str], requested_format: str) -> str:
    if requested_format != "auto":
        return requested_format
    fields = {field.replace("\ufeff", "") for field in fieldnames}
    lower_fields = {field.lower() for field in fields}
    if {"SITEADD", "SCITY"}.issubset(fields) or {"siteadd", "scity"}.issubset(lower_fields):
        return "maris_parcels"
    if {"NUMBER", "STREET"}.issubset(fields):
        return "openaddresses"
    if {"Add_Number", "St_Name"}.issubset(fields) or {"add_number", "st_name"}.issubset(lower_fields):
        return "nad"
    if (
        "fulladdr" in lower_fields
        or "fullname" in lower_fields
        or {"stnum", "name", "type"}.issubset(lower_fields)
        or {"address", "street", "streettype"}.issubset(lower_fields)
    ):
        return "maris"
    if {"STREET_ADD", "ROAD_NAME"}.issubset(fields) or {"street_add", "road_name"}.issubset(lower_fields):
        return "maris"
    if {"house_number", "street_name", "state"}.issubset(lower_fields):
        return "address_record"
    if any(field in lower_fields for field in GENERIC_FULL_ADDRESS_FIELDS):
        return "generic"
    return "openaddresses"


def source_quality_for_format(source_format: str) -> float:
    if source_format == "address_record":
        return 1.0
    if source_format == "maris":
        return 0.92
    if source_format == "openaddresses":
        return 0.86
    if source_format == "generic":
        return 0.80
    if source_format == "nad":
        return 0.76
    if source_format == "maris_parcels":
        return 0.62
    return 0.50


def dict_get(row: Dict[str, str], *names: str) -> str:
    first_match = ""
    for name in names:
        if name in row:
            if first_match == "":
                first_match = row[name]
            if clean_real_value(row[name]):
                return row[name]
            continue
        lower = name.lower()
        for key, value in row.items():
            if key.lower().replace("\ufeff", "") == lower:
                if first_match == "":
                    first_match = value
                if clean_real_value(value):
                    return value
                break
    return first_match


def openaddresses_row_to_record(row: Dict[str, str], address_id: str, state_filter: str) -> Optional[AddressRecord]:
    state = canonical_state(dict_get(row, "REGION", "STATE")) or state_filter
    if state_filter and state and state != state_filter:
        return None

    street_value = clean_real_value(dict_get(row, "STREET"))
    house_number = clean_house_number(dict_get(row, "NUMBER"))
    if not house_number:
        house_number, street_value = split_house_number_from_street(street_value)
    else:
        embedded_number, remainder = split_house_number_from_street(street_value)
        if embedded_number and embedded_number == house_number:
            street_value = remainder

    predir, street_name, street_type, suffixdir = parse_street_line(street_value)
    city = dict_get_place_name(row, "CITY", "DISTRICT", "LOCALITY")
    zip_code = clean_zip_code(dict_get(row, "POSTCODE", "ZIP", "ZIP_CODE"))
    unit_type, unit_value = parse_unit(dict_get(row, "UNIT"))

    if not house_number or not street_name or not state:
        return None
    if not city and not zip_code:
        return None

    return AddressRecord(
        address_id=address_id,
        house_number=house_number,
        predir=predir,
        street_name=street_name,
        street_type=street_type,
        suffixdir=suffixdir,
        unit_type=unit_type,
        unit_value=unit_value,
        city=city,
        state=state,
        zip_code=zip_code,
    )


def nad_row_to_record(row: Dict[str, str], address_id: str, state_filter: str) -> Optional[AddressRecord]:
    state = canonical_state(dict_get(row, "State"))
    if state_filter and state != state_filter:
        return None

    house_number = clean_house_number(dict_get(row, "AddNo_Full")) or clean_house_number(
        " ".join(
            part
            for part in [
                clean_real_value(dict_get(row, "AddNum_Pre")),
                clean_real_value(dict_get(row, "Add_Number")),
                clean_real_value(dict_get(row, "AddNum_Suf")),
            ]
            if part
        )
    )
    if not house_number:
        return None

    predir = canonical_direction(dict_get(row, "St_PreDir"))
    suffixdir = canonical_direction(dict_get(row, "St_PosDir"))
    street_type = canonical_street_type(dict_get(row, "St_PosTyp"))
    street_name_parts = [
        clean_real_value(dict_get(row, "St_PreMod")),
        clean_real_value(dict_get(row, "St_PreTyp")) if not street_type else "",
        clean_real_value(dict_get(row, "St_Name")),
        clean_real_value(dict_get(row, "St_PosMod")),
    ]
    street_name = clean_street_name(" ".join(part for part in street_name_parts if part))
    if not street_name:
        parsed_predir, street_name, parsed_type, parsed_suffixdir = parse_street_line(dict_get(row, "StNam_Full"))
        predir = predir or parsed_predir
        street_type = street_type or parsed_type
        suffixdir = suffixdir or parsed_suffixdir
    if not street_name:
        return None

    city = dict_get_place_name(row, "Post_City", "Inc_Muni", "Census_Plc", "Uninc_Comm")
    zip_code = clean_zip_code(dict_get(row, "Zip_Code"))
    if not city and not zip_code:
        return None

    unit_type, unit_value = parse_unit(dict_get(row, "Unit"))
    if not unit_type:
        unit_type, unit_value = parse_unit(dict_get(row, "Room"))
    if not unit_type:
        unit_type, unit_value = parse_unit(dict_get(row, "Building"))
    if not unit_type:
        unit_type, unit_value = parse_unit(dict_get(row, "Floor"))

    return AddressRecord(
        address_id=address_id,
        house_number=house_number,
        predir=predir,
        street_name=street_name,
        street_type=street_type,
        suffixdir=suffixdir,
        unit_type=unit_type,
        unit_value=unit_value,
        city=city,
        state=state,
        zip_code=zip_code,
    )


def address_record_row_to_record(row: Dict[str, str], address_id: str, state_filter: str) -> Optional[AddressRecord]:
    state = canonical_state(dict_get(row, *GENERIC_STATE_FIELDS))
    if state_filter and state and state != state_filter:
        return None
    state = state or state_filter
    city = dict_get_place_name(row, *GENERIC_CITY_FIELDS)
    zip_code = clean_zip_code(dict_get(row, *GENERIC_ZIP_FIELDS))
    house_number = clean_house_number(dict_get(row, "house_number", "number", "addr_num", "address_number"))
    predir = canonical_direction(dict_get(row, "predir", "pre_dir", "prefix_direction"))
    street_name = clean_street_name(dict_get(row, "street_name", "road_name", "name"))
    street_type = canonical_street_type(dict_get(row, "street_type", "road_type", "type")) or clean_real_token(dict_get(row, "street_type", "road_type", "type"))
    suffixdir = canonical_direction(dict_get(row, "suffixdir", "sufdir", "post_dir", "suffix_direction"))
    unit_type = canonical_unit_type(dict_get(row, "unit_type"))
    unit_value = clean_real_value(dict_get(row, "unit_value", "unit", "suite", "apt")).upper()[:24]

    full_address = clean_real_value(dict_get(row, *GENERIC_FULL_ADDRESS_FIELDS))
    if full_address and (not house_number or not street_name):
        (
            parsed_house_number,
            parsed_predir,
            parsed_street_name,
            parsed_street_type,
            parsed_suffixdir,
            parsed_city,
            parsed_state,
            parsed_zip_code,
        ) = parse_generic_full_address(full_address, city, state, zip_code)
        house_number = house_number or parsed_house_number
        predir = predir or parsed_predir
        street_name = street_name or parsed_street_name
        street_type = street_type or parsed_street_type
        suffixdir = suffixdir or parsed_suffixdir
        city = city or parsed_city
        state = state or parsed_state
        zip_code = zip_code or parsed_zip_code

    if state_filter and state and state != state_filter:
        return None

    record = AddressRecord(
        address_id=address_id,
        house_number=house_number,
        predir=predir,
        street_name=street_name,
        street_type=street_type,
        suffixdir=suffixdir,
        unit_type=unit_type,
        unit_value=unit_value,
        city=city,
        state=state,
        zip_code=zip_code,
    )
    if not record.house_number or not record.street_name or not record.state:
        return None
    return record


def apply_known_address_corrections(record: AddressRecord) -> AddressRecord:
    street_name = KNOWN_STREET_NAME_CORRECTIONS.get(
        (
            record.state.upper(),
            record.zip_code,
            record.city.upper(),
            record.street_name.upper(),
            record.street_type.upper(),
        )
    )
    if street_name:
        record.street_name = street_name
    return record


def maris_row_to_record(row: Dict[str, str], address_id: str, state_filter: str) -> Optional[AddressRecord]:
    state = state_filter or "MS"
    if state != "MS":
        return None

    street_line = clean_real_value(dict_get(row, "STREET_ADD", "FULL_ADDR", "FullAddr", "FULL_ADDRE"))
    full_name_line = clean_real_value(dict_get(row, "FULLNAME", "FullName", "StNam_Full"))
    generic_address = clean_real_value(dict_get(row, "ADDRESS", "Address"))
    if not street_line and re.search(r"\d", generic_address) and re.search(r"[A-Za-z]", generic_address) and " " in generic_address:
        street_line = generic_address

    house_number = clean_house_number(
        dict_get(
            row,
            "ADDR_NUM",
            "ADDRESS_NU",
            "ADDRNUM",
            "NUMBER",
            "STNUM",
            "ST_NUMBER",
            "Address",
            "ADDRESS",
            "AddPre",
            "STNUM_L",
        )
    )
    if not house_number:
        house_number, street_line = split_house_number_from_street(street_line)
    else:
        embedded_number, remainder = split_house_number_from_street(street_line)
        if embedded_number and embedded_number == house_number:
            street_line = remainder

    predir = canonical_direction(dict_get(row, "PRE_DIR", "PREDIR", "PreDir", "PREFIX_DIR", "ST_PREFIXD", "ST_PREDIR", "LgcyPreDir"))
    suffixdir = canonical_direction(dict_get(row, "POST_DIR", "SUFDIR", "SufDir", "SUFFIX_DIR", "SUFFIX_DIR", "ST_SUFFIXD", "ST_POSTDIR", "LgcySufDir"))
    pre_modifier = clean_street_name(dict_get(row, "PreMod", "PREMOD"))
    street_base = clean_street_name(
        dict_get(
            row,
            "ROAD_NAME",
            "ROAD_NAME_",
            "NAME",
            "STREET_NAM",
            "ST_NAME",
            "Street",
            "ST_NAME_1",
            "LgcyStreet",
            "LABEL",
        )
    )
    street_name = clean_street_name(" ".join(part for part in [pre_modifier, street_base] if part)) or street_base
    street_type = dict_get_canonical_street_type(
        row,
        "TYPE",
        "ROAD_TYPE",
        "STREET_TYP",
        "ST_TYPE",
        "ST_TYPE_1",
        "ST_TYPE_2",
        "StreetType",
        "LgcyType",
        "ST_TYPE_1",
        "TYPE_1",
        "TYPE_2",
    )
    predir, street_name, street_type, suffixdir = apply_street_type_fallbacks(
        predir,
        street_name,
        street_type,
        suffixdir,
        street_base,
        full_name_line,
        street_line,
    )
    if not street_name:
        parsed_predir, street_name, parsed_type, parsed_suffixdir = parse_street_line(street_line)
        predir = predir or parsed_predir
        street_type = street_type or parsed_type
        suffixdir = suffixdir or parsed_suffixdir

    city = dict_get_place_name(
        row,
        "COMMUNITY",
        "CITY",
        "City",
        "POST_COMM",
        "Post_Comm",
        "PostComm",
        "POST_CITY",
        "MUNI",
        "L_COMMUNIT",
        "R_COMMUNIT",
        "Uninc_Comm",
        "UnincComm",
        "MSAGComm",
    )
    zip_code = clean_zip_code(dict_get(row, "ZCTA5CE10", "ZIP", "ZIP_CODE", "ZIPCODE", "Zipcode", "POSTCODE", "L_ZIP", "R_ZIP"))

    unit_type = canonical_unit_type(dict_get(row, "UNIT_TYPE"))
    unit_value = clean_real_value(dict_get(row, "UNIT", "UNIT_ID", "UNIT_NUM", "BldgUnit", "BLDG_UNITS", "Room", "Floor", "BUILDING")).upper()[:24]
    if unit_value and not unit_type:
        unit_type, unit_value = parse_unit(unit_value)

    if not house_number or not street_name:
        return None
    if not city and not zip_code:
        return None

    return AddressRecord(
        address_id=address_id,
        house_number=house_number,
        predir=predir,
        street_name=street_name,
        street_type=street_type,
        suffixdir=suffixdir,
        unit_type=unit_type,
        unit_value=unit_value,
        city=city,
        state=state,
        zip_code=zip_code,
    )


def maris_parcel_row_to_record(row: Dict[str, str], address_id: str, state_filter: str) -> Optional[AddressRecord]:
    state = canonical_state(dict_get(row, "SSTATE", "STATE")) or state_filter or "MS"
    if state_filter and state != state_filter:
        return None

    street_line = clean_real_value(dict_get(row, "SITEADD", "SITUSADDR", "SITUS_ADD", "SITE_ADDR", "PROPERTY_ADDRESS"))
    house_number, street_line = split_house_number_from_street(street_line)
    if not house_number or not street_line:
        return None
    if is_parcel_location_descriptor(street_line):
        return None

    predir, street_name, street_type, suffixdir = parse_street_line(street_line)
    if not street_name:
        return None

    city = dict_get_place_name(row, "SCITY", "SITUSCITY", "SITE_CITY", "CITY")
    zip_code = clean_zip_code(dict_get(row, "SZIP", "SITUSZIP", "SITE_ZIP", "ZIP", "ZIP_CODE"))
    if not city and not zip_code:
        return None

    return AddressRecord(
        address_id=address_id,
        house_number=house_number,
        predir=predir,
        street_name=street_name,
        street_type=street_type,
        suffixdir=suffixdir,
        unit_type="",
        unit_value="",
        city=city,
        state=state,
        zip_code=zip_code,
    )


def real_row_to_record(row: Dict[str, str], source_format: str, address_id: str, state_filter: str) -> Optional[AddressRecord]:
    if source_format == "nad":
        record = nad_row_to_record(row, address_id, state_filter)
    elif source_format == "maris":
        record = maris_row_to_record(row, address_id, state_filter)
    elif source_format == "maris_parcels":
        record = maris_parcel_row_to_record(row, address_id, state_filter)
    elif source_format in {"address_record", "generic"}:
        record = address_record_row_to_record(row, address_id, state_filter)
    else:
        record = openaddresses_row_to_record(row, address_id, state_filter)
    if record and not zip_code_matches_state(record.zip_code, record.state):
        return None
    if record:
        record.source_quality = source_quality_for_format(source_format)
        record = apply_known_address_corrections(record)
        record.street_name = strip_duplicate_terminal_street_type(record.street_name, record.street_type)
        street_line = " ".join(part for part in [record.street_name, record.street_type] if part)
        if is_parcel_location_descriptor(street_line):
            return None
        if not record.house_number or not record.street_name or not record.state:
            return None
    return record


def dbf_records_from_stream(raw: io.BufferedIOBase, encoding: str = "latin1") -> Iterator[Dict[str, str]]:
    header = raw.read(32)
    if len(header) < 32:
        return
    record_count = struct.unpack("<I", header[4:8])[0]
    header_length = struct.unpack("<H", header[8:10])[0]
    record_length = struct.unpack("<H", header[10:12])[0]

    fields: List[Tuple[str, int, int]] = []
    offset = 1
    while True:
        descriptor = raw.read(32)
        if not descriptor or descriptor[0] == 0x0D:
            break
        name = descriptor[:11].split(b"\0", 1)[0].decode("ascii", errors="ignore")
        length = descriptor[16]
        fields.append((name, offset, length))
        offset += length

    raw.seek(header_length)
    for _ in range(record_count):
        record = raw.read(record_length)
        if len(record) < record_length:
            break
        if record[:1] == b"*":
            continue
        yield {
            name: record[start:start + length].decode(encoding, errors="ignore").strip()
            for name, start, length in fields
        }


def iter_dbf_rows(path: Path) -> Iterator[Tuple[str, Dict[str, str]]]:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            dbf_members = [member for member in archive.namelist() if member.lower().endswith(".dbf")]
            for member in dbf_members:
                encoding = "latin1"
                cpg_name = Path(member).with_suffix(".cpg").as_posix()
                if cpg_name in archive.namelist():
                    with archive.open(cpg_name) as cpg:
                        cpg_value = cpg.read().decode("ascii", errors="ignore").strip()
                        if cpg_value:
                            encoding = cpg_value
                with archive.open(member) as raw:
                    buffered = io.BytesIO(raw.read())
                    for row in dbf_records_from_stream(buffered, encoding=encoding):
                        yield f"{path}:{member}", row
        return

    if path.suffix.lower() == ".dbf":
        with path.open("rb") as raw:
            for row in dbf_records_from_stream(raw):
                yield str(path), row


def load_real_addresses(
    input_paths: Sequence[Path],
    source_format: str,
    state_filter: str,
    limit: Optional[int] = None,
) -> RealAddressLoadResult:
    files = discover_input_files(input_paths)
    records: List[AddressRecord] = []
    seen_keys: set[str] = set()
    rows_seen = 0
    rows_skipped = 0
    detected_formats: Counter[str] = Counter()

    for file_path in files:
        if source_format in {"auto", "maris"} and file_path.suffix.lower() in {".zip", ".dbf"}:
            dbf_detected_format = ""
            for _stream_name, row in iter_dbf_rows(file_path):
                rows_seen += 1
                if not dbf_detected_format:
                    dbf_detected_format = row_format(tuple(row.keys()), source_format)
                    detected_formats[dbf_detected_format] += 1
                record = real_row_to_record(row, dbf_detected_format, f"REAL_{rows_seen:09d}", state_filter)
                if record is None:
                    rows_skipped += 1
                    continue
                key = query_text_key(canonical_address(record))
                if key in seen_keys:
                    rows_skipped += 1
                    continue
                seen_keys.add(key)
                records.append(record)
                if limit and len(records) >= limit:
                    return RealAddressLoadResult(
                        records=records,
                        input_paths=[str(path) for path in files],
                        source_format=detected_formats.most_common(1)[0][0] if detected_formats else source_format,
                        state=state_filter,
                        rows_seen=rows_seen,
                        rows_loaded=len(records),
                        rows_skipped=rows_skipped,
                    )

        for stream_name, stream in iter_text_streams(file_path):
            try:
                reader = csv.DictReader(stream)
                if not reader.fieldnames:
                    continue
                detected_format = row_format(reader.fieldnames, source_format)
                detected_formats[detected_format] += 1
                for row in reader:
                    rows_seen += 1
                    record = real_row_to_record(row, detected_format, f"REAL_{rows_seen:09d}", state_filter)
                    if record is None:
                        rows_skipped += 1
                        continue
                    key = query_text_key(canonical_address(record))
                    if key in seen_keys:
                        rows_skipped += 1
                        continue
                    seen_keys.add(key)
                    records.append(record)
                    if limit and len(records) >= limit:
                        return RealAddressLoadResult(
                            records=records,
                            input_paths=[str(path) for path in files],
                            source_format=detected_formats.most_common(1)[0][0] if detected_formats else source_format,
                            state=state_filter,
                            rows_seen=rows_seen,
                            rows_loaded=len(records),
                            rows_skipped=rows_skipped,
                        )
            except csv.Error as exc:
                raise ValueError(f"Could not parse real address rows from {stream_name}: {exc}") from exc

    return RealAddressLoadResult(
        records=records,
        input_paths=[str(path) for path in files],
        source_format=detected_formats.most_common(1)[0][0] if detected_formats else source_format,
        state=state_filter,
        rows_seen=rows_seen,
        rows_loaded=len(records),
        rows_skipped=rows_skipped,
    )


def download_openaddresses_ms(cache_dir: Path) -> List[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    with open_url(OPENADDRESSES_RESULTS_URL, timeout=60) as response:
        html = response.read().decode("utf-8", errors="replace")
    urls = sorted(set(OPENADDRESSES_MS_ZIP_RE.findall(html)))
    if not urls:
        raise RuntimeError("No Mississippi OpenAddresses ZIP links were found in the OpenAddresses results index.")

    downloaded: List[Path] = []
    for url in urls:
        filename = url.rsplit("/", 1)[-1]
        source_name = url.rsplit("/us/ms/", 1)[-1].replace("/", "_")
        target = cache_dir / f"{source_name or filename}"
        if target.exists() and target.stat().st_size > 0:
            downloaded.append(target)
            continue
        with open_url(url, timeout=120) as response, target.open("wb") as handle:
            handle.write(response.read())
        downloaded.append(target)
    return downloaded


def download_openaddresses_ms_source_configs(config_dir: Path, refresh: bool = False) -> List[Path]:
    config_dir.mkdir(parents=True, exist_ok=True)
    cached_configs = sorted(path for path in config_dir.glob("*.json") if path.stat().st_size > 0)
    if cached_configs and not refresh:
        print(f"Using cached OpenAddresses Mississippi source configs from {config_dir} ({len(cached_configs)} file(s)).")
        return cached_configs

    with open_url(OPENADDRESSES_MS_SOURCES_API_URL, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    if not isinstance(payload, list):
        raise RuntimeError("OpenAddresses Mississippi source catalog did not return a file list.")

    downloaded: List[Path] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name", ""))
        download_url = str(entry.get("download_url", ""))
        if not name.endswith(".json") or not download_url:
            continue
        target = config_dir / name
        with open_url(download_url, timeout=60) as response:
            target.write_bytes(response.read())
        downloaded.append(target)

    if not downloaded:
        raise RuntimeError("No OpenAddresses Mississippi source configs were downloaded.")
    return sorted(downloaded)


def openaddresses_direct_manifest_path(cache_dir: Path) -> Path:
    return cache_dir / OPENADDRESSES_DIRECT_MANIFEST_FILENAME


def openaddresses_source_name(config_path: Path) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", config_path.stem).strip("_") or "source"


def openaddresses_direct_target_path(cache_dir: Path, source_name: str, layer_index: int, layer_count: int) -> Path:
    suffix = f"_{layer_index + 1}" if layer_count > 1 else ""
    return cache_dir / f"{source_name}{suffix}.csv"


def dict_get_conform_value(row: Dict[str, object], field_name: object) -> str:
    field = str(field_name or "")
    if not field:
        return ""
    if field in row:
        return clean_real_value(row[field])
    lower = field.lower()
    for key, value in row.items():
        if str(key).lower().replace("\ufeff", "") == lower:
            return clean_real_value(value)
    normalized = clean_real_token(field)
    for key, value in row.items():
        if clean_real_token(key) == normalized:
            return clean_real_value(value)
    return ""


def openaddresses_regexp_replace(value: str, pattern: str, replace: str) -> str:
    match = re.search(pattern, value)
    if not match:
        return ""
    result = replace
    if not result and match.groups():
        return clean_real_value(match.group(1) or "")
    for index, group in enumerate(match.groups(), 1):
        result = result.replace(f"${index}", group or "")
    return clean_real_value(result)


def openaddresses_conform_value(row: Dict[str, object], spec: object) -> str:
    if spec is None:
        return ""
    if isinstance(spec, str):
        return dict_get_conform_value(row, spec)
    if isinstance(spec, list):
        return normalize_spaces(" ".join(openaddresses_conform_value(row, item) for item in spec)).strip()
    if not isinstance(spec, dict):
        return clean_real_value(spec)

    function = str(spec.get("function", "")).lower()
    field_value = dict_get_conform_value(row, spec.get("field"))
    if function == "prefixed_number":
        house_number, _street = split_house_number_from_street(field_value)
        return house_number
    if function == "postfixed_street":
        _house_number, street = split_house_number_from_street(field_value)
        return clean_street_name(street)
    if function == "regexp":
        return openaddresses_regexp_replace(field_value, str(spec.get("pattern", "")), str(spec.get("replace", "$1")))
    return ""


def openaddresses_direct_normalized_row(
    attributes: Dict[str, object],
    conform: Dict[str, object],
    source_name: str,
    source_id: object,
    coverage: Dict[str, object],
) -> Optional[Dict[str, str]]:
    house_number = clean_house_number(openaddresses_conform_value(attributes, conform.get("number")))
    street = clean_real_value(openaddresses_conform_value(attributes, conform.get("street")))
    if not house_number or not street:
        for field in OPENADDRESSES_DIRECT_FULL_ADDRESS_FALLBACK_FIELDS:
            full_address = dict_get_conform_value(attributes, field)
            if not full_address:
                continue
            fallback_number, fallback_street = split_house_number_from_street(full_address)
            house_number = house_number or fallback_number
            street = street or fallback_street
            if house_number and street:
                break

    house_number = clean_house_number(house_number)
    street = clean_street_name(street)
    if not house_number or house_number == "0" or not street:
        return None

    city = clean_city_candidate(openaddresses_conform_value(attributes, conform.get("city")))
    if not city and isinstance(coverage, dict) and coverage.get("city"):
        city = clean_city_candidate(coverage.get("city"))
    region = canonical_state(openaddresses_conform_value(attributes, conform.get("region")))
    if not region and isinstance(coverage, dict):
        region = canonical_state(coverage.get("state"))
    region = region or "MS"
    postcode = clean_zip_code(openaddresses_conform_value(attributes, conform.get("postcode")))
    if not city and not postcode:
        return None
    if not zip_code_matches_state(postcode, region):
        return None

    unit = clean_real_value(openaddresses_conform_value(attributes, conform.get("unit"))).upper()[:24]
    return {
        "NUMBER": house_number,
        "STREET": street,
        "UNIT": unit,
        "CITY": city,
        "REGION": region,
        "POSTCODE": postcode,
        "SOURCE": source_name,
        "SOURCE_ID": clean_real_value(source_id),
    }


def arcgis_object_id_field(layer_metadata: Dict[str, object]) -> str:
    object_id_field = str(layer_metadata.get("objectIdField", "") or layer_metadata.get("objectIdFieldName", ""))
    if object_id_field:
        return object_id_field
    for field in layer_metadata.get("fields", []) or []:
        if isinstance(field, dict) and str(field.get("type", "")).lower() == "esrifieldtypeoid":
            return str(field.get("name", "OBJECTID"))
    return "OBJECTID"


def read_arcgis_features_for_object_ids(query_url: str, object_ids: Sequence[int], timeout: int = 45) -> Tuple[List[Dict[str, object]], int, bool]:
    if not object_ids:
        return [], 0, False
    try:
        feature_payload = read_json_url(
            query_url,
            {
                "objectIds": ",".join(str(value) for value in object_ids),
                "outFields": "*",
                "returnGeometry": "false",
                "f": "json",
            },
            timeout=timeout,
        )
        features = [
            feature
            for feature in feature_payload.get("features", []) or []
            if isinstance(feature, dict)
        ]
        return features, 0, False
    except Exception:
        return [], len(object_ids), True


def openaddresses_conform_has_situs_locality(conform: Dict[str, object], coverage: Dict[str, object]) -> bool:
    return bool(conform.get("city") or conform.get("postcode") or (isinstance(coverage, dict) and coverage.get("city")))


def download_openaddresses_ms_direct(
    cache_dir: Path,
    config_dir: Path,
    batch_size: int = 250,
    refresh: bool = False,
    refresh_configs: bool = False,
    include_statewide: bool = False,
) -> List[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    config_paths = download_openaddresses_ms_source_configs(config_dir, refresh=refresh_configs)
    downloaded: List[Path] = []
    manifest_sources: List[Dict[str, object]] = []
    cached_count = 0
    downloaded_count = 0

    for config_path in config_paths:
        source_name = openaddresses_source_name(config_path)
        if source_name in OPENADDRESSES_DIRECT_SKIP_SOURCE_NAMES and not include_statewide:
            manifest_sources.append({"source": source_name, "status": "skipped_statewide_duplicate"})
            continue

        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            manifest_sources.append({"source": source_name, "status": "failed", "error": str(exc)})
            continue

        coverage = config.get("coverage", {}) if isinstance(config.get("coverage"), dict) else {}
        address_layers = [
            layer
            for layer in (config.get("layers", {}).get("addresses", []) if isinstance(config.get("layers"), dict) else [])
            if isinstance(layer, dict)
        ]
        for layer_index, layer in enumerate(address_layers):
            target = openaddresses_direct_target_path(cache_dir, source_name, layer_index, len(address_layers))
            protocol = str(layer.get("protocol", "")).upper()
            if protocol != "ESRI":
                manifest_sources.append(
                    {
                        "source": source_name,
                        "layer": layer.get("name", ""),
                        "status": "skipped_protocol",
                        "protocol": layer.get("protocol", ""),
                    }
                )
                continue
            conform = layer.get("conform", {}) if isinstance(layer.get("conform"), dict) else {}
            if not openaddresses_conform_has_situs_locality(conform, coverage):
                manifest_sources.append(
                    {
                        "source": source_name,
                        "layer": layer.get("name", ""),
                        "status": "skipped_no_situs_locality",
                    }
                )
                continue
            if not refresh and target.exists() and target.stat().st_size > 0:
                downloaded.append(target)
                cached_count += 1
                manifest_sources.append(
                    {
                        "source": source_name,
                        "layer": layer.get("name", ""),
                        "status": "cached",
                        "output": str(target),
                    }
                )
                continue

            layer_url = str(layer.get("data", "")).rstrip("/")
            if not layer_url or not conform:
                manifest_sources.append({"source": source_name, "layer": layer.get("name", ""), "status": "skipped_missing_layer"})
                continue

            temporary_target = target.with_suffix(target.suffix + ".part")
            try:
                metadata = read_json_url(layer_url, {"f": "json"}, timeout=90)
                object_id_field = arcgis_object_id_field(metadata)
                query_url = f"{layer_url}/query"
                id_payload = read_json_url(
                    query_url,
                    {"where": "1=1", "returnIdsOnly": "true", "f": "json"},
                    timeout=120,
                )
                object_ids = sorted(int(value) for value in id_payload.get("objectIds", []) or [])
                rows_written = 0
                object_ids_skipped = 0
                consecutive_failed_batches = 0
                with temporary_target.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=list(OPENADDRESSES_DIRECT_FIELDNAMES))
                    writer.writeheader()
                    for start in range(0, len(object_ids), batch_size):
                        batch = object_ids[start:start + batch_size]
                        features, skipped, failed = read_arcgis_features_for_object_ids(query_url, batch)
                        object_ids_skipped += skipped
                        if failed:
                            consecutive_failed_batches += 1
                            if consecutive_failed_batches >= 3:
                                raise RuntimeError(
                                    f"ArcGIS feature batch failed {consecutive_failed_batches} consecutive times; "
                                    f"skipped {object_ids_skipped} object id(s)."
                                )
                            continue
                        consecutive_failed_batches = 0
                        for feature in features:
                            attributes = feature.get("attributes", {}) if isinstance(feature, dict) else {}
                            if not isinstance(attributes, dict):
                                continue
                            source_id = attributes.get(object_id_field, "")
                            normalized = openaddresses_direct_normalized_row(
                                attributes,
                                conform,
                                source_name,
                                source_id,
                                coverage,
                            )
                            if normalized is None:
                                continue
                            writer.writerow(normalized)
                            rows_written += 1
                temporary_target.replace(target)
                downloaded.append(target)
                downloaded_count += 1
                manifest_sources.append(
                    {
                        "source": source_name,
                        "layer": layer.get("name", ""),
                        "status": "downloaded",
                        "url": layer_url,
                        "object_id_count": len(object_ids),
                        "object_ids_skipped": object_ids_skipped,
                        "rows_written": rows_written,
                        "output": str(target),
                    }
                )
                print(f"Cached OpenAddresses direct source {source_name}: {rows_written:,} usable row(s).")
            except Exception as exc:
                if temporary_target.exists():
                    temporary_target.unlink()
                if target.exists() and target.stat().st_size > 0:
                    downloaded.append(target)
                    manifest_sources.append(
                        {
                            "source": source_name,
                            "layer": layer.get("name", ""),
                            "status": "failed_reused_cache",
                            "error": str(exc),
                            "output": str(target),
                        }
                    )
                else:
                    manifest_sources.append(
                        {
                            "source": source_name,
                            "layer": layer.get("name", ""),
                            "status": "failed",
                            "error": str(exc),
                        }
                    )

    manifest = {
        "source_catalog_url": OPENADDRESSES_MS_SOURCES_API_URL,
        "config_dir": str(config_dir),
        "cache_dir": str(cache_dir),
        "include_statewide": include_statewide,
        "sources": manifest_sources,
    }
    openaddresses_direct_manifest_path(cache_dir).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if cached_count:
        print(f"Reused {cached_count} cached OpenAddresses direct CSV(s) from {cache_dir}.")
    if downloaded_count:
        print(f"Downloaded {downloaded_count} OpenAddresses direct CSV(s) into {cache_dir}.")
    return downloaded


def download_maris_point_addresses(cache_dir: Path) -> List[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    with open_url(MARIS_POINT_ADDRESSES_URL, timeout=60) as response:
        html = response.read().decode("utf-8", errors="replace")
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    urls = sorted(set(MARIS_POINT_ADDRESS_ZIP_RE.findall(html)))
    if not urls:
        raise RuntimeError("No MARIS Mississippi point-address ZIP links were found.")

    downloaded: List[Path] = []
    for url in urls:
        target = cache_dir / url.rsplit("/", 1)[-1]
        downloaded.append(download_file(url, target, timeout=300))
    return downloaded


def maris_parcel_layer_filename(layer_name: str) -> str:
    upper_name = layer_name.upper()
    if "TALLA_WALTH" in upper_name or "TALLAHATCHIE" in upper_name and "WALTHALL" in upper_name:
        return "Tallahatchie_Walthall_maris_parcels.csv"
    county_name = re.sub(r"_?PARCELS.*$", "", layer_name, flags=re.IGNORECASE)
    county_name = county_name.replace("PEARLRIVER", "PearlRiver")
    county_name = re.sub(r"[^A-Za-z0-9]+", "_", county_name).strip("_")
    if not county_name:
        county_name = re.sub(r"[^A-Za-z0-9]+", "_", layer_name).strip("_") or "maris_parcels"
    return f"{county_name}_maris_parcels.csv"


def read_json_url(url: str, params: Optional[Dict[str, object]] = None, timeout: int = 120) -> Dict[str, object]:
    query_url = url
    if params:
        query_url = f"{url}?{urllib.parse.urlencode(params)}"
    with open_url(query_url, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    if isinstance(payload, dict) and "error" in payload:
        raise RuntimeError(f"ArcGIS request failed for {url}: {payload['error']}")
    return payload


def maris_parcel_layers(layer_limit: Optional[int] = None) -> List[Dict[str, object]]:
    service = read_json_url(MARIS_PARCELS_SERVICE_URL, {"f": "json"}, timeout=60)
    layers = [
        layer
        for layer in service.get("layers", [])
        if isinstance(layer, dict)
        and str(layer.get("type", "")).lower() == "feature layer"
        and "PARCEL" in str(layer.get("name", "")).upper()
        and int(layer.get("id", -1)) not in {0, 82, 83}
    ]
    layers.sort(key=lambda layer: int(layer.get("id", 0)))
    return layers[:layer_limit] if layer_limit else layers


def maris_parcel_manifest_path(cache_dir: Path) -> Path:
    return cache_dir / MARIS_PARCEL_MANIFEST_FILENAME


def maris_parcel_manifest_entries(layers: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
    return [
        {
            "id": int(layer["id"]),
            "name": str(layer["name"]),
            "filename": maris_parcel_layer_filename(str(layer["name"])),
        }
        for layer in layers
    ]


def read_maris_parcel_manifest(cache_dir: Path) -> Optional[List[Dict[str, object]]]:
    manifest_path = maris_parcel_manifest_path(cache_dir)
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if manifest.get("service_url") != MARIS_PARCELS_SERVICE_URL:
        return None
    layers = manifest.get("layers")
    if not isinstance(layers, list):
        return None
    entries = []
    for layer in layers:
        if not isinstance(layer, dict) or not layer.get("filename"):
            return None
        entries.append(layer)
    return entries


def write_maris_parcel_manifest(cache_dir: Path, layers: Sequence[Dict[str, object]]) -> None:
    entries = maris_parcel_manifest_entries(layers)
    manifest = {
        "service_url": MARIS_PARCELS_SERVICE_URL,
        "expected_layer_count": MARIS_PARCEL_EXPECTED_LAYER_COUNT,
        "layers": entries,
    }
    maris_parcel_manifest_path(cache_dir).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def write_maris_parcel_manifest_from_paths(cache_dir: Path, paths: Sequence[Path]) -> None:
    manifest = {
        "service_url": MARIS_PARCELS_SERVICE_URL,
        "expected_layer_count": MARIS_PARCEL_EXPECTED_LAYER_COUNT,
        "layers": [{"filename": path.name} for path in paths],
    }
    maris_parcel_manifest_path(cache_dir).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def cached_maris_parcel_paths(cache_dir: Path, layer_limit: Optional[int] = None) -> Optional[List[Path]]:
    entries = read_maris_parcel_manifest(cache_dir)
    if entries:
        selected_entries = entries[:layer_limit] if layer_limit else entries
        paths = [cache_dir / str(entry["filename"]) for entry in selected_entries]
        if paths and all(path.exists() and path.stat().st_size > 0 for path in paths):
            return paths

    if layer_limit:
        return None

    csv_paths = sorted(
        path
        for path in cache_dir.glob("*.csv")
        if path.name != MARIS_PARCEL_MANIFEST_FILENAME and path.stat().st_size > 0
    )
    if len(csv_paths) >= MARIS_PARCEL_EXPECTED_LAYER_COUNT:
        covered_counties = mississippi_counties_in_paths(csv_paths)
        if len(covered_counties) == len(MISSISSIPPI_COUNTIES):
            return csv_paths
    return None


def download_maris_parcels(
    cache_dir: Path,
    layer_limit: Optional[int] = None,
    batch_size: int = 500,
    refresh: bool = False,
) -> List[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    if not refresh:
        cached_paths = cached_maris_parcel_paths(cache_dir, layer_limit=layer_limit)
        if cached_paths:
            if not maris_parcel_manifest_path(cache_dir).exists() and not layer_limit:
                write_maris_parcel_manifest_from_paths(cache_dir, cached_paths)
            print(f"Using cached MARIS parcel CSVs from {cache_dir} ({len(cached_paths)} file(s)).")
            return cached_paths

    layers = maris_parcel_layers(layer_limit=layer_limit)
    downloaded: List[Path] = []
    cached_count = 0
    downloaded_count = 0
    for layer in layers:
        layer_id = int(layer["id"])
        layer_name = str(layer["name"])
        target = cache_dir / maris_parcel_layer_filename(layer_name)
        if not refresh and target.exists() and target.stat().st_size > 0:
            downloaded.append(target)
            cached_count += 1
            continue

        query_url = f"{MARIS_PARCELS_SERVICE_URL}/{layer_id}/query"
        id_payload = read_json_url(
            query_url,
            {
                "where": "SITEADD IS NOT NULL AND SITEADD <> ''",
                "returnIdsOnly": "true",
                "f": "json",
            },
            timeout=120,
        )
        object_ids = sorted(int(value) for value in id_payload.get("objectIds", []) or [])
        temporary_target = target.with_suffix(target.suffix + ".part")
        with temporary_target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(MARIS_PARCEL_OUT_FIELDS))
            writer.writeheader()
            for start in range(0, len(object_ids), batch_size):
                batch = object_ids[start:start + batch_size]
                feature_payload = read_json_url(
                    query_url,
                    {
                        "objectIds": ",".join(str(value) for value in batch),
                        "outFields": ",".join(MARIS_PARCEL_OUT_FIELDS),
                        "returnGeometry": "false",
                        "f": "json",
                    },
                    timeout=120,
                )
                for feature in feature_payload.get("features", []) or []:
                    attributes = feature.get("attributes", {}) if isinstance(feature, dict) else {}
                    writer.writerow({field: attributes.get(field, "") for field in MARIS_PARCEL_OUT_FIELDS})
        temporary_target.replace(target)
        downloaded.append(target)
        downloaded_count += 1
    write_maris_parcel_manifest(cache_dir, layers)
    if cached_count:
        print(f"Reused {cached_count} cached MARIS parcel CSV(s) from {cache_dir}.")
    if downloaded_count:
        print(f"Downloaded {downloaded_count} MARIS parcel CSV(s) into {cache_dir}.")
    return downloaded


def open_url(url: str, timeout: int = 120):
    request = urllib.request.Request(url, headers={"User-Agent": DOWNLOAD_USER_AGENT})
    return urllib.request.urlopen(request, timeout=timeout)


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


# ---------------------------------------------------------------------------
# Address rendering
# ---------------------------------------------------------------------------

def render_component_direction(code: str, style: RenderStyle) -> str:
    if not code:
        return ""
    return DIRECTION_TO_FULL[code] if style.use_full_directional else code


def render_component_street_type(code: str, style: RenderStyle) -> str:
    if not code:
        return ""
    return STREET_TYPE_TO_FULL.get(code, code) if style.use_full_street_type else code


def render_component_unit_type(code: str, style: RenderStyle) -> str:
    if not code:
        return ""
    return UNIT_TYPE_TO_FULL.get(code, code) if style.use_full_unit_type else code


def render_address(record: AddressRecord, style: Optional[RenderStyle] = None) -> str:
    style = style or RenderStyle()
    pieces: List[str] = []

    predir = render_component_direction(record.predir, style)
    street_type = render_component_street_type(record.street_type, style)
    suffixdir = render_component_direction(record.suffixdir, style)
    street_bits: List[str] = []
    if not style.house_after_street_name:
        street_bits.append(record.house_number)
    if predir:
        street_bits.append(predir)
    street_bits.append(record.street_name)
    if style.house_after_street_name:
        street_bits.append(record.house_number)
    if street_type:
        street_bits.append(street_type)
    if suffixdir:
        street_bits.append(suffixdir)
    street_part = " ".join(bit for bit in street_bits if bit)

    unit_part = ""
    if style.include_unit and record.unit_type and record.unit_value:
        if style.slash_unit and record.unit_type == "APT":
            unit_part = f"#{record.unit_value}"
        else:
            unit_part = f"{render_component_unit_type(record.unit_type, style)} {record.unit_value}"

    locality_bits = [record.city, record.state]
    if style.include_zip:
        locality_bits.append(record.zip_code)
    locality_part = " ".join(bit for bit in locality_bits if bit)

    if style.unit_first and unit_part:
        pieces.extend([unit_part, street_part])
    else:
        pieces.append(street_part)
        if unit_part:
            pieces.append(unit_part)

    if style.include_commas:
        address = f"{', '.join(pieces)}, {locality_part}"
    else:
        address = f"{' '.join(pieces)} {locality_part}"

    if style.extra_spaces:
        address = address.replace(",", " , ")
        address = "  ".join(address.split(" "))

    address = normalize_spaces(address)

    if style.uppercase:
        address = address.upper()
    elif style.lowercase:
        address = address.lower()
    else:
        address = titleish(address)

    return normalize_spaces(address)


def canonical_address(record: AddressRecord) -> str:
    return render_address(
        record,
        RenderStyle(
            uppercase=True,
            use_full_directional=False,
            use_full_street_type=False,
            use_full_unit_type=False,
            include_unit=True,
            include_zip=True,
            include_commas=True,
            unit_first=False,
            slash_unit=False,
        ),
    )


# ---------------------------------------------------------------------------
# Address uniqueness tracking
# ---------------------------------------------------------------------------

class AddressFactory:
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self._seen_canonical: set[str] = set()


# ---------------------------------------------------------------------------
# Corruption engine
# ---------------------------------------------------------------------------

Operation = Callable[[AddressRecord, RenderStyle, random.Random], Optional[str]]


def op_expand_street_type(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.use_full_street_type = True
    return "expand_street_type"


def op_expand_directionals(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.predir and not record.suffixdir:
        return None
    style.use_full_directional = True
    return "expand_directional"


def op_expand_unit_type(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.unit_type:
        return None
    style.use_full_unit_type = True
    return "expand_unit_type"


def op_unit_hash(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if record.unit_type != "APT" or not record.unit_value:
        return None
    style.slash_unit = True
    return "hash_unit"


def op_unit_first(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.unit_type:
        return None
    style.unit_first = True
    return "unit_first"


def op_remove_zip(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.include_zip = False
    return "drop_zip"


def op_remove_unit(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.unit_type:
        return None
    style.include_unit = False
    return "drop_unit"


def op_remove_commas(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.include_commas = False
    return "remove_commas"


def op_lowercase(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.lowercase = True
    style.uppercase = False
    return "lowercase"


def op_extra_spaces(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.extra_spaces = True
    return "extra_spaces"


def op_typo_street(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tokens = record.street_name.split()
    candidates = [i for i, tok in enumerate(tokens) if len(tok) >= 4]
    if not candidates:
        return None
    idx = rng.choice(candidates)
    tokens[idx] = mutate_alpha_token(tokens[idx], rng)
    record.street_name = " ".join(tokens)
    return "street_typo"


def op_phonetic_street(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tokens = record.street_name.split()
    candidates = [i for i, tok in enumerate(tokens) if len(tok) >= 4]
    if not candidates:
        return None
    idx = rng.choice(candidates)
    mutated = mutate_phonetic_token(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.street_name = " ".join(tokens)
    return "street_phonetic"


def op_typo_city(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.city.replace(" ", "")) < 4:
        return None
    tokens = record.city.split()
    idx = rng.randrange(len(tokens))
    if len(tokens[idx]) < 4:
        return None
    tokens[idx] = mutate_locality_token(tokens[idx], rng)
    record.city = " ".join(tokens)
    return "city_typo"


def op_heavy_city_typo_no_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.city.replace(" ", "")) < 5:
        return None
    original = record.city
    for _ in range(rng.choice([2, 2, 3])):
        op_typo_city(record, style, rng)
    if record.city == original:
        return None
    record.state = ""
    if rng.random() < 0.65:
        record.zip_code = ""
    style.include_commas = False
    return "heavy_city_typo_no_state"


def op_heavy_city_typo_with_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.city.replace(" ", "")) < 5 or not record.state:
        return None
    original = record.city
    for _ in range(rng.choice([2, 2, 3])):
        op_typo_city(record, style, rng)
    if record.city == original:
        return None
    if rng.random() < 0.70:
        record.zip_code = ""
    style.include_commas = False
    return "heavy_city_typo_with_state"


def op_house_after_street_name(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.house_number or not record.street_name:
        return None
    style.house_after_street_name = True
    style.include_commas = False
    return "house_after_street_name"


def op_phonetic_city(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.city.replace(" ", "")) < 4:
        return None
    tokens = record.city.split()
    idx = rng.randrange(len(tokens))
    if len(tokens[idx]) < 4:
        return None
    mutated = mutate_phonetic_token(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.city = " ".join(tokens)
    return "city_phonetic"


def op_wrong_city(
    record: AddressRecord,
    style: RenderStyle,
    rng: random.Random,
    state_to_cities: Optional[Dict[str, Sequence[str]]] = None,
) -> Optional[str]:
    city_index = state_to_cities or {}
    alternatives = [city for city in city_index.get(record.state, ()) if city != record.city]
    if not alternatives:
        return None
    record.city = rng.choice(alternatives)
    return "wrong_city_same_state" if len(city_index.get(record.state, ())) > 1 else "wrong_city"


def op_typo_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    state_name = STATE_ABBREV_TO_NAME.get(record.state, record.state)
    if len(state_name.replace(" ", "")) < 4:
        return None
    tokens = state_name.split()
    idx = rng.randrange(len(tokens))
    if len(tokens[idx]) < 4:
        return None
    tokens[idx] = mutate_locality_token(tokens[idx], rng)
    record.state = " ".join(tokens)
    return "state_typo"


def op_common_state_typo(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if record.state != "MS":
        return op_typo_state(record, style, rng)
    record.state = rng.choice(
        [
            "Missppi",
            "Missippi",
            "Mississipi",
            "Mississppi",
            "Misissippi",
            "Misisippi",
        ]
    )
    return "common_state_typo"


def op_phonetic_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    state_name = STATE_ABBREV_TO_NAME.get(record.state, record.state)
    if len(state_name.replace(" ", "")) < 4:
        return None
    tokens = state_name.split()
    idx = rng.randrange(len(tokens))
    if len(tokens[idx]) < 4:
        return None
    mutated = mutate_phonetic_token(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.state = " ".join(tokens)
    return "state_phonetic"


def op_drop_city(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.city:
        return None
    record.city = ""
    return "drop_city"


def op_drop_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.state:
        return None
    record.state = ""
    return "drop_state"


def op_zip_truncate(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.zip_code) != 5:
        return None
    record.zip_code = record.zip_code[:4]
    return "zip_truncate"


def op_street_type_confusion(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    alternatives = STREET_TYPE_CONFUSIONS.get(record.street_type, ())
    if not alternatives:
        return None
    record.street_type = rng.choice(list(alternatives))
    return "street_type_confusion"


def op_typo_street_type(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.street_type:
        return None
    mutated = mutate_street_type_token(record.street_type, rng)
    if mutated == record.street_type:
        return None
    record.street_type = mutated
    return "street_type_typo"


def op_extra_or_missing_street_letter(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tokens = record.street_name.split()
    candidates = [idx for idx, token in enumerate(tokens) if len(token) >= 5]
    if not candidates:
        return None
    idx = rng.choice(candidates)
    mutated = mutate_extra_or_missing_letter(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.street_name = " ".join(tokens)
    return "street_letter_extra_missing"


def op_compound_local_typo(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tags: List[str] = []
    for op in (op_typo_street, op_typo_street_type, op_typo_city):
        tag = op(record, style, rng)
        if tag:
            tags.append(tag)

    if len(tags) < 2:
        return None

    if rng.random() < 0.65:
        style.include_zip = False
    if rng.random() < 0.50:
        style.include_commas = False
    if rng.random() < 0.35:
        style.lowercase = True
        style.uppercase = False

    return "|".join(tags)


def op_reordered_locality_typo(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tags: List[str] = []
    for op in (op_house_after_street_name, op_typo_street_type, op_heavy_city_typo_with_state, op_common_state_typo):
        tag = op(record, style, rng)
        if tag:
            tags.append(tag)
    if rng.random() < 0.45:
        tag = op_house_drop_digit(record, style, rng)
        if tag:
            tags.append(tag)

    if len(tags) < 3:
        return None

    if rng.random() < 0.55:
        style.lowercase = True
        style.uppercase = False
    style.include_zip = False
    style.include_commas = False
    return "|".join(tags)


def op_ocr_street(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tokens = record.street_name.split()
    candidates = [idx for idx, token in enumerate(tokens) if len(token) >= 4]
    if not candidates:
        return None
    idx = rng.choice(candidates)
    mutated = mutate_ocr_token(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.street_name = " ".join(tokens)
    return "street_ocr"


def op_ocr_locality(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    choices: List[Tuple[str, str]] = []
    if record.city:
        choices.append(("city", record.city))
    if record.state:
        choices.append(("state", STATE_ABBREV_TO_NAME.get(record.state, record.state)))
    if not choices:
        return None
    field, value = rng.choice(choices)
    mutated = mutate_ocr_token(value, rng)
    if mutated == value:
        return None
    if field == "city":
        record.city = mutated
        return "city_ocr"
    record.state = mutated
    return "state_ocr"


def op_merge_tokens(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    candidates = []
    if " " in record.street_name:
        candidates.append("street")
    if " " in record.city:
        candidates.append("city")
    if not candidates:
        return None
    field = rng.choice(candidates)
    if field == "street":
        record.street_name = record.street_name.replace(" ", "")
        return "merge_street_tokens"
    record.city = record.city.replace(" ", "")
    return "merge_city_tokens"


def op_house_digit_error(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    record.house_number = mutate_numeric_token(record.house_number, rng)
    return "house_digit_error"


def op_house_drop_digit(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    digit_positions = [idx for idx, char in enumerate(record.house_number) if char.isdigit()]
    if len(digit_positions) < 2:
        return None
    idx = rng.choice(digit_positions)
    record.house_number = record.house_number[:idx] + record.house_number[idx + 1:]
    return "house_drop_digit"


def op_house_near_miss(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    shifted = shift_house_number(record.house_number, rng)
    if shifted == record.house_number:
        return None
    record.house_number = shifted
    return "house_near_miss"


def op_zip_digit_error(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    record.zip_code = mutate_numeric_token(record.zip_code, rng)
    return "zip_digit_error"


def op_drop_directional(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.predir and not record.suffixdir:
        return None
    if record.predir and record.suffixdir:
        if rng.random() < 0.5:
            record.predir = ""
        else:
            record.suffixdir = ""
    elif record.predir:
        record.predir = ""
    else:
        record.suffixdir = ""
    return "drop_directional"


def op_strip_leading_zero_unit(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.unit_value or not record.unit_value[0].isdigit():
        return None
    record.unit_value = record.unit_value.lstrip("0") or record.unit_value
    return "strip_unit_zero"


EASY_OPS: Sequence[Operation] = (
    op_expand_street_type,
    op_expand_directionals,
    op_expand_unit_type,
    op_unit_hash,
    op_unit_first,
    op_remove_zip,
    op_remove_unit,
    op_remove_commas,
    op_lowercase,
    op_extra_spaces,
    op_merge_tokens,
)

MEDIUM_OPS: Sequence[Operation] = EASY_OPS + (
    op_typo_street,
    op_phonetic_street,
    op_extra_or_missing_street_letter,
    op_house_after_street_name,
    op_typo_city,
    op_heavy_city_typo_no_state,
    op_heavy_city_typo_with_state,
    op_phonetic_city,
    op_ocr_street,
    op_ocr_locality,
    op_wrong_city,
    op_typo_state,
    op_common_state_typo,
    op_phonetic_state,
    op_drop_city,
    op_drop_state,
    op_zip_truncate,
    op_street_type_confusion,
    op_typo_street_type,
    op_drop_directional,
    op_strip_leading_zero_unit,
)

HARD_OPS: Sequence[Operation] = MEDIUM_OPS + (
    op_compound_local_typo,
    op_reordered_locality_typo,
    op_house_near_miss,
    op_house_drop_digit,
    op_house_digit_error,
    op_zip_digit_error,
)

LOCALITY_OPS: Sequence[Operation] = (
    op_typo_city,
    op_phonetic_city,
    op_heavy_city_typo_with_state,
    op_ocr_locality,
    op_wrong_city,
    op_typo_state,
    op_common_state_typo,
    op_phonetic_state,
    op_drop_city,
    op_drop_state,
)


class Corruptor:
    def __init__(self, rng: random.Random, state_to_cities: Optional[Dict[str, Sequence[str]]] = None) -> None:
        self.rng = rng
        self.state_to_cities = state_to_cities or {}

    def apply_operation(self, op: Operation, record: AddressRecord, style: RenderStyle) -> Optional[str]:
        if op is op_wrong_city:
            return op_wrong_city(record, style, self.rng, self.state_to_cities)
        return op(record, style, self.rng)

    def choose_difficulty(self) -> str:
        return choose_weighted(self.rng, [("easy", 0.35), ("medium", 0.45), ("hard", 0.20)])

    def difficulty_operation_count(self, difficulty: str) -> int:
        if difficulty == "easy":
            return self.rng.randint(1, 2)
        if difficulty == "medium":
            return self.rng.randint(2, 4)
        return self.rng.randint(3, 6)

    def ops_for_difficulty(self, difficulty: str) -> Sequence[Operation]:
        if difficulty == "easy":
            return EASY_OPS
        if difficulty == "medium":
            return MEDIUM_OPS
        return HARD_OPS

    def corrupt(self, base_record: AddressRecord, force_difficulty: Optional[str] = None) -> Tuple[str, str, str]:
        canonical = canonical_address(base_record)
        difficulty = force_difficulty or self.choose_difficulty()

        for _ in range(20):
            record = deepcopy(base_record)
            style = RenderStyle()
            tags: List[str] = []
            pool = list(self.ops_for_difficulty(difficulty))
            self.rng.shuffle(pool)
            num_ops = min(self.difficulty_operation_count(difficulty), len(pool))

            profile_probability = 0.72 if difficulty == "hard" else 0.38 if difficulty == "medium" else 0.0
            if profile_probability and self.rng.random() < profile_probability:
                profile_ops = [
                    op_heavy_city_typo_no_state,
                    op_heavy_city_typo_with_state,
                    op_reordered_locality_typo,
                    op_phonetic_street,
                    op_typo_street_type,
                    op_extra_or_missing_street_letter,
                    op_house_near_miss,
                ]
                self.rng.shuffle(profile_ops)
                target_count = 2 if difficulty == "hard" and self.rng.random() < 0.55 else 1
                for op in profile_ops:
                    tag = self.apply_operation(op, record, style)
                    if tag:
                        tags.append(tag)
                    if len(tags) >= target_count:
                        break

            compound_probability = 0.45 if difficulty == "hard" else 0.18 if difficulty == "medium" else 0.0
            if compound_probability and self.rng.random() < compound_probability:
                tag = self.apply_operation(op_compound_local_typo, record, style)
                if tag:
                    tags.append(tag)

            for op in pool[:num_ops]:
                tag = self.apply_operation(op, record, style)
                if tag:
                    tags.append(tag)

            # Ensure medium/hard examples regularly include locality corruption.
            tag_parts = {part for tag in tags for part in tag.split("|")}
            if difficulty in {"medium", "hard"} and not any(
                tag in {
                    "city_typo",
                    "city_phonetic",
                    "city_ocr",
                    "wrong_city",
                    "wrong_city_same_state",
                    "state_typo",
                    "state_phonetic",
                    "state_ocr",
                    "drop_city",
                    "drop_state",
                }
                for tag in tag_parts
            ):
                locality_pool = list(LOCALITY_OPS)
                self.rng.shuffle(locality_pool)
                for op in locality_pool:
                    tag = self.apply_operation(op, record, style)
                    if tag:
                        tags.append(tag)
                        break

            # Small chance of title-case even when no explicit casing op chosen.
            if not style.uppercase and not style.lowercase and self.rng.random() < 0.25:
                style.lowercase = False

            query = render_address(record, style)
            if query != canonical:
                return query, difficulty, "|".join(tags) if tags else "style_variation"

        # Guaranteed fallback difference.
        fallback = render_address(base_record, RenderStyle(lowercase=True, include_commas=False))
        return fallback, difficulty, "fallback_lowercase"


# ---------------------------------------------------------------------------
# Dataset builder
# ---------------------------------------------------------------------------

class DatasetBuilder:
    def __init__(self, seed: int, real_address_pool: Optional[Sequence[AddressRecord]] = None) -> None:
        self.rng = random.Random(seed)
        self.factory = AddressFactory(self.rng)
        self.real_address_pool = list(real_address_pool or [])
        self.corruptor = Corruptor(self.rng, state_to_cities=self.real_state_to_cities())
        self._real_pool_order = list(self.real_address_pool)
        self.rng.shuffle(self._real_pool_order)
        self.adversarial_reasons: Dict[str, str] = {}

    def real_state_to_cities(self) -> Dict[str, Sequence[str]]:
        cities_by_state: Dict[str, set[str]] = defaultdict(set)
        for record in self.real_address_pool:
            if record.state and record.city:
                cities_by_state[record.state].add(record.city)
        return {state: tuple(sorted(cities)) for state, cities in cities_by_state.items()}

    def build_reference(self, n: int) -> List[AddressRecord]:
        if self.real_address_pool:
            return self.take_real_records(n, "REF")
        raise RuntimeError("No real address pool was supplied for reference generation.")

    def build_negative_bases(self, n: int) -> List[AddressRecord]:
        if self.real_address_pool:
            return self.take_real_records(n, "NEG")
        raise RuntimeError("No real address pool was supplied for negative generation.")

    def adversarial_neighbor_index(self, reference: Sequence[AddressRecord]) -> Dict[str, Dict[Tuple[str, ...], List[AddressRecord]]]:
        indexes: Dict[str, Dict[Tuple[str, ...], List[AddressRecord]]] = {
            "same_house_city": defaultdict(list),
            "same_street_city": defaultdict(list),
            "same_house_zip": defaultdict(list),
            "same_street_zip": defaultdict(list),
        }
        for record in reference:
            street_key = (record.predir, record.street_name, record.street_type, record.suffixdir)
            if record.house_number and record.city and record.state:
                indexes["same_house_city"][(record.house_number, record.city, record.state)].append(record)
            if record.city and record.state and record.street_name:
                indexes["same_street_city"][(*street_key, record.city, record.state)].append(record)
            if record.house_number and record.zip_code:
                indexes["same_house_zip"][(record.house_number, record.zip_code)].append(record)
            if record.zip_code and record.street_name:
                indexes["same_street_zip"][(*street_key, record.zip_code)].append(record)
        return indexes

    def candidate_reference_neighbors(
        self,
        candidate: AddressRecord,
        indexes: Dict[str, Dict[Tuple[str, ...], List[AddressRecord]]],
    ) -> List[Tuple[str, AddressRecord]]:
        neighbors: List[Tuple[str, AddressRecord]] = []
        street_key = (candidate.predir, candidate.street_name, candidate.street_type, candidate.suffixdir)
        lookups = (
            ("same_house_city", (candidate.house_number, candidate.city, candidate.state)),
            ("same_street_city", (*street_key, candidate.city, candidate.state)),
            ("same_house_zip", (candidate.house_number, candidate.zip_code)),
            ("same_street_zip", (*street_key, candidate.zip_code)),
        )
        seen = set()
        for reason, key in lookups:
            if not all(key):
                continue
            for reference in indexes[reason].get(key, ()):
                if reference.address_id in seen:
                    continue
                seen.add(reference.address_id)
                neighbors.append((reason, reference))
        return neighbors

    def adversarial_neighbor_score(self, reference: AddressRecord, candidate: AddressRecord, reason: str) -> float:
        street_similarity = SequenceMatcher(None, reference.street_name.upper(), candidate.street_name.upper()).ratio()
        same_house = reference.house_number == candidate.house_number
        same_city = bool(reference.city and reference.city == candidate.city and reference.state == candidate.state)
        same_zip = bool(reference.zip_code and reference.zip_code == candidate.zip_code)
        same_street = (
            reference.predir == candidate.predir
            and reference.street_name == candidate.street_name
            and reference.street_type == candidate.street_type
            and reference.suffixdir == candidate.suffixdir
        )
        house_gap = 999
        if reference.house_number.isdigit() and candidate.house_number.isdigit():
            house_gap = abs(int(reference.house_number) - int(candidate.house_number))

        score = 0.0
        if same_house and (same_city or same_zip):
            score = max(score, 0.68 + 0.25 * street_similarity)
        if same_street and (same_city or same_zip) and 0 < house_gap <= 20:
            score = max(score, 0.76 + 0.20 * (1.0 - min(house_gap, 20) / 20.0))
        if reason in {"same_house_city", "same_house_zip"} and street_similarity >= 0.45:
            score = max(score, 0.55 + 0.35 * street_similarity)
        if reason in {"same_street_city", "same_street_zip"} and 0 < house_gap <= 30:
            score = max(score, 0.58 + 0.30 * (1.0 - min(house_gap, 30) / 30.0))
        return score

    def take_real_records(self, n: int, prefix: str) -> List[AddressRecord]:
        results: List[AddressRecord] = []
        for source in self._real_pool_order:
            if len(results) >= n:
                break
            candidate = deepcopy(source)
            candidate.address_id = f"{prefix}_{len(results) + 1:06d}"
            key = canonical_address(candidate)
            if key in self.factory._seen_canonical:
                continue
            self.factory._seen_canonical.add(key)
            results.append(candidate)

        if len(results) == n:
            return results

        raise RuntimeError(
            f"Real address pool only had {len(results)} unused unique records for {prefix}; "
            f"{n} are required. Real-only generation will not invent replacement addresses; "
            "provide more source data or reduce --reference-size/--no-match-ratio."
        )

    def build_adversarial_negative_bases(
        self,
        reference: Sequence[AddressRecord],
        n: int,
        extra_excluded: Sequence[AddressRecord] = (),
    ) -> List[AddressRecord]:
        if self.real_address_pool:
            indexes = self.adversarial_neighbor_index(reference)
            excluded = {canonical_address(record) for record in reference}
            excluded.update(canonical_address(record) for record in extra_excluded)
            scored_candidates: List[Tuple[float, float, str, AddressRecord]] = []
            seen_candidates = set()

            for source in self._real_pool_order:
                source_key = canonical_address(source)
                if source_key in self.factory._seen_canonical or source_key in excluded or source_key in seen_candidates:
                    continue
                best_score = 0.0
                best_reason = ""
                for reason, neighbor in self.candidate_reference_neighbors(source, indexes):
                    score = self.adversarial_neighbor_score(neighbor, source, reason)
                    if score > best_score:
                        best_score = score
                        best_reason = reason
                if best_score <= 0.0:
                    continue
                seen_candidates.add(source_key)
                scored_candidates.append((best_score, self.rng.random(), best_reason, source))

            scored_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
            results: List[AddressRecord] = []
            for _score, _tiebreaker, reason, source in scored_candidates:
                if len(results) >= n:
                    break
                candidate = deepcopy(source)
                candidate.address_id = f"NEG_ADV_{len(results) + 1:06d}"
                key = canonical_address(candidate)
                if key in self.factory._seen_canonical:
                    continue
                self.factory._seen_canonical.add(key)
                self.adversarial_reasons[candidate.address_id] = f"near_neighbor_{reason}"
                results.append(candidate)

            if len(results) < n:
                fallback = self.take_real_records(n - len(results), "NEG_ADV_FALLBACK")
                for record in fallback:
                    record.address_id = f"NEG_ADV_{len(results) + 1:06d}"
                    self.adversarial_reasons[record.address_id] = "fallback_holdout"
                    results.append(record)
            return results
        raise RuntimeError("No real address pool was supplied for adversarial negative generation.")

    def planned_difficulties(self, count: int, negative: bool = False, adversarial: bool = False) -> List[str]:
        if count <= 0:
            return []
        if adversarial:
            return ["hard"] * count

        planned: List[str] = []
        hard_floor = max(1, round(count * (0.35 if negative else 0.30))) if count >= 3 else 0
        medium_floor = max(1, round(count * 0.35)) if count >= 2 else 0
        hard_floor = min(hard_floor, count)
        medium_floor = min(medium_floor, max(0, count - hard_floor))

        planned.extend(["hard"] * hard_floor)
        planned.extend(["medium"] * medium_floor)
        remaining = count - len(planned)
        for _ in range(remaining):
            planned.append(choose_weighted(self.rng, [("easy", 0.25), ("medium", 0.40), ("hard", 0.35 if negative else 0.25)]))
        self.rng.shuffle(planned)
        return planned

    def build_reference_query_index(self, reference: Sequence[AddressRecord]) -> Dict[str, List[str]]:
        index: Dict[str, List[str]] = defaultdict(list)
        for record in reference:
            index[query_text_key(canonical_address(record))].append(record.address_id)
        return index

    def query_label_is_valid(
        self,
        query_address: str,
        label: int,
        true_match_id: str,
        reference_query_index: Dict[str, List[str]],
    ) -> bool:
        key = query_text_key(query_address)
        exact_matches = reference_query_index.get(key, [])
        if label == 1:
            if exact_matches and true_match_id not in exact_matches:
                return False
            if len(exact_matches) > 1:
                return False
            return True
        return not exact_matches

    def generate_labeled_query(
        self,
        record: AddressRecord,
        label: int,
        true_match_id: str,
        reference_query_index: Dict[str, List[str]],
        force_difficulty: Optional[str] = None,
    ) -> Tuple[str, str, str]:
        for _ in range(40):
            query_address, difficulty, tags = self.corruptor.corrupt(record, force_difficulty=force_difficulty)
            if self.query_label_is_valid(query_address, label, true_match_id, reference_query_index):
                return query_address, difficulty, tags
        raise RuntimeError(f"Could not generate a label-safe query for {record.address_id}")

    def build_queries(
        self,
        reference: Sequence[AddressRecord],
        negatives: Sequence[AddressRecord],
        adversarial_negatives: Sequence[AddressRecord],
        noisy_per_reference: int,
    ) -> List[QueryExample]:
        queries: List[QueryExample] = []
        q_counter = 1
        reference_query_index = self.build_reference_query_index(reference)

        for record in reference:
            split = stable_split_for_key(record.address_id)
            for difficulty_hint in self.planned_difficulties(noisy_per_reference):
                query_address, difficulty, tags = self.generate_labeled_query(
                    record,
                    label=1,
                    true_match_id=record.address_id,
                    reference_query_index=reference_query_index,
                    force_difficulty=difficulty_hint,
                )
                queries.append(
                    QueryExample(
                        query_id=f"Q_{q_counter:07d}",
                        split=split,
                        label=1,
                        true_match_id=record.address_id,
                        base_address_id=record.address_id,
                        source="reference_variant",
                        difficulty=difficulty,
                        noise_tags=tags,
                        query_address=query_address,
                        canonical_address=canonical_address(record),
                    )
                )
                q_counter += 1

        for record, difficulty_hint in zip(negatives, self.planned_difficulties(len(negatives), negative=True)):
            split = stable_split_for_key(record.address_id)
            query_address, difficulty, tags = self.generate_labeled_query(
                record,
                label=0,
                true_match_id="",
                reference_query_index=reference_query_index,
                force_difficulty=difficulty_hint,
            )
            queries.append(
                QueryExample(
                    query_id=f"Q_{q_counter:07d}",
                    split=split,
                    label=0,
                    true_match_id="",
                    base_address_id=record.address_id,
                    source="holdout_no_match",
                    difficulty=difficulty,
                    noise_tags=tags,
                    query_address=query_address,
                    canonical_address="NO_MATCH",
                )
            )
            q_counter += 1

        for record in adversarial_negatives:
            split = stable_split_for_key(record.address_id)
            query_address, difficulty, tags = self.generate_labeled_query(
                record,
                label=0,
                true_match_id="",
                reference_query_index=reference_query_index,
                force_difficulty="hard",
            )
            adversarial_reason = self.adversarial_reasons.get(record.address_id, "adversarial_holdout")
            combined_tags = "|".join(part for part in ["adversarial_base", adversarial_reason, tags] if part)
            queries.append(
                QueryExample(
                    query_id=f"Q_{q_counter:07d}",
                    split=split,
                    label=0,
                    true_match_id="",
                    base_address_id=record.address_id,
                    source="adversarial_no_match",
                    difficulty="hard",
                    noise_tags=combined_tags,
                    query_address=query_address,
                    canonical_address="NO_MATCH",
                )
            )
            q_counter += 1

        return queries


# ---------------------------------------------------------------------------
# Validation and export
# ---------------------------------------------------------------------------

def validate(
    reference: Sequence[AddressRecord],
    negatives: Sequence[AddressRecord],
    adversarial_negatives: Sequence[AddressRecord],
    queries: Sequence[QueryExample],
) -> None:
    ref_ids = {record.address_id for record in reference}
    neg_ids = {record.address_id for record in negatives}
    adv_neg_ids = {record.address_id for record in adversarial_negatives}
    if len(ref_ids) != len(reference):
        raise ValueError("Duplicate reference address IDs detected.")
    if ref_ids & neg_ids or ref_ids & adv_neg_ids or neg_ids & adv_neg_ids:
        raise ValueError("Reference and negative base IDs overlap.")

    ref_canonical = {canonical_address(record) for record in reference}
    neg_canonical = {canonical_address(record) for record in negatives}
    adv_neg_canonical = {canonical_address(record) for record in adversarial_negatives}
    if ref_canonical & neg_canonical or ref_canonical & adv_neg_canonical or neg_canonical & adv_neg_canonical:
        raise ValueError("A negative base address collides with a reference address.")

    reference_query_index: Dict[str, List[str]] = defaultdict(list)
    for record in reference:
        reference_query_index[query_text_key(canonical_address(record))].append(record.address_id)

    for query in queries:
        if query.label not in (0, 1):
            raise ValueError(f"Invalid label for query {query.query_id}: {query.label}")
        if query.label == 1 and query.true_match_id not in ref_ids:
            raise ValueError(f"Positive query {query.query_id} points to missing reference ID {query.true_match_id}")
        if query.label == 0 and query.true_match_id:
            raise ValueError(f"Negative query {query.query_id} should not have a true_match_id")
        if query.split not in {"train", "validation", "test"}:
            raise ValueError(f"Unexpected split on {query.query_id}: {query.split}")
        if not query.query_address:
            raise ValueError(f"Empty query address for {query.query_id}")
        exact_matches = reference_query_index.get(query_text_key(query.query_address), [])
        if query.label == 1:
            if exact_matches and query.true_match_id not in exact_matches:
                raise ValueError(f"Positive query {query.query_id} collides with a different reference canonical address.")
            if len(exact_matches) > 1:
                raise ValueError(f"Positive query {query.query_id} is exact-ambiguous against multiple reference addresses.")
        elif exact_matches:
            raise ValueError(f"Negative query {query.query_id} exactly matches a reference canonical address.")

    # All variants of the same base record should stay in the same split.
    split_by_base: Dict[str, str] = {}
    for query in queries:
        prior = split_by_base.get(query.base_address_id)
        if prior is not None and prior != query.split:
            raise ValueError(f"Base address {query.base_address_id} appears in multiple splits")
        split_by_base[query.base_address_id] = query.split

    split_counts = Counter(query.split for query in queries)
    if not split_counts:
        raise ValueError("No queries were generated.")


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def summarize(
    reference: Sequence[AddressRecord],
    negatives: Sequence[AddressRecord],
    adversarial_negatives: Sequence[AddressRecord],
    queries: Sequence[QueryExample],
) -> Dict[str, object]:
    by_split = defaultdict(lambda: {"positive": 0, "negative": 0})
    difficulty_counts = Counter(query.difficulty for query in queries)
    noise_counts = Counter(tag for query in queries for tag in query.noise_tags.split("|") if tag)
    source_counts = Counter(query.source for query in queries)
    source_difficulty_counts = defaultdict(Counter)

    for query in queries:
        key = "positive" if query.label == 1 else "negative"
        by_split[query.split][key] += 1
        source_difficulty_counts[query.source][query.difficulty] += 1

    return {
        "reference_records": len(reference),
        "negative_base_records": len(negatives) + len(adversarial_negatives),
        "holdout_negative_base_records": len(negatives),
        "adversarial_negative_base_records": len(adversarial_negatives),
        "query_records": len(queries),
        "splits": by_split,
        "difficulty_distribution": difficulty_counts,
        "query_sources": source_counts,
        "source_difficulty_distribution": source_difficulty_counts,
        "top_noise_tags": noise_counts.most_common(15),
    }


def export_dataset(
    output_dir: Path,
    reference: Sequence[AddressRecord],
    negatives: Sequence[AddressRecord],
    adversarial_negatives: Sequence[AddressRecord],
    queries: Sequence[QueryExample],
    config: Dict[str, object],
) -> Dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)

    reference_rows = []
    for record in reference:
        row = asdict(record)
        row["canonical_address"] = canonical_address(record)
        reference_rows.append(row)

    negative_rows = []
    for record in negatives:
        row = asdict(record)
        row["holdout_canonical_address"] = canonical_address(record)
        negative_rows.append(row)

    adversarial_negative_rows = []
    for record in adversarial_negatives:
        row = asdict(record)
        row["adversarial_canonical_address"] = canonical_address(record)
        adversarial_negative_rows.append(row)

    query_rows = [asdict(query) for query in queries]

    write_csv(
        output_dir / "reference_addresses.csv",
        list(reference_rows[0].keys()) if reference_rows else [],
        reference_rows,
    )
    write_csv(
        output_dir / "holdout_no_match_bases.csv",
        list(negative_rows[0].keys()) if negative_rows else [],
        negative_rows,
    )
    write_csv(
        output_dir / "adversarial_no_match_bases.csv",
        list(adversarial_negative_rows[0].keys()) if adversarial_negative_rows else [],
        adversarial_negative_rows,
    )
    write_csv(
        output_dir / "queries.csv",
        list(query_rows[0].keys()) if query_rows else [],
        query_rows,
    )
    write_csv(
        output_dir / "resolver_input.csv",
        ["query_id", "split", "query_address"],
        (
            {
                "query_id": query.query_id,
                "split": query.split,
                "query_address": query.query_address,
            }
            for query in queries
        ),
    )

    summary = summarize(reference, negatives, adversarial_negatives, queries)
    metadata = {
        "config": config,
        "summary": summary,
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=lambda x: dict(x)), encoding="utf-8")

    return summary


def preview_queries(queries: Sequence[QueryExample], limit: int = 8) -> str:
    lines = []
    for query in queries[:limit]:
        match = query.true_match_id if query.true_match_id else "NO_MATCH"
        lines.append(f"[{query.split}/{query.difficulty}/{query.label}] {query.query_address}  =>  {match}")
    return "\n".join(lines)


def generate_dataset(
    output_dir: Path,
    seed: int,
    args: argparse.Namespace,
    reference: Optional[Sequence[AddressRecord]] = None,
    real_address_pool: Optional[Sequence[AddressRecord]] = None,
    real_address_metadata: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    if not real_address_pool:
        raise RuntimeError(
            "No real address pool was supplied. The generator is real-address-only by default; "
            "provide --real-address-input or a downloader such as --download-maris-parcels."
        )

    builder = DatasetBuilder(
        seed=seed,
        real_address_pool=real_address_pool,
    )
    reference_records = list(reference) if reference is not None else builder.build_reference(args.reference_size)
    if reference is not None:
        builder.factory._seen_canonical.update(canonical_address(record) for record in reference_records)
    positive_queries = len(reference_records) * args.noisy_per_reference
    negative_base_count = max(1, round(positive_queries * args.no_match_ratio)) if args.no_match_ratio > 0 else 0
    adversarial_negative_count = round(negative_base_count * args.adversarial_no_match_share)
    holdout_negative_count = negative_base_count - adversarial_negative_count
    negatives = builder.build_negative_bases(holdout_negative_count) if holdout_negative_count else []
    adversarial_negatives = (
        builder.build_adversarial_negative_bases(reference_records, adversarial_negative_count, extra_excluded=negatives)
        if adversarial_negative_count
        else []
    )
    queries = builder.build_queries(reference_records, negatives, adversarial_negatives, args.noisy_per_reference)

    validate(reference_records, negatives, adversarial_negatives, queries)

    config = {
        "reference_size": args.reference_size,
        "noisy_per_reference": args.noisy_per_reference,
        "no_match_ratio": args.no_match_ratio,
        "adversarial_no_match_share": args.adversarial_no_match_share,
        "seed": seed,
        "address_source": "real",
        "adversarial_negative_base_strategy": "near_neighbor_real_holdout",
    }
    if real_address_metadata:
        config["real_address_source"] = real_address_metadata
    summary = export_dataset(output_dir, reference_records, negatives, adversarial_negatives, queries, config)
    return {"summary": summary, "queries": queries}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate address-resolution data from real address sources.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "address_dataset",
        help="Directory for CSV outputs.",
    )
    parser.add_argument(
        "--paired-output-dir",
        type=Path,
        help="If set, generate separate train_dataset/ and eval_dataset/ directories under this root using different seeds.",
    )
    parser.add_argument(
        "--paired-shared-reference",
        action="store_true",
        help="In paired mode, reuse the same reference database for train/eval and generate only separate query/negative sets.",
    )
    parser.add_argument("--reference-size", type=int, default=5000, help="Number of reference addresses.")
    parser.add_argument("--noisy-per-reference", type=int, default=12, help="Positive noisy variants per reference address.")
    parser.add_argument("--no-match-ratio", type=float, default=0.5, help="Negative query count as a fraction of positive query count.")
    parser.add_argument(
        "--adversarial-no-match-share",
        type=float,
        default=0.5,
        help="Fraction of negative queries generated as adversarial near-match no-matches instead of independent holdout addresses.",
    )
    parser.add_argument(
        "--real-address-input",
        type=Path,
        action="append",
        default=[],
        help="File or directory of real addresses. Supports MARIS/MS811 shapefile ZIP/DBF, MARIS parcel CSV, OpenAddresses CSV, USDOT NAD text, generic address CSVs, and this generator's reference CSV schema.",
    )
    parser.add_argument(
        "--real-address-format",
        choices=["auto", "maris", "maris_parcels", "nad", "openaddresses", "address_record", "generic"],
        default="auto",
        help="Input schema for --real-address-input. Use generic for a bring-your-own CSV with full_address/address plus city/state/zip columns.",
    )
    parser.add_argument(
        "--real-address-state",
        default="MS",
        help="Two-letter state filter for real address inputs. Defaults to MS.",
    )
    parser.add_argument(
        "--real-address-limit",
        type=int,
        help="Optional cap on loaded real addresses after filtering and de-duplication.",
    )
    parser.add_argument(
        "--download-openaddresses-ms",
        action="store_true",
        help="Download archived Mississippi OpenAddresses source ZIPs into --real-address-cache-dir and use them as the real address input.",
    )
    parser.add_argument(
        "--real-address-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "openaddresses_ms",
        help="Cache directory for --download-openaddresses-ms.",
    )
    parser.add_argument(
        "--download-openaddresses-ms-direct",
        action="store_true",
        help="Download current ESRI Mississippi address layers from the OpenAddresses source catalog into --openaddresses-ms-direct-cache-dir and use them as real address input.",
    )
    parser.add_argument(
        "--openaddresses-ms-direct-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "openaddresses_ms_direct",
        help="Cache directory for --download-openaddresses-ms-direct normalized CSV output.",
    )
    parser.add_argument(
        "--openaddresses-ms-source-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "openaddresses_ms_sources",
        help="Cache directory for OpenAddresses Mississippi source JSON definitions.",
    )
    parser.add_argument(
        "--refresh-openaddresses-ms-direct-cache",
        action="store_true",
        help="Force --download-openaddresses-ms-direct to re-query ESRI layers and replace cached normalized CSVs.",
    )
    parser.add_argument(
        "--refresh-openaddresses-ms-source-cache",
        action="store_true",
        help="Force --download-openaddresses-ms-direct to refresh the cached OpenAddresses source JSON definitions.",
    )
    parser.add_argument(
        "--include-openaddresses-ms-statewide-direct",
        action="store_true",
        help="Include older statewide OpenAddresses parcel layers that are skipped by default because the app uses newer MARIS parcels.",
    )
    parser.add_argument(
        "--download-maris-point-addresses",
        action="store_true",
        help="Download public MARIS Mississippi Point Addressing county shapefile ZIPs and use them as the real address input.",
    )
    parser.add_argument(
        "--maris-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "maris_point_addresses",
        help="Cache directory for --download-maris-point-addresses.",
    )
    parser.add_argument(
        "--download-maris-parcels",
        action="store_true",
        help="Download public MARIS statewide parcel situs-address CSVs and use them as a real address input fallback.",
    )
    parser.add_argument(
        "--maris-parcel-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "maris_parcels",
        help="Cache directory for --download-maris-parcels.",
    )
    parser.add_argument(
        "--maris-parcel-layer-limit",
        type=int,
        help="Optional development/testing cap on the number of MARIS parcel layers to download.",
    )
    parser.add_argument(
        "--refresh-maris-parcel-cache",
        action="store_true",
        help="Force --download-maris-parcels to re-query MARIS and replace cached parcel CSVs.",
    )
    parser.add_argument(
        "--download-nad",
        action="store_true",
        help="Download the official USDOT NAD national text ZIP into --nad-cache-path and use it as a real address input. The file is large.",
    )
    parser.add_argument(
        "--nad-cache-path",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "nad" / "TXT.zip",
        help="Cache file for --download-nad.",
    )
    parser.add_argument(
        "--require-ms-county-coverage",
        action="store_true",
        help="Require real Mississippi county inputs to include filenames for all 82 Mississippi counties before generating.",
    )
    parser.add_argument("--seed", type=int, default=4633, help="Random seed for reproducibility.")
    parser.add_argument("--eval-seed", type=int, help="Optional seed for the separately generated evaluation dataset. Defaults to seed + 1000.")
    parser.add_argument("--preview", type=int, default=8, help="Number of sample queries to print.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir = args.output_dir.expanduser().resolve()
    if args.paired_output_dir:
        args.paired_output_dir = args.paired_output_dir.expanduser().resolve()
    args.real_address_cache_dir = args.real_address_cache_dir.expanduser().resolve()
    args.openaddresses_ms_direct_cache_dir = args.openaddresses_ms_direct_cache_dir.expanduser().resolve()
    args.openaddresses_ms_source_cache_dir = args.openaddresses_ms_source_cache_dir.expanduser().resolve()
    args.maris_cache_dir = args.maris_cache_dir.expanduser().resolve()
    args.maris_parcel_cache_dir = args.maris_parcel_cache_dir.expanduser().resolve()
    args.nad_cache_path = args.nad_cache_path.expanduser().resolve()
    args.real_address_input = [path.expanduser().resolve() for path in args.real_address_input]
    if args.reference_size <= 0:
        raise SystemExit("--reference-size must be positive")
    if args.noisy_per_reference <= 0:
        raise SystemExit("--noisy-per-reference must be positive")
    if args.no_match_ratio < 0:
        raise SystemExit("--no-match-ratio must be non-negative")
    if not 0 <= args.adversarial_no_match_share <= 1:
        raise SystemExit("--adversarial-no-match-share must be between 0 and 1")
    if args.real_address_limit is not None and args.real_address_limit <= 0:
        raise SystemExit("--real-address-limit must be positive when set")
    if args.maris_parcel_layer_limit is not None and args.maris_parcel_layer_limit <= 0:
        raise SystemExit("--maris-parcel-layer-limit must be positive when set")
    args.real_address_state = canonical_state(args.real_address_state)
    if not args.real_address_state:
        raise SystemExit("--real-address-state must be a recognized two-letter state code or state name")

    real_address_pool: Optional[List[AddressRecord]] = None
    real_address_metadata: Optional[Dict[str, object]] = None
    if args.download_openaddresses_ms:
        downloaded = download_openaddresses_ms(args.real_address_cache_dir)
        args.real_address_input.extend(downloaded)
    if args.download_openaddresses_ms_direct:
        downloaded = download_openaddresses_ms_direct(
            args.openaddresses_ms_direct_cache_dir,
            args.openaddresses_ms_source_cache_dir,
            refresh=args.refresh_openaddresses_ms_direct_cache,
            refresh_configs=args.refresh_openaddresses_ms_source_cache,
            include_statewide=args.include_openaddresses_ms_statewide_direct,
        )
        args.real_address_input.extend(downloaded)
    if args.download_maris_point_addresses:
        downloaded = download_maris_point_addresses(args.maris_cache_dir)
        args.real_address_input.extend(downloaded)
    if args.download_maris_parcels:
        downloaded = download_maris_parcels(
            args.maris_parcel_cache_dir,
            layer_limit=args.maris_parcel_layer_limit,
            refresh=args.refresh_maris_parcel_cache,
        )
        args.real_address_input.extend(downloaded)
    if args.download_nad:
        nad_path = download_file(NAD_TEXT_ZIP_URL, args.nad_cache_path, timeout=300)
        args.real_address_input.append(nad_path)

    real_input_files: List[Path] = []
    if args.real_address_input:
        real_input_files = discover_input_files(args.real_address_input)

    county_coverage: Optional[Dict[str, object]] = None
    if real_input_files:
        covered_counties = mississippi_counties_in_paths(real_input_files)
        missing_counties = sorted(set(MISSISSIPPI_COUNTIES) - set(covered_counties))
        county_coverage = {
            "covered_counties": covered_counties,
            "covered_county_count": len(covered_counties),
            "expected_county_count": len(MISSISSIPPI_COUNTIES),
            "missing_counties": missing_counties,
        }
        if args.require_ms_county_coverage and missing_counties:
            raise SystemExit(
                "Mississippi county coverage check failed. Missing counties: "
                + ", ".join(missing_counties)
            )

    if real_input_files:
        load_result = load_real_addresses(
            real_input_files,
            source_format=args.real_address_format,
            state_filter=args.real_address_state,
            limit=args.real_address_limit,
        )
        if not load_result.records:
            raise SystemExit("No usable real addresses were loaded from the supplied source data.")
        real_address_pool = load_result.records
        real_address_metadata = {
            "input_paths": load_result.input_paths,
            "source_format": load_result.source_format,
            "state": load_result.state,
            "rows_seen": load_result.rows_seen,
            "rows_loaded": load_result.rows_loaded,
            "rows_skipped": load_result.rows_skipped,
            "mississippi_county_coverage": county_coverage,
            "source_note": (
                "For Mississippi-wide production use, provide the full MS811/MARIS county shapefile ZIP set and enable "
                "--require-ms-county-coverage. Public MARIS parcels are a broad parcel situs-address fallback, not true "
                "address points. Public MARIS point addresses, OpenAddresses, and NAD inputs are supported but should "
                "be verified before treating them as exhaustive."
            ),
        }
        print(
            f"Loaded {load_result.rows_loaded:,} real {load_result.state} addresses "
            f"from {len(load_result.input_paths)} file(s); skipped {load_result.rows_skipped:,} rows."
        )

    if real_address_pool is None:
        raise SystemExit(
            "No usable real address source was supplied. The generator is real-address-only by default; "
            "provide --real-address-input or a downloader such as --download-maris-parcels."
        )

    if args.paired_output_dir:
        eval_seed = args.eval_seed if args.eval_seed is not None else args.seed + 1000
        train_output_dir = args.paired_output_dir / "train_dataset"
        eval_output_dir = args.paired_output_dir / "eval_dataset"
        shared_reference = None
        if args.paired_shared_reference:
            shared_reference = DatasetBuilder(
                args.seed,
                real_address_pool=real_address_pool,
            ).build_reference(args.reference_size)
        train_result = generate_dataset(
            train_output_dir,
            args.seed,
            args,
            reference=shared_reference,
            real_address_pool=real_address_pool,
            real_address_metadata=real_address_metadata,
        )
        eval_result = generate_dataset(
            eval_output_dir,
            eval_seed,
            args,
            reference=shared_reference,
            real_address_pool=real_address_pool,
            real_address_metadata=real_address_metadata,
        )

        print("Generated paired address-resolution datasets successfully.")
        print(json.dumps({
            "train_dataset_dir": str(train_output_dir),
            "eval_dataset_dir": str(eval_output_dir),
            "train_seed": args.seed,
            "eval_seed": eval_seed,
            "shared_reference": bool(args.paired_shared_reference),
            "train_summary": train_result["summary"],
            "eval_summary": eval_result["summary"],
        }, indent=2, default=lambda x: dict(x)))
        print("\nSample train queries:")
        print(preview_queries(train_result["queries"], limit=args.preview))
        print("\nSample eval queries:")
        print(preview_queries(eval_result["queries"], limit=args.preview))
        print(f"\nOutput written to: {args.paired_output_dir}")
        return

    result = generate_dataset(
        args.output_dir,
        args.seed,
        args,
        real_address_pool=real_address_pool,
        real_address_metadata=real_address_metadata,
    )

    print("Generated address-resolution dataset successfully.")
    print(json.dumps(result["summary"], indent=2, default=lambda x: dict(x)))
    print("\nSample queries:")
    print(preview_queries(result["queries"], limit=args.preview))
    print(f"\nOutput written to: {args.output_dir}")


if __name__ == "__main__":
    main()
