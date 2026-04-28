#!/usr/bin/env python3
"""Real address source parsing, downloading, and cache helpers."""
from __future__ import annotations

import csv
import io
import json
import re
import struct
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


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
    "DR": ("RD", "AVE", "ST", "WAY"),
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
class RealAddressLoadResult:
    records: List[AddressRecord]
    input_paths: List[str]
    source_format: str
    state: str
    rows_seen: int
    rows_loaded: int
    rows_skipped: int



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
