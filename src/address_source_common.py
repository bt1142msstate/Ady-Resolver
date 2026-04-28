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
from dataclasses import dataclass, field
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
OPENADDRESSES_COUNTY_ONLY_LOCALITY_STATUS = "county_only_situs"
OPENADDRESSES_COUNTY_ONLY_SOURCE_QUALITY = 0.72
OPENADDRESSES_DIRECT_FIELDNAMES = (
    "NUMBER",
    "STREET",
    "UNIT",
    "CITY",
    "REGION",
    "POSTCODE",
    "SOURCE",
    "SOURCE_ID",
    "COUNTY",
    "LOCALITY_STATUS",
)
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
    skip_reasons: Dict[str, int] = field(default_factory=dict)
    duplicate_rows: int = 0



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
