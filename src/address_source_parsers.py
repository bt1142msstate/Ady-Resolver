#!/usr/bin/env python3
"""Real address row parsing and loading helpers."""
from __future__ import annotations

import csv
import io
import struct
import zipfile
from collections import Counter
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from address_source_common import *  # noqa: F401,F403


def _canonical_address_for_dedupe(record: AddressRecord) -> str:
    """Canonical rendering used for source deduplication without importing noise generation."""
    street_part = " ".join(
        bit
        for bit in [record.house_number, record.predir, record.street_name, record.street_type, record.suffixdir]
        if bit
    )
    pieces = [street_part]
    if record.unit_type and record.unit_value:
        pieces.append(f"{record.unit_type} {record.unit_value}")
    locality_part = " ".join(bit for bit in [record.city, record.state, record.zip_code] if bit)
    return normalize_spaces(f"{', '.join(pieces)}, {locality_part}").upper()

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
                key = query_text_key(_canonical_address_for_dedupe(record))
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
                    key = query_text_key(_canonical_address_for_dedupe(record))
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
