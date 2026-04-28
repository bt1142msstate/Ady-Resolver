#!/usr/bin/env python3
"""CSV/XLSX upload parsing and XLSX report generation for resolver_app."""
from __future__ import annotations

import csv
import io
import re
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree
from xml.sax.saxutils import escape as xml_escape

from address_resolver import normalize_text

ADDRESS_COLUMN_NAMES = {
    "ADDRESS",
    "ADDR",
    "ADDR1",
    "ADDRESS1",
    "INPUT ADDRESS",
    "INPUT",
    "RAW ADDRESS",
    "ORIGINAL ADDRESS",
    "FULL ADDRESS",
    "STREET ADDRESS",
    "MAILING ADDRESS",
    "PHYSICAL ADDRESS",
    "PROPERTY ADDRESS",
    "SITUS ADDRESS",
    "SITE ADDRESS",
    "LOCATION",
}
ID_COLUMN_NAMES = {
    "ID",
    "RECORD ID",
    "SOURCE ID",
    "ROW ID",
    "CUSTOMER ID",
    "ACCOUNT ID",
    "PARCEL ID",
    "PARCEL",
    "PID",
    "APN",
    "OBJECT ID",
    "OBJECTID",
    "FID",
}
MAX_BATCH_ADDRESSES = 20000


def normalize_header(value: str) -> str:
    return " ".join(normalize_text(re.sub(r"[_/-]+", " ", value)).split())


def spreadsheet_column_index(value: str) -> Optional[int]:
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        index = int(text) - 1
        return index if index >= 0 else None
    if not re.fullmatch(r"[A-Za-z]{1,3}", text):
        return None
    index = 0
    for char in text.upper():
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def column_matches_name(value: str, names: set[str], suffix: str = "") -> bool:
    normalized = normalize_header(value)
    if not normalized:
        return False
    if normalized in names:
        return True
    return bool(suffix and normalized.endswith(suffix) and len(normalized) <= 40)


def looks_like_header_row(row: List[str]) -> bool:
    return any(
        column_matches_name(value, ADDRESS_COLUMN_NAMES, "ADDRESS") or column_matches_name(value, ID_COLUMN_NAMES)
        for value in row
    )


def detect_data_column(
    header: List[str],
    requested_column: str = "",
    names: Optional[set[str]] = None,
    suffix: str = "",
    fallback_index: int = 0,
    label: str = "Column",
) -> Tuple[int, bool]:
    names = names or set()
    if requested_column:
        requested = normalize_header(requested_column)
        if len(requested) > 1:
            for index, value in enumerate(header):
                if normalize_header(value) == requested:
                    return index, True
        positional = spreadsheet_column_index(requested_column)
        if positional is not None:
            header_value = header[positional] if positional < len(header) else ""
            return positional, column_matches_name(header_value, names, suffix)
        for index, value in enumerate(header):
            if normalize_header(value) == requested:
                return index, True
        raise ValueError(f"{label} not found: {requested_column}")

    normalized = [normalize_header(value) for value in header]
    for index, value in enumerate(normalized):
        if value in names or (suffix and value.endswith(suffix) and len(value) <= 40):
            return index, True
    return fallback_index, False


def detect_address_column(header: List[str], requested_column: str = "") -> Tuple[int, bool]:
    return detect_data_column(header, requested_column, ADDRESS_COLUMN_NAMES, "ADDRESS", 0, "Address column")


def detect_id_column(header: List[str], requested_column: str = "") -> Tuple[Optional[int], bool]:
    if not requested_column:
        for index, value in enumerate(header):
            if column_matches_name(value, ID_COLUMN_NAMES):
                return index, True
        return None, False
    index, has_header = detect_data_column(header, requested_column, ID_COLUMN_NAMES, "", 0, "ID column")
    return index, has_header


def extract_batch_addresses(
    rows: List[List[str]],
    requested_column: str = "",
    requested_id_column: str = "",
    has_header: Optional[bool] = None,
) -> List[Tuple[int, str, str]]:
    rows = [[str(cell or "").strip() for cell in row] for row in rows if any(str(cell or "").strip() for cell in row)]
    if not rows:
        raise ValueError("The uploaded file did not contain any rows.")
    column_index, address_has_header = detect_address_column(rows[0], requested_column)
    id_index, id_has_header = detect_id_column(rows[0], requested_id_column)
    header_present = looks_like_header_row(rows[0]) if has_header is None else has_header
    start = 1 if (header_present or address_has_header or id_has_header) else 0
    extracted: List[Tuple[int, str, str]] = []
    for row_offset, row in enumerate(rows[start:], start + 1):
        value = row[column_index].strip() if column_index < len(row) else ""
        if value:
            source_id = row[id_index].strip() if id_index is not None and id_index < len(row) else ""
            extracted.append((row_offset, source_id, value))
        if len(extracted) > MAX_BATCH_ADDRESSES:
            raise ValueError(f"Batch files are limited to {MAX_BATCH_ADDRESSES:,} addresses.")
    if not extracted:
        raise ValueError("No addresses were found in the selected column.")
    return extracted


def read_csv_upload(content: bytes) -> List[List[str]]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("latin-1")
    return [row for row in csv.reader(io.StringIO(text))]


def xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def column_letters_from_cell_ref(cell_ref: str) -> str:
    return "".join(char for char in cell_ref if char.isalpha())


def column_letters_to_index(letters: str) -> int:
    index = 0
    for char in letters.upper():
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def worksheet_path_from_workbook(archive: zipfile.ZipFile) -> str:
    try:
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        first_sheet = next(element for element in workbook.iter() if xml_local_name(element.tag) == "sheet")
        relation_id = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
        rels = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        for rel in rels:
            if rel.attrib.get("Id") == relation_id:
                target = rel.attrib.get("Target", "worksheets/sheet1.xml")
                return target if target.startswith("xl/") else f"xl/{target.lstrip('/')}"
    except Exception:
        pass
    return "xl/worksheets/sheet1.xml"


def read_shared_strings(archive: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    values = []
    for item in root:
        if xml_local_name(item.tag) != "si":
            continue
        values.append("".join(text.text or "" for text in item.iter() if xml_local_name(text.tag) == "t"))
    return values


def read_xlsx_upload(content: bytes) -> List[List[str]]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        shared_strings = read_shared_strings(archive)
        worksheet_path = worksheet_path_from_workbook(archive)
        root = ElementTree.fromstring(archive.read(worksheet_path))
        rows: List[List[str]] = []
        for row_element in root.iter():
            if xml_local_name(row_element.tag) != "row":
                continue
            values: Dict[int, str] = {}
            for cell in row_element:
                if xml_local_name(cell.tag) != "c":
                    continue
                cell_ref = cell.attrib.get("r", "")
                column_index = column_letters_to_index(column_letters_from_cell_ref(cell_ref)) if cell_ref else len(values)
                cell_type = cell.attrib.get("t", "")
                value = ""
                if cell_type == "inlineStr":
                    value = "".join(text.text or "" for text in cell.iter() if xml_local_name(text.tag) == "t")
                else:
                    value_node = next((child for child in cell if xml_local_name(child.tag) == "v"), None)
                    if value_node is not None and value_node.text is not None:
                        value = value_node.text
                        if cell_type == "s":
                            shared_index = int(value)
                            value = shared_strings[shared_index] if shared_index < len(shared_strings) else ""
                values[column_index] = value
            if values:
                rows.append([values.get(index, "") for index in range(max(values) + 1)])
        return rows


def read_upload_rows(filename: str, content: bytes) -> List[List[str]]:
    suffix = Path(filename.lower()).suffix
    if suffix == ".xlsx":
        return read_xlsx_upload(content)
    elif suffix == ".csv" or not suffix:
        return read_csv_upload(content)
    raise ValueError("Upload a .csv or .xlsx file.")


def inspect_batch_columns(filename: str, content: bytes) -> Dict[str, object]:
    rows = [[str(cell or "").strip() for cell in row] for row in read_upload_rows(filename, content)]
    rows = [row for row in rows if any(row)]
    if not rows:
        raise ValueError("The uploaded file did not contain any rows.")
    has_header = looks_like_header_row(rows[0])
    header = rows[0]
    data_start = 1 if has_header else 0
    max_columns = max(len(row) for row in rows[:25])
    address_index, _ = detect_address_column(header)
    id_index, _ = detect_id_column(header)
    columns: List[Dict[str, object]] = []
    for index in range(max_columns):
        letter = excel_column_name(index)
        name = header[index] if has_header and index < len(header) else ""
        preview = ""
        for row in rows[data_start : data_start + 10]:
            if index < len(row) and row[index]:
                preview = row[index]
                break
        if len(preview) > 52:
            preview = preview[:49].rstrip() + "..."
        columns.append(
            {
                "value": letter,
                "letter": letter,
                "name": name,
                "preview": preview,
                "index": index,
            }
        )
    return {
        "columns": columns,
        "has_header": has_header,
        "guessed_address_column": excel_column_name(address_index) if address_index is not None and address_index < max_columns else "",
        "guessed_id_column": excel_column_name(id_index) if id_index is not None and id_index < max_columns else "",
    }


def read_batch_upload(
    filename: str,
    content: bytes,
    requested_column: str = "",
    requested_id_column: str = "",
    has_header: Optional[bool] = None,
) -> List[Tuple[int, str, str]]:
    return extract_batch_addresses(read_upload_rows(filename, content), requested_column, requested_id_column, has_header)


def excel_column_name(index: int) -> str:
    index += 1
    letters = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def xlsx_cell(ref: str, value: object, style: int = 0) -> str:
    text = "" if value is None else str(value)
    style_attr = f' s="{style}"' if style else ""
    return f'<c r="{ref}" t="inlineStr"{style_attr}><is><t>{xml_escape(text)}</t></is></c>'


def write_xlsx_report(headers: List[str], rows: List[List[object]]) -> bytes:
    worksheet_rows = []
    all_rows = [headers, *rows]
    for row_index, row in enumerate(all_rows, 1):
        cells = []
        for column_index, value in enumerate(row):
            ref = f"{excel_column_name(column_index)}{row_index}"
            cells.append(xlsx_cell(ref, value, style=1 if row_index == 1 else 0))
        worksheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<cols>'
        '<col min="1" max="1" width="12" customWidth="1"/>'
        '<col min="2" max="9" width="28" customWidth="1"/>'
        '<col min="10" max="15" width="36" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(worksheet_rows)}</sheetData>'
        '</worksheet>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Resolved Addresses" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '</styleSheet>'
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '</Types>'
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        '</Relationships>'
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        archive.writestr("xl/styles.xml", styles_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return output.getvalue()


def batch_report_filename(filename: str) -> str:
    stem = Path(filename).stem or "addresses"
    safe_stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", stem).strip("._") or "addresses"
    return f"{safe_stem}_resolved.xlsx"
