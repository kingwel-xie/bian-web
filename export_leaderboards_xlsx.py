#!/usr/bin/env python3
"""Export captured Binance leaderboard JSON files to an XLSX workbook."""

from __future__ import annotations

import argparse
import json
import re
import zipfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape


HEADERS = ["rank", "nickname", "userId", "grade", "restoredTradingVolume", "tradingVolume", "region"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create an XLSX workbook from leaderboard JSON files.")
    parser.add_argument("--output", required=True, help="Output .xlsx path.")
    parser.add_argument(
        "--sheet",
        action="append",
        default=[],
        metavar="NAME=JSON",
        help="Sheet definition. Can be repeated, e.g. --sheet ENSO=/data/um_enso/...json",
    )
    return parser.parse_args()


def safe_sheet_name(name: str, used: set[str]) -> str:
    cleaned = re.sub(r"[\[\]:*?/\\\\]", "_", name.strip()) or "sheet"
    cleaned = cleaned[:31]
    candidate = cleaned
    index = 2
    while candidate.lower() in used:
        suffix = f"_{index}"
        candidate = f"{cleaned[:31 - len(suffix)]}{suffix}"
        index += 1
    used.add(candidate.lower())
    return candidate


def col_name(index: int) -> str:
    value = ""
    while index:
        index, rem = divmod(index - 1, 26)
        value = chr(65 + rem) + value
    return value


def cell_ref(row: int, col: int) -> str:
    return f"{col_name(col)}{row}"


def cell_xml(row: int, col: int, value: Any, style: int | None = None) -> str:
    ref = cell_ref(row, col)
    style_attr = f' s="{style}"' if style is not None else ""
    if value is None:
        return f'<c r="{ref}"{style_attr}/>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"{style_attr}><v>{value}</v></c>'
    text = escape(str(value))
    return f'<c r="{ref}" t="inlineStr"{style_attr}><is><t>{text}</t></is></c>'


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "")
        if not value:
            return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def restored_trading_volume(row: dict[str, Any]) -> float | None:
    volume = to_decimal(row.get("restoredTradingVolume"))
    if volume is None:
        volume = to_decimal(row.get("tradingVolume"))
    if volume is None:
        grade = to_decimal(row.get("grade"))
        if grade is not None:
            volume = grade * grade
    return float(volume) if volume is not None else None


def sheet_xml(sheet_name: str, json_path: Path) -> str:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    rows = data.get("rows") or []
    resource_id = data.get("resourceId")
    updated_ms = (data.get("meta") or {}).get("updatedTime")
    generated_at = datetime.now(timezone.utc).isoformat()

    xml_rows = []
    summary = [
        ["name", data.get("name") or sheet_name],
        ["url", data.get("url")],
        ["resourceId", resource_id],
        ["count", data.get("count") or len(rows)],
        ["sum", data.get("sum")],
        ["restoredTradingVolumeSum", data.get("restoredTradingVolumeSum")],
        ["updatedTime", updated_ms],
        ["sourceJson", str(json_path)],
        ["exportedAt", generated_at],
    ]
    for row_index, values in enumerate(summary, start=1):
        cells = "".join(cell_xml(row_index, col_index, value, 1 if col_index == 1 else None) for col_index, value in enumerate(values, start=1))
        xml_rows.append(f'<row r="{row_index}">{cells}</row>')

    header_row = len(summary) + 2
    header_cells = "".join(cell_xml(header_row, col_index, header, 1) for col_index, header in enumerate(HEADERS, start=1))
    xml_rows.append(f'<row r="{header_row}">{header_cells}</row>')

    for offset, item in enumerate(rows, start=1):
        row_index = header_row + offset
        values = [
            item.get("sequence"),
            item.get("nickName") or item.get("nickname"),
            item.get("userId"),
            item.get("grade"),
            restored_trading_volume(item),
            item.get("tradingVolume"),
            item.get("region"),
        ]
        cells = "".join(cell_xml(row_index, col_index, value) for col_index, value in enumerate(values, start=1))
        xml_rows.append(f'<row r="{row_index}">{cells}</row>')

    end_col = col_name(len(HEADERS))
    dimension = f"A1:{end_col}{header_row + len(rows)}"
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<dimension ref="{dimension}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols><col min="1" max="1" width="10" customWidth="1"/>'
        '<col min="2" max="2" width="24" customWidth="1"/>'
        '<col min="3" max="3" width="14" customWidth="1"/>'
        '<col min="4" max="4" width="18" customWidth="1"/>'
        '<col min="5" max="6" width="24" customWidth="1"/>'
        '<col min="7" max="7" width="14" customWidth="1"/></cols>'
        f'<sheetData>{"".join(xml_rows)}</sheetData>'
        f'<autoFilter ref="A{header_row}:{end_col}{header_row + len(rows)}"/>'
        '</worksheet>'
    )


def workbook_xml(sheets: list[tuple[str, Path]]) -> str:
    sheet_items = []
    for index, (name, _) in enumerate(sheets, start=1):
        sheet_items.append(f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets>'
        + "".join(sheet_items)
        + '</sheets></workbook>'
    )


def workbook_rels_xml(sheets: list[tuple[str, Path]]) -> str:
    rels = []
    for index in range(1, len(sheets) + 1):
        rels.append(
            f'<Relationship Id="rId{index}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{index}.xml"/>'
        )
    rels.append(
        f'<Relationship Id="rId{len(sheets) + 1}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
    )
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + "".join(rels) + '</Relationships>'


def content_types_xml(sheets: list[tuple[str, Path]]) -> str:
    overrides = [
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
    ]
    for index in range(1, len(sheets) + 1):
        overrides.append(
            f'<Override PartName="/xl/worksheets/sheet{index}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        + "".join(overrides)
        + '</Types>'
    )


def styles_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>'
        '</styleSheet>'
    )


def root_rels_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )


def parse_sheets(values: list[str]) -> list[tuple[str, Path]]:
    if not values:
        raise SystemExit("At least one --sheet NAME=JSON is required.")
    used: set[str] = set()
    sheets = []
    for value in values:
        if "=" not in value:
            raise SystemExit(f"Invalid --sheet value: {value}")
        name, raw_path = value.split("=", 1)
        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            raise SystemExit(f"JSON not found: {path}")
        sheets.append((safe_sheet_name(name, used), path))
    return sheets


def main() -> int:
    args = parse_args()
    sheets = parse_sheets(args.sheet)
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml(sheets))
        zf.writestr("_rels/.rels", root_rels_xml())
        zf.writestr("xl/workbook.xml", workbook_xml(sheets))
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml(sheets))
        zf.writestr("xl/styles.xml", styles_xml())
        for index, (name, path) in enumerate(sheets, start=1):
            zf.writestr(f"xl/worksheets/sheet{index}.xml", sheet_xml(name, path))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
