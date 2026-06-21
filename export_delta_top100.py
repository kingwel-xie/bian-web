#!/usr/bin/env python3
"""Compare two leaderboard snapshots and export top grade deltas."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export top N leaderboard grade deltas.")
    parser.add_argument("--old", required=True, help="Old snapshot JSON.")
    parser.add_argument("--new", required=True, help="New snapshot JSON.")
    parser.add_argument("--output-prefix", required=True, help="Output path prefix without extension.")
    parser.add_argument("--top", type=int, default=100, help="Number of delta rows to export.")
    return parser.parse_args()


def to_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def load_rows(path: Path) -> dict[str, dict[str, object]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = {}
    for row in data.get("rows") or []:
        user_id = str(row.get("userId") or "").strip()
        nickname = str(row.get("nickName") or row.get("nickname") or "").strip()
        key = user_id or nickname
        if not key:
            continue
        rows[key] = row
    return rows


def build_deltas(old_path: Path, new_path: Path, limit: int) -> list[dict[str, object]]:
    old_rows = load_rows(old_path)
    new_rows = load_rows(new_path)
    items = []
    for key, new_row in new_rows.items():
        old_row = old_rows.get(key, {})
        old_grade = to_decimal(old_row.get("grade"))
        new_grade = to_decimal(new_row.get("grade"))
        delta = new_grade - old_grade
        items.append(
            {
                "deltaRank": 0,
                "rank": new_row.get("sequence"),
                "oldRank": old_row.get("sequence"),
                "nickname": new_row.get("nickName") or new_row.get("nickname"),
                "userId": new_row.get("userId"),
                "oldGrade": str(old_grade),
                "newGrade": str(new_grade),
                "deltaGrade": str(delta),
                "rewardCount": new_row.get("rewardCount"),
                "oldRewardCount": old_row.get("rewardCount"),
            }
        )
    items.sort(key=lambda item: to_decimal(item["deltaGrade"]), reverse=True)
    for index, item in enumerate(items[:limit], start=1):
        item["deltaRank"] = index
    return items[:limit]


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "deltaRank",
        "rank",
        "oldRank",
        "nickname",
        "userId",
        "oldGrade",
        "newGrade",
        "deltaGrade",
        "rewardCount",
        "oldRewardCount",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict[str, object]], old_path: Path, new_path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "old": str(old_path),
                "new": str(new_path),
                "count": len(rows),
                "rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def write_xlsx(path: Path, csv_path: Path) -> None:
    try:
        import zipfile
        from xml.sax.saxutils import escape
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(str(exc)) from exc

    rows = list(csv.reader(csv_path.open(encoding="utf-8", newline="")))

    def col_name(index: int) -> str:
        value = ""
        while index:
            index, rem = divmod(index - 1, 26)
            value = chr(65 + rem) + value
        return value

    def cell(row_index: int, col_index: int, value: str, header: bool = False) -> str:
        ref = f"{col_name(col_index)}{row_index}"
        style = ' s="1"' if header else ""
        return f'<c r="{ref}" t="inlineStr"{style}><is><t>{escape(value)}</t></is></c>'

    sheet_rows = []
    for row_index, row in enumerate(rows, start=1):
        cells = "".join(cell(row_index, col_index, value, row_index == 1) for col_index, value in enumerate(row, start=1))
        sheet_rows.append(f'<row r="{row_index}">{cells}</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:J{len(rows)}"/>'
        '<sheetData>' + "".join(sheet_rows) + '</sheetData>'
        f'<autoFilter ref="A1:J{len(rows)}"/>'
        '</worksheet>'
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>'
        '</styleSheet>'
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
        zf.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        zf.writestr("xl/workbook.xml", '<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="delta_top100" sheetId="1" r:id="rId1"/></sheets></workbook>')
        zf.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>')
        zf.writestr("xl/styles.xml", styles_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)


def main() -> int:
    args = parse_args()
    old_path = Path(args.old).resolve()
    new_path = Path(args.new).resolve()
    output_prefix = Path(args.output_prefix).resolve()
    rows = build_deltas(old_path, new_path, args.top)
    csv_path = output_prefix.with_suffix(".csv")
    json_path = output_prefix.with_suffix(".json")
    xlsx_path = output_prefix.with_suffix(".xlsx")
    write_csv(csv_path, rows)
    write_json(json_path, rows, old_path, new_path)
    write_xlsx(xlsx_path, csv_path)
    print(csv_path)
    print(json_path)
    print(xlsx_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
