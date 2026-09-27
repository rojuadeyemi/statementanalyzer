"""CLI.

    python -m statement_analyzer analyze statement.pdf --excel out.xlsx --json out.json
    python -m statement_analyzer check ./statements/      # extraction quality per file
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import StatementAnalyzer, load_statement, write_excel, write_json


def _check(folder: Path) -> int:
    files = sorted(p for p in folder.rglob("*") if p.suffix.lower() in {".pdf", ".json"})
    print(f"{'file':40} {'parser':28} {'rows':>6} {'reconciled':>11} {'secs':>6}  warnings")
    for f in files:
        try:
            s = load_statement(f)
            rate = s.quality.reconciliation_rate
            print(f"{f.name[:40]:40} {s.source[:28]:28} {s.quality.rows:6d} "
                  f"{'n/a' if rate is None else f'{rate:.1%}':>11} {s.quality.seconds:6.1f}  "
                  f"{'; '.join(s.quality.warnings)}")
        except Exception as exc:  # keep going through the batch
            print(f"{f.name[:40]:40} ERROR: {exc}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="statement_analyzer")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("analyze")
    a.add_argument("source")
    a.add_argument("--excel")
    a.add_argument("--json")
    c = sub.add_parser("check")
    c.add_argument("folder", type=Path)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)

    if args.cmd == "check":
        return _check(args.folder)

    analyzer = StatementAnalyzer.from_source(args.source)
    print(analyzer.summary_text())
    if args.excel:
        write_excel(analyzer, args.excel)
    if args.json:
        write_json(analyzer, args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
