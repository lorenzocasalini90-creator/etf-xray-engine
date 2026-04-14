#!/usr/bin/env python3
"""
CheckMyETFs E2E test runner — runs pytest and generates a Markdown report.

Usage: python tests/e2e/run_tests.py
"""

import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPORT_DIR = ROOT
SCREENSHOT_DIR = ROOT / "screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)


def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORT_DIR / f"REPORT_{timestamp}.md"

    print("=" * 60)
    print("  CheckMyETFs — E2E Test Runner")
    print(f"  Target: https://www.checkmyetfs.com")
    print(f"  Time:   {datetime.now().isoformat()}")
    print("=" * 60)
    print()

    t0 = time.time()

    # Run pytest with verbose output
    out_file = REPORT_DIR / f"pytest_output_{timestamp}.txt"
    with open(out_file, "w") as f:
        proc = subprocess.run(
            [
                sys.executable, "-m", "pytest",
                str(ROOT / "test_checkmyetfs.py"),
                "-v",
                "--tb=short",
                "-s",
                "--timeout=180",
            ],
            stdout=f,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(ROOT),
            timeout=2400,  # 40 min global timeout
        )
    result = type("R", (), {
        "stdout": open(out_file).read(),
        "stderr": "",
        "returncode": proc.returncode
    })()

    total_time = time.time() - t0
    stdout = result.stdout
    stderr = result.stderr

    # Print live output
    print(stdout)
    if stderr:
        print(stderr)

    # Parse results
    tests = _parse_results(stdout)
    timings = _parse_timings(stdout)

    n_total = len(tests)
    n_pass = sum(1 for t in tests if t["status"] == "PASSED")
    n_fail = sum(1 for t in tests if t["status"] == "FAILED")
    n_skip = sum(1 for t in tests if t["status"] == "SKIPPED")

    # Generate report
    report = _generate_report(tests, timings, n_total, n_pass, n_fail, n_skip,
                              total_time, timestamp)
    report_path.write_text(report)
    print(f"\nReport saved: {report_path}")

    # Print summary
    print()
    print("=" * 60)
    print(f"  SUMMARY: {n_pass} passed, {n_fail} failed, {n_skip} skipped / {n_total} total")
    print(f"  Duration: {total_time:.0f}s")
    print("=" * 60)

    return result.returncode


def _parse_results(stdout: str) -> list[dict]:
    """Parse pytest verbose output into test results."""
    tests = []
    # Two-pass: first collect all test names in order, then find their status.
    # pytest -v output has: test_path::Class::test_name[browser] STATUS
    # but with -s, print output can split the line
    test_names = []
    for line in stdout.splitlines():
        m = re.search(
            r"test_checkmyetfs\.py::(\w+)::(\w+)\[\w+\]",
            line
        )
        if m:
            key = (m.group(1), m.group(2))
            if key not in [(t["module"], t["name"]) for t in test_names]:
                test_names.append({"module": m.group(1), "name": m.group(2), "status": "UNKNOWN"})
        # Check if same line has status
        for t in test_names:
            pattern = f"{t['name']}\\[\\w+\\]\\s+(PASSED|FAILED|SKIPPED|ERROR)"
            s = re.search(pattern, line)
            if s:
                t["status"] = s.group(1)

    # Also check standalone status lines (when -s print split them)
    # Look for bare PASSED/FAILED lines and assign to the last UNKNOWN test
    for line in stdout.splitlines():
        stripped = line.strip()
        if stripped in ("PASSED", "FAILED", "SKIPPED", "ERROR"):
            for t in test_names:
                if t["status"] == "UNKNOWN":
                    t["status"] = stripped
                    break

    return test_names


def _parse_timings(stdout: str) -> dict[str, str]:
    """Extract [TIMING] lines."""
    timings = {}
    for line in stdout.splitlines():
        m = re.match(r"\[TIMING\]\s+(.+?):\s+(\d+)ms", line.strip())
        if m:
            timings[m.group(1)] = int(m.group(2))
    return timings


def _generate_report(tests, timings, n_total, n_pass, n_fail, n_skip,
                     total_time, timestamp) -> str:
    mins = int(total_time // 60)
    secs = int(total_time % 60)

    lines = [
        "# CheckMyETFs — Test Report",
        f"Data: {timestamp}",
        f"Ambiente: https://www.checkmyetfs.com",
        "",
        "## Sommario",
        f"- Totale test: {n_total}",
        f"- PASS: {n_pass}",
        f"- FAIL: {n_fail}",
        f"- SKIPPED: {n_skip}",
        f"- Durata totale: {mins}m {secs}s",
        "",
    ]

    # Timing table
    if timings:
        THRESHOLDS = {
            "add": (2000, "ETF aggiunta"),
            "analysis": (90000, "Analisi"),
            "cold": (90000, "Cold fetch"),
            "warm": (15000, "Warm/cache"),
            "AI": (30000, "AI generation"),
            "CSV": (3000, "CSV import"),
        }
        lines.append("## Timing critico")
        lines.append("| Operazione | Tempo (ms) | Status |")
        lines.append("|-----------|------------|--------|")
        for label, ms in sorted(timings.items()):
            threshold = 90000
            for key, (th, _) in THRESHOLDS.items():
                if key.lower() in label.lower():
                    threshold = th
                    break
            status = "PASS" if ms < threshold else "WARNING" if ms < threshold * 1.5 else "FAIL"
            lines.append(f"| {label} | {ms:.0f} | {status} |")
        lines.append("")

    # Detail by module
    modules = {}
    for t in tests:
        mod = t["module"]
        if mod not in modules:
            modules[mod] = []
        modules[mod].append(t)

    MODULE_NAMES = {
        "TestModuloA": "A — Input Portafoglio",
        "TestModuloB": "B — Fetch & Coverage",
        "TestModuloC": "C — Analytics",
        "TestModuloD": "D — UI",
        "TestModuloE": "E — Premium AI",
        "TestModuloF": "F — Edge Cases",
        "TestModuloG": "G — Flussi Utente",
    }

    lines.append("## Dettaglio per modulo")
    lines.append("")
    for mod, mod_tests in modules.items():
        mod_label = MODULE_NAMES.get(mod, mod)
        lines.append(f"### {mod_label}")
        lines.append("| Test | Status |")
        lines.append("|------|--------|")
        for t in mod_tests:
            icon = {"PASSED": "PASS", "FAILED": "FAIL", "SKIPPED": "SKIP",
                    "ERROR": "ERROR"}.get(t["status"], t["status"])
            lines.append(f"| {t['name']} | {icon} |")
        lines.append("")

    # Bug list
    failures = [t for t in tests if t["status"] in ("FAILED", "ERROR")]
    if failures:
        lines.append("## Bug trovati")
        lines.append("")
        for t in failures:
            lines.append(f"- [{t['name']}] {t['module']} — FAILED")
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
