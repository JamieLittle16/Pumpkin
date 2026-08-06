#!/usr/bin/env python3
"""Aggregate chunk packet cache benchmark JSON into a Markdown report."""

from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def record_failures(record: dict[str, Any]) -> dict[str, int]:
    failures = record.get("client_failures")
    if isinstance(failures, dict):
        return {
            "reconnects": int(failures.get("reconnects", 0)),
            "decode_errors": int(failures.get("decode_errors", 0)),
            "connect_failures": int(failures.get("connect_failures", 0)),
            "kicks": int(failures.get("kicks", 0)),
        }
    run_dir = Path(str(record.get("run_directory", "")))
    log_path = run_dir / "botmark.stdout.log"
    if not log_path.exists():
        return {"reconnects": 0, "decode_errors": 0, "connect_failures": 0, "kicks": 0}
    text = log_path.read_text(encoding="utf-8", errors="replace")
    return {
        "reconnects": len(re.findall(r"connection dropped, reconnecting", text)),
        "decode_errors": len(re.findall(r"Failed to decode packet", text)),
        "connect_failures": len(
            re.findall(r"failed to connect|connection to .* timed out after", text)
        ),
        "kicks": len(re.findall(r"Kicking in Play State", text)),
    }


def client_failures_total(record: dict[str, Any]) -> int:
    return sum(record_failures(record).values())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=Path)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def median(records: list[dict[str, Any]], path: tuple[str, ...]) -> float | None:
    values: list[float] = []
    for record in records:
        value: Any = record
        for component in path:
            if not isinstance(value, dict) or component not in value:
                value = None
                break
            value = value[component]
        if isinstance(value, int | float):
            values.append(float(value))
    return statistics.median(values) if values else None


def display(value: float | None, divisor: float = 1.0, suffix: str = "") -> str:
    return "—" if value is None else f"{value / divisor:.2f}{suffix}"


def main() -> None:
    args = parse_args()
    records = [
        json.loads(line)
        for line in args.results.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    measured = [record for record in records if not record.get("warmup", False)]
    incomplete = [
        record
        for record in measured
        if not record.get("botmark", {}).get("completed", False)
        or (
            record.get("target_chunks_per_bot", 0) > 0
            and not record.get("botmark", {}).get("target_met", False)
        )
    ]
    contaminated = [
        record for record in measured if client_failures_total(record) > 0
    ]
    clean = [
        record
        for record in measured
        if client_failures_total(record) == 0
        if record.get("botmark", {}).get("completed", False)
        and (
            record.get("target_chunks_per_bot", 0) == 0
            or record.get("botmark", {}).get("target_met", False)
        )
    ]
    groups: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for record in clean:
        groups[(int(record["count"]), str(record["variant"]))].append(record)

    lines = [
        "# Chunk packet cache benchmark results",
        "",
        "Values are medians of measured repetitions; warm-up runs are excluded.",
        f"Incomplete measured runs excluded: {len(incomplete)}.",
        f"Runs excluded for client failures (reconnects/decode errors/etc): {len(contaminated)}.",
        "",
        "Definitions:",
        "- **Load time** = time from completion of the scheduled join burst (all bot",
        "  tasks spawned) until BotMark signalled chunk-delivery quiet (or the",
        "  configured per-bot target where supported). It does not include the",
        "  staggered connect delay.",
        "- **Server CPU** = process CPU seconds from port-ready until the bot",
        "  process exited plus the configured drain interval.",
        "- **Tick stats** = the final rolling 100-tick window captured after the",
        "  drain; they reflect the idle post-drain window, not the join burst.",
        "- **Client failures** = per-bot reconnects, decode errors, connect",
        "  failures and kicks parsed from the BotMark log. Runs with any client",
        "  failure are excluded from the medians because a reconnect re-serialises",
        "  the chunk set (a second instance generation) and inflates server CPU.",
        "",
        "| Bots | Variant | Runs | Load time | Chunks | Server CPU | Peak RSS | Tick max | Tick >50 ms | Tick >100 ms | Serializations | Compressions | Snapshots |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for (count, variant), group in sorted(groups.items()):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(count),
                    variant,
                    str(len(group)),
                    display(median(group, ("botmark", "elapsed_ms")), 1000, "s"),
                    display(median(group, ("botmark", "total_chunks"))),
                    display(median(group, ("server_cpu_seconds",)), suffix="s"),
                    display(median(group, ("peak_rss_bytes",)), 1024 * 1024, " MiB"),
                    display(median(group, ("internal", "tick_max_ms")), suffix=" ms"),
                    display(median(group, ("internal", "ticks_over_50ms"))),
                    display(median(group, ("internal", "ticks_over_100ms"))),
                    display(median(group, ("internal", "serialization_misses"))),
                    display(median(group, ("internal", "preparation_misses"))),
                    display(median(group, ("internal", "snapshot_captures"))),
                ]
            )
            + " |"
        )

    lines.extend(["", "## Relative server CPU", ""])
    by_count: dict[int, dict[str, float]] = defaultdict(dict)
    for (count, variant), group in groups.items():
        value = median(group, ("server_cpu_seconds",))
        if value is not None:
            by_count[count][variant] = value
    lines.extend(
        [
            "| Bots | Variant | vs master | vs stack-disabled |",
            "| ---: | --- | ---: | ---: |",
        ]
    )
    for count, variants in sorted(by_count.items()):
        for variant, value in sorted(variants.items()):
            master = variants.get("master")
            disabled = variants.get("stack-disabled")
            versus_master = None if not master else (value / master - 1.0) * 100
            versus_disabled = None if not disabled else (value / disabled - 1.0) * 100
            lines.append(
                f"| {count} | {variant} | "
                f"{display(versus_master, suffix='%')} | "
                f"{display(versus_disabled, suffix='%')} |"
            )

    lines.extend(["", "## Client failures (all measured runs)", ""])
    lines.extend(
        [
            "| Bots | Variant | Rep | Reconnects | Decode errors | Connect failures | Kicks | Excluded |",
            "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for record in sorted(
        measured,
        key=lambda r: (int(r["count"]), r["variant"], int(r["repetition"])),
    ):
        failures = record_failures(record)
        excluded = "yes" if client_failures_total(record) > 0 else "no"
        lines.append(
            "| "
            + " | ".join(
                [
                    str(record["count"]),
                    record["variant"],
                    str(record["repetition"]),
                    str(failures.get("reconnects", 0)),
                    str(failures.get("decode_errors", 0)),
                    str(failures.get("connect_failures", 0)),
                    str(failures.get("kicks", 0)),
                    excluded,
                ]
            )
            + " |"
        )

    lines.extend(["", "## Per-run detail (clean runs only)", ""])
    lines.extend(
        [
            "| Bots | Variant | Rep | Chunks | Server CPU s | Peak RSS MiB | Serializations | Snapshots |",
            "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for record in sorted(
        clean,
        key=lambda r: (int(r["count"]), r["variant"], int(r["repetition"])),
    ):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(record["count"]),
                    record["variant"],
                    str(record["repetition"]),
                    str(record.get("botmark", {}).get("total_chunks", "—")),
                    display(record.get("server_cpu_seconds")),
                    display(
                        record.get("peak_rss_bytes"),
                        1024 * 1024,
                        " MiB",
                    ),
                    display(record.get("internal", {}).get("serialization_misses")),
                    display(record.get("internal", {}).get("snapshot_captures")),
                ]
            )
            + " |"
        )

    report = "\n".join(lines) + "\n"
    if args.output:
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report, end="")


if __name__ == "__main__":
    main()
