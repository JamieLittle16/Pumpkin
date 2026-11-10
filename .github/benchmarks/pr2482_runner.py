#!/usr/bin/env python3
"""Compare baseline and in-place frame encryption on the SAME GitHub Actions runner.

This is an exploratory microbenchmark, not a proof of end-to-end server improvement.
"""
import json
import math
import os
import statistics
import subprocess
import sys
from pathlib import Path

baseline, candidate = sys.argv[1:3]
scenarios = [
    ("64b", 30000, "64 B contiguous"),
    ("1k", 2200, "1 KiB contiguous"),
    ("16k", 150, "16 KiB contiguous"),
    ("64k", 40, "64 KiB contiguous"),
    ("1m", 3, "1 MiB contiguous"),
    ("64k-split-64b", 38, "64 KiB split into 64 B writes"),
    ("64k-split-1k", 38, "64 KiB split into 1 KiB writes"),
    ("64k-split-16k", 38, "64 KiB split into 16 KiB writes"),
]
# Paired alternate orders to limit bias from changing runner load.
pairs = 12
records = []

def measure(exe: str, scenario: str, iterations: int) -> float:
    result = subprocess.run([exe, scenario, str(iterations)], capture_output=True,
                            text=True, timeout=120, check=True)
    value = float(result.stdout.strip().splitlines()[-1])
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"Invalid sample {value}")
    return value

print(f"Runner: {os.environ.get('RUNNER_NAME','unknown')}; "
      f"OS: {os.environ.get('RUNNER_OS','unknown')}", flush=True)
for scenario, iterations, label in scenarios:
    baseline_ns, candidate_ns, paired = [], [], []
    for repeat in range(pairs):
        if repeat % 2 == 0:
            b = measure(baseline, scenario, iterations)
            p = measure(candidate, scenario, iterations)
        else:
            p = measure(candidate, scenario, iterations)
            b = measure(baseline, scenario, iterations)
        baseline_ns.append(b)
        candidate_ns.append(p)
        paired.append(100.0 * (b - p) / b)
    b_med = statistics.median(baseline_ns)
    p_med = statistics.median(candidate_ns)
    improvement = 100.0 * (b_med - p_med) / b_med
    spread = statistics.quantiles(paired, n=4, method="inclusive")
    row = {
        "scenario": scenario, "label": label, "iterations_per_sample": iterations,
        "pairs": pairs, "baseline_ns_per_iter_median": b_med,
        "candidate_ns_per_iter_median": p_med,
        "improvement_pct": improvement,
        "paired_improvement_pct_median": statistics.median(paired),
        "paired_improvement_pct_iqr": [spread[0], spread[2]],
        "baseline_ns_per_iter_samples": baseline_ns,
        "candidate_ns_per_iter_samples": candidate_ns,
    }
    records.append(row)
    print(f"{label:32} | {b_med:12,.1f} ns/op | "
          f"{p_med:12,.1f} ns/op | {improvement:+7.2f}% | "
          f"paired IQR [{spread[0]:+.2f}%, {spread[2]:+.2f}%]", flush=True)

Path("pr-2482-benchmark.json").write_text(json.dumps({
    "baseline_commit": "10f1fde9367c827f168eaabdc8f343b1ce6f676c",
    "candidate_commit": "eb57397997f1bbc306e03fb14eb4b85e8ae926a9",
    "method": "12 paired measurements/scenario alternating execution order; "
              "release build LTO=false; actual TCPNetworkEncoder->Tokio BufWriter->Sink; "
              "includes fresh encryption setup, frame clone, encrypt, write and flush; "
              "GitHub shared runner, no network I/O",
    "results": records,
}, indent=2))
