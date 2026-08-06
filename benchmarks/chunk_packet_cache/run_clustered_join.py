#!/usr/bin/env python3
"""Run reproducible clustered-join comparisons for the chunk packet cache."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


METRICS_PREFIX = "PUMPKIN_BENCHMARK_METRICS "
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    import mmap

    with path.open("rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
            h.update(mapped)
    return h.hexdigest()


def directory_sha256(root: Path) -> str:
    """Content hash of all regular files under root (stable ordering, path excluded)."""
    entries: list[tuple[str, bytes]] = []
    for base, _, files in os.walk(root, followlinks=False):
        for name in sorted(files):
            full = Path(base) / name
            if full.is_symlink():
                continue
            rel = str(full.relative_to(root))
            entries.append((rel, full.read_bytes()))
    entries.sort(key=lambda item: item[0])
    h = hashlib.sha256()
    for rel, data in entries:
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(data)
        h.update(b"\0")
    return h.hexdigest()


def cpu_governor() -> list[str] | None:
    candidates = [
        Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        Path("/sys/devices/system/cpu/cpu0/cpufreq/governor"),
    ]
    for path in candidates:
        try:
            return [line.strip() for line in path.read_text().splitlines() if line.strip()]
        except OSError:
            continue
    return None


@dataclass
class ProcessSample:
    cpu_seconds: float = 0.0
    peak_rss_bytes: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--botmark", type=Path, required=True)
    parser.add_argument("--server-template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--counts", default="1,8,32,64")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--delay-ms", type=int, default=25)
    parser.add_argument("--chunk-quiet-seconds", type=float, default=2.0)
    parser.add_argument("--target-chunks-per-bot", type=int, default=0)
    parser.add_argument("--drain-seconds", type=float, default=3.0)
    parser.add_argument("--load-timeout", type=float, default=180.0)
    parser.add_argument("--stall-seconds", type=float, default=300.0)
    parser.add_argument("--startup-timeout", type=float, default=60.0)
    parser.add_argument("--port", type=int, default=25570)
    parser.add_argument("--server-cpus")
    parser.add_argument("--bot-cpus")
    parser.add_argument("--preparation-threads", type=int, default=2)
    return parser.parse_args()


def command_with_affinity(command: list[str], cpus: str | None) -> list[str]:
    return ["taskset", "-c", cpus, *command] if cpus else command


def wait_for_port(process: subprocess.Popen[Any], port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited during startup with {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"server did not listen on port {port} within {timeout}s")


def ensure_port_free(port: int) -> None:
    """Refuse to start a server if another process already owns the port.

    Two concurrent runners share the default port, and an orphaned server from
    an aborted run otherwise causes every later run to connect to the stale
    instance and stall. Check once at startup so a second runner fails loudly
    instead of silently invalidating a campaign.
    """
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            raise SystemExit(
                f"port {port} already in use; another server or runner is active"
            )
    except (SystemExit, socket.timeout):
        raise
    except OSError:
        return


def read_process_sample(pid: int, clock_ticks: int, page_size: int) -> ProcessSample | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
        rss_pages = int(fields[23])
        cpu_ticks = int(fields[13]) + int(fields[14])
        return ProcessSample(cpu_ticks / clock_ticks, rss_pages * page_size)
    except (FileNotFoundError, IndexError, ValueError):
        return None


def monitor_process(
    process: subprocess.Popen[Any], result: ProcessSample, stop: threading.Event
) -> None:
    clock_ticks = os.sysconf("SC_CLK_TCK")
    page_size = os.sysconf("SC_PAGE_SIZE")
    while not stop.wait(0.1):
        sample = read_process_sample(process.pid, clock_ticks, page_size)
        if sample is None:
            continue
        result.cpu_seconds = max(result.cpu_seconds, sample.cpu_seconds)
        result.peak_rss_bytes = max(result.peak_rss_bytes, sample.peak_rss_bytes)


def write_config(
    directory: Path,
    port: int,
    cache_enabled: bool,
    capacity_mib: int,
    preparation_threads: int,
) -> None:
    # Unknown TOML fields are ignored by older revisions. Including both cache
    # layouts lets one template configure master, #2490, and the completed stack.
    config = f"""
seed = "8675309"
allow_nether = false
allow_end = false
default_level_name = "world"
white_list = false
enforce_whitelist = false

[logging]
enabled = true
threads = false
color = false
timestamp = false
file = "latest.log"

[commands]
use_console = false
use_tty = false
log_console = false
broadcast_console_to_ops = false

[networking.java]
enabled = true
address = "127.0.0.1:{port}"
encryption = false
online_mode = false
max_players = 1000
view_distance = 16
simulation_distance = 10
chunk_packet_cache_mib = {capacity_mib}

[networking.java.compression]
enabled = true
threshold = 256
level = 4

[networking.java.authentication]
enabled = false

[networking.java.chunk_packet_cache]
enabled = {str(cache_enabled).lower()}
capacity_mib = {capacity_mib}
preparation_threads = {preparation_threads}

[networking.bedrock]
enabled = false
""".strip()
    (directory / "pumpkin.toml").write_text(config + "\n", encoding="utf-8")


def stop_process(process: subprocess.Popen[Any], timeout: float = 30.0) -> None:
    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.SIGINT)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def popen(command: list[str], **kwargs: Any) -> subprocess.Popen[Any]:
    """Spawn a child in its own process group so it can never be orphaned by a
    runner killed from outside, and can be signalled as a group in cleanup."""
    kwargs.setdefault("start_new_session", True)
    return subprocess.Popen(command, **kwargs)


def parse_internal_metrics(log_path: Path) -> dict[str, Any]:
    if not log_path.exists():
        return {}
    for raw_line in reversed(log_path.read_text(encoding="utf-8", errors="replace").splitlines()):
        line = ANSI_ESCAPE.sub("", raw_line)
        marker = line.find(METRICS_PREFIX)
        if marker >= 0:
            return json.loads(line[marker + len(METRICS_PREFIX) :])
    return {}


def parse_client_failures(log_path: Path) -> dict[str, Any]:
    """Count per-bot connection failures from the BotMark client log."""
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


def parse_botmark_completion(log_path: Path) -> dict[str, Any]:
    if not log_path.exists():
        return {}
    pattern = re.compile(r"BOTMARK_CHUNKS_COMPLETE elapsed_ms=(\d+) total_chunks=(\d+)")
    target_pattern = re.compile(r"BOTMARK_TARGET_MET target_chunks_per_bot=(\d+)")
    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    target_met = any(target_pattern.search(line) for line in lines)
    for line in reversed(lines):
        match = pattern.search(line)
        if match:
            return {
                "completed": True,
                "target_met": target_met,
                "elapsed_ms": int(match.group(1)),
                "total_chunks": int(match.group(2)),
            }
    return {"completed": False, "target_met": False}


def run_once(
    args: argparse.Namespace,
    variant: dict[str, Any],
    count: int,
    repetition: int,
    warmup: bool,
) -> dict[str, Any]:
    label = f"{variant['name']}-n{count}-r{repetition}{'-warmup' if warmup else ''}"
    run_dir = args.output / "runs" / label
    if run_dir.exists():
        shutil.rmtree(run_dir)
    shutil.copytree(args.server_template, run_dir)
    write_config(
        run_dir,
        args.port,
        bool(variant["cache_enabled"]),
        int(variant["capacity_mib"]),
        args.preparation_threads,
    )

    server_log = run_dir / "server.stdout.log"
    bot_log = run_dir / "botmark.stdout.log"
    environment = os.environ.copy()
    environment["PUMPKIN_BENCHMARK_METRICS"] = "1"
    server_command = command_with_affinity([str(Path(variant["binary"]).resolve())], args.server_cpus)

    started = time.monotonic()
    with server_log.open("w", encoding="utf-8") as server_output:
        server = popen(
            server_command,
            cwd=run_dir,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=server_output,
            stderr=subprocess.STDOUT,
        )
        sample = ProcessSample()
        monitor_stop = threading.Event()
        monitor = threading.Thread(
            target=monitor_process, args=(server, sample, monitor_stop), daemon=True
        )
        monitor.start()
        try:
            wait_for_port(server, args.port, args.startup_timeout)
            ready = time.monotonic()
            ready_sample = read_process_sample(
                server.pid, os.sysconf("SC_CLK_TCK"), os.sysconf("SC_PAGE_SIZE")
            ) or ProcessSample()
            bot_args = [
                str(args.botmark.resolve()),
                "--ip",
                f"127.0.0.1:{args.port}",
                "--count",
                str(count),
                "--delay",
                str(args.delay_ms),
                "--timeout",
                "10000",
                "--spam-message-delay-min",
                "100000000",
                "--spam-message-delay-max",
                "100000001",
                "--exit-when-chunks-quiet",
                "--chunk-quiet-ms",
                str(round(args.chunk_quiet_seconds * 1000)),
            ]
            if args.target_chunks_per_bot > 0:
                # Requires a BotMark build that implements per-bot delivery
                # targets; the checked-in quiet-exit patch does not add it.
                bot_args.extend(
                    [
                        "--target-chunks-per-bot",
                        str(args.target_chunks_per_bot),
                    ]
                )
            bot_command = command_with_affinity(bot_args, args.bot_cpus)
            with bot_log.open("w", encoding="utf-8") as bot_output:
                bots = popen(
                    bot_command,
                    cwd=run_dir,
                    stdout=bot_output,
                    stderr=subprocess.STDOUT,
                )
                bot_started = time.monotonic()
                stall_deadline = bot_started + min(
                    args.stall_seconds,
                    count * args.delay_ms / 1000.0 + args.load_timeout,
                )
                while bots.poll() is None and time.monotonic() < stall_deadline:
                    time.sleep(2.0)
                if bots.poll() is None:
                    # Normal delivery completes in seconds; a run that does not
                    # finish within the stall budget has a bot that dropped and
                    # failed to recover, so abort instead of burning the full
                    # load timeout.
                    print(
                        f"NOTE: {label} aborted (delivery stall)",
                        file=sys.stderr,
                    )
                    stop_process(bots, timeout=10)
                bot_finished = time.monotonic()
                if args.drain_seconds > 0:
                    time.sleep(args.drain_seconds)
                finished_sample = read_process_sample(
                    server.pid, os.sysconf("SC_CLK_TCK"), os.sysconf("SC_PAGE_SIZE")
                ) or sample
        finally:
            stop_process(server)
            monitor_stop.set()
            monitor.join(timeout=2)
    finished = time.monotonic()

    return {
        "schema": 1,
        "scenario": "clustered_join",
        "variant": variant["name"],
        "revision_binary": str(Path(variant["binary"]).resolve()),
        "cache_enabled": bool(variant["cache_enabled"]),
        "capacity_mib": int(variant["capacity_mib"]),
        "count": count,
        "delay_ms": args.delay_ms,
        "chunk_quiet_seconds": args.chunk_quiet_seconds,
        "target_chunks_per_bot": args.target_chunks_per_bot,
        "drain_seconds": args.drain_seconds,
        "load_timeout": args.load_timeout,
        "repetition": repetition,
        "warmup": warmup,
        "startup_seconds": ready - started,
        "bot_window_seconds": bot_finished - bot_started,
        "total_seconds": finished - started,
        "startup_cpu_seconds": ready_sample.cpu_seconds,
        "server_cpu_seconds": max(
            0.0, finished_sample.cpu_seconds - ready_sample.cpu_seconds
        ),
        "peak_rss_bytes": sample.peak_rss_bytes,
        "internal": parse_internal_metrics(server_log),
        "botmark": parse_botmark_completion(bot_log),
        "client_failures": parse_client_failures(bot_log),
        "run_directory": str(run_dir),
    }


def main() -> None:
    args = parse_args()
    ensure_port_free(args.port)
    args.output.mkdir(parents=True, exist_ok=True)
    variants = json.loads(args.matrix.read_text(encoding="utf-8"))
    counts = [int(value) for value in args.counts.split(",")]
    results_path = args.output / "results.jsonl"

    for path in [args.botmark, args.server_template]:
        if not path.exists():
            raise FileNotFoundError(path)
    for variant in variants:
        if not Path(variant["binary"]).exists():
            raise FileNotFoundError(variant["binary"])

    environment = {
        "schema": 2,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "server_cpus": args.server_cpus,
        "bot_cpus": args.bot_cpus,
        "matrix": variants,
        "binary_sha256": {
            variant["name"]: file_sha256(Path(variant["binary"]))
            for variant in variants
        },
        "botmark_sha256": file_sha256(args.botmark),
        "server_template_sha256": directory_sha256(args.server_template),
        "cpu_governor": cpu_governor(),
        "preparation_threads": args.preparation_threads,
        "delay_ms": args.delay_ms,
        "chunk_quiet_seconds": args.chunk_quiet_seconds,
        "target_chunks_per_bot": args.target_chunks_per_bot,
        "load_timeout": args.load_timeout,
        "stall_seconds": args.stall_seconds,
        "drain_seconds": args.drain_seconds,
    }
    (args.output / "environment.json").write_text(
        json.dumps(environment, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    total_repetitions = args.warmups + args.repetitions
    with results_path.open("a", encoding="utf-8") as results:
        for count in counts:
            for repetition in range(total_repetitions):
                order = variants[repetition % len(variants) :] + variants[: repetition % len(variants)]
                for variant in order:
                    record = run_once(
                        args,
                        variant,
                        count,
                        repetition,
                        repetition < args.warmups,
                    )
                    results.write(json.dumps(record, sort_keys=True) + "\n")
                    results.flush()
                    print(
                        f"{record['variant']} count={count} repetition={repetition} "
                        f"cpu={record['server_cpu_seconds']:.2f}s "
                        f"rss={record['peak_rss_bytes'] / 1048576:.1f}MiB",
                        flush=True,
                    )


if __name__ == "__main__":
    main()
