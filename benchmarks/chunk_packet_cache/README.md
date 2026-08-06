# Chunk packet cache benchmark

This suite compares four fixed server revisions:

1. upstream `master`;
2. the original `#2490` prototype;
3. the completed stack with retention disabled;
4. the completed stack with retention enabled.

The first scenario is a clustered join against an identical pre-generated
server directory. It measures the strongest cache workload while retaining a
cache-disabled stack control for snapshot and delivery overhead.

## Prerequisites

- Build every server revision in a detached worktree with `--release`.
- Prepare and build [Pumpkin-MC/BotMark](https://github.com/Pumpkin-MC/BotMark)
  with `prepare_botmark.sh`. The small local patch acknowledges chunk batches;
  without it, the server can stop sending chunks after its initial batch and
  invalidate the comparison.
- The checked-in BotMark patch exits once every bot has received at least one
  chunk and delivery goes quiet; that is the reproducible baseline. Pass a
  nonzero `--target-chunks-per-bot` only when using a BotMark build that
  implements per-bot delivery targets, and validate per-bot counts there. The
  reported numbers must not be presented as equal-per-bot delivery without that
  support.
- Create a server template containing a pre-generated `world` directory.
- Disable online mode and encryption only inside this isolated benchmark
  environment.
- Prefer a second machine for BotMark. When running locally, pass disjoint CPU
  sets to `--server-cpus` and `--bot-cpus`.

Copy `matrix.example.json`, then replace the binary paths with the fixed
artifacts:

```sh
benchmarks/chunk_packet_cache/prepare_botmark.sh /tmp/pumpkin-botmark

python3 benchmarks/chunk_packet_cache/run_clustered_join.py \
  --matrix benchmarks/chunk_packet_cache/matrix.local.json \
  --botmark /path/to/botmark \
  --server-template /path/to/pregenerated-server-template \
  --output /path/to/results \
  --counts 1,8,32,64 \
  --warmups 1 \
  --repetitions 5 \
  --server-cpus 0-5 \
  --bot-cpus 6-9
```

The runner rotates variant order between repetitions, copies the same template
for every run, samples process CPU and RSS from `/proc`, and writes one JSON
record per run. Runs are never compared from debug builds.

Generate the report with:

```sh
python3 benchmarks/chunk_packet_cache/analyze.py \
  /path/to/results/results.jsonl \
  --output /path/to/results/summary.md
```

Set `PUMPKIN_BENCHMARK_METRICS=1` automatically enables the completed stack's
shutdown telemetry. It reports cache work, background-CPU waits, and the final
100 tick samples without adding per-hit logging to the hot path.

BotMark currently exercises joins, rotation, swinging, and chat, but its
movement loop is disabled. Therefore this runner deliberately claims only the
clustered-join scenario. Convoy and independent exploration require a
deterministic path-capable client before their results are publishable.

