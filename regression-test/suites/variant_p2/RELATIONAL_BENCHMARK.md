# VARIANT relational benchmark

`relational_performance.groovy` extends the existing `variant_p2` GitHub Events
workload with GROUP BY, ORDER BY, and broadcast/shuffle equality JOIN. The load
phase runs `load.groovy` and verifies its expected 44,273,863 rows. Pass
`--stream-load` to read the same public hourly files over HTTPS when regression
S3 credentials are unavailable. HTTP reads use a socket timeout and resumable
download. If an interrupted run has committed all but a known set of files,
pass those names with `--resume-files`; this appends only those files before the
same exact row-count check and prepare phase.
It then creates one small dimension table per selected real JSON path:

| Key | Source path | CAST comparison |
| --- | --- | --- |
| actor_login | `actor['login']` | STRING |
| repo_name | `repo['name']` | STRING |
| payload_action | `payload['action']` | STRING |
| actor_id | `actor['id']` | BIGINT |

The default pair is `actor_login,actor_id`, covering one string and one numeric
path. Use `--keys` to select any subset of the four paths.

Each query has native VARIANT and explicit CAST forms. SQL cache and query cache
are disabled. GROUP BY correctness uses bidirectional EXCEPT between complete
native and CAST group results before timing compact aggregate checksums. JOIN
checks count and ID sum and uses one dimension row per key to avoid many-to-many
output explosion. ORDER BY compares the first 1,000 IDs with an ID tie-breaker.

Mixed-type semantics belong in `variant_p0/test_variant_relational_corners.groovy`.
They are deliberately excluded from these performance pairs: CAST may merge
strings with numbers or round integers above 2^53. Matching CAST results alone
cannot prove correct native mixed-type behavior.

Build and start this worktree's FE/BE using the repository environment workflow.
Performance requires `BUILD_TYPE=RELEASE` and `./build.sh --be --fe`; do not pass
an explicit `-j`. Synchronize worktree ports after building. The runner requires
Python 3.11+, Linux `/proc`, and the local `output/` runtime. Use a dedicated
database/config because `load.groovy` recreates `github_events`.

Run from the worktree root, substituting the verified regression config:

```bash
python3 regression-test/suites/variant_p2/run_relational_benchmark.py load \
  --conf tmp/regression-conf.auto.groovy --stream-load \
  --output tmp/relational-load

python3 regression-test/suites/variant_p2/run_relational_benchmark.py query \
  --conf tmp/regression-conf.auto.groovy \
  --cpus 16,17,18,19,20,21,22,23 --warmups 2 --repeats 7 \
  --output tmp/relational-query

python3 regression-test/suites/variant_p2/run_relational_benchmark.py query \
  --conf tmp/regression-conf.auto.groovy \
  --cpus 16,17,18,19,20,21,22,23 --keys actor_login \
  --warmups 0 --repeats 3 --spill --output tmp/relational-spill
```

Choose eight distinct physical cores after inspecting host topology and load.
The load phase does not change affinity and rejects an already eight-core-bound
FE or BE process. Doris may independently pin a few service threads. The query
phase binds every FE and BE thread to the same eight cores, checks affinity
during the run, and restores previous masks afterward. This restricts the
database servers; the regression client runs outside that mask. CPU affinity
does not reserve cores against unrelated host processes.

The runner verifies the running BE matches the installed Release binary. Each
new evidence directory contains revision/binary/affinity metadata, host load,
regression logs, query plans, every warmup/measured latency and result SHA-256,
and median/min/max summaries. Latency includes client round-trip and result
consumption. Native/CAST order alternates each round. Interpret forced-spill
measurements separately from ordinary queries. A completed repeat run checks
repeatability, not long-duration soak, concurrent ingestion, or crash recovery.

The query suite can also run directly through `run-regression-test.sh` using
`VARIANT_BENCH_PHASE=prepare|query`, `VARIANT_BENCH_ROWS`, `VARIANT_BENCH_KEYS`,
`VARIANT_BENCH_REPEATS`, `VARIANT_BENCH_WARMUPS`, `VARIANT_BENCH_RESULTS`, and
`VARIANT_BENCH_SPILL`. Direct runs do not enforce Release or CPU affinity and
must not be reported as an eight-core performance result.
