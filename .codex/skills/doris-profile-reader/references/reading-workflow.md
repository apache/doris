# Doris Profile Reading Workflow

## 1. Confirm Profile Shape

Start from the query id, elapsed time, SQL, FE `Summary`, `Execution Profile`, `MergedProfile`, and `DetailProfile`.

- Use `MergedProfile` first to find candidate fragments/operators quickly.
- Use `DetailProfile` to check skew, per-instance outliers, and whether a large merged time is just parallel accumulation.
- Use the plan text to map operator names back to tables, join sides, grouping keys, exchanges, limits, and runtime-filter direction.
- If the plan has joins, keep two layers in the analysis: immediate runtime bottleneck and plan-shape cause. A scan or hash build may be the active cost while a bad join order/RF direction is the reason that cost exists.
- Keep query-lifecycle time separate from BE operator work. FE `Plan Time`, `Schedule Time`, `Wait and Fetch Result Time`, and client/result fetch time can explain wall-clock latency, especially for small or empty queries, but they are not scan/join/aggregation CPU. If they dominate, report them as a separate lifecycle finding before ranking BE operators.
- If `Is Cached: Yes`, `Total Instances Num: 0`, or the BE detail is absent, the profile is not enough for operator bottleneck analysis. Re-run with profile enabled and SQL cache disabled.

## 2. Build a Counter Classification Before Ranking

Classify each large counter before interpreting it:

- Active work: `ExecTime` after qualifying child wait evidence, plus direct operator custom timers such as scan CPU, hash table build/probe, sort, aggregation, serialization, compression, I/O, decompression, spill read/write.
- Data volume: `InputRows`, `RowsProduced`, `RowsRead`, `ScanRows`, `ScanBytes`, bytes sent/received, blocks produced.
- Wait/backpressure: `WaitForDependencyTime`, `WaitForDependency[...]Time`, `WaitForDataN`, `DataArrivalWaitTime`, `FirstBatchArrivalWaitTime`, `WaitForRpcBufferQueue`, `WaitForBroadcastBuffer`, local-exchange buffer waits, `PendingFinishDependency`, `WaitWorkerTime`, memory reserve waits.
- Resource pressure: memory peak, spill counters, scanner queue wait, remote/local bytes, cache miss/read timers.
- Optimizer/context info: join type, colocate/shuffle flags, partitioner, grouping keys, runtime-filter info, predicates, projections.
- Query lifecycle/context: FE plan time, schedule time, wait/fetch result time, result sink/client fetch time. Useful for wall-clock explanation, not proof of BE operator cost.

Only active work, data volume, and resource pressure can directly support a bottleneck claim. Wait/backpressure counters explain pipeline shape and symptoms, not direct operator cost by themselves.

## 3. Rank Candidate Bottlenecks

Use this priority order:

1. Query elapsed and the largest active operator families.
2. Large rows/bytes that explain the active time.
3. Skew in `max` versus `avg` or a single instance much slower than peers.
4. Spill, memory peak, and queue/thread waits.
5. Runtime-filter effectiveness and wait.
6. Network/exchange serialization, compression, and remote bytes.
7. Init/open/close only if they remain large after excluding parallel accumulation and waits.

Do not rank by the largest wait-like counter. A profile where `EXCHANGE_OPERATOR WaitForData0` is the largest counter commonly means the exchange waited for upstream data.

When several scans or branches each contribute meaningful rows, bytes, memory, or active time, do not force a single-operator bottleneck. First summarize the branch family or operator family, then name the strongest member only as an example or leading contributor.

After ranking the direct cost, perform a join-order pass for multi-join plans. Identify build/probe sides, RF source/target sides, and whether a more selective branch was scheduled too late to prune a large scan or build. Use `join-order-diagnosis.md` for the decision rules.

## 4. Interpret Merged Timers Correctly

Merged profile values often include `sum`, `avg`, `max`, and `min`. Treat them differently:

- `sum` can exceed wall-clock elapsed time because it accumulates across parallel fragments, drivers, scan ranges, and scanner threads.
- `max` is usually better for wall-clock suspicion.
- `avg` plus `max` reveals skew.
- `min` is useful only for skew context.

If an operator appears many times, a long summed `ExecTime` may represent normal parallel CPU. Compare per-instance `max` to query elapsed and compare rows/bytes per instance.

## 5. Read Pipeline Waits as Dependencies

Pipeline tasks register wait/dependency counters for start prerequisites, upstream data readiness, downstream write availability, memory reservation, runtime filters, and local/remote queues. These counters answer "what was this driver waiting for", not "where CPU was spent".

Examples:

- `WaitForDependency[OLAP_SCAN_OPERATOR_DEPENDENCY]Time`: scan source was blocked by its dependency state.
- `WaitForDependency[HASH_JOIN_BUILD_DEPENDENCY]Time`: probe/build sequencing or shared build state dependency.
- `WaitForData0`: exchange source waited for sender queue data.
- `WaitForRpcBufferQueue`: exchange sink waited for RPC buffer capacity.
- `WaitForLocalExchangeBufferN`: sender waited for a local exchange channel.
- `WaitWorkerTime`: pipeline task waited for worker scheduling.
- `ScannerWorkerWaitTime`: scanners waited in the scan worker pool.

Large waits are actionable only after you identify the upstream/downstream resource that caused them.

## 6. Diagnose by Operator Family

Use the operator guide and choose the family-specific proof:

- Scan bottleneck: high scan active time plus large `ScanRows`/`ScanBytes`, high `ScannerCpuTime`, I/O/decompression/lazy-read/predicate timers, or scanner queue wait.
- Scan/RF latency trap: if scan `ExecTime` is close to `RuntimeFilterInfo` `RFx WaitTime`, `AcquireRuntimeFilter`, or `WaitForRuntimeFilter`, treat the scan as waiting for filters, not spending CPU in storage. Detail profiles can expose `WaitForRuntimeFilter` even when merged `RFx WaitTime` is zero or the filter timed out. `TOPN OPT`, `TopNFilterSourceNodeIds`, or `SharedPredicate` on a scan should be read as the same filter-wait family before blaming storage CPU. Then decide whether the wait was justified by `FilterRows` and downstream savings.
- Join bottleneck: high build/probe active time, large build/probe rows, hash table memory, non-equi conjunct time, runtime filter publish/build, or spill in partitioned join.
- Aggregation bottleneck: high build/merge/hash-table times, large `InputRows`, large hash table size/memory, or spill.
- Sort/window bottleneck: high sort/evaluation active time, large input rows, memory pressure, or spill.
- Exchange bottleneck: high serialization/compression/send/receive/merge time, high `RpcMaxTime`/`RpcAvgTime` on `DATA_STREAM_SINK_OPERATOR`, and bytes. Data-arrival waits alone are not enough.
- Sink/write bottleneck: high append/write/commit/close/load timers and bytes/rows. Output `PendingFinishDependency` alone is not enough.

For repeated CTEs, repeated common subplans, expanded `UNION ALL` branches, or scalar-subquery fan-in plans, do not summarize from one representative branch. Build a small branch inventory: table/source, scan rows/bytes, rows produced, major join/aggregation rows, memory/spill evidence, and whether the apparent time is active work or RF/dependency wait. Then aggregate the pattern mentally. Repeated branches can make the true bottleneck "many medium scans plus RF waits" rather than one single giant operator. A branch with the highest apparent wall time may only be waiting for RFs; still report sibling branches with large scan volume, large output rows, or heavy aggregation/join memory. In scalar-subquery fan-in plans, top-level `CROSS_JOIN_OPERATOR` instances may only combine one-row branch results; a multi-second `CROSS_JOIN_OPERATOR` dependency wait usually means the cross join waited for slower child branches, not that nested-loop join CPU dominated.

Small dimension scans often show high apparent `ExecTime` or detail scanner timers because they waited for runtime filters or because merged counters accumulate across many tiny scanners. Before promoting a dimension scan over a fact branch, compare rows/bytes, branch role, output impact, and whether the timer is mostly RF wait. If the fact branches dominate row volume or downstream aggregation, report the dimension scan as supporting/filtering work instead of the main bottleneck.

## 7. Runtime Filter Pass

If runtime filters appear, always answer:

- Which join/build side produced the filter.
- Which scan/probe side applied it.
- Whether it filtered rows, was always true, arrived too late, or forced meaningful wait.
- Whether a long wait is justified by saved scan work.

See `runtime-filters.md`.

## 8. Join Order Pass

For every multi-join profile, always answer:

- Which side each important join builds on, and which side probes.
- Whether the build side is unexpectedly large compared with the probe/other available side.
- Whether runtime filters flow from a selective side to a large scan early enough to save work.
- Whether paired profiles, hints, rewritten SQL, memo estimates, or actual rows prove a better order.
- Whether memo contains an unchosen reversed join expression, or schema/distribution keys show that the current RF source should instead be an RF target.

If the active bottleneck is a scan or hash build that would disappear under a better join order, report the scan/build as the runtime bottleneck and the join order/RF direction as the likely root cause.

When only a slow profile is available, still make a join-order judgment. Use "likely" when the evidence is circumstantial, but do not answer "not proven" merely because there is no paired fast profile. Empty probes, empty final results after huge build/scan work, huge intermediates later eliminated, and RF-source inversion are enough to call the order likely bad when build/probe/RF sides and row counts are visible.

Use this red-flag pass before writing the conclusion:

- Expensive RF source emits empty/tiny RF and all targets skip: join order/RF direction is likely bad, not proven good.
- Huge build/shuffle before zero/tiny probe or result: build/probe order or build scheduling is likely bad, even if another predicate explains why the probe is empty.
- Huge intermediate before a contradictory or highly selective inner/non-equi join: join order/predicate placement is bad.
- Tiny semi/subquery key set is applied after a large fact scan/build: join order/RF timing is likely bad.

If one of those red flags matches, the conclusion must say `bad` or `likely bad`. Use `not proven` only for the exact alternate legal plan or repair, not for whether the observed order is wrong.

If your reasoning says "plan shape", "predicate placement", "empty probe", "late contradictory predicate", or "expensive RF source", translate that into an explicit join-order diagnosis. The required output is not complete until it says whether the join order/build-probe/RF direction is good, suspicious, or bad.

See `join-order-diagnosis.md`.

## 9. Output Standard

A good profile explanation is evidence-bounded:

- Quote exact operator and counter names.
- State whether each quoted timer is active, wait, or accumulated.
- Tie scan/join/exchange metrics to table or plan node when possible.
- For joins, include a short build/probe/RF-direction judgment, even when the direct bottleneck is a scan.
- Avoid generic tuning advice unless the profile proves the need.
- End with missing evidence if the profile is incomplete.
