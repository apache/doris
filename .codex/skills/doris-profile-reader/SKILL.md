---
name: doris-profile-reader
description: Interpret Apache Doris query runtime profiles, especially profile bottleneck triage, misleading wait counters, per-operator metric priority, scan, join-order/runtime-filter analysis, and evidence-bounded performance explanations. Use when given a Doris profile, query id, profile URL/text, or a request to explain Doris query performance.
---

# Doris Profile Reader

## Purpose

Use this skill to identify the real bottleneck in an Apache Doris query runtime profile. The core rule is to separate active work from dependency, queue, and backpressure waits before naming an operator as expensive. When the plan contains joins, also separate the immediate runtime bottleneck from the plan-shape cause, especially bad join order and runtime-filter direction.

## Required Reading Order

1. Read `references/reading-workflow.md` for the analysis workflow and output contract.
2. Read `references/counter-semantics.md` for counter meaning and priority, especially wait counters.
3. Read `references/operator-guide.md` for the relevant operator family.
4. Read `references/join-order-diagnosis.md` when the profile or plan has multiple joins, a large hash/nested-loop build, a large scan that might have been pruned by a join, paired fast/slow plans, hints/reordered SQL, or a request about join shape/reorder.
5. Read `references/runtime-filters.md` when a profile or plan includes `RuntimeFilterInfo`, `RF... <-`, `RF... ->`, `JRFs`, `WaitForRuntimeFilter`, or `AcquireRuntimeFilter`.
6. Use `references/source-profile-inventory.md` as the source-backed operator/counter inventory. If a counter or operator is not in the narrative docs, do not ignore it; look it up in this inventory and classify it by the rules in `counter-semantics.md`.

## Non-Negotiable Interpretation Rules

- Do not call `WaitForDependencyTime`, `WaitForDependency[...]Time`, `WaitForData0`, `WaitForDataN`, `WaitForRpcBufferQueue`, `WaitForBroadcastBuffer`, `PendingFinishDependency`, or pipeline blocked/wait counters direct operator compute cost. They are dependency, data-arrival, queue, memory, or backpressure signals.
- Do not rank operators by the largest wait-like counter alone. First rank by `ExecTime`/active timers, direct custom timers, rows/bytes, memory/spill, and skew.
- Treat merged-profile `sum` timers as accumulated across parallel instances. A timer can exceed query elapsed time and still be normal when many scanners, drivers, or fragments run in parallel.
- For scans, prioritize `RowsRead`, `ScanRows`, `ScanBytes`, `ScannerCpuTime`, `ScannerGetBlockTime`, `ScannerWorkerWaitTime`, I/O/decompression timers, predicate/lazy-read timers, and row-filter counters. `ScannerWorkerWaitTime` is important, but it indicates scanner scheduling/thread-pool wait rather than scan CPU.
- For runtime filters, distinguish source/build side from target/probe scan side. In plan text, `RFxxx <- expr` is produced from the build side; `RFxxx -> expr` is applied at a target scan. In profiles, `RuntimeFilterInfo` and scan-side wait/filter counters decide whether the RF helped or just waited.
- For joins, always identify build side and probe/target side before judging the order. A scan can be the immediate active bottleneck while the root cause is still bad join order if a selective join/RF source is scheduled too late to prune that scan.
- Do not require a paired fast profile before naming likely bad join order. A single slow profile can be enough when it proves large wasted build/scan work before an empty probe/result, an RF source side that had to scan massively before it could emit an empty/tiny filter, or a huge intermediate later eliminated by a highly selective or contradictory join predicate.
- Do not treat "the current RF made other scans wait and then skip" as proof that the join order is good. If producing that empty/tiny RF required the only expensive scan, the RF source/target choice is itself the join-order question.
- When a large build/source side is paid before an empty/tiny probe, preserved side, semi-join key set, or contradictory join can eliminate the result, call the build/probe order likely bad unless the profile proves that ordering is semantically forced and no earlier pruning/short-circuit is possible.
- If a strong single-profile join-order pattern matches, do not hedge as "suspicious", "close to likely bad", or "not proven". Use `likely bad` when a better legal order still needs validation, and reserve `not proven` for the exact alternate shape, not for the join-order diagnosis itself.
- When a join query has plan-shape evidence, the answer must explicitly judge join order/build-probe/RF direction as good, suspicious, or bad. Do not replace that judgment with a vague "predicate issue" or "plan shape issue".
- A long `InitTime`, `OpenTime`, `CloseTime`, or profile total can matter, but only after confirming it is not accumulated across many instances and not dominated by a known wait/dependency branch.

## Standard Answer Shape

When explaining a profile, answer in this order:

1. `Conclusion`: one or two sentences naming the likely bottleneck and, when joins are involved, whether the plan shape/join order is likely the cause.
2. `Evidence`: profile counters with operator names, values, and whether each is active work, data volume, wait/backpressure, memory/spill, or runtime-filter evidence.
3. `Reasoning`: how the evidence maps to the execution plan, which side is build/probe or RF source/target, why misleading counters are discounted, and whether the join order is reasonable.
4. `Next checks`: the smallest additional profile/log/code checks needed if the conclusion is still uncertain.

Always preserve uncertainty. Use "proven", "likely", and "not proven" explicitly when the profile lacks enough detail.

## Scripts

- `scripts/extract_source_profile_inventory.py`: scan Doris source for factory-created operators and counter/info registrations.

Scripts are evidence-generation helpers. They do not replace the reading workflow.
