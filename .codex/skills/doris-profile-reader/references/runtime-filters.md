# Runtime Filter Interpretation

## Direction

Doris runtime filters are generated from join build-side expressions and applied on target/probe-side scan expressions when the planner can map the slots.

Plan notation:

- `RF000[min_max] <- build_expr`: the filter is produced by this join/build side.
- `RF000[min_max] -> target_expr`: the filter is applied to this scan/target side.
- `RFs=[RF0 build->target]`: physical plan join-side relationship.
- `JRFs= RF0` on a scan: the scan has joined runtime filters to apply.

Do not invert these. The arrow into the RF (`<-`) is source/build. The arrow out of the RF (`->`) is target/scan.

## Where to Look in Profiles

Build/source side:

- Join sink or build operator `RuntimeFilterInfo`.
- `PublishTime` when publishing filters is visible.
- Build-side row count, join type, hash table size, and build time.

Target/scan side:

- Scan `WaitForRuntimeFilter` or `AcquireRuntimeFilter`.
- `RuntimeFilterInfo` entries such as `RF0 InputRows`, `RF0 FilterRows`, `RF0 AlwaysTrueFilterRows`, `RF0 WaitTime`.
- Top-N or dynamic filter evidence such as `TOPN OPT`, `TopNFilterSourceNodeIds`, and `SharedPredicate`. These can add `WaitForRuntimeFilter` even when individual merged `RFx WaitTime` counters are zero or the filter state is `TIMEOUT`.
- Scan rows/bytes and row-filter counters before deciding whether the RF helped.
- If scan `ExecTime` is approximately equal to RF wait counters, the scan was mostly waiting for runtime filters. Do not report that as scanner CPU or storage I/O without supporting `ScannerCpuTime`, `IOTimer`, `DecompressorTimer`, or `ScanBytes` evidence.
- `RFx WaitTime`, scan-level `WaitForRuntimeFilter`, `AcquireRuntimeFilter`, and scanner child timers are not guaranteed to be additive. They can overlap, be per-filter, or be accumulated at different scopes. Use them to classify latency and causality, not to compute exact residual CPU by subtraction.
- `RFx InputRows`, `RFx FilterRows`, and `RFx AlwaysTrueFilterRows` are per-filter/parallel counters. Do not add all filters together as if each row were counted once.
- Storage-pushed RF pruning may be visible as `RowsKeyRangeFiltered`, `RowsStatsFiltered`, `RowsZoneMapRuntimePredicateFiltered`, `RowsBloomFilterFiltered`, or related scan-detail counters. A zero `RFx FilterRows` does not by itself prove the RF was useless.

Exchange/sink side:

- Runtime filters may be carried through data stream sinks between fragments. Use the plan to connect source and target.
- Multicast/local-exchange sources can expose runtime-filter waits while distributing data to multiple consumers. Classify those waits as consumer/filter synchronization first; then identify which downstream scan or branch actually benefited.

## Effectiveness Rules

Useful filter:

- `FilterRows` is large relative to `InputRows`.
- Scan `RowsRead`/downstream rows are much lower than `ScanRows`.
- Scan-detail pruning counters such as key-range, zone-map, bloom, stats, or condition-filter rows are large.
- Filter wait is small compared with saved scan/join work.
- The target table is large enough that pruning matters.
- The filter is produced by a small/selective build side early enough to avoid large target-side scan or join-build work.

Weak or useless filter:

- `AlwaysTrueFilterRows` dominates.
- `FilterRows = 0` or tiny relative to `InputRows`.
- `WaitTime` is visible but no meaningful rows are filtered.
- Filter arrives after most scan work, or scan had no large table to prune.

Potentially harmful filter:

- Long `WaitForRuntimeFilter`/`AcquireRuntimeFilter` and little filtering.
- Scan `ExecTime` dominated by `RFx WaitTime`, especially on a small dimension table or a branch whose target-side filtering is weak.
- Broadcast or build side is slow, causing scan-side idle time without saving I/O.
- Many filters add predicate overhead while scan rows are already small.
- The filter source is available only after the large table has already been scanned or built into a hash table; this usually points to a join-order/RF-direction problem rather than a scan implementation problem.
- The expensive scan produces the RF, while other large scans wait for it and then skip. If the RF source had to scan huge data to produce an empty/tiny filter, question the source/target choice; the source side may be the side that should have received an earlier RF instead.
- Empty/tiny filter output after a massive source scan is a red flag, not a success signal. Classify the target-side skip as saved downstream work, but classify the source-side massive work as likely bad RF direction unless concrete plan evidence rules out an earlier pruning direction.

## Filter Types

- Bloom/in filter: good for equality join pruning; check false positive and target cardinality indirectly by rows filtered.
- Min/max filter: cheap range pruning; useful only when build-side min/max is selective for the target.
- Bitmap filter: specialized nested-loop/bitmap path; check build bitmap construction and target filtering.
- IN/IN_OR_BLOOM variants: planner/runtime chooses representation based on cardinality and settings.

The filter type alone does not prove benefit. Always use target-side rows and wait.

## Common Mistakes

- Mistaking source/build side for target/scan side because of arrows.
- Treating `WaitForRuntimeFilter` as scan CPU.
- Calling a runtime filter effective from its existence only.
- Treating an active large scan as a pure scan problem without asking whether an earlier RF from a different join order should have pruned it.
- Calling an empty RF useful without asking whether the plan paid too much to produce that empty RF.
- Calling a target-side skip proof of good order while ignoring that source-side scan/build dominated the query.
- Ignoring `AlwaysTrueFilterRows`, which often means the filter was not selective.
- Ignoring that filter counters may be accumulated across parallel scan instances.
- Calling a filter useless only because one `RFx FilterRows` is zero while scan-detail key-range, zone-map, bloom, or stats pruning removed rows.
- Subtracting RF wait counters mechanically from `ExecTime` or from each other despite overlapping scopes.

## Example Pattern

In one calibration profile, the plan showed min/max filters like `RF000[min_max] <- build_key` at the build side and `RF000[min_max] -> scan_key` at a scan. The profile had `RuntimeFilterInfo` with RF wait/filter counters, but the filters waited only tens of milliseconds and filtered no meaningful rows. The real evidence was large scan volume and active aggregation/exchange work, not runtime filter wait.
