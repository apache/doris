# Join Order Diagnosis

Use this file after the normal bottleneck pass. Keep the skill's main discipline: first prove the immediate runtime cost, then decide whether that cost is a symptom of a bad join order, bad build/probe choice, or late/missing runtime filter.

## 1. Identify Build, Probe, Source, and Target

Hash join:

- `HASH_JOIN_SINK_OPERATOR` is the build side. Its decisive counters are `InputRows`/`BuildRows`, `BuildTime`, `BuildHashTableTime`, `BuildTableInsertTime`, `MemoryUsageHashTable`, `MemoryUsageBuildBlocks`, and `RuntimeFilterInfo`.
- `HASH_JOIN_OPERATOR` is the probe side. Its decisive counters are `ProbeRows`, `ProbeIntermediateRows`, probe expression/search/output timers, and final `RowsProduced`.
- In non-pipeline or older profiles, a single `VHASH_JOIN_NODE` may contain both sides. Use `BuildRows`, `ProbeRows`, `BuildTime`, `ProbeTime`, hash-table memory, and plan child order/RF arrows to identify the sides.
- In Doris plans, the hash table is normally built from the join's build/right child, but verify with counters or RF arrows when the profile is merged or the plan has been transformed.

Nested-loop/cross join:

- `CROSS_JOIN_SINK_OPERATOR` is the build/materialized side.
- `CROSS_JOIN_OPERATOR` is the probe/loop side. Its decisive counters are `ProbeRows`, `LoopGenerateJoin`, `JoinConjunctsEvaluationTime`, `FilteredByJoinConjunctsTime`, and `OutputTempBlocksTime`.
- A nested-loop join over a large intermediate result is often a plan-shape problem even when the direct operator cost is correctly attributed to nested-loop active timers.

Runtime filters:

- `RFxxx <- expr` is the source/build side that produces the filter.
- `RFxxx -> expr` is the target/probe scan side that may be pruned.
- A useful order usually produces filters from a small/selective build side early enough to prune a large probe-side scan. A suspicious order scans/builds a large fact side before the small/selective branch can produce its RF.
- An empty RF is not automatically a good outcome. If the query spends most of its time scanning the RF source just to produce an empty/tiny filter, then the RF source side may be the wrong side even though downstream targets skipped work.
- Empty downstream target scans prove only that the RF arrived before those targets did work. They do not prove the source choice was good. If the source scan/build dominates elapsed time, treat the plan as "RF direction likely bad: the expensive side produced the pruning signal instead of receiving one" unless there is concrete counterevidence.

Memo and plan alternatives:

- If memo or verbose explain contains multiple physical expressions for the same join group, inspect both child orders. A reversed physical expression is valid counterfactual evidence even when no fast profile is present.
- Compare the chosen side against table keys, distribution keys, colocate/bucket properties, and available RF targets. If the chosen RF source uses non-key predicates and scans massively, while the other side's join key can target the source table's key/prefix/distribution key, classify the chosen order as suspicious or bad.
- Do not accept the optimizer's estimated lower cost as proof of correctness when actual profile rows contradict the estimate. A low estimated build cardinality that turns into a huge zero-output scan is evidence of bad join-order costing.

## 2. Decide Whether the Join Order Is Reasonable

Treat join-order diagnosis as a plan-shape layer, separate from immediate runtime cost. Report both layers.

Strong evidence of bad join order:

- A paired fast/slow profile, hint, or SQL rewrite with the same intended result changes only join shape/RF direction and turns a large scan/hash build into tiny or zero scan rows.
- A large fact table is scanned or used as hash-build input before a selective dimension/subquery branch is reduced to a few rows.
- The slow plan builds a hash table on millions/billions of rows while an alternate legal shape builds on a tiny side and uses RF to prune the large side.
- A scan is the active bottleneck only because the RF that could prune it is produced later, on the wrong side, or after the scan has already done most work.
- The plan creates a huge intermediate result and a later join/filter reduces it to zero or a tiny row count, especially with contradictory predicates or predicates that could have been pushed below the join.
- Estimates in explain/memo say a branch is large, but profile actual rows show it is tiny, causing the optimizer to choose the wrong build/probe side.

Single slow profile evidence can also be strong enough. Do not require a fast profile when these patterns appear:

- Empty-probe or empty-result build waste: a hash join builds/shuffles millions or billions of rows, but `ProbeRows` or final output is zero/tiny. The direct cost is build/scan, but the plan-shape cause is that the large build side was scheduled before proving the probe/output side was empty or highly selective. If join type semantics make a swap uncertain, say "likely bad join order/build-side scheduling" rather than "not join order".
- RF-source inversion: the current RF source/build side is itself the expensive unpruned scan and produces zero/tiny rows only after scanning a huge table. Its RF target scans then wait and skip work. This is not merely a scan/index problem; it means the plan chose the side that needed pruning as the side responsible for producing the pruning signal. Check whether the expensive side's join key is a table key/prefix, distribution key, or colocated key and whether no RF is applied to that side.
- Expensive-empty source: a scan/build side does massive active work and outputs zero/tiny rows, then its RF makes other branches skip. Classify this as likely bad join order/RF direction even if that branch also has a missing index, non-key predicate, bad literal, or stale estimate. The other anomaly explains why the branch is expensive or empty; it does not make the expensive branch a good RF source.
- Late emptying predicate: a later join, semi join, analytic/filter, or non-equi conjunct reduces a large intermediate to zero/tiny rows. If that predicate references only a subset of the joined tables or is contradictory, classify the earlier large join/intermediate as bad join order or missed predicate placement.
- Ineffective RF to a large side: the plan contains an RF targeting a large scan, but profile shows it is ignored, not pushed down, arrives after scan work, has zero filter rows, or the large scan still produces millions of rows. If a small/selective branch exists, this is evidence of bad RF direction/timing even without a fast profile.
- Estimate/actual inversion: the optimizer estimates the build/source side tiny, but profile proves it performed massive scan work to produce zero/tiny rows, while downstream tables only waited for its RF. Treat this as a likely join-order/RF-direction error. Do not stop at "the build side scan is slow" unless the profile proves no join alternative could prune it.
- Key-target inversion: the expensive source table is scanned by non-key/non-prefix predicates, while its join key is a key prefix, distribution key, or otherwise better pruning target. If current order makes that table produce the RF instead of receive one, call the join order/RF direction bad unless profile proves the alternative cannot prune it.
- Huge build for empty/tiny probe: a hash join build side scans/shuffles/builds millions or billions of rows while the probe side is zero/tiny and final output is zero/tiny. Report direct cost as build/scan, and report plan-shape root cause as likely bad join order/build-side scheduling even for an outer join unless the profile proves no earlier empty-preserved-side check or legal reorder can avoid the build.
- Contradictory non-equi join late: an inner nested-loop/cross join has conjuncts that cannot both be true, or a highly selective non-equi relation, but the plan first materializes a large intermediate. This is not just expression simplification; it is bad join order/predicate placement because the impossible/selective relation was applied after the row explosion.

When any strong single-profile pattern above matches, do not write "join order not proven" as the main diagnosis. Use one of these verdicts:

- `bad`: a paired plan, memo alternative, legal rewrite, key/RF target evidence, or inner-join contradiction shows the expensive work should have been avoided.
- `likely bad`: the profile alone proves huge wasted scan/build/intermediate work before an empty/tiny side or late predicate, but legal alternatives still need confirmation.
- `suspicious`: only for weak patterns where build/probe/RF sides or row-flow evidence are incomplete. Do not use `suspicious` for expensive-empty source, huge build for empty/tiny probe, tiny semi key after fact build, or contradictory non-equi join late.

Keep uncertainty scoped correctly:

- It is acceptable to say the exact fix or alternate legal order is not proven.
- It is not acceptable to demote the diagnosis to "not join order" or "only scan/literal/index problem" when the profile already proves the large side was paid before an empty/tiny side could prune it.
- In the conclusion, put the join verdict before coexisting anomalies when the user's question is profile performance: "runtime bottleneck is X; join order/build scheduling is likely bad; bad literal/stats/index may explain why the selective side became empty."

Weaker evidence:

- A single profile shows large scan/build work and late/weak RF, but there is no alternate plan, no SQL/hint comparison, and no stats/memo evidence.
- Outer/semi/anti joins restrict legal reorder. A costly order may be required by semantics unless an equivalent rewrite or optimizer rule is known.

Do not stop at "scan bottleneck" when the scan exists because of join order. Say: "runtime bottleneck is the scan/hash build; likely plan-shape root cause is join order/RF direction because ..."

## 3. Join Type Constraints

- Inner joins are usually reorderable; choose the shape that builds on the smaller/selective side and probes/prunes the larger side.
- For left outer join, the left side is preserved. For right outer join, the right side is preserved. Swapping can change semantics unless the optimizer rewrites the join equivalently.
- Outer join preservation does not make every build/probe choice good. If a preserved-side scan/probe is empty after its own predicates while the non-preserved build side was already fully scanned/built, report the direct build waste and classify the order as likely bad scheduling or missed empty-side short-circuit/predicate placement. Keep the semantic caveat, but do not downgrade it to a pure scan problem.
- For outer joins, distinguish logical join order from physical work order. Even when logical reordering is constrained, a profile can still prove bad physical build-side scheduling if Doris builds the non-preserved side before discovering the preserved side has zero rows.
- For left outer joins in hash execution, `ProbeRows=0` with huge right/build `BuildRows` is a likely bad physical work order: the build side was paid before the preserved side was known empty. State that even if the SQL contains another bug, and make the legal-rewrite caveat secondary.
- For semi/anti joins, the preserved/output side and filter side matter. A good order applies the selective semi/anti result before scanning or building large downstream joins when legal.
- For semi joins, a tiny key set from the filter side that is applied only after a large fact scan/build is strong evidence of bad join order or RF timing.
- For non-equi joins, nested-loop may be required, but the order can still be bad if it materializes a huge intermediate before applying a selective or impossible predicate. A contradictory join conjunct on an inner join is a join-order/predicate-placement failure when earlier joins create large rows that the contradictory join later discards. State "join order/predicate placement is bad"; do not stop at "constant folding/predicate simplification missing".

## 4. Distinguish Join Order From Other Anomalies

Other anomalies can coexist with bad join order:

- Bad literal rewrite, stale stats, missing index, weak partition pruning, or an expensive scalar expression may explain why one branch is selective or empty.
- They do not explain why the engine scanned/built another huge side before using that selectivity, or why an empty/tiny branch was unable to prune the expensive side.
- State both: "runtime bottleneck is large scan/build; another anomaly explains selectivity; join order/RF direction is still wrong because the plan pays the large side before the selective/empty branch can prune it."

Avoid this failure mode: calling the expensive RF-source scan a pure access-path problem when the rest of the plan proves that all other large scans waited for its empty/tiny RF. That is exactly the case where the RF source/target choice should be challenged.

Avoid this failure mode: calling a large build before an empty probe "only an outer join semantic constraint". If the preserved side is empty after its own predicates, then the build side contributed nothing to the result; the plan order/scheduling failed to exploit the empty side.

Avoid this failure mode: calling an impossible non-equi join only a predicate simplification issue. If a large intermediate is produced before the impossible join, the bad symptom is join order/predicate placement: the impossible or highly selective relation was applied too late.

## 5. Output Checklist

When a query has joins, include these facts:

- `Runtime bottleneck`: the active scan/join/build/probe cost proven by counters.
- `Join-order diagnosis`: whether the order/build side/RF direction is good, suspicious, or bad.
- `Build/probe evidence`: build side rows/time/memory, probe side rows/time, and join type.
- `RF evidence`: source side, target side, wait, filtering/pruning, and whether it arrived early enough.
- `Counterfactual evidence`: paired profile, hint, rewrite, memo, or stats that shows a better legal order. If absent, say the join-order conclusion is likely but not proven.
