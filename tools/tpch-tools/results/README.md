<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Databricks TPC-H Result Summary

This directory stores three Databricks SQL Warehouse TPC-H SF1000 result sets collected on 2026-03-15:

| Label | Run ID | Warehouse ID | Size | Scope |
| --- | --- | --- | --- | --- |
| medium baseline | `20260315T041645Z` | `8aa191202f247888` | `medium` | Full 22-query run, 3 passes per query |
| medium q20 rerun | `20260315T044033Z` | `8aa191202f247888` | `medium` | `q20` rerun only, 3 passes |
| large baseline | `20260315T044414Z` | `06d5d2d7689aec4a` | `large` | Full 22-query run, 3 passes per query |

All runs target `catalog=workspace`, `schema=tpch_sf1000_ext`, and the pass order `cold`, `hot1`, `hot2`.

## High-Level Results

| View | Successful statements | Failed statements | Cold total | Hot1 total | Hot2 total | Best-hot total | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| medium baseline | 64 / 66 | 2 | 474.749 s | 16.703 s | 13.462 s | 12.874 s | `q20` failed in `cold` and `hot2` |
| medium q20 rerun | 3 / 3 | 0 | 16.384 s | 3.469 s | 3.136 s | 3.136 s | rerun of `q20` only |
| medium merged view | 66 / 66 | 0 | 491.133 s | 16.961 s | 16.598 s | 16.010 s | baseline with `q20` replaced by rerun |
| large baseline | 64 / 66 | 2 | 43.513 s | 36.400 s | 35.413 s | 32.435 s | `q16` failed in `cold` and `hot1` |

`medium merged view` is not a raw artifact. It is the practical full-run view formed by keeping the original medium baseline and replacing the failed `q20` row with the successful rerun.

## Run-By-Run Findings

### Medium Baseline

- The baseline completed 21 of 22 queries across all three passes. The only unstable query was `q20`.
- `q20` failed in `cold` and `hot2` with `IncompleteRead(4296009 bytes read)`, but `hot1` succeeded in `3.211 s`.
- The slowest cold queries were `q5` (`71.708 s`), `q21` (`50.399 s`), `q9` (`46.629 s`), `q18` (`43.876 s`), and `q4` (`34.846 s`).
- The slowest best-hot queries among the fully successful baseline queries were `q19` (`1.362 s`), `q22` (`1.356 s`), `q21` (`1.319 s`), `q5` (`0.569 s`), and `q2` (`0.561 s`).

### Medium q20 Rerun

- The rerun shows that `q20` can complete successfully on the same medium warehouse.
- `q20` finished in `16.384 s` for `cold`, `3.469 s` for `hot1`, and `3.136 s` for `hot2`.
- After merging the rerun back into the medium baseline, the medium result becomes a full `22 / 22` successful query set.

### Large Baseline

- The large warehouse also completed 21 of 22 queries across all three passes. The unstable query here was `q16`.
- `q16` failed in `cold` with `IncompleteRead(703547 bytes read)` and in `hot1` with `IncompleteRead(672370 bytes read)`, then succeeded in `hot2` with `2.383 s`.
- The slowest cold queries were `q1` (`7.662 s`), `q2` (`4.379 s`), `q20` (`3.259 s`), `q3` (`2.210 s`), and `q7` (`1.609 s`).
- The slowest best-hot queries were `q2` (`3.246 s`), `q20` (`3.076 s`), `q1` (`1.521 s`), `q3` (`1.441 s`), and `q8` (`1.424 s`).

## Interpretation Notes

- The cold totals are the most stable cross-run comparison point in the current artifacts. On the 21 queries that have complete three-pass data in both warehouse sizes, the large warehouse cold total is `43.513 s` versus `481.283 s` for the medium merged view, or about `11.06x` faster.
- The hot totals are not directly comparable without caveats. The medium baseline `queries.csv` includes `from_result_cache`, and the CSV explicitly marks `18` `hot1` rows plus `18` `hot2` rows as cache hits. Three `cold` rows are also explicitly marked as cache hits.
- The large baseline and the `q20` rerun only store `metric_duration_ms` and do not expose the cache-hit flag or the detailed execution metrics present in the medium baseline output. This means the hot-path comparison is not apples-to-apples.
- Because `q20` and `q16` both succeed in at least one pass, the failures look more like intermittent read or fetch instability than deterministic SQL correctness issues.

## Recommended Follow-Up

- Keep the medium merged view as the current medium reference result, because it restores the missing `q20` measurements without changing any other query.
- Rerun `q16` on the large warehouse to close the only missing query in that run.
- If these results will be used for warehouse sizing or engine comparisons, disable or clear result cache before each pass, or at minimum capture cache-hit metadata consistently for every run.
