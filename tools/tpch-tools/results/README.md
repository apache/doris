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

# Databricks TPC-H 结果总结

这个目录保存了 2026-03-15 采集的 3 份 Databricks SQL Warehouse TPC-H SF1000 原始结果。其中 medium 的最终口径由基线结果加 `q20` 补跑结果合并得到，large 则直接使用基线结果。

| 原始结果 | Run ID | Warehouse ID | 规格 | 范围 |
| --- | --- | --- | --- | --- |
| medium 基线 | `20260315T041645Z` | `8aa191202f247888` | `medium` | 完整 22 条查询，每条执行 3 个 pass |
| medium q20 补跑 | `20260315T044033Z` | `8aa191202f247888` | `medium` | 只补跑 `q20`，共 3 个 pass |
| large 基线 | `20260315T044414Z` | `06d5d2d7689aec4a` | `large` | 完整 22 条查询，每条执行 3 个 pass |

所有结果都基于 `catalog=workspace`、`schema=tpch_sf1000_ext`，执行顺序都是 `cold`、`hot1`、`hot2`。

## 总览

| 视图 | 成功语句数 | 失败语句数 | Cold 总耗时 | Hot1 总耗时 | Hot2 总耗时 | Best-hot 总耗时 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| medium 最终结果 | 66 / 66 | 0 | 491.133 s | 16.961 s | 16.598 s | 16.010 s | 主体来自 medium 基线，`q20` 使用补跑结果补齐 |
| large 基线 | 64 / 66 | 2 | 43.513 s | 36.400 s | 35.413 s | 32.435 s | `q16` 在 `cold` 和 `hot1` 失败 |

`medium 最终结果` 不是单个原始产物，而是保留 medium 基线的全部结果，再用 `q20` 的成功补跑结果替换失败记录后得到的完整视图。

## 分组结论

### Medium 最终结果

- Medium 最终结果以 `20260315T041645Z` 为主体，`q20` 使用 `20260315T044033Z` 的补跑结果补齐，因此最终得到完整的 `22 / 22` 成功结果。
- 原始 medium 基线里，`q20` 在 `cold` 和 `hot2` 都报了 `IncompleteRead(4296009 bytes read)`，但补跑后 `cold`、`hot1`、`hot2` 分别成功，耗时 `16.384 s`、`3.469 s`、`3.136 s`。
- Cold 最慢的 5 条查询是 `q5`（`71.708 s`）、`q21`（`50.399 s`）、`q9`（`46.629 s`）、`q18`（`43.876 s`）和 `q4`（`34.846 s`）。
- Best-hot 最慢的 5 条查询是 `q20`（`3.136 s`）、`q19`（`1.362 s`）、`q22`（`1.356 s`）、`q21`（`1.319 s`）和 `q5`（`0.569 s`）。

### Large 基线

- large 仓库这组结果也是 22 条查询里有 21 条在 3 个 pass 上都执行成功，唯一不稳定的是 `q16`。
- `q16` 在 `cold` 报 `IncompleteRead(703547 bytes read)`，在 `hot1` 报 `IncompleteRead(672370 bytes read)`，但在 `hot2` 成功，耗时 `2.383 s`。
- Cold 最慢的 5 条查询是 `q1`（`7.662 s`）、`q2`（`4.379 s`）、`q20`（`3.259 s`）、`q3`（`2.210 s`）和 `q7`（`1.609 s`）。
- Best-hot 最慢的 5 条查询是 `q2`（`3.246 s`）、`q20`（`3.076 s`）、`q1`（`1.521 s`）、`q3`（`1.441 s`）和 `q8`（`1.424 s`）。

## 解读注意事项

- 目前这批产物里，Cold 总耗时是更稳定的横向比较口径。在两种 warehouse 规格下都拥有完整三次执行结果的 21 条查询上，large 的 Cold 总耗时是 `43.513 s`，medium 最终结果是 `481.283 s`，也就是 large 大约快 `11.06x`。
- Hot 总耗时不能直接横比。medium 基线的 `queries.csv` 带有 `from_result_cache` 字段，其中明确标记了 `18` 条 `hot1` 和 `18` 条 `hot2` 为缓存命中，另外还有 `3` 条 `cold` 也被明确标记为缓存命中。
- medium 最终结果里的 `q20` 补跑，以及 large 基线结果，都只保留了 `metric_duration_ms`，没有暴露像 medium 基线那样的缓存命中标记和更细的执行指标，所以 hot 路径的对比不是严格同口径。
- `q20` 和 `q16` 都至少在一个 pass 上成功过，因此这些失败更像是间歇性的读取或结果拉取不稳定，而不是 SQL 本身存在确定性错误。

## 后续建议

- 当前 medium 结果建议以 `medium 最终结果` 为准，因为它只补齐了缺失的 `q20`，没有改动其他查询的结果。
- 建议在 large warehouse 上补跑 `q16`，把这组结果里唯一缺失的查询补完整。
- 如果后续要把这些结果用于 warehouse 选型或引擎对比，建议在每个 pass 前显式关闭或清理结果缓存，至少也要保证每次运行都一致地记录 cache-hit 元数据。
