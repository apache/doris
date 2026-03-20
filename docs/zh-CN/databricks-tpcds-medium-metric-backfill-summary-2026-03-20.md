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

# Databricks TPC-DS SF1000 Medium Metric Backfill Summary

本文记录 2026-03-20 对 Databricks TPC-DS SF1000 `medium` 结果做的两轮 metric backfill 补跑。
本说明只覆盖这次补跑，不改原有总表文档，也不改 `tools/tpcds-tools/results/README.md` 的历史描述。

## 来源目录

- 原始主结果目录：
  [databricks-test-medium-round2](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-round2)
- 第一轮补跑目录：
  [databricks-test-medium-missing-round1](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-missing-round1)
- 第二轮补跑目录：
  [databricks-test-medium-missing-round2](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-missing-round2)

## 背景

原始 `round2` 结果的统计口径仍然是 runner 里的 `metric_duration_ms`，其优先来源是
`system.query.history.total_duration_ms`，拿不到时才退回 `wall_duration_ms`。

在原始主结果中，有一批 query 虽然执行成功，但没有及时回填到
`system.query.history.total_duration_ms`，因此落成了 `wall_duration_ms` fallback。
这会让 Databricks 侧与 Doris 侧按 engine-side metric 对比时不够稳定，所以在 2026-03-20
针对缺失项补跑了两轮。

## 补跑范围

第一轮补跑覆盖了 17 个 query：

`query16`, `query32`, `query50`, `query62`, `query87`, `query88`, `query89`, `query90`,
`query91`, `query92`, `query93`, `query94`, `query95`, `query96`, `query97`, `query98`,
`query99`

执行口径保持不变：

- 查询集：`tools/tpcds-tools/queries/sf1000`
- Pass：`cold`, `hot1`, `hot2`
- Catalog / Schema：`test.tpcds_sf1000_ext`
- Warehouse size：`medium`

## Round 1 结果

结果文件：

- [20260320T114043Z_summary.json](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-missing-round1/20260320T114043Z_summary.json)
- [20260320T114043Z_query_summary.csv](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-missing-round1/20260320T114043Z_query_summary.csv)

| Item | Value |
| --- | --- |
| Total | `51` |
| Succeeded | `51` |
| Failed | `0` |
| `query_history_total_duration_ms` | `34` |
| `wall_duration_ms_fallback` | `17` |

Round 1 结论：

- 第一轮补跑全部执行成功。
- 17 个 query 对应的 51 行里，有 34 行补成了 `query_history_total_duration_ms`。
- 还剩 17 行仍然是 fallback。

Round 1 补跑后仍未补上的对象是：

- `query94`: `hot1`, `hot2`
- `query95`: `cold`, `hot1`, `hot2`
- `query96`: `cold`, `hot1`, `hot2`
- `query97`: `cold`, `hot1`, `hot2`
- `query98`: `cold`, `hot1`, `hot2`
- `query99`: `cold`, `hot1`, `hot2`

## Round 2 结果

结果文件：

- [20260320T115616Z_summary.json](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-missing-round2/20260320T115616Z_summary.json)
- [20260320T115616Z_query_summary.csv](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-missing-round2/20260320T115616Z_query_summary.csv)

第二轮只补跑了 Round 1 后仍未补上的 6 个 query：

`query94`, `query95`, `query96`, `query97`, `query98`, `query99`

| Item | Value |
| --- | --- |
| Total | `18` |
| Succeeded | `18` |
| Failed | `0` |
| `query_history_total_duration_ms` | `17` |
| `wall_duration_ms_fallback` | `1` |

Round 2 结论：

- 第二轮补跑也全部执行成功。
- 18 行里有 17 行补成了 `query_history_total_duration_ms`。
- 只剩 `query99:hot2` 仍然是 `wall_duration_ms_fallback`。

## 当前最终状态

把下面三组结果按同一 `(query, pass)` 口径合并后：

- 原始主结果 [20260318T081120Z_queries.csv](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-round2/20260318T081120Z_queries.csv)
- 第一轮补跑 [20260320T114043Z_queries.csv](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-missing-round1/20260320T114043Z_queries.csv)
- 第二轮补跑 [20260320T115616Z_queries.csv](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-missing-round2/20260320T115616Z_queries.csv)

当前 Databricks `medium` 最终状态是：

| Metric Source | Rows |
| --- | ---: |
| `query_history_total_duration_ms` | `308` |
| `wall_duration_ms_fallback` | `1` |

唯一剩余的 fallback 项是：

- `query99:hot2`

因此，这次 2026-03-20 的两轮补跑已经把原先缺失 history metric 的绝大部分行补齐，
当前只剩 1 个尾部对象未回填，Databricks `medium` 结果已经可以认为接近完全使用
`system.query.history.total_duration_ms` 口径。
