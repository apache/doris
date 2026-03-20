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

# Databricks TPC-DS SF1000 Results

本目录汇总了 2026-03-18 基于新 workspace `test.tpcds_sf1000_ext` 跑出的 Databricks TPC-DS SF1000 结果。
当前先整理 `medium` 结果，口径使用 runner 默认的 `metric_duration_ms` 汇总。

## Environment

| Item | Value |
| --- | --- |
| Workspace | `https://dbc-8dfd98c8-ab16.cloud.databricks.com` |
| Catalog | `test` |
| Schema | `tpcds_sf1000_ext` |
| Warehouse | `test` / `19f0f7c7efcf6c90` |
| Warehouse size | `medium` |
| Query set | `tools/tpcds-tools/queries/sf1000/query1.sql` ~ `query99.sql` |
| Passes | `cold`, `hot1`, `hot2` |
| Runner | `tools/tpcds-tools/bin/run-databricks-tpcds-samples.py` |

## Sources

- 主结果目录：
  [databricks-test-medium-round2](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-round2)
- 补跑目录：
  [databricks-test-medium-rerun-8q](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-rerun-8q)

最终结果口径是：

- 以 `round2` 为主体
- 把这 8 条补跑结果覆盖进去：
  - `query16`
  - `query32`
  - `query50`
  - `query62`
  - `query92`
  - `query94`
  - `query95`
  - `query99`

## Overall

| Metric | Value |
| --- | --- |
| Query count | `103` |
| Cold Total | `881.702 s` |
| Hot1 Total | `936.509 s` |
| Hot2 Total | `736.911 s` |
| Best Hot Total | `729.024 s` |

## Heaviest Queries

### Cold Top 10

| Query | Time |
| --- | --- |
| `query16` | `88.693 s` |
| `query78` | `28.711 s` |
| `query22` | `27.790 s` |
| `query67` | `27.237 s` |
| `query4` | `24.812 s` |
| `query23` | `23.611 s` |
| `query14` | `23.191 s` |
| `query75` | `23.001 s` |
| `query88` | `22.968 s` |
| `query23_1` | `22.358 s` |

### Best Hot Top 10

| Query | Time |
| --- | --- |
| `query78` | `28.477 s` |
| `query22` | `26.918 s` |
| `query67` | `26.253 s` |
| `query23` | `23.578 s` |
| `query23_1` | `22.875 s` |
| `query88` | `22.701 s` |
| `query75` | `22.545 s` |
| `query14` | `21.435 s` |
| `query14_1` | `21.263 s` |
| `query4` | `19.411 s` |

## Notes

- 这份结果已经把 `round2` 中剩余的 8 条 SQL 兼容性问题通过补跑覆盖掉，所以当前可以视为完整的 `medium` TPC-DS SF1000 结果。
- 汇总时间口径使用当前 runner 里的 `metric_duration_ms`：
  - 优先取 `system.query.history.total_duration_ms`
  - 如果系统表没及时回填，再退回脚本本地的 `wall_duration_ms`
- 当前还没有整理 `large` 结果，因此本 README 只记录 `medium`。
