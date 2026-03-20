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

# Doris TPC-DS SF1000 Results

本目录汇总了 2026-03-18 基于 `databricks_test.tpcds_sf1000_ext` 跑出的 Doris TPC-DS SF1000 结果。
当前整理的是 `medium` 对比场景，口径使用 FE query profile 的 `Summary - Total`。

## Environment

| Item | Value |
| --- | --- |
| FE Query | `3.90.78.41:9030` |
| FE HTTP | `3.90.78.41:8030` |
| Catalog | `databricks_test` |
| Schema | `tpcds_sf1000_ext` |
| Test size | `medium` |
| Query set | `tools/tpcds-tools/queries/sf1000/query1.sql` ~ `query99.sql` |
| Passes | `cold`, `hot1`, `hot2` |
| Runner | `tools/tpcds-tools/bin/run-doris-tpcds-samples.py` |
| Metric source | `profile_total` |

## Sources

- 主结果目录：
  [doris-databricks-test-medium-profile-merged](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/doris/doris-databricks-test-medium-profile-merged)
- 合并来源目录：
  [doris-databricks-test-medium-profile-round1](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/doris/doris-databricks-test-medium-profile-round1)
  [doris-databricks-test-medium-profile-rerun-s3-last](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/doris/doris-databricks-test-medium-profile-rerun-s3-last)

当前结果口径是：

- 以 `profile-merged` 为当前最佳结果
- 该目录由 `profile-round1` 与 `profile-rerun-s3-last` 合并而成
- 当前仍有 1 条 SQL 未完全收敛：
  - `query88`

## Overall

| Metric | Value |
| --- | --- |
| Query count | `103` |
| Succeeded Objects | `306` |
| Failed Objects | `3` |
| Cold Total | `561.249 s` |
| Hot1 Total | `428.794 s` |
| Hot2 Total | `331.182 s` |
| Best Hot Total | `330.221 s` |

## Heaviest Queries

### Cold Top 10

| Query | Time |
| --- | --- |
| `query4` | `23.111 s` |
| `query9` | `21.036 s` |
| `query78` | `18.725 s` |
| `query23` | `14.475 s` |
| `query11` | `14.345 s` |
| `query23_1` | `13.886 s` |
| `query28` | `12.997 s` |
| `query13` | `12.344 s` |
| `query67` | `12.108 s` |
| `query75` | `11.974 s` |

### Best Hot Top 10

| Query | Time |
| --- | --- |
| `query4` | `19.006 s` |
| `query23_1` | `13.695 s` |
| `query23` | `13.530 s` |
| `query11` | `13.111 s` |
| `query67` | `11.975 s` |
| `query78` | `11.909 s` |
| `query9` | `11.663 s` |
| `query14_1` | `9.948 s` |
| `query28` | `9.207 s` |
| `query68` | `9.195 s` |

## Notes

- 这份结果已经把 `profile-round1` 中剩余的 4 条 S3 / `ExpiredToken` 冷跑失败通过补跑覆盖掉，所以当前只剩 `query88` 三轮 timeout。
- 汇总时间口径使用当前 Doris runner 里的 `metric_duration_ms`：
  - 优先取 FE query profile 的 `Summary - Total`
  - 如果 profile 没取到，再退回脚本本地的 `wall_duration_ms`
- 因为 `query88:cold/hot1/hot2` 仍失败，所以这份结果是当前最佳结果，但还不是完整的 `309/309` 最终结果。
- 旧的 Doris wall-time 结果仍保留在其他目录里，仅作历史参考，不再用于主结论。
