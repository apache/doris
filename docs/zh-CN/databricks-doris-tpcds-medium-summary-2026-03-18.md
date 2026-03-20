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

# Databricks and Doris TPC-DS SF1000 Medium Summary

本文汇总了 2026-03-18 基于 `test.tpcds_sf1000_ext` / `databricks_test.tpcds_sf1000_ext`
跑出的 Databricks 与 Doris TPC-DS SF1000 `medium` 结果。

当前文档以新口径为准：

- Databricks 使用 runner 的 `metric_duration_ms`
- Doris 使用 FE query profile 的 `Summary - Total`
- Doris 最新汇总使用 `profile-round1` 主结果加上后续 rerun 覆盖原失败项
- 旧的 Doris wall-time 结果保留，但不再作为主结论

## Environment

| Item | Databricks | Doris |
| --- | --- | --- |
| Endpoint | `https://dbc-8dfd98c8-ab16.cloud.databricks.com` | `3.90.78.41:9030` |
| FE HTTP | N/A | `3.90.78.41:8030` |
| Catalog | `test` | `databricks_test` |
| Schema | `tpcds_sf1000_ext` | `tpcds_sf1000_ext` |
| Size | `medium` | 对齐 `medium` 场景 |
| Query set | `tools/tpcds-tools/queries/sf1000/query1.sql` ~ `query99.sql` | `tools/tpcds-tools/queries/sf1000/query1.sql` ~ `query99.sql` |
| Passes | `cold`, `hot1`, `hot2` | `cold`, `hot1`, `hot2` |
| Runner | `tools/tpcds-tools/bin/run-databricks-tpcds-samples.py` | `tools/tpcds-tools/bin/run-doris-tpcds-samples.py` |
| Doris metric source | N/A | `profile_total` 优先，拿不到才退回 `wall_time` |

## Sources

- Databricks 主结果目录：
  [databricks-test-medium-round2](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-round2)
- Databricks 补跑目录：
  [databricks-test-medium-rerun-8q](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/databricks/databricks-test-medium-rerun-8q)
- Databricks 汇总 README：
  [results/README.md](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/README.md)
- Doris 新口径结果目录：
  [doris-databricks-test-medium-profile-merged](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/doris/doris-databricks-test-medium-profile-merged)
- Doris 合并来源目录：
  [doris-databricks-test-medium-profile-round1](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/doris/doris-databricks-test-medium-profile-round1)
  [doris-databricks-test-medium-profile-rerun-s3-last](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/doris/doris-databricks-test-medium-profile-rerun-s3-last)

当前主结果口径是：

- Databricks：`round2` 为主体，`rerun-8q` 覆盖 8 条失败 SQL
- Doris：`profile-merged` 为当前主结果
- Doris 旧目录 `doris-databricks-test-medium-round1` 等 wall-time 结果仅作历史保留

## Overall

| Run | Total | Succeeded | Failed | Cold Total | Hot1 Total | Hot2 Total | Best Hot Total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Databricks final | `309` | `309` | `0` | `881.702 s` | `936.509 s` | `736.911 s` | `729.024 s` |
| Doris final-so-far | `309` | `306` | `3` | `561.249 s` | `428.794 s` | `331.182 s` | `330.221 s` |

## Metric Source

Databricks：

- 继续使用 runner 里的 `metric_duration_ms`
- 其优先口径是 `system.query.history.total_duration_ms`

Doris：

- 新 runner 已改为优先抓 FE profile 的 `Summary - Total`
- 306 条成功 query 全部是：
  `metric_source=profile_total`
- 没有成功 query 落到 `wall_time_fallback`

因此这版 Doris 结果和 Databricks 相比，已经比早先 wall-time 口径更公平。

## Failed Queries

### Databricks Final

Databricks 最终结果没有剩余失败项。

### Doris Final-So-Far

Doris 在覆盖最新 rerun 后还剩 3 个失败对象：

- `query88:cold`
- `query88:hot1`
- `query88:hot2`

按类型分组：

- timeout 类：
  `query88`

典型报错：

- `query is timeout, killed by timeout checker`

## Doris Rerun Notes

### S3 / ExpiredToken 补跑

目录：

- [doris-databricks-test-medium-rerun-s3-cold-after-refresh](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/doris/doris-databricks-test-medium-rerun-s3-cold-after-refresh)
- [doris-databricks-test-medium-profile-rerun-s3-last](/mnt/disk2/yunyou/chenjunwei/doris/tools/tpcds-tools/results/doris/doris-databricks-test-medium-profile-rerun-s3-last)

结论：

- 在执行 `refresh catalog databricks_test` 后，之前剩余的 7 个 S3 失败 query：
  `query62`
  `query64`
  `query66`
  `query77`
  `query80`
  `query85`
  `query91`
  已经全部恢复。
- 后续 `profile-round1` 中再次出现的 4 个 S3 冷跑失败：
  `query89`
  `query91`
  `query94`
  `query99`
  也已经在 `profile-rerun-s3-last` 中全部恢复。
- 这说明对象本身可以读取，问题更接近 vendored credential 过期或 catalog 缓存刷新时机。

### Timeout Query

`query88` 单独重跑时仍然表现异常缓慢，最终在整轮 `profile-round1` 中 `cold/hot1/hot2` 三轮都超时。

Databricks 同一条 `query88` 的时间大约是：

- `cold=22.968 s`
- `hot1=22.701 s`
- `hot2=22.759 s`

因此 `query88` 当前更像 Doris 执行性能/长尾问题，而不是 profile 统计口径问题。

## Current Status

- Databricks 这边已经是完整的 `medium` 结果。
- Doris 新口径结果已经跑完，且大部分成功行都走了 engine-side profile 时间。
- 当前 Doris 只剩 1 个唯一失败 query：
  - `query88` 三轮 timeout
- 也就是说：
  统计口径已经修正
  绝大多数失败项已经通过 rerun 覆盖
  目前只剩 `q88`

## Notes

- 如果继续补跑 Doris，建议在正式跑前先执行一次：
  `refresh catalog databricks_test`
- 当前文档已经不再使用旧的 wall-time Doris 总表做主比较。
- 旧结果目录仍然保留，方便回溯：
  - `doris-databricks-test-medium-round1`
  - `doris-databricks-test-medium-rerun-s3-cold`
  - `doris-databricks-test-medium-rerun-timeout`
  - `doris-databricks-test-medium-rerun-timeout-qt3600`
