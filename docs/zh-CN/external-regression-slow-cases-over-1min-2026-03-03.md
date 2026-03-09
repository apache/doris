# Doris PR 外表流水线 externregression >1分钟 Case 全量分析（2026-03-03）

## 1. 数据范围与方法
- 样本构建：`build 897097`（[PR #60934](https://github.com/apache/doris/pull/60934)）与 `build 897373`（[PR #60795](https://github.com/apache/doris/pull/60795)）。
- 固定映射：build 897097 -> [PR #60934](https://github.com/apache/doris/pull/60934)，build 897373 -> [PR #60795](https://github.com/apache/doris/pull/60795)。
- 统计口径：基于 `ScriptContext.groovy:100 -> 120` 的 case 执行窗口。
- 分析口径：每条 case 给出统一四项：`耗时画像`、`脚本特征`、`慢点判断`、`波动判断`。
- 口径说明：本文耗时结论来自 `2026-03-03` 的历史 externregression 样本；若 `master` 后续改动了 suite 标签或脚本循环，需以“当前代码状态”说明为准。

## 2. Case 总览（51条）

| Rank | Case | 897097(s, [PR #60934](https://github.com/apache/doris/pull/60934)) | 897373(s, [PR #60795](https://github.com/apache/doris/pull/60795)) | Avg(s) | Delta(s) | Delta% |
|---:|---|---:|---:|---:|---:|---:|
| 1 | `test_trino_hive_tpch_sf1_parquet` | 1063.158 | 1012.972 | 1038.065 | 50.186 | 4.83% |
| 2 | `test_iceberg_table_cache` | 785.451 | 620.980 | 703.216 | 164.471 | 23.39% |
| 3 | `test_hive_text_write_insert` | 670.228 | 567.349 | 618.788 | 102.879 | 16.63% |
| 4 | `test_hive_compress_type` | 345.079 | 270.609 | 307.844 | 74.470 | 24.19% |
| 5 | `refactor_storage_backup_restore_object_storage` | 319.537 | 309.864 | 314.700 | 9.673 | 3.07% |
| 6 | `test_iceberg_statistics` | 303.172 | 301.740 | 302.456 | 1.432 | 0.47% |
| 7 | `test_hive_special_char_partition` | 262.755 | 262.657 | 262.706 | 0.098 | 0.04% |
| 8 | `refactor_storage_param_s3_load` | 245.681 | 238.083 | 241.882 | 7.598 | 3.14% |
| 9 | `test_hive_write_insert` | 237.872 | 153.236 | 195.554 | 84.636 | 43.28% |
| 10 | `test_hive_write_partitions` | 235.007 | 167.840 | 201.423 | 67.167 | 33.35% |
| 11 | `test_paimon_catalog` | 214.931 | 167.496 | 191.214 | 47.435 | 24.81% |
| 12 | `test_paimon_table_meta_cache` | 205.898 | 178.956 | 192.427 | 26.942 | 14.00% |
| 13 | `test_hms_event_notification_multi_catalog` | 186.982 | 186.193 | 186.588 | 0.789 | 0.42% |
| 14 | `test_hms_event_notification` | 186.590 | 185.182 | 185.886 | 1.408 | 0.76% |
| 15 | `test_hive_warmup_select` | 186.411 | 173.103 | 179.757 | 13.308 | 7.40% |
| 16 | `test_hive_meta_cache` | 154.177 | 155.927 | 155.052 | 1.750 | 1.13% |
| 17 | `test_tvf_p0` | 150.506 | 149.623 | 150.065 | 0.883 | 0.59% |
| 18 | `test_paimon_statistics` | 148.172 | 147.641 | 147.906 | 0.531 | 0.36% |
| 19 | `test_hdfs_json_load` | 147.645 | 144.028 | 145.837 | 3.617 | 2.48% |
| 20 | `test_hive_orc` | 147.538 | 105.816 | 126.677 | 41.722 | 32.94% |
| 21 | `test_hive_metadata_refresh_interval` | 142.044 | 141.729 | 141.887 | 0.315 | 0.22% |
| 22 | `test_complex_types` | 141.834 | 117.988 | 129.911 | 23.846 | 18.36% |
| 23 | `test_streaming_mysql_job` | 139.184 | 137.976 | 138.580 | 1.208 | 0.87% |
| 24 | `test_hive_multi_partition_mtmv` | 138.620 | 136.593 | 137.606 | 2.027 | 1.47% |
| 25 | `test_streaming_postgres_job` | 123.166 | 123.114 | 123.140 | 0.052 | 0.04% |
| 26 | `test_file_meta_cache` | 120.833 | 117.393 | 119.113 | 3.440 | 2.89% |
| 27 | `test_streaming_mysql_job_errormsg` | 114.657 | 113.301 | 113.979 | 1.356 | 1.19% |
| 28 | `test_iceberg_write_insert` | 105.043 | 107.527 | 106.285 | 2.484 | 2.34% |
| 29 | `test_wide_table` | 101.693 | 69.648 | 85.671 | 32.045 | 37.40% |
| 30 | `test_iceberg_insert_overwrite` | 101.509 | 100.131 | 100.820 | 1.378 | 1.37% |
| 31 | `test_hive_statistic` | 98.536 | 72.968 | 85.752 | 25.568 | 29.82% |
| 32 | `test_file_cache_query_limit` | 86.909 | 73.970 | 80.440 | 12.939 | 16.09% |
| 33 | `paimon_time_travel` | 85.783 | 72.239 | 79.011 | 13.544 | 17.14% |
| 34 | `test_orc_tiny_stripes` | 78.980 | 58.390 | 68.685 | 20.590 | 29.98% |
| 35 | `test_hive_refresh_mtmv` | 75.421 | 77.781 | 76.601 | 2.360 | 3.08% |
| 36 | `test_streaming_postgres_job_all_type` | 76.851 | 75.282 | 76.066 | 1.569 | 2.06% |
| 37 | `test_streaming_mysql_job_all_type` | 76.621 | 76.095 | 76.358 | 0.526 | 0.69% |
| 38 | `test_hdfs_parquet_group0` | 73.867 | 76.490 | 75.178 | 2.623 | 3.49% |
| 39 | `test_jdbc_row_count` | 73.576 | 75.177 | 74.376 | 1.601 | 2.15% |
| 40 | `test_streaming_mysql_job_priv` | 74.121 | 72.405 | 73.263 | 1.716 | 2.34% |
| 41 | `test_hive_ctas` | 73.965 | 65.354 | 69.660 | 8.611 | 12.36% |
| 42 | `paimon_base_filesystem` | 73.930 | 71.129 | 72.530 | 2.801 | 3.86% |
| 43 | `test_hive_pct_mtmv` | 73.605 | 72.936 | 73.270 | 0.669 | 0.91% |
| 44 | `test_hive_get_schema_from_table` | 72.695 | 58.360 | 65.528 | 14.335 | 21.88% |
| 45 | `test_iceberg_hadoop_case_sensibility` | 70.900 | 68.483 | 69.692 | 2.417 | 3.47% |
| 46 | `test_hdfs_parquet_group5` | 70.279 | 69.839 | 70.059 | 0.440 | 0.63% |
| 47 | `test_lower_case_meta_with_lower_table_conf_show_and_select` | 63.165 | 62.123 | 62.644 | 1.042 | 1.66% |
| 48 | `test_iceberg_full_schema_change` | 62.458 | 58.720 | 60.589 | 3.738 | 6.17% |
| 49 | `test_external_and_internal_describe` | 61.763 | 61.753 | 61.758 | 0.010 | 0.02% |
| 50 | `test_iceberg_mtmv` | 61.532 | 60.154 | 60.843 | 1.378 | 2.26% |
| 51 | `test_iceberg_write_partitions` | 51.667 | 60.404 | 56.035 | 8.737 | 15.59% |

## 3. 逐 Case 分析（1 Case = 1 分析）

### Case 01: `test_trino_hive_tpch_sf1_parquet`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=1063.158s`（17m43s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=1012.972s`（16m53s），平均 `1038.065s`，波动 `50.186s`（4.83%）。
- Suite：历史样本对应旧路径；当前代码路径为 `regression-test/suites/external_table_p2/trino_connector/hive/test_trino_hive_tpch_sf1_parquet.groovy`（标签 `p2,external`）。
- 脚本特征：`q01~q22` 被 `run_tpch()` 串行执行；脚本中 `run_tpch()` 被连续调用 3 轮（`enable_file_cache=false/true/true`）。
- 慢点判断：该 case 本质是 `22 * 3 = 66` 条 TPCH SF1 查询的累计耗时，不是单条语句异常慢。
- 补充判断：日志直接给出三轮时间：`381266ms, 339030ms, 339178ms`（897097）和 `350673ms, 328446ms, 330131ms`（897373），三轮和几乎等于 case 总时长。
- 当前代码状态：external 流水线配置为 `testGroups=external` 且 `excludeGroups=p1,p2`，因此该 case 在当前 externregression 口径下应被过滤。
- 波动判断：两次样本非常稳定（4.83%），耗时特征确定性高。

### Case 02: `test_iceberg_table_cache`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=785.451s`（13m05s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=620.980s`（10m21s），平均 `703.216s`，波动 `164.471s`（23.39%）。
- Suite：`regression-test/suites/external_table_p0/iceberg/test_iceberg_table_cache.groovy`。
- 脚本特征：历史样本阶段为 DML/Schema/Partition 三段完整覆盖，`spark_iceberg` 调用 48 次，`refresh table` 11 次，并在 `with_cache/no_cache` 两个 catalog 间反复校验。
- 慢点判断：每次外部变更都要走“Spark 执行 -> no-cache 查询 -> with-cache 查询 -> refresh -> 再查”，跨引擎一致性验证链路很长。
- 补充判断：日志分段显示耗时主要在 Schema + Partition 段（897097 比 897373 分别多 `59.210s` 与 `88.881s`），说明慢点在元数据演进相关操作。
- 当前代码状态：Schema Change 已精简为代表性两类（保留 `RENAME COLUMN`、`ALTER TYPE`，移除 `ADD COLUMN`、`DROP COLUMN`），`spark_iceberg` 调用约从 48 次降到 40 次，预期 case 总耗时会下降。
- 波动判断：两次样本存在中等波动（23.39%），对环境状态有一定敏感度。

### Case 03: `test_hive_text_write_insert`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=670.228s`（11m10s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=567.349s`（9m27s），平均 `618.788s`，波动 `102.879s`（16.63%）。
- Suite：`regression-test/suites/external_table_p0/hive/write/test_hive_text_write_insert.groovy`。
- 脚本特征：历史样本阶段外层为 `hivePrefix=["hive2","hive3"]`，内层 `format_compressions=["gzip","bzip2","zstd","snappy","lz4"]`，每个组合执行 `q01~q04`。
- 慢点判断：历史样本总执行规模约为 `2 * 5 * 4 = 40` 组子场景；每组都包含多次 `INSERT/INSERT OVERWRITE` 与 `order_qt` 校验。
- 补充判断：q03/q04 还包含建表/删表和分区写入路径，且写入列为 all_types 宽表（含复杂类型），写放大明显。
- 当前代码状态：该脚本现已收敛为 `hivePrefix=["hive3"]`，执行规模降为约 `1 * 5 * 4 = 20` 组子场景，预期耗时会低于本文历史样本。
- 波动判断：两次样本存在中等波动（16.63%），对环境状态有一定敏感度。

### Case 04: `test_hive_compress_type`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=345.079s`（5m45s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=270.609s`（4m31s），平均 `307.844s`，波动 `74.470s`（24.19%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hive_compress_type.groovy`。
- 脚本特征：当前代码已是 `hivePrefix=["hive3"]`，总计约 21 条查询；其中 `q21~q33` 会重复两轮（`file_split_size=0` 与 `8388608`）。
- 慢点判断：主要瓶颈不是 `parquet_lz4/lzo` 小表，而是 `test_compress_partitioned`。脚本 `explain` 明确该表 `totalFileSize=734675596`（约 700MB，16 files）；`q22/q32`（全表 count）+ `q23/q33`（`watchid` 过滤但无分区条件）至少 4 次全量扫描，且是 text + 多压缩格式解码，CPU/IO 都重。
- 补充判断：`parquet_lz4_compression` 与 `parquet_lzo_compression` 在仓库内数据包仅为 KB 级（约 15KB/34KB），`q42~q48` 与 `lzo_1~lzo_8` 不是主要耗时来源；5 分钟更多是大表重复扫出来的累计结果。
- 波动判断：两次样本存在中等波动（24.19%），对环境状态有一定敏感度。

### Case 05: `refactor_storage_backup_restore_object_storage`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=319.537s`（5m20s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=309.864s`（5m10s），平均 `314.700s`，波动 `9.673s`（3.07%）。
- Suite：`regression-test/suites/external_table_p0/refactor_storage_param/backup_restore_object_storage.groovy`。
- 脚本特征：定义 `backupAndRestore` 流程（`BACKUP SNAPSHOT -> 等待 FINISHED -> RESTORE SNAPSHOT -> 再等待`），并在多个对象存储配置上重复执行。
- 慢点判断：`backupAndRestore(...)` 在脚本内总执行约 27 轮（`test_backup_restore` 的 7 轮 * 3 次调用 + 额外 6 轮直调），每轮都涉及对象存储 IO 和元数据状态流转。
- 补充判断：`Awaitility` 轮询等待会直接计入 case 耗时，这也是该 case 稳定在 5 分钟左右的主要原因。
- 波动判断：两次样本非常稳定（3.07%），耗时特征确定性高。

### Case 06: `test_iceberg_statistics`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=303.172s`（5m03s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=301.740s`（5m02s），平均 `302.456s`，波动 `1.432s`（0.47%）。
- Suite：`regression-test/suites/external_table_p0/iceberg/test_iceberg_statistics.groovy`。
- 脚本特征：语句不多，但包含两个同步统计任务：`analyze table sample_mor_parquet with sync` 与 `analyze table sample_cow_parquet with sync`。
- 慢点判断：`with sync` 会阻塞直到统计完成，属于重操作（扫描数据文件并写入内部统计表），因此短脚本也会耗时数分钟。
- 补充判断：后续仅查询 `internal.__internal_schema.column_statistics` 做校验，说明主要时间花在 analyze 本身。
- 波动判断：两次样本非常稳定（0.47%），耗时特征确定性高。

### Case 07: `test_hive_special_char_partition`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=262.755s`（4m23s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=262.657s`（4m23s），平均 `262.706s`，波动 `0.098s`（0.04%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hive_special_char_partition.groovy`。
- 脚本特征：在 `hive2` 与 `hive3` 两套 catalog 上都执行；对 3 个“特殊字符分区值”做 `eachWithIndex` 与双层笛卡尔组合插入（3x3）。
- 慢点判断：该 case 既测 Hive 侧写入又测 Doris 侧写入，且每轮都伴随 `refresh catalog + show partitions + 条件查询`，分区元数据操作密集。
- 补充判断：特殊字符分区会触发额外的解析/转义与分区匹配验证，执行链路更长，因此稳定但不快。
- 波动判断：两次样本非常稳定（0.04%），耗时特征确定性高。

### Case 08: `refactor_storage_param_s3_load`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=245.681s`（4m06s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=238.083s`（3m58s），平均 `241.882s`，波动 `7.598s`（3.14%）。
- Suite：`regression-test/suites/external_table_p0/refactor_storage_param/s3_load.groovy`。
- 脚本特征：先 `SELECT ... INTO OUTFILE` 到对象存储，再反复执行 `s3Load(...)`；活跃代码中 `s3Load` 调用约 48 次（含 `shouldFail` 反例路径）。
- 慢点判断：每次 load 都是 `LOAD LABEL + show load 轮询直到 FINISHED/FAILED`，远端存储读 + 导入事务 + 状态轮询叠加。
- 补充判断：覆盖 S3/OBS/COS 多种 endpoint、鉴权参数和 path-style 组合，属于“兼容矩阵”测试，天生耗时高。
- 波动判断：两次样本非常稳定（3.14%），耗时特征确定性高。

### Case 09: `test_hive_write_insert`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=237.872s`（3m58s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=153.236s`（2m33s），平均 `195.554s`，波动 `84.636s`（43.28%）。
- Suite：`regression-test/suites/external_table_p0/hive/write/test_hive_write_insert.groovy`。
- 脚本特征：`hivePrefix=["hive3"]`，`format_compressions=["parquet_snappy","orc_zlib"]`，每个压缩格式执行 `q01~q04`（共 8 组写测场景）。
- 慢点判断：q01~q04 都包含大量 `INSERT/INSERT OVERWRITE` 到 all_types 宽表及分区表，写后又紧跟 `order_qt` 校验，写读交替开销大。
- 补充判断：该用例宽列与复杂类型字段多，单次写入/覆盖代价较高，因此在环境波动时会出现较大抖动。
- 波动判断：两次样本波动较大（43.28%），外部环境负载或远端服务状态影响明显。

### Case 10: `test_hive_write_partitions`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=235.007s`（3m55s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=167.840s`（2m48s），平均 `201.423s`，波动 `67.167s`（33.35%）。
- Suite：`regression-test/suites/external_table_p0/hive/write/test_hive_write_partitions.groovy`。
- 脚本特征：`hive2/hive3` 双 catalog * `parquet_snappy/orc_zlib` 双格式，循环执行 `q01~q04`（共 16 组）；每组含多次分区 `INSERT` 与 `INSERT OVERWRITE`。
- 慢点判断：该 case 主打分区表写入路径，反复建表/删表、分区覆盖、读回校验，元数据与数据文件都频繁变更。
- 补充判断：额外还有 `test_doris_write_hive_partition_table`（每个 catalog 执行一次）验证 Doris/Hive 对同分区交替写入兼容性，进一步拉长耗时。
- 波动判断：两次样本波动较大（33.35%），外部环境负载或远端服务状态影响明显。

### Case 11: `test_paimon_catalog`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=214.931s`（3m35s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=167.496s`（2m47s），平均 `191.214s`，波动 `47.435s`（24.81%）。
- Suite：`regression-test/suites/external_table_p0/paimon/test_paimon_catalog.groovy`。
- 脚本特征：`test_cases(force_jni_scanner, enable_file_cache)` 被执行 4 轮（`false/false`,`false/true`,`true/false`,`true/true`）；每轮会跑 `qt_all_type`（单次 25 条）并追加约 90 条 `qt_cXX` 查询。
- 慢点判断：该 case 是大规模 Paimon 查询矩阵验证，单轮约 140 条断言查询，4 轮约 560 条查询级校验，累计耗时自然到分钟级。
- 补充判断：除查询外还包含 catalog 建删、视图/CTAS 校验等元数据动作，因此对外部存储与扫描路径负载敏感。
- 波动判断：两次样本存在中等波动（24.81%），对环境状态有一定敏感度。

### Case 12: `test_paimon_table_meta_cache`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=205.898s`（3m26s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=178.956s`（2m59s），平均 `192.427s`，波动 `26.942s`（14.00%）。
- Suite：`regression-test/suites/external_table_p0/paimon/test_paimon_table_meta_cache.groovy`。
- 脚本特征：当前分支未找到同名脚本文件，本条基于两次构建日志分析；日志显示该用例分为 `Test 1: DML(INSERT)` 与 `Test 2: Schema Change(ADD COLUMN)` 两段。
- 慢点判断：两段都包含 `Spark Paimon` 外部 DDL/DML（DROP/CREATE/INSERT/ALTER）+ Doris `refresh table`，并在缓存 catalog 间切换校验，链路较长。
- 补充判断：分段耗时（897097/897373）分别为 `DML 78.990s/67.808s`、`Schema 104.724s/91.693s`，说明慢点主要在 Schema 变更段。
- 波动判断：两次样本波动较小（14.00%），总体可复现。

### Case 13: `test_hms_event_notification_multi_catalog`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=186.982s`（3m07s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=186.193s`（3m06s），平均 `186.588s`，波动 `0.789s`（0.42%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hms_event_notification_multi_catalog.groovy`。
- 脚本特征：使用两个开启 HMS 事件增量同步的 catalog（batch size 分别 10000/100000）做一致性对照；脚本显式 `sleep(wait_time)` 15 次，`wait_time=10000ms`。
- 慢点判断：仅固定等待就约 `150s`，再叠加建库/改库、建表/删表、插入、列变更、分区改名等事件同步验证，最终稳定在约 186s。
- 补充判断：每次事件后都在两个 catalog 反复 `switch + show/desc/select` 验证传播结果，属于“强一致校验”型慢用例。
- 波动判断：两次样本非常稳定（0.42%），耗时特征确定性高。

### Case 14: `test_hms_event_notification`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=186.590s`（3m07s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=185.182s`（3m05s），平均 `185.886s`，波动 `1.408s`（0.76%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hms_event_notification.groovy`。
- 脚本特征：同样是 HMS 事件增量同步验证，`wait_time=10000ms`，显式 sleep 次数也是 15 次。
- 慢点判断：与 Case13 一样，固定等待约 `150s` 是主要耗时来源，其余约 30~40 秒为元数据事件操作和结果校验。
- 补充判断：该用例比 multi-catalog 版本路径更短，但仍保留完整的 DDL/DML 事件链验证，所以总时长接近。
- 波动判断：两次样本非常稳定（0.76%），耗时特征确定性高。

### Case 15: `test_hive_warmup_select`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=186.411s`（3m06s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=173.103s`（2m53s），平均 `179.757s`，波动 `13.308s`（7.40%）。
- Suite：`regression-test/suites/external_table_p0/cache/test_hive_warmup_select.groovy`。
- 脚本特征：围绕 `WARM UP SELECT` 做三类测试：基础预热、负例语法校验、权限校验（含建用户/授权/回收/再次验证）。
- 慢点判断：基础与权限测试会多次执行对 `tpch1_parquet.lineitem` 的预热扫描；预热本质是主动触发文件读取与缓存填充，IO 开销显著。
- 补充判断：该脚本实际执行约 11 次 warmup 语句（含应失败场景），并跨连接做权限校验，导致总时长稳定在 3 分钟左右。
- 波动判断：两次样本波动较小（7.40%），总体可复现。

### Case 16: `test_hive_meta_cache`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=154.177s`（2m34s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=155.927s`（2m36s），平均 `155.052s`，波动 `1.750s`（1.13%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hive_meta_cache.groovy`。
- 脚本特征：按阶段系统性覆盖 `file.meta.cache`、`partition.cache`、`schema.cache` 以及 `get_schema_from_table` 组合；每阶段都伴随建删 catalog/库/表、插入、刷新、desc/show create 校验。
- 慢点判断：这是“缓存一致性行为测试”，不是单纯写入测试，核心成本在反复触发缓存失效/命中并校验可见性。
- 补充判断：脚本中 `refresh table`、`alter catalog ... ttl`、`alter table add columns` 反复出现，元数据路径操作密度高，因此耗时稳定且偏长。
- 波动判断：两次样本非常稳定（1.13%），耗时特征确定性高。

### Case 17: `test_tvf_p0`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=150.506s`（2m31s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=149.623s`（2m30s），平均 `150.065s`，波动 `0.883s`（0.59%）。
- Suite：`regression-test/suites/external_table_p0/tvf/test_tvf_p0.groovy`。
- 脚本特征：共 11 条 HDFS TVF 查询，覆盖 parquet/orc、nested types、bad page header、viewfs 挂载等多种读取路径。
- 慢点判断：该 case 核心是文件解析压力测试，多个样例文件偏大且包含复杂嵌套结构（`row_cross_pages_2.parquet` 注释量级约 149923 行）。
- 补充判断：每条查询都直接走外部文件读取与解析，CPU 解码 + 远端读开销累加后形成稳定的 150s 级耗时。
- 波动判断：两次样本非常稳定（0.59%），耗时特征确定性高。

### Case 18: `test_paimon_statistics`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=148.172s`（2m28s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=147.641s`（2m28s），平均 `147.906s`，波动 `0.531s`（0.36%）。
- Suite：`regression-test/suites/external_table_p0/paimon/test_paimon_statistics.groovy`。
- 脚本特征：逻辑很短，但包含一个阻塞式统计任务：`analyze table all_table with sync`。
- 慢点判断：`with sync` 需要等待统计完成（扫描并写入 `column_statistics`），这是该 case 绝大部分耗时来源。
- 补充判断：后续只做一次统计表查询断言，说明慢点集中在 analyze 阶段而非校验阶段。
- 波动判断：两次样本非常稳定（0.36%），耗时特征确定性高。

### Case 19: `test_hdfs_json_load`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=147.645s`（2m28s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=144.028s`（2m24s），平均 `145.837s`，波动 `3.617s`（2.48%）。
- Suite：`regression-test/suites/load_p0/stream_load/test_hdfs_json_load.groovy`。
- 脚本特征：定义 `q1~q15` 并全部顺序执行；`q1~q14` 各执行两轮 load（新旧 reader 对比），`q15` 一轮，共约 29 次 HDFS JSON load 任务。
- 慢点判断：每次 load 后都调用 `check_load_result` 轮询 `show load`（最多 30s），再 `sync + qt_select` 校验，属于“导入任务串行累加”型耗时。
- 补充判断：覆盖 jsonpaths/json_root/exprs/fuzzy_parse/strip_outer_array 等大量参数组合，兼容性矩阵本身决定了分钟级时长。
- 波动判断：两次样本非常稳定（2.48%），耗时特征确定性高。

### Case 20: `test_hive_orc`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=147.538s`（2m28s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=105.816s`（1m46s），平均 `126.677s`，波动 `41.722s`（32.94%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hive_orc.groovy`。
- 脚本特征：外层循环跑 `hive2` 和 `hive3` 两套 catalog；每套都会执行完整 ORC 验证链（predicate pushdown、topn、decimal、varbinary mapping、异常路径等）。
- 慢点判断：仅 `test_topn + test_topn_abs` 就是 `16 列 * 2 排序方向 * 2 组 = 64` 条查询/每 catalog；再叠加 predicate pushdown、兼容语法和 varbinary 相关测试，单 catalog 查询量超过百条。
- 补充判断：该用例同时包含正常路径与预期失败路径，且大量查询带排序/过滤，遇到环境抖动时总时长会明显放大。
- 波动判断：两次样本波动较大（32.94%），外部环境负载或远端服务状态影响明显。

### Case 21: `test_hive_metadata_refresh_interval`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=142.044s`（2m22s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=141.729s`（2m22s），平均 `141.887s`，波动 `0.315s`（0.22%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hive_metadata_refresh_interval.groovy`。
- 脚本特征：按 `hive2`、`hive3` 两轮执行；每轮固定有 `sleep(10000)` 和 `sleep(60000)` 两段等待。
- 慢点判断：仅显式等待基线就是 `70s/轮 * 2轮 = 140s`，几乎覆盖 case 总耗时；其余只剩建表和 `show tables` 校验。
- 补充判断：该用例本质是在验证 `metadata_refresh_interval_sec` 自动刷新是否生效，等待时间是测试目标本身，不是异常慢。
- 波动判断：两次样本非常稳定（0.22%），耗时特征确定性高。

### Case 22: `test_complex_types`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=141.834s`（2m22s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=117.988s`（1m58s），平均 `129.911s`，波动 `23.846s`（18.36%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_complex_types.groovy`。
- 脚本特征：`hive2/hive3` 两轮；每轮执行约 15 条复杂类型查询（`struct_element`、`map_keys/map_values`、`map_contains_key`、`array_max`，覆盖 parquet/orc 两套路径）。
- 慢点判断：复杂类型函数对外表数据做解码与表达式计算，且每轮都在外部 catalog 上全量校验，累计后达到分钟级。
- 补充判断：`byd`、`complex_offsets_check`、`date_dict` 等多表校验混合执行，导致读取路径和数据页类型较分散，单轮耗时不集中但总量高。
- 波动判断：两次样本存在中等波动（18.36%），对环境状态有一定敏感度。

### Case 23: `test_streaming_mysql_job`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=139.184s`（2m19s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=137.976s`（2m18s），平均 `138.580s`，波动 `1.208s`（0.87%）。
- Suite：`regression-test/suites/job_p0/streaming_job/cdc/test_streaming_mysql_job.groovy`。
- 脚本特征：先构造 3 张 MySQL 源表并创建 STREAMING JOB，再做两轮增量变更校验（`INSERT/UPDATE/DELETE` + 再次 `INSERT`）。
- 慢点判断：存在 `Awaitility.await().atMost(300s)` 任务就绪等待 + 两次 `sleep(60000)` 增量同步等待，固定等待窗口是主耗时。
- 补充判断：还要校验 3 张目标表快照与增量结果、读取 jobs/tasks 状态，属于“端到端 CDC 完整链路”而非单 SQL 测试。
- 波动判断：两次样本非常稳定（0.87%），耗时特征确定性高。

### Case 24: `test_hive_multi_partition_mtmv`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=138.620s`（2m19s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=136.593s`（2m17s），平均 `137.606s`，波动 `2.027s`（1.47%）。
- Suite：`regression-test/suites/mtmv_p0/test_hive_multi_partition_mtmv.groovy`。
- 脚本特征：`hive2/hive3` 两轮；每轮先在 Hive 侧建多分区表并写入 6 个分区数据，再分别测试“按 year 分区 MV”和“按 region 分区 MV”。
- 慢点判断：单轮包含 9 次 `REFRESH MATERIALIZED VIEW` + 5 次 `REFRESH catalog` + 9 次 `waitingMTMVTaskFinished`，刷新和任务等待串行叠加。
- 补充判断：除增量数据外，还覆盖加分区/删分区场景并逐次 `show partitions` 验证，属于重元数据演进链路。
- 波动判断：两次样本非常稳定（1.47%），耗时特征确定性高。

### Case 25: `test_streaming_postgres_job`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=123.166s`（2m03s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=123.114s`（2m03s），平均 `123.140s`，波动 `0.052s`（0.04%）。
- Suite：`regression-test/suites/job_p0/streaming_job/cdc/test_streaming_postgres_job.groovy`。
- 脚本特征：创建 2 张 PG 源表并启动 STREAMING JOB，随后做两轮增量变更同步验证。
- 慢点判断：同样有 `Awaitility.await().atMost(300s)` 就绪等待和两次 `sleep(60000)` 增量等待，固定等待时间决定了分钟级耗时。
- 补充判断：脚本还包含 `xmin/xmax` 读取和 jobs/tasks 状态检查，确保 CDC 事务链路正确，耗时稳定但不可省。
- 波动判断：两次样本非常稳定（0.04%），耗时特征确定性高。

### Case 26: `test_file_meta_cache`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=120.833s`（2m01s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=117.393s`（1m57s），平均 `119.113s`，波动 `3.440s`（2.89%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_file_meta_cache.groovy`。
- 脚本特征：双层循环 `fileFormat(PARQUET/ORC) * hivePrefix(hive2/hive3)` 共 4 轮；每轮都做 `create/insert/truncate/drop/recreate/insert overwrite`。
- 慢点判断：每轮固定 4 次 `refresh catalog` 并立刻查询校验，4 轮共 16 次 catalog 刷新，元数据重载成本是主因。
- 补充判断：测试目标是“文件元数据缓存是否正确失效与重建”，所以故意频繁变更 Hive 表文件并触发刷新。
- 波动判断：两次样本非常稳定（2.89%），耗时特征确定性高。

### Case 27: `test_streaming_mysql_job_errormsg`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=114.657s`（1m55s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=113.301s`（1m53s），平均 `113.979s`，波动 `1.356s`（1.19%）。
- Suite：`regression-test/suites/job_p0/streaming_job/cdc/test_streaming_mysql_job_errormsg.groovy`。
- 脚本特征：先故意制造源/目标类型与长度不匹配，触发 CDC 任务失败；再 `ALTER JOB` 调 `load.max_filter_ratio=1` 并 `RESUME` 让任务恢复。
- 慢点判断：包含两段 `Awaitility.await().atMost(300s)`（先等失败，再等恢复成功），任务状态收敛等待是主要耗时。
- 补充判断：该 case 同时验证错误消息内容和恢复后统计（`filteredRows/scannedRows`），流程比普通 CDC 更长。
- 波动判断：两次样本非常稳定（1.19%），耗时特征确定性高。

### Case 28: `test_iceberg_write_insert`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=105.043s`（1m45s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=107.527s`（1m48s），平均 `106.285s`，波动 `2.484s`（2.34%）。
- Suite：`regression-test/suites/external_table_p0/iceberg/write/test_iceberg_write_insert.groovy`。
- 脚本特征：`hive2/hive3` * `parquet_zstd/orc_zlib` 两层循环，每组执行 `q01~q04`（非分区+分区、Iceberg 与 Hive 互读校验）；末尾还有额外一次 `q01` 错误文件系统场景。
- 慢点判断：`q01~q04` 内部是大量宽表 `INSERT INTO`（复杂类型列）+ 读回校验，累计为 40+ 次写入路径，写放大明显。
- 补充判断：每轮还要建/删 catalog、建库建表，属于“写入兼容矩阵”测试，耗时稳定在 100s 级。
- 波动判断：两次样本非常稳定（2.34%），耗时特征确定性高。

### Case 29: `test_wide_table`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=101.693s`（1m42s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=69.648s`（1m10s），平均 `85.671s`，波动 `32.045s`（37.40%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_wide_table.groovy`。
- 脚本特征：在 `hive2/hive3` 两个 catalog 上分别执行 8 条宽表 decimal 语义查询（含子查询排序、范围比较、聚合）。
- 慢点判断：SQL 数量不多，但都在 `wide_table1_orc` 上读取高列号字段（如 `col534`），ORC 解码和排序开销集中。
- 补充判断：该 case 对数据冷热状态较敏感，所以两次构建出现较大波动（101s vs 69s）。
- 波动判断：两次样本波动较大（37.40%），外部环境负载或远端服务状态影响明显。

### Case 30: `test_iceberg_insert_overwrite`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=101.509s`（1m42s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=100.131s`（1m40s），平均 `100.820s`，波动 `1.378s`（1.37%）。
- Suite：`regression-test/suites/external_table_p0/iceberg/write/test_iceberg_insert_overwrite.groovy`。
- 脚本特征：与 Case28 同样是 `hive2/hive3 * 2格式 * q01~q04` 矩阵，但核心写操作全部改为 `INSERT OVERWRITE`。
- 慢点判断：`INSERT OVERWRITE` 会重复重写目标分区/文件，IO 与元数据提交开销高于普通 append，累计约 40 次覆盖写。
- 补充判断：同时覆盖分区表和非分区表、跨 catalog 读写一致性，链路重且稳定。
- 波动判断：两次样本非常稳定（1.37%），耗时特征确定性高。

### Case 31: `test_hive_statistic`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=98.536s`（1m39s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=72.968s`（1m13s），平均 `85.752s`，波动 `25.568s`（29.82%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hive_statistic.groovy`。
- 脚本特征：`hive2/hive3` 两轮；每轮执行 `analyze table logs1_parquet (log_time) with sync`，随后走 `show proc` 多级路径定位 ID，并读写 `internal.__internal_schema.column_statistics`。
- 慢点判断：`analyze ... with sync` 阻塞等待统计完成是主耗时；后续统计表回查与 `alter table ... set stats` 是次要成本。
- 补充判断：该用例混合“外表统计收集 + 内部统计表校验”，路径比普通 analyze 更长，环境负载高时波动会放大。
- 波动判断：两次样本存在中等波动（29.82%），对环境状态有一定敏感度。

### Case 32: `test_file_cache_query_limit`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=86.909s`（1m27s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=73.970s`（1m14s），平均 `80.440s`，波动 `12.939s`（16.09%）。
- Suite：`regression-test/suites/external_table_p0/cache/test_file_cache_query_limit.groovy`。
- 脚本特征：脚本会调用 `curl` 读取/清理 BE 缓存指标（`process.waitFor()`），并以 `totalWaitTime=file_cache_background_monitor_interval_ms/1000` 执行 6 轮显式 `Thread.sleep` 等待。
- 慢点判断：缓存清理、配置切换、指标更新都靠轮询等待；等待总时长约等于 `6 * totalWaitTime`，再叠加两轮 TPCH `lineitem` 聚合查询灌缓存。
- 补充判断：该 case 不是单纯查询性能，而是“缓存限流参数生效性”验证，必须等待后台监控周期推进。
- 波动判断：两次样本存在中等波动（16.09%），对环境状态有一定敏感度。

### Case 33: `paimon_time_travel`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=85.783s`（1m26s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=72.239s`（1m12s），平均 `79.011s`，波动 `13.544s`（17.14%）。
- Suite：`regression-test/suites/external_table_p0/paimon/paimon_time_travel.groovy`。
- 脚本特征：时间旅行查询规模很大：`snapshot(4)*24 + branch(2)*12 + tag(4)*12 + schema_change(2)*2`，再加时区与错误路径，整体约 180+ 条查询。
- 慢点判断：大量 `FOR VERSION AS OF`/`FOR TIME AS OF`、`@branch/@tag` 语义反复触发快照解析和历史读取，累计耗时明显。
- 补充判断：还覆盖 `force_jni_scanner` 与多时区切换（`+08/+06/+10`）及异常断言，验证范围广导致执行链路变长。
- 波动判断：两次样本存在中等波动（17.14%），对环境状态有一定敏感度。

### Case 34: `test_orc_tiny_stripes`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=78.980s`（1m19s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=58.390s`（0m58s），平均 `68.685s`，波动 `20.590s`（29.98%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_orc_tiny_stripes.groovy`。
- 脚本特征：参数矩阵规模大：`enable_orc_lazy_materialization` 两态 * `orc_configs` 11 组 * `qt_test_1~qt_test_14` 14 条查询，共约 308 次查询执行。
- 慢点判断：每组都先改 ORC 读取参数再跑整套过滤/排序查询，属于“读取参数组合压测”，累计成本高。
- 补充判断：不同 stripe 阈值与读取策略会放大解码开销差异，因此该 case 波动相对大。
- 波动判断：两次样本存在中等波动（29.98%），对环境状态有一定敏感度。

### Case 35: `test_hive_refresh_mtmv`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=75.421s`（1m15s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=77.781s`（1m18s），平均 `76.601s`，波动 `2.360s`（3.08%）。
- Suite：`regression-test/suites/mtmv_p0/test_hive_refresh_mtmv.groovy`。
- 脚本特征：`hive2/hive3` 两轮；每轮包含 7 次 `REFRESH MATERIALIZED VIEW`（auto/complete 混合）+ 5 次 `REFRESH catalog` + 7 次 MTMV 任务等待。
- 慢点判断：刷新链路多且每次刷新都要等任务状态收敛，等待累计是主要耗时。
- 补充判断：脚本还覆盖“未 refresh catalog 的旧数据可见性”与“列重命名导致任务失败/恢复”两条异常链路。
- 波动判断：两次样本非常稳定（3.08%），耗时特征确定性高。

### Case 36: `test_streaming_postgres_job_all_type`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=76.851s`（1m17s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=75.282s`（1m15s），平均 `76.066s`，波动 `1.569s`（2.06%）。
- Suite：`regression-test/suites/job_p0/streaming_job/cdc/test_streaming_postgres_job_all_type.groovy`。
- 脚本特征：源表覆盖 29 类 PG 类型（含 `bytea/uuid/json/jsonb/inet/cidr/array/point`），先快照同步再做一轮增量插入。
- 慢点判断：`Awaitility` 等待任务就绪 + 固定 `sleep(60000)` 等待增量生效是主要耗时。
- 补充判断：宽类型映射与 desc/select 全量比对增加了解析开销，但相对等待窗口是次要因素。
- 波动判断：两次样本非常稳定（2.06%），耗时特征确定性高。

### Case 37: `test_streaming_mysql_job_all_type`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=76.621s`（1m17s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=76.095s`（1m16s），平均 `76.358s`，波动 `0.526s`（0.69%）。
- Suite：`regression-test/suites/job_p0/streaming_job/cdc/test_streaming_mysql_job_all_type.groovy`。
- 脚本特征：源表覆盖大规模 MySQL 全类型（含 unsigned、`decimal(65,30)`、`json/set/bit/binary/varbinary/enum`）。
- 慢点判断：同样由“任务就绪等待 + `sleep(60000)` 增量等待”主导，总时长接近固定值。
- 补充判断：两条超宽行全量同步与回查会增加 CPU 解析成本，但对总时长影响小于等待。
- 波动判断：两次样本非常稳定（0.69%），耗时特征确定性高。

### Case 38: `test_hdfs_parquet_group0`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=73.867s`（1m14s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=76.490s`（1m16s），平均 `75.178s`，波动 `2.623s`（3.49%）。
- Suite：`regression-test/suites/external_table_p0/tvf/test_hdfs_parquet_group0.groovy`。
- 脚本特征：顺序执行 54 条 `order_qt_test_*` HDFS Parquet 读取 + 2 条预期失败用例（损坏/越界校验）。
- 慢点判断：每条查询都读取不同 Parquet 样例文件（多压缩、多编码、嵌套/损坏样本），文件打开与解码成本持续累加。
- 补充判断：该组样例覆盖面广但都走 TVF 外部读路径，属于典型“单条不慢、总量致慢”。
- 波动判断：两次样本非常稳定（3.49%），耗时特征确定性高。

### Case 39: `test_jdbc_row_count`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=73.576s`（1m14s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=75.177s`（1m15s），平均 `74.376s`，波动 `1.601s`（2.15%）。
- Suite：`regression-test/suites/external_table_p0/jdbc/test_jdbc_row_count.groovy`。
- 脚本特征：串行测试 8 类 JDBC catalog（MySQL/PG/SQLServer/Oracle 及 lower_case 变体），每类都 `show table stats` 轮询直到 row count 非 `-1`。
- 慢点判断：轮询里有大量 `Thread.sleep(1s)`（6 组最多 60 次 + 2 组最多 30 次，理论上限 420s+），实际靠提前收敛降低但仍是主耗时。
- 补充判断：每类 catalog 前还有 1 秒启动等待和建删 catalog 开销，远端元数据就绪速度决定总时长。
- 波动判断：两次样本非常稳定（2.15%），耗时特征确定性高。

### Case 40: `test_streaming_mysql_job_priv`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=74.121s`（1m14s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=72.405s`（1m12s），平均 `73.263s`，波动 `1.716s`（2.34%）。
- Suite：`regression-test/suites/job_p0/streaming_job/cdc/test_streaming_mysql_job_priv.groovy`。
- 脚本特征：权限链路很长：先验证无 LOAD 权限创建 job 失败，再授予权限创建 job 成功，再改 MySQL 凭据触发 PAUSED，再补复制权限恢复 RUNNING。
- 慢点判断：包含 3 段 `Awaitility.await().atMost(300s)` 等待（成功、失败、恢复），状态机收敛等待占大头。
- 补充判断：同时穿插用户/权限 DDL、PAUSE/ALTER/RESUME JOB 和增量写入验证，属于 CDC 权限恢复全流程测试。
- 波动判断：两次样本非常稳定（2.34%），耗时特征确定性高。

### Case 41: `test_hive_ctas`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=73.965s`（1m14s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=65.354s`（1m05s），平均 `69.660s`，波动 `8.611s`（12.36%）。
- Suite：`regression-test/suites/external_table_p0/hive/ddl/test_hive_ctas.groovy`。
- 脚本特征：用例集中覆盖 CTAS/写入矩阵：`create table ... as` 7 次、`insert into` 23 次、`insert overwrite` 12 次，另有 `parquet/orc` 两种格式 all_types 路径。
- 慢点判断：持续的 CTAS + 插入/覆盖写会反复触发外表文件生成与元数据提交，写路径累计成本高。
- 补充判断：每个阶段都要做 Doris/Hive 双侧结果比对（`order_qt` + `order_qt_hive_docker`），校验链路进一步拉长。
- 波动判断：两次样本波动较小（12.36%），总体可复现。

### Case 42: `paimon_base_filesystem`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=73.930s`（1m14s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=71.129s`（1m11s），平均 `72.530s`，波动 `2.801s`（3.86%）。
- Suite：`regression-test/suites/external_table_p0/paimon/paimon_base_filesystem.groovy`。
- 脚本特征：一次性创建 4 套 filesystem catalog（OSS/OBS/COS/COSN），每套都执行 `@incr`、`FOR VERSION AS OF`、`$snapshots` 查询，再做 JNI scanner 开关前后两轮读一致性校验。
- 慢点判断：跨 4 种对象存储端点的元数据与数据读取链路都要走一遍，网络和认证路径叠加导致耗时稳定偏高。
- 补充判断：该 case 目标是验证多云文件系统兼容，不追求单链路极致速度。
- 波动判断：两次样本非常稳定（3.86%），耗时特征确定性高。

### Case 43: `test_hive_pct_mtmv`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=73.605s`（1m14s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=72.936s`（1m13s），平均 `73.270s`，波动 `0.669s`（0.91%）。
- Suite：`regression-test/suites/mtmv_p0/test_hive_pct_mtmv.groovy`。
- 脚本特征：构造两张不同分区策略 Hive 表后做 `UNION ALL` MTMV，执行 2 次 MV 刷新（AUTO + 指定分区）和 1 次 catalog 刷新。
- 慢点判断：核心耗时来自两次 `waitingMTMVTaskFinishedByMvName` 等待任务完成，而不是 SQL 文本长度。
- 补充判断：该用例重点验证 PCT（分区变更跟踪）在 union 场景下的正确性，必须走完整刷新链路。
- 波动判断：两次样本非常稳定（0.91%），耗时特征确定性高。

### Case 44: `test_hive_get_schema_from_table`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=72.695s`（1m13s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=58.360s`（0m58s），平均 `65.528s`，波动 `14.335s`（21.88%）。
- Suite：`regression-test/suites/external_table_p0/hive/test_hive_get_schema_from_table.groovy`。
- 脚本特征：`hive2/hive3` 两轮；每轮先遍历 `show databases -> show tables`，再执行 `topn(13列*2)` + `topn_abs(13列*2)` 共 52 条排序查询，外加 7 条多格式 schema 读取校验。
- 慢点判断：`get_schema_from_table=true` 下会走真实文件 schema 推断路径，叠加大量 `order by ... limit` 查询后总耗时上升明显。
- 补充判断：该 case 同时覆盖 parquet/orc/csv/text，数据和格式路径多，环境波动时更容易出现时长抖动。
- 波动判断：两次样本存在中等波动（21.88%），对环境状态有一定敏感度。

### Case 45: `test_iceberg_hadoop_case_sensibility`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=70.900s`（1m11s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=68.483s`（1m08s），平均 `69.692s`，波动 `2.417s`（3.47%）。
- Suite：`regression-test/suites/external_table_p0/iceberg/test_iceberg_hadoop_case_sensibility.groovy`。
- 脚本特征：核心循环 `case_type = 0/1/2` 跑 3 轮完整流程，每轮都覆盖库表建删、大小写冲突、插入/覆盖、信息_schema 校验、预期失败断言与 S3 TVF 物理文件检查。
- 慢点判断：这是“大小写语义矩阵”测试，单轮语句数量大且含大量异常分支校验，三轮累计后自然达到 1 分钟量级。
- 补充判断：DML 路径在不同 `only_test_lower_case_table_names` 策略下重复执行，保证语义一致性的代价就是执行时长。
- 波动判断：两次样本非常稳定（3.47%），耗时特征确定性高。

### Case 46: `test_hdfs_parquet_group5`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=70.279s`（1m10s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=69.839s`（1m10s），平均 `70.059s`，波动 `0.440s`（0.63%）。
- Suite：`regression-test/suites/external_table_p0/tvf/test_hdfs_parquet_group5.groovy`。
- 脚本特征：执行 55 条 `order_qt_test_*` HDFS Parquet 查询 + 2 条预期失败测试，覆盖 case-insensitive/array/nested/metadata/旧版本样本文件。
- 慢点判断：与 group0 类似，耗时来自“多文件反复打开与解码”的总量效应，不是个别慢 SQL。
- 补充判断：该组样本格式差异更杂，兼容性覆盖面更广，保持了稳定但不低的执行时长。
- 波动判断：两次样本非常稳定（0.63%），耗时特征确定性高。

### Case 47: `test_lower_case_meta_with_lower_table_conf_show_and_select`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=63.165s`（1m03s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=62.123s`（1m02s），平均 `62.644s`，波动 `1.042s`（1.66%）。
- Suite：`regression-test/suites/external_table_p0/lower_case/test_lower_case_meta_with_lower_table_conf_show_and_select.groovy`。
- 脚本特征：组合矩阵非常大：`use_meta_cache(2) * lower_case_meta_names(2) * only_test_lower_case_table_names(0/1/2)` 共 12 组 catalog；全脚本包含 84 次 `refresh catalog`、48 次 `wait_table_sync`、50 次 insert、80 条 qt 校验。
- 慢点判断：每个组合都重复“多种大小写读写 + refresh + show tables + 负例断言”，属于高重复元数据验证链路，累计开销显著。
- 补充判断：`wait_table_sync` 由 `Awaitility` 驱动（单次最多 10s），在 `use_meta_cache=false` 路径上会反复触发等待。
- 波动判断：两次样本非常稳定（1.66%），耗时特征确定性高。

### Case 48: `test_iceberg_full_schema_change`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=62.458s`（1m02s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=58.720s`（0m59s），平均 `60.589s`，波动 `3.738s`（6.17%）。
- Suite：`regression-test/suites/external_table_p0/iceberg/test_iceberg_full_schema_change.groovy`。
- 脚本特征：对 `parquet` 与 `orc` 两张 schema 变更后的 Iceberg 表分别执行同一套复杂类型查询（脚本内 37 条模板查询，实际两表共约 74 次执行）。
- 慢点判断：大量 `STRUCT_ELEMENT/MAP_VALUES/ARRAY_SIZE` 条件与 `ORDER BY id` 排序组合，复杂列解码成本高。
- 补充判断：该 case 不做写入，主要时间都花在 schema-evolution 后的读取正确性验证上。
- 波动判断：两次样本波动较小（6.17%），总体可复现。

### Case 49: `test_external_and_internal_describe`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=61.763s`（1m02s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=61.753s`（1m02s），平均 `61.758s`，波动 `0.010s`（0.02%）。
- Suite：`regression-test/suites/external_table_p0/test_external_and_internal_describe.groovy`。
- 脚本特征：先在 external Hive catalog 上做 `desc` 与 `show proc`（含列注释开关），再在 internal 表上重复同样的 `desc/show proc` 路径解析。
- 慢点判断：主要成本在多轮 `show proc` 目录级遍历（catalog->db->table->index_schema）和外部元数据访问，不是数据扫描。
- 补充判断：该用例强调元信息一致性，查询量不大但每步都依赖元数据层，故耗时稳定在 60s 左右。
- 波动判断：两次样本非常稳定（0.02%），耗时特征确定性高。

### Case 50: `test_iceberg_mtmv`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=61.532s`（1m02s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=60.154s`（1m00s），平均 `60.843s`，波动 `1.378s`（2.26%）。
- Suite：`regression-test/suites/mtmv_p0/test_iceberg_mtmv.groovy`。
- 脚本特征：覆盖 5 个 MV 构建场景（基础、ts 分区、date 分区、unpartition、union rewrite），共 16 次 `REFRESH MATERIALIZED VIEW`，并有 14 次 `waitingMTMVTaskFinishedByMvName` + 2 次通用等待。
- 慢点判断：MTMV 刷新后必须等待任务完成再验结果，刷新-等待链路多，决定了该 case 的分钟级耗时。
- 补充判断：脚本还包含 `analyze table ... with sync`、rewrite explain 校验、`$partitions` 读取，验证面广但成本也高。
- 波动判断：两次样本非常稳定（2.26%），耗时特征确定性高。

### Case 51: `test_iceberg_write_partitions`
- 耗时画像：`build 897097（[PR #60934](https://github.com/apache/doris/pull/60934)）=51.667s`（0m52s），`build 897373（[PR #60795](https://github.com/apache/doris/pull/60795)）=60.404s`（1m00s），平均 `56.035s`，波动 `8.737s`（15.59%）。
- Suite：`regression-test/suites/external_table_p0/iceberg/write/test_iceberg_write_partitions.groovy`。
- 脚本特征：`hive2/hive3 * parquet_snappy/orc_zlib` 共 4 轮；每轮执行 `q01+q02`，含约 12 条分区写入/对齐场景（基础分区、datetime 分区、列顺序错位）及对应读回校验。
- 慢点判断：每轮都在 Iceberg 分区表上连续写入并校验，文件提交与分区元数据更新频繁，累计后达到近 1 分钟。
- 补充判断：该 case 覆盖“分区表达式 + 列对齐”写入兼容性，属于多写入路径验证，波动高于纯读类用例。
- 波动判断：两次样本存在中等波动（15.59%），对环境状态有一定敏感度。
