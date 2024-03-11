---
{
    "title": "变量",
    "language": "zh-CN"
}
---

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

# 变量

本文档主要介绍当前支持的变量（variables）。

Doris 中的变量参考 MySQL 中的变量设置。但部分变量仅用于兼容一些 MySQL 客户端协议，并不产生其在 MySQL 数据库中的实际意义。

## 变量设置与查看

### 查看

可以通过 `SHOW VARIABLES [LIKE 'xxx'];` 查看所有或指定的变量。如：

```sql
SHOW VARIABLES;
SHOW VARIABLES LIKE '%time_zone%';
```

### 设置

部分变量可以设置全局生效或仅当前会话生效。

注意，在 1.1 版本之前，设置全局生效后，后续新的会话连接中会沿用设置值，但当前会话中的值不变。
而在 1.1 版本（含）之后，设置全局生效后，后续新的会话连接中会沿用设置值，当前会话中的值也会改变。

仅当前会话生效，通过 `SET var_name=xxx;` 语句来设置。如：

```sql
SET exec_mem_limit = 137438953472;
SET forward_to_master = true;
SET time_zone = "Asia/Shanghai";
```

全局生效，通过 `SET GLOBAL var_name=xxx;` 设置。如：

```sql
SET GLOBAL exec_mem_limit = 137438953472
```

> 注1：只有 ADMIN 用户可以设置变量的全局生效。

既支持当前会话生效又支持全局生效的变量包括：

- `time_zone`
- `wait_timeout`
- `sql_mode`
- `enable_profile`
- `query_timeout`
- `insert_timeout`<version since="dev"></version>
- `exec_mem_limit`
- `batch_size`
- `allow_partition_column_nullable`
- `insert_visible_timeout_ms`
- `enable_fold_constant_by_be`

只支持全局生效的变量包括：

- `default_rowset_type`
- `default_password_lifetime`
- `password_history`
- `validate_password_policy`

同时，变量设置也支持常量表达式。如：

```sql
SET exec_mem_limit = 10 * 1024 * 1024 * 1024;
SET forward_to_master = concat('tr', 'u', 'e');
```

### 在查询语句中设置变量

在一些场景中，我们可能需要对某些查询有针对性的设置变量。 通过使用SET_VAR提示可以在查询中设置会话变量（在单个语句内生效）。例子：

```sql
SELECT /*+ SET_VAR(exec_mem_limit = 8589934592) */ name FROM people ORDER BY name;
SELECT /*+ SET_VAR(query_timeout = 1, enable_partition_cache=true) */ sleep(3);
```

注意注释必须以/*+ 开头，并且只能跟随在SELECT之后。

## 支持的变量

> 注：
> 
> 以下内容由 `docs/generate-config-and-variable-doc.sh` 自动生成。
> 
> 如需修改，请修改 `fe/fe-core/src/main/java/org/apache/doris/qe/SessionVariable.java` 和 `fe/fe-core/src/main/java/org/apache/doris/qe/GlobalVariable.java` 中的描述信息。

### `allow_partition_column_nullable`

是否允许 NULLABLE 列作为 PARTITION 列。开启后，RANGE PARTITION 允许 NULLABLE PARTITION 列（LIST PARTITION当前不支持）。默认开。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `analyze_timeout`

待补充

类型：`int`

默认值：`43200`

只读变量：`false`

仅全局变量：`true`

### `audit_plugin_max_batch_bytes`

待补充

类型：`long`

默认值：`52428800`

只读变量：`false`

仅全局变量：`true`

### `audit_plugin_max_batch_interval_sec`

待补充

类型：`long`

默认值：`60`

只读变量：`false`

仅全局变量：`true`

### `audit_plugin_max_sql_length`

待补充

类型：`int`

默认值：`4096`

只读变量：`false`

仅全局变量：`true`

### `auto_analyze_end_time`

该参数定义自动ANALYZE例程的结束时间

类型：`String`

默认值：`23:59:59`

只读变量：`false`

仅全局变量：`true`

### `auto_analyze_start_time`

该参数定义自动ANALYZE例程的开始时间

类型：`String`

默认值：`00:00:00`

只读变量：`false`

仅全局变量：`true`

### `auto_analyze_table_width_threshold`

参与自动收集的最大表宽度，列数多于这个参数的表不参与自动收集

类型：`int`

默认值：`100`

只读变量：`false`

仅全局变量：`true`

### `auto_broadcast_join_threshold`

待补充

类型：`double`

默认值：`0.8`

只读变量：`false`

仅全局变量：`false`

### `auto_increment_increment`

待补充

类型：`int`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `autocommit`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `batch_size`

待补充

类型：`int`

默认值：`4064`

只读变量：`false`

仅全局变量：`false`

### `character_set_client`

待补充

类型：`String`

默认值：`utf8mb4`

只读变量：`false`

仅全局变量：`false`

### `character_set_connection`

待补充

类型：`String`

默认值：`utf8mb4`

只读变量：`false`

仅全局变量：`false`

### `character_set_results`

待补充

类型：`String`

默认值：`utf8mb4`

只读变量：`false`

仅全局变量：`false`

### `character_set_server`

待补充

类型：`String`

默认值：`utf8mb4`

只读变量：`false`

仅全局变量：`false`

### `cloud_cluster`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `codegen_level`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `collation_connection`

待补充

类型：`String`

默认值：`utf8mb4_0900_bin`

只读变量：`false`

仅全局变量：`false`

### `collation_database`

待补充

类型：`String`

默认值：`utf8mb4_0900_bin`

只读变量：`false`

仅全局变量：`false`

### `collation_server`

待补充

类型：`String`

默认值：`utf8mb4_0900_bin`

只读变量：`false`

仅全局变量：`false`

### `cpu_resource_limit`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `create_table_partition_max_num`

建表时创建分区的最大数量

类型：`int`

默认值：`10000`

只读变量：`false`

仅全局变量：`false`

### `decimal_overflow_scale`

当decimal数值计算结果精度溢出时，计算结果最多可保留的小数位数

类型：`int`

默认值：`6`

只读变量：`false`

仅全局变量：`false`

### `default_password_lifetime`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`true`

### `default_rowset_type`

待补充

类型：`String`

默认值：`beta`

只读变量：`false`

仅全局变量：`true`

### `default_storage_engine`

待补充

类型：`String`

默认值：`olap`

只读变量：`false`

仅全局变量：`false`

### `default_tmp_storage_engine`

待补充

类型：`String`

默认值：`olap`

只读变量：`false`

仅全局变量：`false`

### `delete_without_partition`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `describe_extend_variant_column`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `disable_colocate_plan`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `disable_empty_partition_prune`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `disable_streaming_preaggregations`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `div_precision_increment`

待补充

类型：`int`

默认值：`4`

只读变量：`false`

仅全局变量：`false`

### `drop_table_if_ctas_failed`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `dry_run_query`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `dump_nereids_memo`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_agg_spill`

控制是否启用聚合算子落盘。默认为 false。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `experimental_enable_agg_state`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_analyze_complex_type_column`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_audit_plugin`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`true`

### `enable_auto_analyze`

该参数控制是否开启自动收集

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`true`

### `enable_bucket_shuffle_downgrade`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_bucket_shuffle_join`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_cbo_statistics`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_common_expr_pushdown`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_count_on_index_pushdown`

是否启用count_on_index pushdown。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_cte_materialize`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_decimal256`

控制是否在计算过程中使用Decimal256类型

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_delete_sub_predicate_v2`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_distinct_streaming_aggregation`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_dphyp_optimizer`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_dphyp_trace`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_eliminate_sort_node`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_exchange_node_parallel_merge`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_ext_func_pred_pushdown`

启用外部表（如通过ODBC或JDBC访问的表）查询中谓词的函数下推

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_fallback_to_original_planner`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_file_cache`

是否启用file cache。该变量只有在be.conf中enable_file_cache=true时才有效，如果be.conf中enable_file_cache=false，该BE节点的file cache处于禁用状态。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_fold_nondeterministic_fn`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_function_pushdown`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_hash_join_early_start_probe`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_insert_strict`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_inverted_index_query`

是否启用inverted index query。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_join_spill`

控制是否启用join算子落盘。默认为 false。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `deprecated_enable_local_exchange`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_materialized_view_rewrite`

是否开启基于结构信息的物化视图透明改写

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_memtable_on_sink_node`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_minidump`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_nereids_dml`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `experimental_enable_nereids_dml_with_pipeline`

在新优化器中，使用pipeline引擎执行DML

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_nereids_rules`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `enable_nereids_timeout`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_odbc_transcation`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_orc_lazy_materialization`

控制 orc reader 是否启用延迟物化技术。默认为 true。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_page_cache`

控制是否启用page cache。默认为 true。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_parallel_outfile`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_parquet_lazy_materialization`

控制 parquet reader 是否启用延迟物化技术。默认为 true。

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_partition_cache`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_profile`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_push_down_no_group_agg`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_pushdown_minmax_on_unique`

是否启用pushdown minmax on unique table。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_pushdown_string_minmax`

是否启用string类型min max下推。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_runtime_filter_prune`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_scan_node_run_serial`

是否开启ScanNode串行读，以避免limit较小的情况下的读放大，可以提高查询的并发能力

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_share_hash_table_for_broadcast_join`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_single_distinct_column_opt`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `experimental_enable_single_replica_insert`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_sort_spill`

控制是否启用排序算子落盘。默认为 false。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_spilling`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_sql_cache`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_stats`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_strict_consistency_dml`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_strong_consistency_read`

用以开启强一致读。Doris 默认支持同一个会话内的强一致性，即同一个会话内对数据的变更操作是实时可见的。如需要会话间的强一致读，则需将此变量设置为true。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_two_phase_read_opt`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `enable_unicode_name_support`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_unique_key_partial_update`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_variant_access_in_original_planner`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `enable_vectorized_engine`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `event_scheduler`

待补充

类型：`String`

默认值：`OFF`

只读变量：`false`

仅全局变量：`false`

### `exec_mem_limit`

待补充

类型：`long`

默认值：`2147483648`

只读变量：`false`

仅全局变量：`false`

### `expand_runtime_filter_by_inner_join`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `external_agg_bytes_threshold`

待补充

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `external_agg_partition_bits`

待补充

类型：`int`

默认值：`8`

只读变量：`false`

仅全局变量：`false`

### `external_sort_bytes_threshold`

待补充

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `external_table_analyze_part_num`

收集外表统计信息行数时选取的采样分区数，默认-1表示全部分区

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `external_table_auto_analyze_interval_in_millis`

控制对外表的自动ANALYZE的最小时间间隔，在该时间间隔内的外表仅ANALYZE一次

类型：`long`

默认值：`86400000`

只读变量：`false`

仅全局变量：`true`

### `extract_wide_range_expr`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `fallback_other_replica_when_fixed_corrupt`

当开启use_fix_replica时遇到故障，是否漂移到其他健康的副本

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `faster_float_convert`

是否启用更快的浮点数转换算法，注意会影响输出格式

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `file_cache_base_path`

指定block file cache在BE上的存储路径，默认 'random'，随机选择BE配置的存储路径。

类型：`String`

默认值：`random`

只读变量：`false`

仅全局变量：`false`

### `file_split_size`

待补充

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `forbid_unknown_col_stats`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `forward_to_master`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `fragment_transmission_compression_codec`

待补充

类型：`String`

默认值：`none`

只读变量：`false`

仅全局变量：`false`

### `generate_stats_factor`

待补充

类型：`int`

默认值：`5`

只读变量：`false`

仅全局变量：`false`

### `group_by_and_having_use_alias_first`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `group_commit`

待补充

类型：`String`

默认值：`off_mode`

只读变量：`false`

仅全局变量：`false`

### `group_concat_max_len`

待补充

类型：`long`

默认值：`2147483646`

只读变量：`false`

仅全局变量：`false`

### `have_query_cache`

待补充

类型：`boolean`

默认值：`false`

只读变量：`true`

仅全局变量：`false`

### `huge_table_auto_analyze_interval_in_millis`

控制对大表的自动ANALYZE的最小时间间隔，在该时间间隔内大小超过huge_table_lower_bound_size_in_bytes的表仅ANALYZE一次

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`true`

### `huge_table_default_sample_rows`

定义开启开启大表自动sample后，对大表的采样比例

类型：`long`

默认值：`4194304`

只读变量：`false`

仅全局变量：`true`

### `huge_table_lower_bound_size_in_bytes`

大小超过该值的表将会自动通过采样收集统计信息

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`true`

### `ignore_runtime_filter_ids`

在IGNORE_RUNTIME_FILTER_IDS列表中的runtime filter将不会被生成

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `ignore_shape_nodes`

'explain shape plan' 命令中忽略的PlanNode 类型

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `init_connect`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`true`

### `inline_cte_referenced_threshold`

待补充

类型：`int`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `insert_timeout`

待补充

类型：`int`

默认值：`14400`

只读变量：`false`

仅全局变量：`false`

### `insert_visible_timeout_ms`

待补充

类型：`long`

默认值：`10000`

只读变量：`false`

仅全局变量：`false`

### `interactive_timeout`

待补充

类型：`int`

默认值：`3600`

只读变量：`false`

仅全局变量：`false`

### `internal_session`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `inverted_index_conjunction_opt_threshold`

在match_all中求取多个倒排索引的交集时,如果最大的倒排索引中的总数是最小倒排索引中的总数的整数倍,则使用跳表来优化交集操作。

类型：`int`

默认值：`1000`

只读变量：`false`

仅全局变量：`false`

### `inverted_index_max_expansions`

这个参数用来限制查询时扩展的词项（terms）的数量，以此来控制查询的性能

类型：`int`

默认值：`50`

只读变量：`false`

仅全局变量：`false`

### `inverted_index_skip_threshold`

在倒排索引中如果预估命中量占比总量超过百分比阈值，则跳过索引直接进行匹配。

类型：`int`

默认值：`50`

只读变量：`false`

仅全局变量：`false`

### `jdbc_clickhouse_query_final`

是否在查询 ClickHouse JDBC 外部表时，对查询 SQL 添加 FINAL 关键字。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `join_order_time_limit`

待补充

类型：`long`

默认值：`1000`

只读变量：`false`

仅全局变量：`false`

### `language`

待补充

类型：`String`

默认值：`/palo/share/english/`

只读变量：`true`

仅全局变量：`false`

### `license`

待补充

类型：`String`

默认值：`Apache License, Version 2.0`

只读变量：`true`

仅全局变量：`false`

### `load_stream_per_node`

待补充

类型：`int`

默认值：`2`

只读变量：`false`

仅全局变量：`false`

### `lower_case_table_names`

待补充

类型：`int`

默认值：`0`

只读变量：`true`

仅全局变量：`false`

### `materialized_view_rewrite_enable_contain_external_table`

基于结构信息的透明改写，是否使用包含外表的物化视图

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `materialized_view_rewrite_success_candidate_num`

异步物化视图透明改写成功的结果集合，允许参与到CBO候选的最大数量

类型：`int`

默认值：`3`

只读变量：`false`

仅全局变量：`false`

### `max_allowed_packet`

待补充

类型：`int`

默认值：`1048576`

只读变量：`false`

仅全局变量：`false`

### `max_execution_time`

待补充

类型：`int`

默认值：`900000`

只读变量：`false`

仅全局变量：`false`

### `max_instance_num`

待补充

类型：`int`

默认值：`64`

只读变量：`false`

仅全局变量：`false`

### `max_msg_size_of_result_receiver`

Max message size during result deserialization, change this if you meet error like "MaxMessageSize reached"

类型：`int`

默认值：`104857600`

只读变量：`false`

仅全局变量：`false`

### `max_pushdown_conditions_per_column`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `max_scan_key_num`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `max_table_count_use_cascades_join_reorder`

待补充

类型：`int`

默认值：`10`

只读变量：`false`

仅全局变量：`false`

### `memo_max_group_expression_size`

待补充

类型：`int`

默认值：`10000`

只读变量：`false`

仅全局变量：`false`

### `min_revocable_mem`

待补充

类型：`long`

默认值：`33554432`

只读变量：`false`

仅全局变量：`false`

### `minidump_path`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `nereids_timeout_second`

待补充

类型：`int`

默认值：`5`

只读变量：`false`

仅全局变量：`false`

### `nereids_trace_event_mode`

待补充

类型：`String`

默认值：`all`

只读变量：`false`

仅全局变量：`false`

### `net_buffer_length`

待补充

类型：`int`

默认值：`16384`

只读变量：`true`

仅全局变量：`false`

### `net_read_timeout`

待补充

类型：`int`

默认值：`60`

只读变量：`false`

仅全局变量：`false`

### `net_write_timeout`

待补充

类型：`int`

默认值：`60`

只读变量：`false`

仅全局变量：`false`

### `num_scanner_threads`

ScanNode扫描数据的最大并发，默认为0，采用BE的doris_scanner_thread_pool_thread_num

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `parallel_exchange_instance_num`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `parallel_fragment_exec_instance_num`

待补充

类型：`int`

默认值：`8`

只读变量：`false`

仅全局变量：`false`

### `parallel_pipeline_task_num`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `parallel_sync_analyze_task_num`

待补充

类型：`int`

默认值：`2`

只读变量：`false`

仅全局变量：`false`

### `partition_pruning_expand_threshold`

待补充

类型：`int`

默认值：`10`

只读变量：`false`

仅全局变量：`false`

### `partitioned_hash_agg_rows_threshold`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `partitioned_hash_join_rows_threshold`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `password_history`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`true`

### `performance_schema`

待补充

类型：`String`

默认值：`OFF`

只读变量：`true`

仅全局变量：`false`

### `plan_nereids_dump`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `prefer_join_method`

待补充

类型：`String`

默认值：`broadcast`

只读变量：`false`

仅全局变量：`false`

### `profile_level`

待补充

类型：`int`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `profiling`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `query_cache_size`

待补充

类型：`long`

默认值：`1048576`

只读变量：`false`

仅全局变量：`true`

### `query_cache_type`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `repeat_max_num`

待补充

类型：`int`

默认值：`10000`

只读变量：`false`

仅全局变量：`false`

### `resource_group`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `rewrite_count_distinct_to_bitmap_hll`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `round_precise_decimalv2_value`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `runtime_filter_jump_threshold`

待补充

类型：`int`

默认值：`2`

只读变量：`false`

仅全局变量：`false`

### `runtime_filter_prune_for_external`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `scan_queue_mem_limit`

待补充

类型：`long`

默认值：`107374182`

只读变量：`false`

仅全局变量：`false`

### `scanner_scale_up_ratio`

ScanNode自适应的增加扫描并发，最大允许增长的并发倍率，默认为0，关闭该功能

类型：`double`

默认值：`0.0`

只读变量：`false`

仅全局变量：`false`

### `send_batch_parallelism`

待补充

类型：`int`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `session_context`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`

### `show_all_fe_connection`

when it's true show processlist statement list all fe's connection

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `show_full_dbname_in_info_schema_db`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`true`

### `show_hidden_columns`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `show_user_default_role`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_bad_tablet`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_delete_bitmap`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_delete_predicate`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_delete_sign`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_missing_version`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `skip_storage_engine_merge`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `sql_auto_is_null`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `sql_converter_service_url`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`true`

### `sql_dialect`

解析sql使用的方言

类型：`String`

默认值：`doris`

只读变量：`false`

仅全局变量：`false`

### `sql_mode`

待补充

类型：`long`

默认值：`1`

只读变量：`false`

仅全局变量：`false`

### `sql_quote_show_create`

待补充

类型：`boolean`

默认值：`true`

只读变量：`false`

仅全局变量：`false`

### `sql_safe_updates`

待补充

类型：`int`

默认值：`0`

只读变量：`false`

仅全局变量：`false`

### `stats_insert_merge_item_count`

控制统计信息相关INSERT攒批数量

类型：`int`

默认值：`200`

只读变量：`false`

仅全局变量：`true`

### `storage_engine`

待补充

类型：`String`

默认值：`olap`

只读变量：`false`

仅全局变量：`false`

### `system_time_zone`

待补充

类型：`String`

默认值：`Asia/Shanghai`

只读变量：`true`

仅全局变量：`false`

### `table_stats_health_threshold`

取值在0-100之间，当自上次统计信息收集操作之后数据更新量达到 (100 - table_stats_health_threshold)% ，认为该表的统计信息已过时

类型：`int`

默认值：`60`

只读变量：`false`

仅全局变量：`true`

### `test_query_cache_hit`

用于测试查询缓存是否命中，如果未命中指定类型的缓存，则会报错

类型：`String`

默认值：`none`

可选值：`none`, `sql_cache`, `partition_cache`

只读变量：`false`

仅全局变量：`false`

### `time_zone`

待补充

类型：`String`

默认值：`Asia/Shanghai`

只读变量：`false`

仅全局变量：`false`

### `topn_opt_limit_threshold`

待补充

类型：`long`

默认值：`1024`

只读变量：`false`

仅全局变量：`false`

### `trace_nereids`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `transaction_isolation`

待补充

类型：`String`

默认值：`REPEATABLE-READ`

只读变量：`false`

仅全局变量：`false`

### `transaction_read_only`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `trim_tailing_spaces_for_external_table_query`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `truncate_char_or_varchar_columns`

是否按照表的 schema 来截断 char 或者 varchar 列。默认为 false。
因为外表会存在表的 schema 中 char 或者 varchar 列的最大长度和底层 parquet 或者 orc 文件中的 schema 不一致的情况。此时开启改选项，会按照表的 schema 中的最大长度进行截断。

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `tx_isolation`

待补充

类型：`String`

默认值：`REPEATABLE-READ`

只读变量：`false`

仅全局变量：`false`

### `tx_read_only`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `use_fix_replica`

待补充

类型：`int`

默认值：`-1`

只读变量：`false`

仅全局变量：`false`

### `use_rf_default`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `use_v2_rollup`

待补充

类型：`boolean`

默认值：`false`

只读变量：`false`

仅全局变量：`false`

### `validate_password_policy`

待补充

类型：`long`

默认值：`0`

只读变量：`false`

仅全局变量：`true`

### `version`

待补充

类型：`String`

默认值：`5.7.99`

只读变量：`true`

仅全局变量：`false`

### `version_comment`

待补充

类型：`String`

默认值：`Doris version doris-2.1.0-rc11-8be50a95ae`

只读变量：`true`

仅全局变量：`false`

### `wait_full_block_schedule_times`

待补充

类型：`int`

默认值：`2`

只读变量：`false`

仅全局变量：`false`

### `wait_timeout`

待补充

类型：`int`

默认值：`28800`

只读变量：`false`

仅全局变量：`false`

### `workload_group`

待补充

类型：`String`

默认值：``

只读变量：`false`

仅全局变量：`false`



