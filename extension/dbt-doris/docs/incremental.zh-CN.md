# dbt-doris Incremental

本文描述当前代码的实际行为。开发计划和未完成项见
[dbt-doris TODO](dbt-doris-todo-list.zh-CN.md)。

## 策略总览

| `incremental_strategy` | 目标表 | 普通增量 SQL | dbt 物理 Staging |
| --- | --- | --- | :---: |
| `append` | Duplicate Key | `INSERT INTO ... SELECT` | 否 |
| `merge` | MOW Unique Key | `INSERT INTO ... SELECT` Upsert | 否 |
| `delete+insert`，目标为无 Sequence 的 MOW Unique Key | MOW Unique Key | 自动路由到单条 Upsert | 否 |
| `delete+insert`，目标为 MOR Unique Key | MOR Unique Key | 事务内 `DELETE USING` + `INSERT` | 是 |
| `delete+insert`，目标带 Sequence Column | Unique Key | 前置拒绝；改用 `merge` | 否 |
| `insert_overwrite` | Doris 可写目标表 | 原生 `INSERT OVERWRITE` | 否 |

未显式配置策略时，dbt 的策略名仍为标准 `default`：

- 配置了 `unique_key`：按 `merge` 执行；
- 没有 `unique_key`：按 `append` 执行。

`microbatch` 和 Doris 4.1+ 原生 `MERGE INTO` 尚未启用。

## 为什么大多数策略不再创建物理临时表

`append`、`merge` 和 `insert_overwrite` 都只需要一条最终 DML。普通
`on_schema_change='ignore'` 运行先建立一个不保存数据的逻辑 View；最终 DML
读取该 View：

```sql
insert into target (`id`, `value`)
select source.`id`, source.`value`
from model__dbt_tmp source;
```

因此 Model 数据只写入目标一次；逻辑 View 只保存 SQL 定义，同时让 dbt 获得
准确的 `VARCHAR(n)` 等类型，并保持标准五 Key Strategy Macro 对
`temp_relation` 的契约。成功后 View 会被清理。

以下场景仍会创建物理表：

- `on_schema_change` 为 `fail`、`append_new_columns` 或
  `sync_all_columns` 时，必须在修改目标 Schema 前冻结本批结果；否则读取
  `{{ this }}` 的 Model SQL 可能在 DROP/ADD 后无法再次执行；
- MOR Unique Key 执行真实 `delete+insert` 时，必须冻结同一批 Source，供
  DELETE 和 INSERT 各读取一次；
- `--full-refresh` 必须先完整构建中间表，再原子交换目标；
- 自定义多语句 Strategy 默认获得命名的物理 Source Relation。

## `append`

```jinja
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    duplicate_key=['id'],
    distributed_by=['id']
  )
}}

select id, value
from {{ ref('source_model') }}

{% if is_incremental() %}
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
```

Append 不检查 Key，也不去重。重复选择同一批数据会产生重复行。已有目标必须是
Duplicate Key 表，否则 Adapter 会要求 `--full-refresh`。

## `merge`：兼容 Doris 2.1+ 的全行 Upsert

```jinja
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['tenant_id', 'id'],
    distributed_by=['tenant_id']
  )
}}
```

首次运行创建 Merge-on-Write Unique Key 表；后续运行执行一条
`INSERT INTO ... SELECT`。同 Key 行被完整替换，新 Key 被插入，本批未出现的旧
Key 保留。

Adapter 会校验：

- `unique_key` 存在，且 Model 输出包含全部 Key；
- 现有目标是 MOW Unique Key；
- 配置 Key 与物理 Key 的顺序和内容完全一致；
- 同一批 Source 不得出现重复 Key；即使目标有 Sequence Column，当前 Adapter
  也要求上游先确定性去重。

当前 `merge` 是全行 Upsert，不是 Doris 4.1 的原生 `MERGE INTO`。
`merge_update_columns`、`merge_exclude_columns` 和
`incremental_predicates` 会在写数据前报错。

## `delete+insert`

配置名必须包含加号：

```jinja
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['id'],
    distributed_by=['id']
  )
}}
```

不能写成 `delete_insert`。

对于默认创建、且没有 Sequence Column 的 MOW Unique Key 目标，按 Key删除后
插入与全行 Upsert 的最终结果相同，因此 Adapter 自动走 `merge` 的单条写入
路径，不创建物理 Staging。带 Sequence Column 时，DELETE Tombstone 可能继承
旧 Sequence，并继续压制低 Sequence 的替换 INSERT；因此 Adapter 前置拒绝该
组合，用户应改用 `merge` 保留 Doris Sequence 语义。

现有目标为 MOR Unique Key 时，执行真正的两语句路径：

```text
CTAS staging
  -> 校验 staging 内没有重复 Key
  -> BEGIN
  -> DELETE target USING staging
  -> INSERT INTO target SELECT ... FROM staging
  -> COMMIT
  -> DROP staging
```

这条路径要求正式发布的 Doris 3.0+，并要求部署模式支持显式事务中的
`DELETE + INSERT SELECT`。Doris 2.1 会在事务内拒绝 DELETE，不会先删除后以成功
状态继续；数据库语句失败时 Adapter 显式回滚连接。版本号为 `0.0.0` 的源码构建
无法证明具备该事务能力，会在创建 Staging 前明确拒绝。

要在首次构建时创建 MOR 目标，可明确配置：

```jinja
properties={
  'enable_unique_key_merge_on_write': 'false'
}
```

同一个 Model 不应并发运行真实的 staged `delete+insert`；当前物理 Staging 名称
是确定的，异常退出留下的 Relation 会在下次运行开始时清理。

## `insert_overwrite`

整表覆盖：

```jinja
{{ config(
  materialized='incremental',
  incremental_strategy='insert_overwrite'
) }}
```

指定分区：

```jinja
{{ config(
  materialized='incremental',
  incremental_strategy='insert_overwrite',
  partition_by=['event_date'],
  overwrite_partitions=['p202607', 'p202608']
) }}
```

自动识别本批涉及的分区：

```jinja
overwrite_partitions='*'
```

对应 Doris 原生 SQL 分别为：

```sql
insert overwrite table target ...;
insert overwrite table target partition (`p202607`, `p202608`) ...;
insert overwrite table target partition(*) ...;
```

`PARTITION(*)` 要求 Doris 2.1.3+。空 Source 无法自动识别需要清空的分区；需要清空
空分区时应显式列出分区名。

## Schema Change 与 Full Refresh

支持 dbt 1.12 的四种 `on_schema_change`：

- `ignore`
- `fail`
- `append_new_columns`
- `sync_all_columns`

Doris Column Schema Change 可能异步执行。Adapter 在 ALTER 后轮询
`SHOW ALTER TABLE COLUMN`，直到 `FINISHED`；`CANCELLED` 和超时会使本轮失败。
增量运行不能修改 Unique Key 列类型，这种变更必须使用 `--full-refresh`。

表到表的 Full Refresh 会先创建完整中间表，再使用 Doris
`REPLACE WITH TABLE ... swap=true` 原子交换。View 切换成 Table 时 Doris
没有跨对象类型的原子 rename；Adapter 先保留旧对象到 Backup，再切换新表。若
第二步失败，下次运行会先把 Backup 恢复到标准目标名，而不会删除唯一可用副本。

## 失败前置校验

以下配置会在目标数据写入前失败：

- 内置策略不支持 `incremental_predicates`；
- `merge` 不支持局部更新列配置；
- `merge` 与 `enable_unique_key_merge_on_write=false` 冲突；
- `delete+insert` 用于带物理 Sequence Column 的目标；
- dbt-doris 尚未实现 `grants` 配置；
- 裸 `sequence_col` 配置未实现；应使用 Doris Table Property；
- Key 缺失、非法、与物理 Unique Key 不一致；
- Source 缺少 Key 或同批 Key 重复；
- 分区覆盖配置用于非 `insert_overwrite` 策略；
- `overwrite_partitions` 为空、混用 `*` 与分区名，或包含不安全标识符。
