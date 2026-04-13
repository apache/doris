# 表级别事件驱动预热（Event-Driven Warmup）用户指南

## 概述

事件驱动预热（Event-Driven Warmup）是 Apache Doris 存算分离架构下的一项数据缓存预热功能。当源集群（Source Compute Group）发生数据写入时，系统会自动将新写入的数据预热到目标集群（Destination Compute Group）的本地 File Cache 中，使目标集群上的查询能够直接命中本地缓存，避免远程读取 S3/HDFS，大幅降低查询延迟。

**表级别预热**是对原有**集群级别预热**的增强。集群级别预热会将源集群上所有表的新写入数据全量同步到目标集群，而表级别预热允许用户精确指定需要预热的表范围，只同步真正需要的数据，减少不必要的网络传输和缓存空间占用。

### 适用场景

| 场景 | 说明 |
|------|------|
| **读写分离架构** | 写入集群负责数据导入，读集群负责查询。只需预热查询涉及的核心表 |
| **数据仓库分层** | ODS/DWD/DWS/ADS 多层结构，不同读集群只关注特定层的表 |
| **多租户场景** | 不同业务团队使用不同的读集群，各自只需要预热自己关心的表 |
| **成本优化** | 集群中表数量多（数百甚至数千张），但查询热点只集中在少数表上 |

---

## 语法

### 基本语法

```sql
WARM UP COMPUTE GROUP <目标集群> WITH COMPUTE GROUP <源集群>
ON TABLES (
    {INCLUDE|EXCLUDE} '<pattern>'
    [, {INCLUDE|EXCLUDE} '<pattern>' ...]
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- INCLUDE 和 EXCLUDE 可以任意顺序书写，至少包含一条 INCLUDE
```

### 语法说明

- **`<目标集群>`**：数据将被预热到此集群的本地缓存中
- **`<源集群>`**：数据写入发生在此集群上，系统监听此集群的写入事件
- **`ON TABLES (...)`**：可选子句，指定需要预热的表范围。省略时为集群级别全量预热
- **`PROPERTIES`**：固定属性，`sync_mode` 为 `event_driven`，`sync_event` 为 `load`

### 模式格式

所有模式使用 **`'库名.表名'`** 格式，用单引号包裹。支持以下通配符：

| 通配符 | 含义 | 示例 |
|--------|------|------|
| `*` | 匹配任意数量的任意字符（包括零个） | `'ods.*'` 匹配 ods 库下所有表 |
| `?` | 匹配恰好一个任意字符 | `'db.log_202?'` 匹配 `log_2020` 到 `log_2029` |

不使用通配符时为精确匹配：`'sales.orders'` 仅匹配 `sales` 库的 `orders` 表。

---

## INCLUDE 与 EXCLUDE 规则

### 基本概念

- **INCLUDE**：声明需要预热的表范围。只有被 INCLUDE 规则匹配到的表才会被纳入预热
- **EXCLUDE**：从 INCLUDE 结果中排除特定表。用于在大范围 INCLUDE 基础上去除不需要的表

### 计算逻辑

系统按以下两步计算最终需要预热的表集合：

```
第一步：计算 INCLUDE 集合
    → 遍历所有内表，凡是匹配任意一条 INCLUDE 规则的表，纳入候选集合

第二步：计算 EXCLUDE 集合
    → 在候选集合中，凡是匹配任意一条 EXCLUDE 规则的表，从候选集合中移除

最终预热范围 = INCLUDE 匹配的表 − EXCLUDE 匹配的表
```

### 书写规则

1. **INCLUDE 和 EXCLUDE 可以任意顺序书写**。系统内部会自动将规则按类型分组，并进行排序和去重

   ```sql
   -- ✅ 以下两种写法等价，系统内部自动规范化
   ON TABLES (
       INCLUDE 'ods.*',
       INCLUDE 'dw.*',
       EXCLUDE '*.tmp_*'
   )

   ON TABLES (
       INCLUDE 'ods.*',
       EXCLUDE '*.tmp_*',
       INCLUDE 'dw.*'
   )
   ```

2. **至少包含一条 INCLUDE 规则**。纯 EXCLUDE 没有意义（默认不纳入任何表）

   ```sql
   -- ❌ 错误：没有 INCLUDE
   ON TABLES (
       EXCLUDE 'mydb.tmp_table'
   )
   ```

3. **匹配区分大小写**。库名和表名的匹配遵循 Doris 的命名规则，大小写敏感

4. **所有模式使用单引号包裹**

   ```sql
   ON TABLES (
       INCLUDE 'mydb.orders',
       INCLUDE 'mydb.dim_*',
       EXCLUDE 'mydb.dim_*_bak'
   )
   ```

---

## 查看预热 Job

```sql
SHOW WARM UP JOB;
```

也可以按 Job ID 查看单个 Job：

```sql
SHOW WARM UP JOB WHERE id = <job_id>;
```

### 输出列说明

`SHOW WARM UP JOB` 输出共 **15 列**，其中 13 列为已有字段，2 列为表级别预热新增：

| 列名 | 说明 |
|------|------|
| JobId | Job 唯一标识 |
| SrcComputeGroup | 源集群（被监听写入事件的集群） |
| DstComputeGroup | 目标集群（缓存被预热的集群） |
| Status | Job 状态（RUNNING / FINISHED / CANCELLED 等） |
| Type | Job 类型（CLUSTER） |
| SyncMode | 同步模式（如 `EVENT_DRIVEN (LOAD)`） |
| CreateTime | 创建时间 |
| StartTime | 启动时间 |
| FinishBatch | 已完成批次数 |
| AllBatch | 总批次数 |
| FinishTime | 完成时间（event-driven 类型持续运行，通常显示 N/A） |
| ErrMsg | 错误信息 |
| Tables | 显式指定的表列表（用于 ONCE/PERIODIC 模式的表级别 Job） |
| **TableFilter** | **新增**。`ON TABLES` 规则，以规范化 JSON 格式展示。集群级别 Job 此列为空 |
| **MatchedTables** | **新增**。当前实际匹配的表名列表。基于内部表 ID 反查，始终展示最新名称 |

### MatchedTables 与 TableFilter 的区别

- `TableFilter` 是规范化后的规则表达式（canonical JSON），展示格式固定，与书写顺序无关
- `MatchedTables` 是当前实际匹配的表，会反映表的新建、重命名和删除等变化

### 输出示例

**集群级别 Job（无 ON TABLES）**：

```
+-------+-----------------+-----------------+---------+---------+---------------------+---------------------------+-----------+--------+-------------+---------------+
| JobId | SrcComputeGroup | DstComputeGroup | Status  | Type    | SyncMode            | CreateTime                | ...       | Tables | TableFilter | MatchedTables |
+-------+-----------------+-----------------+---------+---------+---------------------+---------------------------+-----------+--------+-------------+---------------+
| 13418 | write_cg        | read_cg         | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | 2024-01-01 10:00:00.000   | ...       |        |             |               |
+-------+-----------------+-----------------+---------+---------+---------------------+---------------------------+-----------+--------+-------------+---------------+
```

> 集群级别 Job 的 `TableFilter` 和 `MatchedTables` 均为空。

**表级别 Job（带 ON TABLES）**：

```
+-------+-----------------+-----------------+---------+---------+---------------------+---------------------------+-----------+--------+-----------------------------------------------+-----------------------------------------+
| JobId | SrcComputeGroup | DstComputeGroup | Status  | Type    | SyncMode            | CreateTime                | ...       | Tables | TableFilter                                   | MatchedTables                           |
+-------+-----------------+-----------------+---------+---------+---------------------+---------------------------+-----------+--------+-----------------------------------------------+-----------------------------------------+
| 13419 | ingestion_cg    | analytics_cg    | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | 2024-01-01 10:05:00.000   | ...       |        | {"include":["ods.*"],"exclude":["ods.tmp_*"]} | ods.orders, ods.payments, ods.users     |
+-------+-----------------+-----------------+---------+---------+---------------------+---------------------------+-----------+--------+-----------------------------------------------+-----------------------------------------+
```

> `TableFilter` 展示规范化后的 JSON。`MatchedTables` 是当前实际匹配到的表名（按字典序排列）。

**多个 Job 同时存在**：

```
+-------+-----------------+-----------------+---------+---------+---------------------+-----------+-----------------------------------------------+----------------------------------------------+
| JobId | SrcComputeGroup | DstComputeGroup | Status  | Type    | SyncMode            | ...       | TableFilter                                   | MatchedTables                                |
+-------+-----------------+-----------------+---------+---------+---------------------+-----------+-----------------------------------------------+----------------------------------------------+
| 13418 | write_cg        | read_cg         | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | ...       |                                               |                                              |
| 13419 | ingestion_cg    | analytics_cg    | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | ...       | {"include":["ods.*"],"exclude":["ods.tmp_*"]} | ods.orders, ods.payments, ods.users           |
| 13420 | ingestion_cg    | realtime_cg     | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | ...       | {"include":["dw.fact_*","dw.dim_*"]}          | dw.dim_date, dw.fact_orders, dw.fact_sales   |
+-------+-----------------+-----------------+---------+---------+---------------------+-----------+-----------------------------------------------+----------------------------------------------+
```

> Job 13418 是集群级别（全量预热），13419 和 13420 是不同规则的表级别 Job，它们可以共存。

---

## 取消预热 Job

```sql
CANCEL WARM UP JOB WHERE id = <job_id>;
```

取消后，系统停止监听源集群的写入事件，不再向目标集群预热数据。已预热的缓存数据不受影响（不会被主动清除，按正常 LRU 策略淘汰）。

---

## 使用示例

### 示例 1：预热指定的几张表

最简单的场景：只需要预热确切的几张表。不使用通配符即为精确匹配。

```sql
-- 场景：报表系统只查询 sales.orders、sales.customers、inventory.stock_level 三张表
WARM UP COMPUTE GROUP report_cg WITH COMPUTE GROUP business_cg
ON TABLES (
    INCLUDE 'sales.orders',
    INCLUDE 'sales.customers',
    INCLUDE 'inventory.stock_level'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

### 示例 2：预热整个数据库

使用 `*` 通配符匹配库下所有表。

```sql
-- 场景：analytics_db 库有 50 张表，全部需要预热到分析集群
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP load_cg
ON TABLES (
    INCLUDE 'analytics_db.*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

### 示例 3：预热整个库但排除临时表

INCLUDE 配合 EXCLUDE 实现"除了某些表以外全部预热"。

```sql
-- 场景：ods 库有数百张表，其中 tmp_ 开头的是临时表不需要预热
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP ingestion_cg
ON TABLES (
    INCLUDE 'ods.*',
    EXCLUDE 'ods.tmp_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 结果：预热 ods 库下除 tmp_ 开头以外的所有表
-- 例：ods.orders ✅  ods.users ✅  ods.tmp_log ❌  ods.tmp_detail ❌
```

### 示例 4：预热多个库

多条 INCLUDE 规则取并集。

```sql
-- 场景：数据仓库的 ODS 层和 DW 层需要预热，其他层不需要
WARM UP COMPUTE GROUP query_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE 'ods.*',
    INCLUDE 'dw.*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 结果：预热 ods 库和 dw 库下的所有表
```

### 示例 5：多库预热，排除备份表和测试表

INCLUDE 多库后用 EXCLUDE 统一排除特定模式。

```sql
-- 场景：预热 ods 和 dw 两个库，但排除所有库中 _bak 后缀的备份表和 test_ 前缀的测试表
WARM UP COMPUTE GROUP query_cg WITH COMPUTE GROUP load_cg
ON TABLES (
    INCLUDE 'ods.*',
    INCLUDE 'dw.*',
    EXCLUDE '*.*_bak',
    EXCLUDE '*.test_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 结果：
--   INCLUDE = ods 库所有表 ∪ dw 库所有表
--   EXCLUDE = 任意库中 _bak 结尾的表 ∪ 任意库中 test_ 开头的表
--   最终预热 = INCLUDE − EXCLUDE
--   例：ods.orders ✅  dw.fact_sales ✅  ods.orders_bak ❌  dw.test_report ❌
```

### 示例 6：按命名规范匹配事实表和维度表

当表名遵循 `fact_xxx`、`dim_xxx` 等命名规范时，用多条 INCLUDE 规则分别匹配。

```sql
-- 场景：dw 库中的表命名为 fact_xxx（事实表）、dim_xxx（维度表）、tmp_xxx（临时表）、stg_xxx（暂存表）
-- 只需预热事实表和维度表
WARM UP COMPUTE GROUP bi_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE 'dw.fact_*',
    INCLUDE 'dw.dim_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 结果：预热 dw.fact_sales、dw.fact_orders、dw.dim_product、dw.dim_region 等
--       不预热 dw.tmp_load、dw.stg_raw 等
```

### 示例 7：使用 `?` 通配符匹配单个字符

`?` 可以匹配恰好一个任意字符，适用于匹配编号或年份。

```sql
-- 场景：logs 库中有 access_2020 到 access_2029 的年度日志表
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE 'logs.access_202?'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 匹配：logs.access_2020, logs.access_2021, ..., logs.access_2029
-- 不匹配：logs.access_20234（? 只匹配一个字符）
```

### 示例 8：多种模式灵活组合

精确匹配和通配符可以自由组合。

```sql
-- 场景：
-- 1. 精确预热 core_db.config 和 core_db.metadata 两张配置表
-- 2. 预热 report_db 库下所有 monthly_ 开头的月报表
-- 3. 预热所有库中 sales_ 开头的表
-- 4. 预热 log_db 中 log_A、log_B、log_C 等单字母后缀的日志表（使用 ? 通配符）
-- 5. 排除所有库中的 _archive 后缀表
WARM UP COMPUTE GROUP query_cg WITH COMPUTE GROUP write_cg
ON TABLES (
    INCLUDE 'core_db.config',
    INCLUDE 'core_db.metadata',
    INCLUDE 'report_db.monthly_*',
    INCLUDE '*.sales_*',
    INCLUDE 'log_db.log_?',
    EXCLUDE '*.*_archive'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

### 示例 9：集群级别全量预热（不使用 ON TABLES）

省略 `ON TABLES` 子句即为集群级别全量预热，与原有语法完全兼容。

```sql
-- 场景：源集群上所有表的新写入数据都需要预热到目标集群
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

### 示例 10：一张表预热到多个目标集群

需要为每个目标集群分别创建 Job。

```sql
-- 场景：sales.orders 表需要同时预热到三个不同用途的读集群
WARM UP COMPUTE GROUP realtime_cg WITH COMPUTE GROUP write_cg
ON TABLES (INCLUDE 'sales.orders')
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");

WARM UP COMPUTE GROUP batch_cg WITH COMPUTE GROUP write_cg
ON TABLES (INCLUDE 'sales.orders')
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");

WARM UP COMPUTE GROUP ad_hoc_cg WITH COMPUTE GROUP write_cg
ON TABLES (INCLUDE 'sales.orders')
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");
```

### 示例 11：复杂的数据仓库分层预热

一个综合性的真实场景示例。

```sql
-- 场景：
-- 数仓有 ods、dwd、dws、ads 四层
-- analytics_cg 需要预热 dws 和 ads 两层，但排除所有临时表和备份表
-- 另外还需要预热 ods 层中的几张核心表
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE 'dws.*',
    INCLUDE 'ads.*',
    INCLUDE 'ods.order_detail',
    INCLUDE 'ods.user_profile',
    EXCLUDE '*.tmp_*',
    EXCLUDE '*.*_bak',
    EXCLUDE '*.*_backup'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- INCLUDE 集合 = dws 库所有表 ∪ ads 库所有表 ∪ ods.order_detail ∪ ods.user_profile
-- EXCLUDE 集合 = 任意库中 tmp_ 开头的表 ∪ _bak 结尾的表 ∪ _backup 结尾的表
-- 最终预热 = INCLUDE − EXCLUDE
```

---

## 模式速查表

以下是常用的模式写法：

| 模式 | 含义 | 匹配示例 | 不匹配示例 |
|------|------|----------|------------|
| `'mydb.*'` | mydb 库下所有表 | mydb.orders, mydb.users | other_db.orders |
| `'*.orders'` | 所有库中名为 orders 的表 | sales.orders, prod.orders | sales.order_detail |
| `'mydb.order_*'` | mydb 库下 order_ 开头的表 | mydb.order_2024, mydb.order_detail | mydb.orders |
| `'*.*_bak'` | 所有库中 _bak 结尾的表 | db1.orders_bak, db2.users_bak | db1.orders |
| `'mydb.log_202?'` | mydb 库下 log_202 + 一个字符的表 | mydb.log_2023, mydb.log_2024 | mydb.log_20234 |
| `'*.*'` | 所有库的所有表 | 匹配一切 | — |
| `'db?.orders'` | db + 一个字符的库中的 orders 表 | db1.orders, db2.orders | db10.orders |
| `'sales.orders'` | 精确匹配 sales 库的 orders 表 | sales.orders | sales.order_detail |
| `'*.fact_*'` | 所有库中 fact_ 开头的表 | dw.fact_sales, ods.fact_user | dw.dim_product |
| `'dw.*_daily'` | dw 库下 _daily 结尾的表 | dw.sales_daily, dw.traffic_daily | dw.sales_weekly |

> **提示**：`.`（点号）在模式中是字面量，用作库名和表名的分隔符，不是通配符。

---

## 行为说明与注意事项

### 模式匹配时机

`ON TABLES` 中的匹配规则在 Job **创建时执行一次**，并在后续**每 60 秒自动重新评估**。系统会定期扫描所有符合条件的表，将匹配到的表 ID 下发到源集群的 BE 节点。

**这意味着**：
- 创建 Job 后**新建的表**，只要表名匹配模式规则，会在下一个刷新周期（最长 60 秒内）**自动纳入预热范围**
- 已匹配的表被 **DROP** 后，下次刷新时**自动从预热范围移除**
- 已匹配的表被 **RENAME** 后，是否继续预热取决于新表名是否仍匹配模式

> **延迟窗口**：新建表到被纳入预热之间最多有 60 秒延迟。在此期间写入的数据不会触发预热，但后续写入的数据会正常预热。

### 表被删除（DROP TABLE）

如果已匹配的某张表被删除：
- 该表不会再有新数据写入，因此自然不会再产生预热动作
- **Job 不会自动取消**，其余匹配的表继续正常预热
- 下次定期刷新后（≤60s），`SHOW WARM UP JOB` 的 `MatchedTables` 中不再显示已删除的表

### 表被重命名（ALTER TABLE RENAME）

如果已匹配的某张表被重命名：
- **是否继续预热取决于新表名是否仍匹配模式**
  - 例如模式为 `INCLUDE 'db.order_*'`，将 `order_2024` 重命名为 `order_2024_v2` → 仍匹配 → 继续预热
  - 将 `order_2024` 重命名为 `archive_2024` → 不再匹配 → 停止预热
- 变化在下次定期刷新后生效（≤60s）
- `SHOW WARM UP JOB` 的 `MatchedTables` 始终展示当前匹配的表名

### Schema Change

Schema Change（加列、改列类型等）**不影响预热**。Schema Change 过程中产生的新数据仍会被正确预热。

### 创建时无匹配表

如果 `ON TABLES` 的规则在创建时匹配不到任何表，Job 创建会报错：

```
ERROR: No tables matched the ON TABLES filter
```

请检查规则中的库名和表名是否正确。

> 虽然系统会定期刷新匹配结果（可自动纳入新表），但创建时仍要求至少匹配一张表，以防止拼写错误导致创建了一个无效的 Job。如果需要提前创建 Job、稍后再建表，请先创建至少一张匹配的表。

### 集群级别 Job 与表级别 Job 共存

同一对源集群和目标集群之间，可以同时存在集群级别 Job 和表级别 Job。

```sql
-- Job 1：集群级别，预热所有表
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");

-- Job 2：表级别，只预热 db.hot_table
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
ON TABLES (INCLUDE 'db.hot_table')
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");
```

在这种情况下，`db.hot_table` 的数据会被两个 Job 分别触发预热。目标集群会自动去重，第二次发送会被跳过，不会有实际开销。但为了管理清晰，建议避免这种重叠配置。

### 大小写

库名和表名的匹配遵循 Doris 的命名规则：
- Doris 默认将未加反引号的标识符转为小写
- 匹配时区分大小写
- 建议在模式中使用与实际库名/表名一致的大小写

---

## 完整操作流程

以下是使用表级别事件驱动预热的完整操作步骤：

### 第一步：确认集群环境

```sql
-- 查看当前的 Compute Group
SHOW COMPUTE GROUPS;
```

确认源集群（负责数据写入）和目标集群（需要缓存预热）的名称。

### 第二步：确认需要预热的表

```sql
-- 查看指定库下的表
SHOW TABLES FROM ods;
SHOW TABLES FROM dw;
```

明确哪些表需要预热，选择合适的模式。

### 第三步：创建预热 Job

```sql
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
ON TABLES (
    INCLUDE 'ods.*',
    INCLUDE 'dw.fact_*',
    EXCLUDE 'ods.tmp_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

### 第四步：查看 Job 状态

```sql
SHOW WARM UP JOB;
```

确认 Job 状态为 RUNNING，检查 `MatchedTables` 列中匹配到的表是否符合预期。

### 第五步：验证预热效果

在目标集群上查询已预热表的数据，观察查询延迟是否降低（首次查询应命中本地缓存，无需远程读取）。

### 需要调整时：取消并重建

```sql
-- 取消旧 Job
CANCEL WARM UP JOB WHERE id = <job_id>;

-- 创建新 Job（规则可以调整）
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
ON TABLES (
    INCLUDE 'ods.*',
    INCLUDE 'dw.*',
    EXCLUDE '*.tmp_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

> **提示**：由于系统会自动追踪表的新建/删除/重命名，大多数情况下不需要手动重建 Job。只有在需要修改 INCLUDE/EXCLUDE 规则本身时才需要取消并重建。

---

## 常见问题

### Q：INCLUDE 和 EXCLUDE 的书写顺序有影响吗？

**没有影响**。系统内部会自动将规则按类型（INCLUDE/EXCLUDE）分组处理。以下两种写法完全等价：

```sql
-- 写法 A
ON TABLES (INCLUDE 'ods.*', EXCLUDE '*.tmp_*', INCLUDE 'dw.*')

-- 写法 B
ON TABLES (INCLUDE 'dw.*', INCLUDE 'ods.*', EXCLUDE '*.tmp_*')
```

两者产生的 Job 完全相同。如果已经存在其中一个，再创建另一个会被判定为重复 Job。

### Q：不使用通配符的模式（如 `'sales.orders'`）是精确匹配吗？

**是的**。不含 `*` 或 `?` 的模式完全等价于精确匹配，只有表名完全相同才会匹配。

### Q：怎么表达"匹配 fact_ 或 dim_ 开头的表"？

写两条 INCLUDE 规则即可：

```sql
ON TABLES (
    INCLUDE 'dw.fact_*',
    INCLUDE 'dw.dim_*'
)
```

多条 INCLUDE 规则之间是并集关系。

### Q：创建 Job 后新建了符合模式的表，会自动预热吗？

**会**。系统每 60 秒自动重新评估模式规则。新建的表如果匹配模式，会在下一个刷新周期内自动纳入预热范围。在此期间（最多 60 秒）写入的数据不会触发预热，刷新后的新写入会正常预热。

### Q：表被 RENAME 后还会继续预热吗？

**取决于新表名是否仍匹配模式**。例如模式为 `INCLUDE 'db.order_*'`，将 `order_2024` 改名为 `archive_2024` 后不再匹配 `order_*`，预热会停止。如果改名为 `order_2024_v2`，仍匹配 `order_*`，预热继续。变化在下次定期刷新后生效。

### Q：可以对同一个目标集群创建多个表级别 Job 吗？

**可以**。多个 Job 独立运行。如果不同 Job 覆盖了相同的表，目标集群会自动去重。

### Q：ON TABLES 只写 EXCLUDE 不写 INCLUDE 可以吗？

**不可以**。至少需要一条 INCLUDE 规则。默认情况下没有表被纳入预热范围，EXCLUDE 无从排除。

### Q：模式匹配大小写敏感吗？

**是的**。`INCLUDE 'ODS.*'` 和 `INCLUDE 'ods.*'` 不同。请使用与实际库名/表名一致的大小写。

### Q：可以只预热某个库下某个分区的数据吗？

表级别预热不支持分区粒度。`ON TABLES` 的过滤粒度是**表级别**，被匹配表上的所有新写入（包括所有分区）都会被预热。

### Q：集群级别 Job 已经在跑了，还能创建表级别 Job 吗？

**可以**。两者独立运行。重叠部分的数据会在目标集群自动去重，不会产生额外开销。

### Q：`*` 能匹配 `.` 吗？

**能**。`*` 匹配任意字符，包括 `.`。但由于库名和表名中不会出现 `.`（`.` 仅作为分隔符），实际使用中不会产生歧义。例如 `'*.*'` 匹配所有库的所有表。

### Q：模式中可以只写库名不写表名吗？

**不可以**。模式格式必须为 `'库名.表名'`（中间有 `.` 分隔符）。如果要匹配整个库，请使用 `'库名.*'`。

### Q：新建表到被自动纳入预热，延迟有多大？

最长 **60 秒**（默认刷新周期）。系统每 60 秒自动重新评估模式规则并更新预热范围。在此延迟窗口内写入新表的数据不会触发预热，刷新后的写入会正常预热。

### Q：所有匹配的表都被删除了，Job 会怎样？

Job 保持 **RUNNING** 状态。预热范围为空，不会触发任何预热动作。如果之后创建了新的匹配表，下次刷新时会自动纳入预热范围，Job 自动恢复工作。

### Q：RENAME 后表不再匹配模式，已经在预热中的数据怎么办？

RENAME 后，下次刷新时（≤60s）该表会从预热范围移除。已经完成的预热数据仍保留在目标集群的缓存中（直到被缓存淘汰），只是后续写入不再触发新的预热。
