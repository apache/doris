# 表级别 Event-Driven 预热 设计文档

## 一、背景

### 现状

当前 Event-Driven 预热只支持**集群级别**的同步关系：

```sql
WARM UP COMPUTE GROUP read_cluster WITH COMPUTE GROUP write_cluster
    PROPERTIES("sync_mode" = "event_driven", "sync_event" = "load");
```

这意味着源集群上**所有表**的 Load/Compaction/Schema Change 操作都会触发对目标集群的预热。在以下场景中，这种粒度过粗：

1. **选择性预热**：用户只希望对几张核心业务表开启实时预热，其他表的数据变更不需要（也不应该）消耗网络和缓存空间。
2. **多目标分发**：同一张表可能需要同步到不同的目标集群（如 table_A → read_cluster_1, table_B → read_cluster_2），而集群级别 job 无法精确控制。
3. **资源控制**：集群级别预热可能产生大量不必要的 S3 下载流量和 File Cache 淘汰，影响系统整体性能。

### 目标

支持 **DB/表级别** 的 Event-Driven 预热，使用户可以通过模式匹配精确指定哪些表的数据变更需要自动同步到目标集群。不支持分区级别过滤——event-driven 是常驻 job，分区会动态增删，指定分区的实用性有限。

---

## 二、面向用户的使用方式

### 2.1 SQL 语法

在**集群级别语法基础上**，通过 `ON TABLES (...)` 子句增加表级别过滤。这保持了源集群的显式指定，在现有语法基础上增加可选子句。

```sql
-- 表级别 event-driven 预热
WARM UP COMPUTE GROUP <target_cluster> WITH COMPUTE GROUP <source_cluster>
ON TABLES (
    INCLUDE '<glob_pattern>'
    [, INCLUDE '<glob_pattern>' ...]
    [, EXCLUDE '<glob_pattern>']
    [, EXCLUDE '<glob_pattern>' ...]
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);

-- 集群级别 event-driven 预热（兼容现有语法，不使用 ON TABLES）
WARM UP COMPUTE GROUP <target_cluster> WITH COMPUTE GROUP <source_cluster>
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

所有模式均使用 glob 通配符语法（`*` 匹配任意字符，`?` 匹配单个字符），格式为 `'库名.表名'`。精确匹配只需不使用通配符即可（如 `'sales.orders'`）。

示例:

```sql
-- 示例 1：数据仓库经典分层架构 — 预热 ODS 层全部表，排除临时表
-- 场景：ODS 层有 ods_order、ods_user、ods_tmp_log 等数百张表，
--       只需排除 ods_tmp_ 开头的临时表
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP ingestion_cg
ON TABLES (
    INCLUDE 'ods.*',
    EXCLUDE 'ods.tmp_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 解析：
--   INCLUDE 集合 = ods 库下所有表（ods.order, ods.user, ods.tmp_log, ods.tmp_detail, ...）
--   EXCLUDE 集合 = ods 库下 tmp_ 开头的表（ods.tmp_log, ods.tmp_detail）
--   最终预热 = ods.order, ods.user, ...（不含 ods.tmp_*）

-- 示例 2：跨库精确指定 — 只预热核心业务表
-- 场景：多个业务库各有几十张表，但只需要预热其中几张关键表
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

-- 示例 3：按命名规范匹配事实表和维度表
-- 场景：DW 库中表命名规范为 fact_xxx、dim_xxx、tmp_xxx、stg_xxx，
--       只需要预热事实表和维度表
WARM UP COMPUTE GROUP bi_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE 'dw.fact_*',
    INCLUDE 'dw.dim_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);

-- 示例 4：多库预热，排除备份表和测试表
-- 场景：需要预热 ODS 和 DW 两层，但排除所有库中的 _bak 后缀表和 test_ 前缀表
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
-- 解析：
--   INCLUDE 集合 = ods.* ∪ dw.*（两个库的所有表）
--   EXCLUDE 集合 = 所有 _bak 后缀表 ∪ 所有 test_ 前缀表
--   最终预热 = (ods.* ∪ dw.*) − (*.*_bak ∪ *.test_*)

-- 示例 5：不使用 ON TABLES → 集群级别全量预热（兼容现有语法）
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

> **备注**：§2.3 保留了其他候选方案（A-E）的完整描述供对比参考，最终选择了方案 F（纯 Glob ON TABLES 子句），详见 §2.5。

### 2.2 业界表过滤模式调研

在选择模式匹配语法之前，调研了业界主流数据同步/CDC 系统对 "db.table" 过滤的实现方式：

| 系统 | 模式类型 | `.` 分隔符处理 | 格式 | 多模式分隔 | 典型示例 |
|------|---------|--------------|------|-----------|---------|
| **MySQL Replication** | SQL LIKE (`%`, `_`) | `.` 为字面量（无歧义） | `db.table` | 多个 `replicate-wild-do-table` 行 | `db%.order_%` |
| **Canal** | Perl 正则 | `\\.` 转义 | `db\.table` | 逗号 | `canal\\..*,mysql.test1` |
| **Debezium** | Java 正则 (anchored) | `.` 不转义（依赖 anchored） | `db.table` | 逗号 | `inventory.orders, inventory.customers` |
| **Flink CDC** | Java 正则 | **拆成两个独立属性** | `database-name` + `table-name` 各自为 regex | 各属性内 `\|` 分隔 | `database-name=mydb`, `table-name=orders.*` |

#### 核心发现

1. **正则是主流**：Canal、Debezium、Flink CDC 均使用正则表达式，只有 MySQL Replication 使用 LIKE 模式
2. **`.` 歧义是核心问题**：在 `db.table` 格式中，`.` 既是 db 和 table 的分隔符，又是正则中"匹配任意字符"的元字符
   - **Canal**：强制 `\\.` 转义（丑但准确）
   - **Debezium**：不转义，依赖 anchored 全量匹配（`.` 在正则中匹配任意字符，刚好也能匹配 `.` 本身，大多数场景能工作）
   - **Flink CDC**：拆成 `database-name` 和 `table-name` 两个独立属性，彻底避免歧义（最优雅）
3. **Glob 模式无 `.` 歧义**：`.` 在 glob 中是字面量，不需要转义，天然做 db/table 分隔符

---

### 2.3 候选方案对比

#### 方案 A：Glob 模式（`*` `?`）

**语法**：单个 `"tables"` 属性，值为逗号分隔的 glob 模式列表，每个模式格式为 `db_pattern.table_pattern`。

| 通配符 | 含义 | 示例 |
|--------|------|------|
| `*` | 匹配零个或多个任意字符 | `order_*` 匹配 `order_2024`、`order_detail` |
| `?` | 匹配恰好一个任意字符 | `log_?` 匹配 `log_a`、`log_1`，不匹配 `log_ab` |

**示例**：

```sql
-- 精确匹配两张表
"tables" = "db1.orders, db1.customers"

-- 整库匹配
"tables" = "analytics_db.*"

-- 前缀匹配
"tables" = "db1.order_*"

-- 跨库匹配
"tables" = "*.fact_*, *.dim_*"

-- 多字符 DB 通配
"tables" = "db?.table1"
```

**模式示例汇总**：

| 模式 | 含义 | 匹配示例 |
|------|------|----------|
| `db1.table1` | 精确匹配 | `db1.table1` |
| `db1.*` | 整库匹配 | `db1.table1`, `db1.table2`, ... |
| `db1.order_*` | 前缀匹配 | `db1.order_2024`, `db1.order_detail` |
| `db1.*_fact` | 后缀匹配 | `db1.sales_fact`, `db1.user_fact` |
| `*.*` | 全局匹配（等价于不设置 tables） | 全部 |
| `*.user_*` | 跨库匹配 | `db1.user_info`, `db2.user_log` |
| `db?.table1` | DB 名单字符通配 | `db1.table1`, `db2.table1` |

**优点**：
- `.` 是字面量，**无歧义**，天然做分隔符
- 语法对 SQL 用户最直观，学习成本最低
- 不存在转义问题——用户不需要了解正则语法
- 实现简单：glob→regex 转换仅需将 `*`→`.*`、`?`→`.`，其余字符 escape

**缺点**：
- 表达能力弱于正则：无法表达"或"（`a|b`）、字符类（`[0-9]`）、量词（`{3,5}`）等
- 需要一个 glob→regex 的编译层（但代码量极小，约 20 行）

**转义注意**：
- glob 中 `.` `(` `)` `[` `]` `{` `}` `\` `^` `$` `|` `+` 均为字面量，编译时需要对其进行正则转义
- 用户**无需关心**任何转义问题

---

#### 方案 B：正则模式 + 单属性（Debezium/Canal 风格）

**语法**：单个 `"tables"` 属性，值为逗号分隔的正则表达式列表，每个正则的格式为 `db_regex.table_regex`（以第一个 `.` 为分隔）。

> 参考 Debezium 的 `table.include.list`：逗号分隔的正则列表，每个正则匹配 `databaseName.tableName` 的全限定名。

**示例**：

```sql
-- 精确匹配两张表
"tables" = "db1.orders, db1.customers"

-- 整库匹配（注意 .* 是正则语法）
"tables" = "analytics_db..*"

-- 前缀匹配
"tables" = "db1.order_.*"

-- 跨库匹配（.* 匹配任意 DB）
"tables" = ".*.fact_.*, .*.dim_.*"

-- 更复杂的模式：名称包含数字后缀
"tables" = "db1.order_[0-9]{4}"
```

**优点**：
- 表达能力最强：支持字符类、量词、分组、交替等
- 与 Debezium 生态一致，Java 开发者熟悉
- 一个属性搞定，语法结构简单

**缺点 / 注意事项**：
- **`.` 歧义**是核心问题：在 `db1.order_.*` 中，第一个 `.` 是 db/table 分隔符，但在正则语义中它匹配任意字符
  - **解析策略**：以**第一个** `.` 作为 db 和 table 的分隔符。这意味着 db 名称的正则中不能包含未转义的 `.`
  - 实际上，由于 Doris 的 db 名不含 `.`，这在绝大多数场景都没有问题
  - 但如果用户希望在 db 部分使用 `.`（如匹配 `db.v2` 这样的 db 名），必须写成 `db\.v2.table_regex`，此处 `\.` 表示字面量点号，需要在 SQL 字符串中写为 `db\\.v2.table_regex`（因为 SQL 字符串中 `\` 需要双写）
- **SQL 字符串中的双重转义**：正则中的 `\` 在 SQL 字符串常量中需要写成 `\\`。例如：
  - 正则 `db\.table` 在 SQL 中写成 `"tables" = "db\\.table"`
  - 正则 `order_\d+` 在 SQL 中写成 `"tables" = "db1.order_\\d+"`
  - 这对不熟悉正则的用户是**显著的认知负担**
- **全库匹配容易写错**：用户可能写 `db1.*` 期望匹配 db1 下所有表，但在正则中 `*` 是量词（匹配前面的元素零次或多次），写法应为 `db1..*`（`.` 匹配任意字符 + `*` 匹配零次或多次）。不过我们可以检测到这类常见错误并给出提示。

**对比 Canal 的 `\\.` 强制转义风格**：
- Canal 要求用户写 `canal\\..*` 才能匹配 `canal` 库下所有表
- 这比 Debezium 更严格，但也更丑陋
- 我们如果采用正则方案，建议**不要求** `.` 转义（Debezium 风格），而是用第一个 `.` 做分隔

---

#### 方案 C：正则模式 + 分离属性（Flink CDC 风格）

**语法**：使用**两个独立属性** `"databases"` 和 `"tables"`，各自为独立的正则表达式。匹配时将它们内部拼接为 `^(databases_regex)\.(tables_regex)$` 进行全限定名匹配。

> 参考 Flink CDC 的 `database-name` 和 `table-name`：各自独立的正则属性，引擎内部拼接为 `database-name + \\. + table-name` 进行全路径匹配。

**示例**：

```sql
-- 精确匹配 db1 下的 orders 和 customers
PROPERTIES(
    "sync_mode" = "event_driven",
    "sync_event" = "load",
    "databases" = "db1",
    "tables" = "orders|customers"
);

-- 整库匹配
PROPERTIES(
    "sync_mode" = "event_driven",
    "sync_event" = "load",
    "databases" = "analytics_db",
    "tables" = ".*"
);

-- 前缀匹配
PROPERTIES(
    "sync_mode" = "event_driven",
    "sync_event" = "load",
    "databases" = "db1",
    "tables" = "order_.*"
);

-- 跨库匹配所有 fact 和 dim 表
PROPERTIES(
    "sync_mode" = "event_driven",
    "sync_event" = "load",
    "databases" = ".*",
    "tables" = "fact_.*|dim_.*"
);

-- 精确两个 DB + 各自不同表
PROPERTIES(
    "sync_mode" = "event_driven",
    "sync_event" = "load",
    "databases" = "db1|db2",
    "tables" = "order_.*|customer_.*"
);
```

**优点**：
- **彻底消除 `.` 歧义**：db 和 table 的正则各自独立，`.` 不可能被误解为分隔符
- db 和 table 的正则各自含义清晰，不需要考虑"第一个 `.` 做分隔"的规则
- 与 Flink CDC 生态一致

**缺点 / 注意事项**：
- **无法对不同 DB 指定不同表模式**：如果用户需要"db1 下的 order 表 + db2 下的 customer 表"，用单组属性无法表达。上面最后一个示例 `databases=db1|db2, tables=order_.*|customer_.*` 实际匹配的是 `(db1 OR db2) × (order_.* OR customer_.*)` 的笛卡尔积——db2 下的 order 表也会被匹配到。要实现精确控制，需要创建两个不同的 Job。
- **语法更复杂**：两个属性比一个属性的心智负担更高
- **SQL 字符串中的转义问题同样存在**：`\d` 需写成 `\\d`，`\.` 需写成 `\\.` 等
- `"tables"` 属性名与集群级别含义冲突（原来的 `"tables"` 在 ONCE 模式下表示具体表列表），需要改名或做区分

---

#### 方案 D：SQL LIKE 模式（MySQL Replication 风格）

**语法**：单个 `"tables"` 属性，值为逗号分隔的 SQL LIKE 模式列表，每个模式格式为 `db_like.table_like`。

| 通配符 | 含义 | 示例 |
|--------|------|------|
| `%` | 匹配零个或多个任意字符 | `order_%` 匹配 `order_2024`、`order_detail` |
| `_` | 匹配恰好一个任意字符 | `log__` 匹配 `log_a`、`log_1`，不匹配 `log_ab` |

> 参考 MySQL 的 `--replicate-wild-do-table`：每行一个 LIKE 模式，格式为 `db.table`。

**示例**：

```sql
-- 精确匹配
"tables" = "db1.orders, db1.customers"

-- 整库匹配
"tables" = "analytics_db.%"

-- 前缀匹配
"tables" = "db1.order_%"

-- 跨库匹配
"tables" = "%.fact_%, %.dim_%"
```

**优点**：
- SQL 用户最熟悉的通配语法（`%` `_`），零学习成本
- `.` 是字面量，无歧义
- 与 MySQL 生态一致

**缺点 / 注意事项**：
- **`_` 在 Doris 表名中极其常见**（如 `order_detail`），但 `_` 在 LIKE 中是"匹配单字符"的通配符。精确匹配含 `_` 的表名时需要转义：`order\_detail`，这在 SQL 字符串中要写成 `order\\_detail`
  - 例如：想精确匹配 `db1.order_detail`，必须写 `"tables" = "db1.order\\_detail"`
  - 这是一个**极高频的踩坑点**，严重影响用户体验
- 表达能力弱于正则和 glob

---

#### 方案 E：ON TABLES 子句 + 混合匹配模式（结构化 SQL 风格）

**语法**：在 WARM UP 语句中新增 `ON TABLES (...)` 子句，内部由逗号分隔的规则列表组成。每条规则由 `INCLUDE/EXCLUDE` + 匹配方式 + 模式 构成。

**匹配方式**：
- `TABLE db.table`：精确匹配（字面量）
- `GLOB 'pattern'`：glob 通配符匹配（`*` `?`）
- `REGEX 'pattern'`：正则表达式匹配

**语法约束**：
- INCLUDE 和 EXCLUDE 规则**可以任意顺序书写**，系统内部自动将其分组为 INCLUDE 集合和 EXCLUDE 集合，与书写顺序无关
- 不使用 `ON TABLES` 子句 → 集群级别全量预热（兼容现有语法）

**语义（集合运算）**：
1. **第一步：计算 INCLUDE 集合** — 遍历系统中所有 `db.table`，如果匹配任意一条 INCLUDE 规则，则纳入候选集合
2. **第二步：计算 EXCLUDE 集合** — 在候选集合中，如果匹配任意一条 EXCLUDE 规则，则从候选集合中移除
3. **最终结果** = INCLUDE 集合 − EXCLUDE 集合
4. 不匹配任何 INCLUDE 规则的表**不纳入**预热范围

> 该语义等价于：`result = {t | t ∈ AllTables ∧ matches_any_include(t) ∧ ¬matches_any_exclude(t)}`

**示例**：

```sql
-- 示例 1：数据仓库经典分层架构 — 预热 ODS 层全部表，排除临时表
-- 场景：ODS 层有 ods_order、ods_user、ods_tmp_log 等数百张表，
--       只需排除 ods_tmp_ 开头的临时表
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP ingestion_cg
ON TABLES (
    INCLUDE GLOB 'ods.*',
    EXCLUDE GLOB 'ods.tmp_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 解析：
--   INCLUDE 集合 = ods 库下所有表（ods.order, ods.user, ods.tmp_log, ods.tmp_detail, ...）
--   EXCLUDE 集合 = ods 库下 tmp_ 开头的表（ods.tmp_log, ods.tmp_detail）
--   最终预热 = ods.order, ods.user, ...（不含 ods.tmp_*）

-- 示例 2：跨库精确指定 — 只预热核心业务表
-- 场景：多个业务库各有几十张表，但只需要预热其中几张关键表
WARM UP COMPUTE GROUP report_cg WITH COMPUTE GROUP business_cg
ON TABLES (
    INCLUDE TABLE sales.orders,
    INCLUDE TABLE sales.customers,
    INCLUDE TABLE inventory.stock_level
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);

-- 示例 3：正则匹配事实表和维度表 — 预热 DW 层的 fact_ 和 dim_ 前缀表
-- 场景：DW 库中表命名规范为 fact_xxx、dim_xxx、tmp_xxx、stg_xxx，
--       只需要预热事实表和维度表
WARM UP COMPUTE GROUP bi_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE REGEX '^dw\.(fact_|dim_).*$'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);

-- 示例 4：glob + regex 混合 — 预热多层多库，排除特定模式
-- 场景：需要预热 ODS 和 DW 两层，但排除所有库中的 _bak 后缀表和 test_ 前缀表
WARM UP COMPUTE GROUP query_cg WITH COMPUTE GROUP load_cg
ON TABLES (
    INCLUDE GLOB 'ods.*',
    INCLUDE GLOB 'dw.*',
    EXCLUDE GLOB '*.*_bak',
    EXCLUDE REGEX '^(ods|dw)\.test_.*$'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 解析：
--   INCLUDE 集合 = ods.* ∪ dw.*（两个库的所有表）
--   EXCLUDE 集合 = 所有 _bak 后缀表 ∪ ods/dw 库下 test_ 前缀表
--   最终预热 = (ods.* ∪ dw.*) − (*.*_bak ∪ (ods|dw).test_*)

-- 示例 5：不使用 ON TABLES → 集群级别全量预热（兼容现有语法）
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

**规则评估逻辑（集合运算）**：

```
输入：系统中所有表 AllTables = {(db1, t1), (db1, t2), (db2, t1), ...}
输入：规则列表 rules = [INCLUDE ..., INCLUDE ..., EXCLUDE ..., EXCLUDE ...]

// 第一步：计算 INCLUDE 集合
include_set = {}
for table in AllTables:
    full_name = table.db + "." + table.name
    for rule in rules where rule.type == INCLUDE:
        if rule.matches(full_name):
            include_set.add(table)
            break  // 匹配任意一条 INCLUDE 即可

// 第二步：从 INCLUDE 集合中排除 EXCLUDE 匹配的表
result_set = include_set
for table in include_set:
    full_name = table.db + "." + table.name
    for rule in rules where rule.type == EXCLUDE:
        if rule.matches(full_name):
            result_set.remove(table)
            break  // 匹配任意一条 EXCLUDE 即排除

// result_set 即为最终需要预热的表集合
```

**优点**：
- **表达能力最强**：同时支持精确匹配、glob、regex 三种模式，用户按需选择
- **原生 INCLUDE/EXCLUDE**：不需要正则的 negative lookahead 等复杂语法就能实现排除
- **语义简单直观**：先算 INCLUDE 再减 EXCLUDE，集合运算逻辑清晰，没有规则顺序依赖
- **结构化 SQL 语法**：比在 PROPERTIES 字符串里塞模式更符合 SQL 的结构化理念
- **无 `.` 歧义**：
  - `TABLE db.table` 中 `.` 是精确字面量
  - `GLOB 'pattern'` 中 `.` 是 glob 字面量
  - `REGEX 'pattern'` 中 `.` 是正则语义，但包在引号里，用户清楚自己在写正则
- **可扩展性好**：未来可以增加更多匹配方式（如 `LIKE 'pattern'`）

**缺点 / 注意事项**：
- **SQL 语法解析复杂**：需要在 FE parser (ANTLR/CUP) 中新增 `ON TABLES` 子句、`INCLUDE/EXCLUDE` 关键字、`TABLE/GLOB/REGEX` 匹配方式的语法规则，改动较大
- **REGEX 模式在 SQL 中的转义**：regex 字符串用单引号包裹，如果模式中需要单引号本身，需要 `''` 转义。反斜杠 `\` 的处理取决于 Doris SQL parser 的字符串转义规则：
  - 如果 parser 处理 `\` 转义（如 `\n`→换行），则 regex `\d` 需写成 `'\\d'`
  - 如果 parser 按字面量处理字符串，则 `'\d'` 即可
  - 需要明确定义并文档化
- **模式→ table_id 解析**：需要先遍历系统所有表算 INCLUDE 集合，再遍历一次算 EXCLUDE 集合。本设计采用定期刷新（默认 60s），每次遍历是常规操作

**转义注意**：
- `TABLE` 精确匹配：无转义问题
- `GLOB`：同方案 A，用户无需转义
- `REGEX`：SQL 字符串中 `\` 的处理依赖 parser。如果 parser 不处理 `\` 转义，regex 写起来最自然（`'\d+'`）；如果处理，则需要 `'\\d+'`

---

#### 方案 F：纯 Glob ON TABLES 子句（方案 E 简化版）

**核心思想**：保留方案 E 的 `ON TABLES (INCLUDE/EXCLUDE ...)` 结构化 SQL 子句，但**仅支持 glob 通配符匹配**，去掉 `TABLE`（精确匹配）和 `REGEX`（正则匹配）两种模式。

**语法**：

```sql
WARM UP COMPUTE GROUP <target> WITH COMPUTE GROUP <source>
ON TABLES (
    {INCLUDE|EXCLUDE} '<glob_pattern>'
    [, {INCLUDE|EXCLUDE} '<glob_pattern>' ...]
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 注：INCLUDE 和 EXCLUDE 可以任意顺序书写，至少包含一条 INCLUDE
```

**关键简化**：
- 移除 `TABLE`/`GLOB`/`REGEX` 关键字——只有一种匹配方式，无需声明
- 所有模式均为 glob 语法，用单引号包裹
- 精确匹配通过不使用通配符的 glob 实现：`INCLUDE 'sales.orders'` 等价于原方案 E 的 `INCLUDE TABLE sales.orders`
- "或"关系通过多条 INCLUDE 规则表达：`INCLUDE 'dw.fact_*', INCLUDE 'dw.dim_*'` 替代原方案 E 的 `INCLUDE REGEX '^dw\.(fact_|dim_).*$'`

**语法约束**（同方案 E）：
- INCLUDE 和 EXCLUDE 规则可以任意顺序书写，系统内部自动分组、排序和去重，生成规范化的 canonical JSON
- 至少包含一条 INCLUDE 规则
- 不使用 `ON TABLES` 子句 → 集群级别全量预热

**语义（集合运算，同方案 E）**：
1. **第一步：计算 INCLUDE 集合** — 匹配任意一条 INCLUDE 规则的表纳入候选
2. **第二步：计算 EXCLUDE 集合** — 在候选集合中匹配任意一条 EXCLUDE 规则的表被移除
3. **最终结果** = INCLUDE 集合 − EXCLUDE 集合

**示例**：

```sql
-- 示例 1：预热 ODS 全库，排除临时表
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP ingestion_cg
ON TABLES (
    INCLUDE 'ods.*',
    EXCLUDE 'ods.tmp_*'
)
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");

-- 示例 2：精确匹配几张核心表
WARM UP COMPUTE GROUP report_cg WITH COMPUTE GROUP business_cg
ON TABLES (
    INCLUDE 'sales.orders',
    INCLUDE 'sales.customers',
    INCLUDE 'inventory.stock_level'
)
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");

-- 示例 3：事实表 + 维度表（用多条 INCLUDE 替代正则的 "或"）
WARM UP COMPUTE GROUP bi_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE 'dw.fact_*',
    INCLUDE 'dw.dim_*'
)
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");

-- 示例 4：多库预热，排除备份表和测试表
WARM UP COMPUTE GROUP query_cg WITH COMPUTE GROUP load_cg
ON TABLES (
    INCLUDE 'ods.*',
    INCLUDE 'dw.*',
    EXCLUDE '*.*_bak',
    EXCLUDE '*.test_*'
)
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");
```

**与方案 E 的对比**：

| 维度 | 方案 E（混合模式） | 方案 F（纯 Glob） |
|------|-------------------|-------------------|
| 匹配方式 | TABLE + GLOB + REGEX | 仅 GLOB |
| 语法关键字 | `TABLE`/`GLOB`/`REGEX` | 无（仅 `INCLUDE`/`EXCLUDE`） |
| 学习成本 | 低（但需了解三种模式） | ✅✅ 最低（只需学 `*` 和 `?`） |
| 转义问题 | REGEX 部分可能有 | ✅ 完全没有 |
| 表达能力 | 最强（正则无限制） | ✅ 中高（覆盖 95%+ 实际场景） |
| "或"表达 | 一条 REGEX 规则 | 多条 INCLUDE 规则 |
| Parser 复杂度 | 需解析 3 种匹配方式 | ✅ 最简单（单引号字符串） |
| `.` 歧义 | REGEX 部分有（但用户预期） | ✅ 完全没有 |
| 可扩展性 | 已包含全部模式 | 未来可增加 GLOB/REGEX 关键字 |

**方案 F 无法覆盖而方案 E 可以的场景**：

| 场景 | 方案 E 写法 | 方案 F 替代方案 |
|------|------------|----------------|
| 匹配编号库 db1-db99 | `INCLUDE REGEX '^db\d+\..*$'` | 多条 INCLUDE：`'db1.*', 'db2.*', ...`（若数量少可接受） |
| 复杂字符类 `[a-f]` | `INCLUDE REGEX '^db\.[a-f].*$'` | `?` 只能匹配单个任意字符，无法限定范围 |
| 否定字符类 `[^tmp]` | `INCLUDE REGEX '^db\.[^t].*$'` | 使用 INCLUDE + EXCLUDE 组合实现 |

> 实际场景中，数据仓库的表命名规范（前缀、后缀）和按库组织的结构，使得 `*` 和 `?` 通配符 + INCLUDE/EXCLUDE 组合足以覆盖绝大多数需求。

**优点**：
- **语法最简**：用户无需在 TABLE/GLOB/REGEX 三种模式间选择，减少心智负担
- **零转义问题**：glob 中 `.` 是字面量，`*` `?` 是通配符，无歧义
- **学习成本最低**：大多数用户已熟悉文件通配符 `*` `?`
- **实现最简**：Parser 只需解析 `INCLUDE/EXCLUDE` + 单引号字符串，无需多种匹配模式的分支
- **充分表达力**：INCLUDE/EXCLUDE 集合运算 + glob 通配符覆盖 95% 以上的实际需求

**缺点**：
- **无法单条规则表达"或"**：`fact_*` 或 `dim_*` 需要两条 INCLUDE，而非一条 REGEX
- **无字符类/数字范围**：无法用 `[0-9]`、`[a-z]` 等正则字符类
- 以上两个缺陷在实际使用中很少成为阻碍

---

### 2.4 方案对比总结

| 维度 | A. Glob | B. 正则+单属性 | C. 正则+分离属性 | D. SQL LIKE | E. ON TABLES 子句 | F. 纯 Glob ON TABLES |
|------|---------|--------------|----------------|------------|-----------------|---------------------|
| **`.` 歧义** | ✅ 无 | ⚠️ 有 | ✅ 无 | ✅ 无 | ✅ 无 | ✅ 无 |
| **转义问题** | ✅ 无 | ⚠️ `\` 双写 | ⚠️ `\` 双写 | ⚠️ `_` 需转义 | ⚠️ REGEX 部分可能需要 | ✅ 无 |
| **学习成本** | ✅ 低 | ⚠️ 中 | ⚠️ 中 | ✅ 低 | ✅ 低（集合运算） | ✅✅ 最低 |
| **表达能力** | ⚠️ 中 | ✅ 强 | ✅ 强（笛卡尔积） | ⚠️ 弱 | ✅✅ 最强（混合+排除） | ✅ 中高（INCLUDE/EXCLUDE） |
| **INCLUDE/EXCLUDE** | ❌ 不支持 | ❌ 不支持 | ❌ 不支持 | ❌ 不支持 | ✅ 原生支持 | ✅ 原生支持 |
| **不同 DB 不同表** | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| **业界对标** | — | Debezium | Flink CDC | MySQL Repl. | — | — |
| **SQL 结构化程度** | ⚠️ 字符串 | ⚠️ 字符串 | ⚠️ 字符串 | ⚠️ 字符串 | ✅ SQL 子句 | ✅ SQL 子句 |
| **实现复杂度** | ✅ 低 | ✅ 低 | ⚠️ 中 | ✅ 低 | ⚠️ 较高（parser 改动） | ✅ 低（parser 简单） |

### 2.5 最终选择

> **选择方案 F：纯 Glob ON TABLES 子句（方案 E 简化版）**
>
> 理由：
> 1. 继承方案 E 的 ON TABLES 结构化 SQL 子句 + INCLUDE/EXCLUDE 集合运算语义
> 2. 仅保留 glob 通配符匹配，去掉 TABLE/GLOB/REGEX 关键字选择，语法最简
> 3. **零转义问题**——glob 中 `.` 是字面量，`*` `?` 是通配符，没有任何歧义
> 4. **学习成本最低**——用户只需掌握 `*`（匹配任意字符）和 `?`（匹配单个字符）
> 5. **实现简单**——Parser 仅需解析 INCLUDE/EXCLUDE + 单引号字符串，无需多种匹配模式分支
> 6. **表达力充分**——INCLUDE/EXCLUDE 集合运算 + glob 覆盖 95% 以上实际场景
> 7. **可扩展性**——未来如需 REGEX 支持，可增加 `GLOB`/`REGEX` 关键字，向后兼容（无关键字默认为 GLOB）
> 8. **规则顺序无关**——INCLUDE/EXCLUDE 可自由混写，系统自动规范化为 canonical JSON，降低用户心智负担
>
> 方案 F 是"够用就好"与"极致简洁"之间的最优平衡。放弃了 REGEX 的极端灵活性，换来了语法统一、零转义、低实现复杂度。

#### 核心架构决策：动态模式匹配

> **FE 持久化模式规则（不持久化 table_id），定期重新评估模式并下发 table_id 到 BE。**
>
> 与"创建时一次性解析为 table_id 集合"的静态方案不同，本方案采用**动态匹配**：
> - **持久化内容**：ON TABLES 子句中的 INCLUDE/EXCLUDE 规则（模式字符串）
> - **不持久化** table_id 集合
> - FE **定期**（默认每 60 秒）重新评估模式，匹配当前存在的所有 DB/Table → 得到最新的 table_id 集合
> - 如果 table_id 集合发生变化，FE 向所有源集群 BE 推送更新后的 table_id 列表
>
> **好处**：
> 1. **新建表自动纳入**：创建 Job 后新建的匹配表，在下次刷新时自动被纳入预热范围
> 2. **删除表自动移除**：表被 DROP 后，下次刷新时自动从预热范围移除
> 3. **RENAME 跟随模式**：表重命名后，是否继续预热取决于新表名是否仍匹配模式（而非固定的 table_id）
> 4. **语义直观**：用户指定的是"模式"，系统行为始终与模式保持一致

#### 语法约束

1. `ON TABLES` 子句可选。不使用 → 集群级别全量预热（兼容现有语法）
2. `ON TABLES` 内部 `INCLUDE` 和 `EXCLUDE` 规则**可以任意顺序书写**，系统内部自动分组、排序和去重，生成规范化表示
3. 至少包含一条 `INCLUDE` 规则（纯 `EXCLUDE` 无意义，因为默认不纳入）
4. 大小写敏感（与 Doris 的 DB/表名一致）
5. 所有模式使用单引号包裹，统一为 glob 语法（`*` 匹配任意字符，`?` 匹配单个字符）
6. 精确匹配通过不使用通配符的 glob 实现：`INCLUDE 'db.table'`

> **规范化（Canonicalization）**：无论用户以何种顺序书写规则，系统在创建 Job 时会自动执行以下规范化操作：
> 1. 将所有规则按类型分组：提取全部 INCLUDE 规则和全部 EXCLUDE 规则
> 2. 在每组内按模式字符串字典序排序
> 3. 在每组内去除重复的模式字符串
> 4. 生成 canonical JSON 作为持久化标识和重复检测依据
>
> 示例：以下两条 SQL 产生**完全相同**的 canonical JSON，因此会被判定为重复 Job：
> ```sql
> -- SQL A
> ON TABLES (INCLUDE 'dw.*', EXCLUDE 'dw.tmp_*', INCLUDE 'ods.*')
> -- SQL B
> ON TABLES (INCLUDE 'ods.*', INCLUDE 'dw.*', EXCLUDE 'dw.tmp_*')
> -- 两者的 canonical JSON 均为：
> -- {"include":["dw.*","ods.*"],"exclude":["dw.tmp_*"]}
> ```

### 2.6 完整 SQL 示例

```sql
-- 示例 1：数据仓库经典分层架构 — 预热 ODS 层全部表，排除临时表
-- 场景：ODS 层有 ods_order、ods_user、ods_tmp_log 等数百张表，
--       只需排除 ods_tmp_ 开头的临时表
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP ingestion_cg
ON TABLES (
    INCLUDE 'ods.*',
    EXCLUDE 'ods.tmp_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 解析：
--   INCLUDE 集合 = ods 库下所有表（ods.order, ods.user, ods.tmp_log, ods.tmp_detail, ...）
--   EXCLUDE 集合 = ods 库下 tmp_ 开头的表（ods.tmp_log, ods.tmp_detail）
--   最终预热 = ods.order, ods.user, ...（不含 ods.tmp_*）

-- 示例 2：跨库精确指定 — 只预热核心业务表
-- 场景：多个业务库各有几十张表，但只需要预热其中几张关键表
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

-- 示例 3：按命名规范匹配事实表和维度表
-- 场景：DW 库中表命名规范为 fact_xxx、dim_xxx、tmp_xxx、stg_xxx，
--       只需要预热事实表和维度表
WARM UP COMPUTE GROUP bi_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE 'dw.fact_*',
    INCLUDE 'dw.dim_*'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);

-- 示例 4：多库预热，排除备份表和测试表
-- 场景：需要预热 ODS 和 DW 两层，但排除所有库中的 _bak 后缀表和 test_ 前缀表
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
-- 解析：
--   INCLUDE 集合 = ods.* ∪ dw.*（两个库的所有表）
--   EXCLUDE 集合 = 所有 _bak 后缀表 ∪ 所有 test_ 前缀表
--   最终预热 = (ods.* ∪ dw.*) − (*.*_bak ∪ *.test_*)

-- 示例 5：使用 ? 单字符通配 — 匹配年份分表
WARM UP COMPUTE GROUP analytics_cg WITH COMPUTE GROUP etl_cg
ON TABLES (
    INCLUDE 'logs.access_202?'
)
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
-- 匹配：logs.access_2020, logs.access_2021, ..., logs.access_2029
-- 不匹配：logs.access_20234（多于一个字符）

-- 示例 6：不使用 ON TABLES → 集群级别全量预热（兼容现有语法）
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
PROPERTIES (
    "sync_mode" = "event_driven",
    "sync_event" = "load"
);
```

### 2.7 SHOW 展示

```sql
SHOW WARM UP JOB;
-- 或按 Job ID 查看
SHOW WARM UP JOB WHERE id = 13418;
```

#### 当前已有列（13 列）

| # | 列名 | 示例值 | 说明 |
|---|------|--------|------|
| 1 | JobId | 13418 | Job 唯一标识 |
| 2 | SrcComputeGroup | write_cg | 源集群（被监听写入事件的集群） |
| 3 | DstComputeGroup | read_cg | 目标集群（缓存被预热的集群） |
| 4 | Status | RUNNING | Job 状态（RUNNING / FINISHED / CANCELLED / ...） |
| 5 | Type | CLUSTER | Job 类型（CLUSTER = 集群级别） |
| 6 | SyncMode | EVENT_DRIVEN (LOAD) | 同步模式（ONCE / PERIODIC / EVENT_DRIVEN） |
| 7 | CreateTime | 2024-01-01 10:00:00.000 | 创建时间 |
| 8 | StartTime | 2024-01-01 10:00:01.000 | 开始时间 |
| 9 | FinishBatch | 0 | 已完成批次数（event-driven 类型通常为 0） |
| 10 | AllBatch | 0 | 总批次数（event-driven 类型通常为 0） |
| 11 | FinishTime | N/A | 完成时间（event-driven 类型长期运行，此值为 N/A） |
| 12 | ErrMsg | | 错误信息 |
| 13 | Tables | db1.t1, db2.t2 | 当前已有的表列表字段（用于 TABLE 类型的 ONCE/PERIODIC job） |

#### 新增列（2 列）

| # | 列名 | 示例值 | 说明 |
|---|------|--------|------|
| 14 | **TableFilter** | `{"include":["ods.*"],"exclude":["ods.tmp_*"]}` | **新增**：ON TABLES 规则的 canonical JSON 表示 |
| 15 | **MatchedTables** | `ods.order, ods.user, ods.payment` | **新增**：当前匹配的表名列表 |

#### SHOW 输出示例

**示例 1：集群级别 event-driven job（无 ON TABLES）**

```
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------------------+-------------+----------+------------+--------+--------+-------------+---------------+
| JobId | SrcComputeGroup  | DstComputeGroup  | Status  | Type    | SyncMode            | CreateTime              | StartTime               | FinishBatch | AllBatch | FinishTime | ErrMsg | Tables | TableFilter | MatchedTables |
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------------------+-------------+----------+------------+--------+--------+-------------+---------------+
| 13418 | write_cg         | read_cg          | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | 2024-01-01 10:00:00.000 | 2024-01-01 10:00:01.000 |           0 |        0 | N/A        |        |        |             |               |
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------------------+-------------+----------+------------+--------+--------+-------------+---------------+
```

> 集群级别 Job：`TableFilter` 和 `MatchedTables` 两列为空。

**示例 2：表级别 event-driven job（带 ON TABLES）**

```
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------------------+-------------+----------+------------+--------+--------+-------------------------------------------------------+-----------------------------------------------------+
| JobId | SrcComputeGroup  | DstComputeGroup  | Status  | Type    | SyncMode            | CreateTime              | StartTime               | FinishBatch | AllBatch | FinishTime | ErrMsg | Tables | TableFilter                                           | MatchedTables                                       |
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------------------+-------------+----------+------------+--------+--------+-------------------------------------------------------+-----------------------------------------------------+
| 13419 | ingestion_cg     | analytics_cg     | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | 2024-01-01 10:05:00.000 | 2024-01-01 10:05:01.000 |           0 |        0 | N/A        |        |        | {"include":["ods.*"],"exclude":["ods.tmp_*"]}         | ods.orders, ods.users, ods.payments                 |
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------------------+-------------+----------+------------+--------+--------+-------------------------------------------------------+-----------------------------------------------------+
```

**示例 3：多个 Job 共存**

```
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------------------+-------------+----------+------------+--------+--------+-------------------------------------------------------+---------------------------------------------+
| JobId | SrcComputeGroup  | DstComputeGroup  | Status  | Type    | SyncMode            | CreateTime              | ...         | TableFilter                                           | MatchedTables                               |
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------+-------------------------------------------------------+---------------------------------------------+
| 13418 | write_cg         | read_cg          | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | 2024-01-01 10:00:00.000 | ...         |                                                       |                                             |
| 13419 | ingestion_cg     | analytics_cg     | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | 2024-01-01 10:05:00.000 | ...         | {"include":["ods.*"],"exclude":["ods.tmp_*"]}         | ods.orders, ods.users, ods.payments         |
| 13420 | ingestion_cg     | realtime_cg      | RUNNING | CLUSTER | EVENT_DRIVEN (LOAD) | 2024-01-01 10:10:00.000 | ...         | {"include":["dw.fact_*","dw.dim_*"]}                  | dw.fact_sales, dw.fact_orders, dw.dim_date  |
+-------+------------------+------------------+---------+---------+---------------------+-------------------------+-------------+-------------------------------------------------------+---------------------------------------------+
```

> **TableFilter** 展示说明：
> - 展示规范化后的 canonical JSON 格式
> - 无论创建时 INCLUDE/EXCLUDE 的书写顺序如何，SHOW 展示的都是规范化后的结果
> - 集群级别 Job（无 ON TABLES）：该列为空
> - 仅含 INCLUDE 无 EXCLUDE 时：JSON 中不含 `"exclude"` 键，如 `{"include":["dw.fact_*","dw.dim_*"]}`
>
> **MatchedTables** 展示说明：
> - 基于 FE 最近一次定期评估的 table_id 集合，反查当前表名
> - **动态更新**：创建 Job 后新建的匹配表，在下一次刷新后会出现在 MatchedTables 中
> - 已 DROP 的表不会出现（下次刷新后自动移除）
> - 如果某张表被 RENAME 且新名仍匹配模式，展示新名；若不再匹配，则从列表消失
> - 匹配表较多时（如超过 100 张），展示前 100 张表名并附 `... and N more` 后缀

#### 实现变更

```java
// ShowWarmUpCommand.java — 新增 2 列
private static final ImmutableList<String> WARM_UP_JOB_TITLE_NAMES = new ImmutableList.Builder<String>()
        .add("JobId")
        .add("SrcComputeGroup")
        .add("DstComputeGroup")
        .add("Status")
        .add("Type")
        .add("SyncMode")
        .add("CreateTime")
        .add("StartTime")
        .add("FinishBatch")
        .add("AllBatch")
        .add("FinishTime")
        .add("ErrMsg")
        .add("Tables")
        .add("TableFilter")       // 新增
        .add("MatchedTables")     // 新增
        .build();

// CloudWarmUpJob.getJobInfo() — 在末尾追加 2 个字段
public List<String> getJobInfo() {
    List<String> info = Lists.newArrayList();
    // ... 原有 13 个字段 ...
    
    // 新增字段 14: TableFilter (canonical JSON)
    info.add(tableFilterExpr);  // 空串 = 集群级别

    // 新增字段 15: MatchedTables (当前匹配的表名)
    if (currentTableIds == null || currentTableIds.isEmpty()) {
        info.add("");
    } else {
        List<String> tableNames = new ArrayList<>();
        for (Long tableId : currentTableIds) {
            Table table = Env.getCurrentInternalCatalog().getTableByTableId(tableId);
            if (table != null) {
                Database db = Env.getCurrentInternalCatalog().getDbOfTable(table);
                tableNames.add(db.getFullName() + "." + table.getName());
            }
        }
        Collections.sort(tableNames);
        if (tableNames.size() > 100) {
            int extra = tableNames.size() - 100;
            info.add(String.join(", ", tableNames.subList(0, 100)) + "... and " + extra + " more");
        } else {
            info.add(String.join(", ", tableNames));
        }
    }
    return info;
}
```

### 2.8 取消

```sql
CANCEL WARM UP JOB WHERE id = 13418;
```

行为与现有集群级别 event-driven job 相同。

---

## 三、详细设计

### 3.1 模式匹配引擎

#### 3.1.1 规则数据模型

FE 侧将 `ON TABLES (...)` 子句解析为结构化的规则列表：

```java
/**
 * 单条表过滤规则，对应 ON TABLES 子句中的一条 INCLUDE/EXCLUDE 声明。
 * 方案 F 仅支持 glob 匹配，所有模式统一编译为 Java 正则。
 */
public class TableFilterRule {
    public enum RuleType { INCLUDE, EXCLUDE }

    private final RuleType ruleType;       // INCLUDE or EXCLUDE
    private final String rawPattern;       // 原始 glob 模式字符串，如 "ods.*"
    private final Pattern compiledPattern; // 编译后的 Java 正则

    /**
     * 将 glob 模式编译为 Java 正则。
     * glob 中 '*' → '.*', '?' → '.', 其余字符转义。
     */
    public static TableFilterRule create(RuleType ruleType, String globPattern) {
        Pattern compiled = compileGlob(globPattern);
        return new TableFilterRule(ruleType, globPattern, compiled);
    }

    /** 将 glob 模式编译为锚定的 Java 正则 */
    private static Pattern compileGlob(String glob) {
        StringBuilder regex = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*': regex.append(".*"); break;
                case '?': regex.append("."); break;
                case '.': case '(': case ')': case '[': case ']':
                case '{': case '}': case '\\': case '^': case '$':
                case '|': case '+':
                    regex.append('\\').append(c); break;
                default: regex.append(c);
            }
        }
        regex.append("$");
        return Pattern.compile(regex.toString());
    }

    /** 测试 "db.table" 全限定名是否匹配此规则 */
    public boolean matches(String fullTableName) {
        return compiledPattern.matcher(fullTableName).matches();
    }
}
```

#### 3.1.2 ON TABLES 过滤器

```java
/**
 * ON TABLES 子句的完整过滤器。
 * 语义：INCLUDE 集合 − EXCLUDE 集合。
 */
public class OnTablesFilter {
    private final List<TableFilterRule> includeRules;  // 所有 INCLUDE 规则
    private final List<TableFilterRule> excludeRules;  // 所有 EXCLUDE 规则
    private final String rawExpression;                // 原始 SQL 片段，用于 SHOW 展示

    /**
     * 判断一张表是否应该预热。
     * 1. 如果匹配任意一条 INCLUDE 规则 → 纳入候选
     * 2. 如果候选中匹配任意一条 EXCLUDE 规则 → 排除
     */
    public boolean shouldWarmUp(String dbName, String tableName) {
        String fullName = dbName + "." + tableName;

        // 第一步：检查是否匹配任意 INCLUDE
        boolean included = includeRules.stream()
                .anyMatch(rule -> rule.matches(fullName));
        if (!included) {
            return false;  // 不匹配任何 INCLUDE → 不纳入
        }

        // 第二步：检查是否匹配任意 EXCLUDE
        boolean excluded = excludeRules.stream()
                .anyMatch(rule -> rule.matches(fullName));
        return !excluded;  // 被 EXCLUDE 匹配 → 排除
    }
}
```

#### 3.1.3 模式解析流程

```
用户输入（规则可以任意顺序书写）:
    ON TABLES (
        INCLUDE 'ods.*',
        EXCLUDE 'ods.tmp_*',
        INCLUDE 'dw.fact_orders'
    )
            │
            ▼
    FE Parser 解析 ON TABLES 子句
            │
            ▼
    校验：至少包含一条 INCLUDE 规则
            │
            ▼
    规范化（Canonicalization）：
    1. 按类型分组 → INCLUDE: ["ods.*", "dw.fact_orders"], EXCLUDE: ["ods.tmp_*"]
    2. 每组内按字典序排序 → INCLUDE: ["dw.fact_orders", "ods.*"], EXCLUDE: ["ods.tmp_*"]
    3. 每组内去除重复
    4. 生成 canonical JSON → {"include":["dw.fact_orders","ods.*"],"exclude":["ods.tmp_*"]}
            │
            ▼
    编译每条规则为 TableFilterRule 对象（glob → Java 正则）
    includeRules = [
        "dw.fact_orders" → Pattern("^dw\\.fact_orders$"),
        "ods.*"          → Pattern("^ods\\..*$")
    ]
    excludeRules = [
        "ods.tmp_*"      → Pattern("^ods\\.tmp_.*$")
    ]
            │
            ▼
    构造 OnTablesFilter 对象
            │
            ▼
    调用 resolveTableIds(filter) → 解析为 table_id 集合
```

#### 3.1.4 模式 → table_id 集合的动态解析（FE 侧）

FE **定期**遍历所有 Database/Table 元数据，将匹配的表解析为 `table_id` 集合。每次刷新后，如果 table_id 集合发生变化，将更新后的集合下发到所有源集群 BE。

```java
public Set<Long> resolveTableIds(OnTablesFilter filter) {
    Set<Long> result = new HashSet<>();
    for (Database db : Env.getCurrentInternalCatalog().getAllDbs()) {
        for (Table table : db.getTables()) {
            if (filter.shouldWarmUp(db.getFullName(), table.getName())) {
                result.add(table.getId());
            }
        }
    }
    return result;
}
```

**存储策略：持久化模式规则（canonical JSON），table_id 集合由运行时动态计算**

| 存储内容 | 持久化 | 用途 |
|----------|--------|------|
| `tableFilterRules`（INCLUDE/EXCLUDE 规则列表） | ✅ EditLog | **核心持久化数据**。FE 恢复时用于重建 `OnTablesFilter`，定期重新评估匹配 |
| `tableFilterExpr`（canonical JSON 字符串） | ✅ EditLog | **规范化标识**。用于 JobKey 重复检测 + SHOW 展示 |
| `currentTableIds`（当前匹配的 table_id 集合） | ❌ 仅运行时 | FE 内存中维护，每次刷新后更新，下发到 BE |

> **设计要点**：
> - **不持久化 table_id 集合**——FE 启动或主从切换后从 EditLog 恢复模式规则，立即重新评估得到最新 table_id 集合
> - **定期刷新**——默认每 60 秒评估一次，如果 table_id 集合变化则推送到 BE
> - **新建表自动纳入**——创建 Job 后新建的匹配表，在下次刷新周期内自动被纳入预热范围
> - **删除表自动移除**——表被 DROP 后，下次刷新时 table_id 自动从集合中消失
> - **RENAME 跟随模式**——表被重命名后，按新名称重新匹配模式决定是否继续预热

### 3.2 FE 侧变更

#### 3.2.1 CloudWarmUpJob 扩展

```java
public class CloudWarmUpJob {
    // 持久化字段（GSON 序列化到 EditLog）
    @SerializedName(value = "tableFilterExpr")
    protected String tableFilterExpr = "";              // canonical JSON，用于 JobKey 重复检测 + SHOW 展示

    @SerializedName(value = "tableFilterRules")
    protected List<PersistedTableFilterRule> tableFilterRules = new ArrayList<>();  // 模式规则，用于定期重新评估

    // 运行时字段（不持久化）
    private transient OnTablesFilter onTablesFilter;    // 编译后的过滤器，从 tableFilterRules 构建
    private transient Set<Long> currentTableIds = new HashSet<>();  // 最近一次解析得到的 table_id 集合

    public boolean hasTableFilter() {
        return !tableFilterRules.isEmpty();
    }

    // 用于 GSON 序列化的内部类
    public static class PersistedTableFilterRule {
        @SerializedName("ruleType")
        public String ruleType;  // "INCLUDE" or "EXCLUDE"
        @SerializedName("pattern")
        public String pattern;   // glob 模式，如 "ods.*"
    }

    /**
     * 从规则列表生成 canonical JSON 字符串。
     * 规范化步骤：
     * 1. 按 ruleType 分组为 INCLUDE 和 EXCLUDE 两组
     * 2. 每组内按 pattern 字典序排序
     * 3. 每组内去除重复 pattern
     * 4. 序列化为紧凑 JSON：{"include":["a","b"],"exclude":["x"]}
     *
     * 该 canonical JSON 用于：
     * - JobKey 重复检测（不同书写顺序但语义相同的规则 → 相同 canonical JSON → 判定为重复 Job）
     * - SHOW WARM UP JOB 的 TableFilter 列展示
     */
    public static String canonicalize(List<PersistedTableFilterRule> rules) {
        List<String> includes = rules.stream()
            .filter(r -> "INCLUDE".equals(r.ruleType))
            .map(r -> r.pattern)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
        List<String> excludes = rules.stream()
            .filter(r -> "EXCLUDE".equals(r.ruleType))
            .map(r -> r.pattern)
            .distinct()
            .sorted()
            .collect(Collectors.toList());

        // 生成紧凑 JSON（使用 Gson 确保转义正确）
        JsonObject json = new JsonObject();
        JsonArray incArr = new JsonArray();
        includes.forEach(incArr::add);
        json.add("include", incArr);
        if (!excludes.isEmpty()) {
            JsonArray excArr = new JsonArray();
            excludes.forEach(excArr::add);
            json.add("exclude", excArr);
        }
        return json.toString();  // 紧凑格式，无多余空格
    }

    // FE 启动/主从切换后从 tableFilterRules 重建 OnTablesFilter
    public void rebuildOnTablesFilter() {
        if (tableFilterRules.isEmpty()) return;
        List<TableFilterRule> rules = tableFilterRules.stream()
            .map(r -> new TableFilterRule(
                "INCLUDE".equals(r.ruleType) ? RuleType.INCLUDE : RuleType.EXCLUDE,
                r.pattern))
            .collect(Collectors.toList());
        this.onTablesFilter = new OnTablesFilter(rules);
    }
}
```

**关键设计决策**：

| 决策点 | 决定 | 原因 |
|--------|------|------|
| 持久化内容 | 模式规则列表 + canonical JSON | 规则是重新评估的依据，canonical JSON 用于 JobKey 去重，table_id 不持久化 |
| table_id 集合 | 运行时动态计算，不持久化 | 支持新建表自动纳入、删除表自动移除 |
| 规则顺序 | 用户可任意顺序书写，内部自动规范化 | 简化用户体验，canonical JSON 保证语义相同的规则集产生相同标识 |
| 模式匹配时机 | 创建时匹配一次 + **定期刷新**（默认 60s） | 动态跟踪 catalog 变化 |
| RENAME 行为 | 取决于新表名是否匹配模式 | 语义直观：模式描述"意图"，而非固定的 table_id |
| 新建表 | **自动纳入**（下次刷新时） | 减少运维负担 |

#### 3.2.2 WarmUpClusterCommand 与 SQL 解析扩展

FE Parser 需要支持 `ON TABLES (...)` 子句。解析后 `WarmUpClusterCommand` 新增字段：

```java
public class WarmUpClusterCommand extends Command {
    // 新增
    private final List<TableFilterRule> onTablesRules;  // ON TABLES 子句的规则列表（可为 null，表示集群级别）

    // 解析时校验：
    // 1. 至少有一条 INCLUDE 规则
    // 2. glob 模式格式合法（包含 '.'，格式为 'db_pattern.table_pattern'）
    // 注意：INCLUDE 和 EXCLUDE 可以任意顺序书写，无需校验顺序
}
```

#### 3.2.3 CacheHotspotManager.createJob() 扩展

当前逻辑在 `createJob()` 中根据 `sync_mode` 属性创建 event-driven job。需要增加 ON TABLES 规则的解析：

```java
public long createJob(WarmUpClusterCommand stmt) {
    Map<String, String> properties = stmt.getProperties();
    String syncModeStr = properties.get("sync_mode");

    if ("event_driven".equals(syncModeStr)) {
        List<TableFilterRule> onTablesRules = stmt.getOnTablesRules();

        // 解析 ON TABLES 规则 → 初次 table_id 集合（用于校验 + 首次下发 BE）
        Set<Long> initialTableIds = Collections.emptySet();
        String tableFilterExpr = "";
        List<CloudWarmUpJob.PersistedTableFilterRule> persistedRules = new ArrayList<>();
        if (onTablesRules != null && !onTablesRules.isEmpty()) {
            OnTablesFilter filter = new OnTablesFilter(onTablesRules);
            initialTableIds = resolveTableIds(filter);
            if (initialTableIds.isEmpty()) {
                throw new DdlException("ON TABLES rules do not match any existing table");
            }
            // 将规则转为可持久化格式
            for (TableFilterRule rule : onTablesRules) {
                CloudWarmUpJob.PersistedTableFilterRule pr = new CloudWarmUpJob.PersistedTableFilterRule();
                pr.ruleType = rule.getRuleType().name();
                pr.pattern = rule.getPattern();
                persistedRules.add(pr);
            }
            // 生成 canonical JSON（规范化：分组 + 排序 + 去重）
            tableFilterExpr = CloudWarmUpJob.canonicalize(persistedRules);
        }

        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
            .setJobId(jobId)
            .setSrcClusterName(srcCluster)
            .setDstClusterName(dstCluster)
            .setJobType(JobType.CLUSTER)
            .setSyncMode(SyncMode.EVENT_DRIVEN)
            .setSyncEvent(syncEvent)
            .setTableFilterExpr(tableFilterExpr)        // canonical JSON，持久化 + JobKey
            .setTableFilterRules(persistedRules)        // 持久化，用于定期重新评估
            .build();

        // 设置运行时状态
        job.rebuildOnTablesFilter();
        job.setCurrentTableIds(initialTableIds);
        // ... runEventDrivenJob 首次下发时携带 initialTableIds
    }
}
```

**注意**：Job 类型保持 `CLUSTER`（因为基于集群语法创建），但内部持有 `tableFilterRules` 标识是否为表级别过滤。

#### 3.2.4 定期刷新机制（核心新增）

`CacheHotspotManager` 新增定期刷新调度，负责动态追踪 catalog 变化并推送 table_id 更新到 BE。

```java
// CacheHotspotManager 中新增
private static final long TABLE_FILTER_REFRESH_INTERVAL_MS = 60_000;  // 60 秒

// 在现有调度器中增加定期任务（或复用已有 ScheduledExecutorService）
private void startTableFilterRefresher() {
    scheduler.scheduleAtFixedRate(
        this::refreshAllTableFilters,
        TABLE_FILTER_REFRESH_INTERVAL_MS,
        TABLE_FILTER_REFRESH_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
}

/**
 * 定期刷新所有带 ON TABLES 过滤的 event-driven job 的 table_id 集合。
 * 如果某个 job 的匹配表集合发生变化，向源集群 BE 推送更新。
 */
private void refreshAllTableFilters() {
    for (CloudWarmUpJob job : getRunningEventDrivenJobs()) {
        if (!job.hasTableFilter()) continue;
        try {
            OnTablesFilter filter = job.getOnTablesFilter();
            Set<Long> newTableIds = resolveTableIds(filter);
            Set<Long> oldTableIds = job.getCurrentTableIds();

            if (!newTableIds.equals(oldTableIds)) {
                LOG.info("Table filter changed for job {}: {} tables → {} tables, "
                         + "added={}, removed={}",
                         job.getJobId(), oldTableIds.size(), newTableIds.size(),
                         Sets.difference(newTableIds, oldTableIds),
                         Sets.difference(oldTableIds, newTableIds));
                job.setCurrentTableIds(newTableIds);
                // 向所有源集群 BE 推送更新的 table_id 集合
                pushTableFilterToSourceBEs(job, newTableIds);
            }
        } catch (Exception e) {
            LOG.warn("Failed to refresh table filter for job {}", job.getJobId(), e);
        }
    }
}

/**
 * 向源集群所有 BE 重新发送 SET_JOB + 最新 table_ids。
 * 复用现有 warm_up_tablets RPC，BE 侧会更新已注册 job 的过滤器。
 */
private void pushTableFilterToSourceBEs(CloudWarmUpJob job, Set<Long> tableIds) {
    List<TNetworkAddress> srcBes = fetchBeToThriftAddress(job.getSrcClusterName());
    for (TNetworkAddress be : srcBes) {
        TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
        request.setJob_id(job.getJobId());
        request.setBatch_id(0);
        request.setType(TWarmUpTabletsRequestType.SET_JOB);
        request.setEvent(TWarmUpEventType.LOAD);
        request.setTable_ids(new ArrayList<>(tableIds));  // 更新后的 table_id 列表
        try {
            BackendServiceProxy.getInstance().warmUpTablets(be, request);
        } catch (Exception e) {
            LOG.warn("Failed to push table filter to BE {} for job {}",
                     be, job.getJobId(), e);
            // 下次刷新周期会重试
        }
    }
}
```

**刷新流程时序图**：

```
                     FE (CacheHotspotManager)                    BE (CloudWarmUpManager)
                            │                                           │
         ┌─────── 每 60s ──┤                                           │
         │                  │                                           │
         │  resolveTableIds │                                           │
         │  (遍历 catalog)  │                                           │
         │       │          │                                           │
         │       ▼          │                                           │
         │  新建表 order_2025                                           │
         │  → table_id 集合变化                                         │
         │       │          │                                           │
         │       ▼          │                                           │
         │  pushTableFilter │── SET_JOB(job_id, LOAD, table_ids=[...])─→│
         │  ToSourceBEs     │                                           │ set_event():
         │                  │                                           │ 更新 _event_driven_filters
         │                  │                                           │
         └──────────────────┤                                           │
                            │                                  新写入 order_2025:
                            │                                  commit_rowset → warm_up_rowset
                            │                                  → filter.should_warmup(table_id) ✓
                            │                                  → 触发预热
```

> **注意事项**：
> - 刷新周期内（最长 60 秒）新建的表不会被预热，这是可接受的延迟
> - `pushTableFilterToSourceBEs` 失败不会影响 Job 状态，下次刷新时重试
> - 如果某次刷新后 table_id 集合为空（所有匹配表都被 DROP 了），仍然推送空集合到 BE，BE 侧 `should_warmup` 全部返回 false — Job 保持 RUNNING 但不产生任何预热动作
> - FE 主从切换后，新 Master 立即执行一次 `refreshAllTableFilters`，确保 BE 状态与最新 catalog 一致

#### 3.2.5 重复 Job 检测

当前 `JobKey` = `(src, dst, syncMode)`。

**调整后**：`JobKey` = `(src, dst, syncMode, tableFilterExpr)`，其中 `tableFilterExpr` 为 **canonical JSON**。

```java
// repeatJobDetectionSet 的 key 扩展
public static class JobKey {
    String srcCluster;
    String dstCluster;
    SyncMode syncMode;
    String tableFilterExpr;   // canonical JSON（空串 = 集群级别，非空 = 规范化后的 ON TABLES 表达式）

    // equals/hashCode 使用所有字段
}
```

**规范化保证**：由于 `tableFilterExpr` 是经过规范化（分组 + 字典序排序 + 去重）的 canonical JSON，不同书写顺序但语义相同的 ON TABLES 子句会产生**相同的 canonical JSON**，从而正确判定为重复 Job。

```
-- 用户创建 Job A：
ON TABLES (INCLUDE 'dw.*', EXCLUDE 'dw.tmp_*', INCLUDE 'ods.*')
-- canonical JSON → {"include":["dw.*","ods.*"],"exclude":["dw.tmp_*"]}

-- 用户创建 Job B（与 A 语义相同，顺序不同）：
ON TABLES (INCLUDE 'ods.*', INCLUDE 'dw.*', EXCLUDE 'dw.tmp_*')
-- canonical JSON → {"include":["dw.*","ods.*"],"exclude":["dw.tmp_*"]}

-- Job A 和 Job B 的 JobKey 相同 → Job B 被拒绝为重复 Job ✓
```

**行为**：
- 同一 `(src, dst)` 对允许创建一个集群级别 + 多个不同 ON TABLES 规则的表级别 event-driven job
- canonical JSON 相同的 ON TABLES 表达式会被判定为重复 job 并拒绝（无论书写顺序）
- ON TABLES 规则有交集但 canonical JSON 不同的情况：**允许创建**，依赖目标 BE 的去重机制

#### 3.2.6 getTabletReplicaInfos 无需修改

> 关键洞察：**FE 侧的 `getTabletReplicaInfos` 不需要任何修改**。
>
> 原因：过滤在 BE 侧完成。BE 只为匹配表的 rowset 调用 FE RPC，不匹配的 rowset 在 BE 侧就被跳过了。FE 对于收到的请求，行为与现有集群级别完全一致——根据 job 的 `dstClusterName` 返回目标集群的副本信息。

### 3.3 BE 侧变更

#### 3.3.1 EventDrivenJobFilter 数据结构

```cpp
struct EventDrivenJobFilter {
    // nullopt = 集群级别（不过滤，预热所有表）
    // has_value 且非空 = 表级别过滤
    std::optional<std::unordered_set<int64_t>> table_ids;

    bool should_warmup(int64_t table_id) const {
        if (!table_ids.has_value()) return true;  // 集群级别，不过滤
        return table_ids->contains(table_id);
    }
};
```

> **设计说明**：使用 `std::optional` 而非空集合来表示"不过滤"语义，避免空集合的歧义（空集合可以被误解为"没有任何表需要预热"）。语义清晰：
> - `nullopt`：集群级别 Job，不做表过滤
> - `optional<set>{...}`：表级别 Job，仅预热 set 中的表

#### 3.3.2 set_event 扩展

```cpp
Status CloudWarmUpManager::set_event(int64_t job_id,
                                     TWarmUpEventType::type event,
                                     const std::vector<int64_t>& table_ids,  // 新增
                                     bool has_table_filter,                   // 新增：是否设置了表过滤
                                     bool clear) {
    std::lock_guard<std::mutex> lock(_mtx);
    if (event == TWarmUpEventType::type::LOAD) {
        if (clear) {
            _tablet_replica_cache.erase(job_id);
            _event_driven_filters.erase(job_id);
        } else {
            // 注册 job（仅首次）
            if (!_tablet_replica_cache.contains(job_id)) {
                static_cast<void>(_tablet_replica_cache[job_id]);
            }
            // 始终更新过滤器（支持 FE 定期刷新推送）
            EventDrivenJobFilter filter;
            if (has_table_filter) {
                // 表级别 Job：设置 table_ids 集合
                filter.table_ids.emplace(table_ids.begin(), table_ids.end());
            }
            // else: 集群级别 Job，table_ids 保持 nullopt
            _event_driven_filters[job_id] = std::move(filter);
        }
    }
    return Status::OK();
}
```

> **与旧版的关键区别**：旧版对已注册的 job 调用 `set_event` 是 no-op（`else if (!contains)`）。新版对已注册的 job 仍会**更新过滤器**，这是支持 FE 定期刷新推送 table_id 集合的关键。
>
> **线程安全**：`_mtx` 保护整个操作，`get_replica_info()` 中的读操作也持有同一把锁，确保一致性。
>
> **`has_table_filter` 参数语义**：
> - `has_table_filter = false`：集群级别 Job（兼容旧版本语法，无 ON TABLES 子句）
> - `has_table_filter = true, table_ids 为空`：表级别 Job 但当前没有匹配的表（所有匹配表都被 DROP 了）
> - `has_table_filter = true, table_ids 非空`：表级别 Job，正常过滤
>
> 也可以只用 Thrift 的 `optional` 语义判断：如果 `table_ids` 字段存在（即使为空列表）则视为表级别 Job；如果 `table_ids` 字段缺失则为集群级别 Job。

新增成员变量：

```cpp
// job_id → 过滤器（每个 event-driven job 一个）
std::map<int64_t, EventDrivenJobFilter> _event_driven_filters;
```

> `EventDrivenJobFilter` 的 `table_ids` 为 `std::optional`，因此 `_event_driven_filters` 中一定有该 job_id 的 entry——不再依赖"找不到 entry"来表示"不过滤"。

#### 3.3.3 get_replica_info 增加过滤

修改 `get_replica_info()` 增加 `table_id` 参数，在遍历 jobs 时进行过滤：

```cpp
std::vector<TReplicaInfo> CloudWarmUpManager::get_replica_info(
        int64_t tablet_id, int64_t table_id, bool bypass_cache, bool& cache_hit) {
    std::vector<TReplicaInfo> replicas;
    std::vector<int64_t> cancelled_jobs;
    std::lock_guard<std::mutex> lock(_mtx);
    cache_hit = false;
    for (auto& [job_id, cache] : _tablet_replica_cache) {
        // ★ 新增：表级别过滤
        auto filter_it = _event_driven_filters.find(job_id);
        if (filter_it != _event_driven_filters.end()
                && !filter_it->second.should_warmup(table_id)) {
            // should_warmup 内部：nullopt → true（集群级别），set.contains() → 表级别判断
            continue;  // 该 job 不关注此表，跳过
        }

        // ... 后续逻辑不变（缓存查找 → FE RPC → 缓存写入）
    }
    // ...
}
```

#### 3.3.4 _warm_up_rowset 入口获取 table_id

```cpp
void CloudWarmUpManager::_warm_up_rowset(RowsetMeta& rs_meta, int64_t sync_wait_timeout_ms) {
    int64_t table_id = rs_meta.tablet_schema()->table_id();  // O(1) 内存访问
    bool cache_hit = false;
    auto replicas = get_replica_info(rs_meta.tablet_id(), table_id, false, cache_hit);
    if (replicas.empty()) {
        VLOG_DEBUG << "No need to warmup tablet=" << rs_meta.tablet_id()
                   << ", table_id=" << table_id
                   << ", skipping rowset=" << rs_meta.rowset_id().to_string();
        g_file_cache_event_driven_warm_up_skipped_rowset_num << 1;
        return;
    }
    // ... 后续逻辑不变
}
```

#### 3.3.5 Thrift RPC 扩展

```thrift
// BackendService.thrift - TWarmUpTabletsRequest 扩展
struct TWarmUpTabletsRequest {
    1: required i64 job_id
    2: required i64 batch_id
    3: optional list<TJobMeta> job_metas
    4: required TWarmUpTabletsRequestType type
    5: optional TWarmUpEventType event
    // 新增
    6: optional list<i64> table_ids           // 关注的表 ID 列表
}
```

> **`optional` 语义与 `std::optional` 的映射**：Thrift 的 `optional` 字段在 C++ 生成代码中有 `__isset.table_ids` 标志：
> - 字段未设置（`__isset.table_ids == false`）→ 集群级别 Job → `EventDrivenJobFilter.table_ids = nullopt`
> - 字段已设置（`__isset.table_ids == true`）→ 表级别 Job → `EventDrivenJobFilter.table_ids = {ids...}`
>
> 这样旧版本 FE 不发送 `table_ids` 字段，新版本 BE 自动识别为集群级别，**完美向后兼容**。

### 3.4 过滤流程图

**数据写入时的过滤流程**（BE 侧，每次 commit_rowset 触发）：

```
  commit_rowset(rs_meta)
         │
         ▼
  warm_up_rowset(rs_meta)
         │
         ▼
  _warm_up_rowset(rs_meta)
         │
         ├─ table_id = rs_meta.tablet_schema()->table_id()
         │
         ▼
  get_replica_info(tablet_id, table_id, ...)
         │
         ├─ for each (job_id, filter) in _event_driven_filters:
         │     │
         │     ├─ filter.table_ids == nullopt？ → 集群级别，不过滤 ✓
         │     │
         │     ├─ filter.table_ids->contains(table_id)？
         │     │   ├─ 是 → 继续查询 FE 获取 replica ✓
         │     │   └─ 否 → 跳过该 job ✗
         │     │
         │     ▼
         │   [FE RPC: getTabletReplicaInfos] ← 仅对匹配的 job
         │     │
         │     ▼
         │   缓存 replica → replicas 列表
         │
         ▼
  _do_warm_up_rowset(rs_meta, replicas, ...)
         │
         ▼
  [brpc: PWarmUpRowsetRequest → Target BE]
```

**定期刷新流程**（FE 侧，默认每 60 秒）：

```
  CacheHotspotManager.refreshAllTableFilters()
         │
         ├─ for each running event-driven job with ON TABLES:
         │     │
         │     ├─ resolveTableIds(onTablesFilter)
         │     │   └─ 遍历所有 DB/Table 元数据
         │     │   └─ 对每个 db.table 调用 filter.shouldWarmUp()
         │     │   └─ 收集匹配的 table_id
         │     │
         │     ├─ newTableIds != oldTableIds ?
         │     │   ├─ 否 → 无变化，跳过
         │     │   └─ 是 → 有变化（新建/删除/重命名表导致）
         │     │         │
         │     │         ├─ job.setCurrentTableIds(newTableIds)
         │     │         │
         │     │         └─ pushTableFilterToSourceBEs(job, newTableIds)
         │     │               │
         │     │               └─ for each source BE:
         │     │                     SET_JOB(job_id, LOAD, table_ids=[...])
         │     │                       │
         │     │                       ▼ BE 侧
         │     │                     set_event() → 更新 _event_driven_filters
         │     │
         │     ▼
         │   (next job)
         │
         ▼
  done (下次 60s 后再执行)
```

---

## 四、多维关系分析

### 4.1 同一表 → 多个目标集群

```
                    ┌──────────────┐
                    │ read_cluster1│
  table_A ──────→   ├──────────────┤
    (写集群上)       │ read_cluster2│
                    ├──────────────┤
                    │ read_cluster3│
                    └──────────────┘
```

**支持方式**：为每个目标集群创建独立的 event-driven job。

```sql
WARM UP COMPUTE GROUP read_cluster1 WITH COMPUTE GROUP write_cg
ON TABLES (INCLUDE 'db.table_A')
PROPERTIES("sync_mode" = "event_driven", "sync_event" = "load");

WARM UP COMPUTE GROUP read_cluster2 WITH COMPUTE GROUP write_cg
ON TABLES (INCLUDE 'db.table_A')
PROPERTIES("sync_mode" = "event_driven", "sync_event" = "load");
```

**实现**：每个 job 有独立的 `job_id`，在 BE 的 `_tablet_replica_cache` 中各自有独立的槽位。`get_replica_info()` 遍历所有注册的 job，同一 rowset 会被发送到多个目标集群——这正是期望行为。

### 4.2 多个表/模式 → 同一目标集群

```
  db1.* ──────┐
              ├──→  read_cluster
  db2.order_* ┘
```

**方案**：在一个 job 中使用多模式组合：

```sql
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP write_cg
ON TABLES (
    INCLUDE 'db1.*',
    INCLUDE 'db2.order_*'
)
PROPERTIES("sync_mode" = "event_driven", "sync_event" = "load");
```

一个 job、一个 ID，管理简单。

### 4.3 集群级别 job 与表级别 job 共存

当同一 `(src, dst)` 对上同时存在集群级别和带表过滤的 event-driven job 时：

```
Job 1: 集群级别 event-driven (src → dst) — 覆盖所有表
Job 2: 表级别 event-driven (src → dst, ON TABLES(INCLUDE 'db1.table_A')) — 仅 table_A
```

**table_A 的 rowset 会被预热几次？**

- `get_replica_info()` 遍历所有已注册 job。Job 1（`table_ids = nullopt`，不过滤）匹配，Job 2（`table_A` 在 `table_ids` 中）也匹配。
- 结果：**table_A 的 rowset 会被预热两次**——分别通过 Job 1 和 Job 2。

**应对策略**：

| 策略 | 描述 | 推荐 |
|------|------|------|
| 允许重复 | 目标 BE 已有 `add_rowset_warmup_state()` 去重，第二次会被跳过 | ✅ 推荐 |
| 禁止共存 | 集群级别 job 存在时不允许创建同集群对的表级别 job | 过于限制 |
| BE 侧去重 | `warm_up_rowset()` 中记录已发送的 `(rowset_id, target_be)`，避免重复 | 优化方向 |

> **推荐**：允许共存，利用目标 BE 的 `add_rowset_warmup_state()` 进行去重。第二次发送会在目标 BE 侧被短路跳过，开销很小。

---

## 五、边界情况与容错

### 5.1 表被删除（DROP TABLE）

**场景**：event-driven job 的 `ON TABLES (INCLUDE 'db1.order_*')` 匹配到 3 张表，其中 `order_tmp` 被 DROP 了。

| 时间点 | 行为 |
|--------|------|
| DROP 后的 commit_rowset | 不会再发生，因为表已不存在 |
| 下次定期刷新（≤60s 内） | FE 重新评估模式 → `order_tmp` 不再匹配 → table_id 集合缩小 → 推送更新到 BE |
| BE 侧过滤器 | 收到更新后，`order_tmp` 的 table_id 从 `EventDrivenJobFilter` 中移除 |
| Job 状态 | **不自动取消**。其他匹配的表继续正常预热 |

> Job 保持 RUNNING。SHOW WARM UP JOB 中 `MatchedTables` 基于最新评估结果，已删除的表不会出现。

### 5.2 新建表（CREATE TABLE）

**场景**：`ON TABLES (INCLUDE 'db1.order_*')`，用户新建了 `db1.order_2025`。

**行为**：

| 时间点 | 行为 |
|--------|------|
| 创建表后立即写入 | **暂不预热**——此时 BE 的过滤器中尚无 `order_2025` 的 table_id |
| 下次定期刷新（≤60s 内） | FE 重新评估模式 → `order_2025` 匹配 `order_*` → table_id 集合扩大 → 推送更新到 BE |
| 刷新后的写入 | BE 过滤器已包含 `order_2025` → 触发预热 ✅ |

```
        Job 创建 (INCLUDE 'db1.order_*')
             │ 匹配: order_2023, order_2024
             │
             ▼
    t=0   预热: order_2023, order_2024
             │
    t=30  CREATE TABLE db1.order_2025
             │
    t=60  FE 定期刷新 → 发现 order_2025
             │ 匹配: order_2023, order_2024, order_2025
             │ 推送更新到 BE
             │
             ▼
    t=61  预热: order_2023, order_2024, order_2025 ← 新表自动纳入
```

> **关键点**：
> - 新建表在刷新周期内（最长 60 秒）不会被预热，这是可接受的延迟
> - 新表被纳入后，只有**新写入的 rowset** 会触发预热，不会回溯预热历史数据
> - `SHOW WARM UP JOB` 的 `MatchedTables` 在下次刷新后自动包含新表名

### 5.3 表被重命名（ALTER TABLE RENAME）

**场景**：Job 创建时 `ON TABLES (INCLUDE 'db1.order_*')` 匹配到 `db1.order_2024`（table_id=1001），后被重命名为 `db1.archive_2024`。

**行为**（取决于新表名是否仍匹配模式）：

| 场景 | 下次刷新后 | 是否继续预热 |
|------|-----------|-------------|
| RENAME `order_2024` → `order_2024_v2` | `order_2024_v2` 匹配 `order_*` → 保留 | ✅ 继续 |
| RENAME `order_2024` → `archive_2024` | `archive_2024` 不匹配 `order_*` → 移除 | ❌ 停止 |
| RENAME `other_table` → `order_new` | `order_new` 匹配 `order_*` → 新增 | ✅ 自动纳入 |

> **与静态 table_id 方案的区别**：
> - 静态方案：RENAME 不影响预热（因为 table_id 不变）
> - **动态方案**（本设计）：RENAME 后按新名称重新匹配模式
> - 动态方案更符合用户直觉——用户通过模式描述"意图"（"预热所有 order_ 开头的表"），RENAME 后新名不再符合意图时自然停止预热
>
> SHOW WARM UP JOB 的 `MatchedTables` 始终展示当前匹配的表名列表。

### 5.4 Schema Change

Schema Change 会创建新的 tablet（new_tablet_id）和新的 index，但 **table_id 不变**。

- 过滤基于 table_id → ✅ Schema Change 产生的新 rowset 仍会被预热
- Schema Change 期间的 commit_rowset（`cloud_schema_change_job.cpp:420`）传入了 `_job_id`，会触发同步等待预热

### 5.5 Source BE 扩缩容

**扩容**：新加入的 Source BE 没有注册 event-driven 触发器。

**解决方案**：
1. FE 定期检查 Source Cluster 的 BE 列表，对新增 BE 补发 `SET_JOB` RPC（包含当前 table_ids）
2. 或者在 BE 启动时，主动向 FE 拉取需要注册的 event-driven job 列表
3. 定期刷新 `pushTableFilterToSourceBEs` 已覆盖此场景——即使 table_id 集合未变化，也可以定期向所有 BE 推送（包括新 BE）

> 表级别复用与集群级别相同的 BE 扩容感知机制。

### 5.6 FE 主从切换

CloudWarmUpJob 通过 `EditLog` 持久化。新增字段 `tableFilterRules` 和 `tableFilterExpr` 都有 `@SerializedName`。

FE 切主后：
1. 从 EditLog 恢复所有 job，包括 `tableFilterRules` 和 `tableFilterExpr`
2. 调用 `rebuildOnTablesFilter()` 从规则列表重建编译后的过滤器
3. **立即执行一次 `refreshAllTableFilters()`**——重新评估模式，获取最新 table_id 集合
4. 向所有 Source BE 重新发送 `SET_JOB` 注册触发器，携带最新 `table_ids`

> 不依赖持久化的 table_id 集合——新 Master 始终从当前 catalog 状态重新评估，确保一致性。

### 5.7 并发写入（多集群写同一表）

```
cluster_A (写 table_X) ──┐
                          ├──→ read_cluster (预热 table_X)
cluster_B (写 table_X) ──┘
```

当前实现中，event-driven job 只向**一个源集群**注册触发器。需要为每个写入源分别创建 job：

```sql
WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP cluster_A
ON TABLES (INCLUDE 'db.table_X')
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");

WARM UP COMPUTE GROUP read_cg WITH COMPUTE GROUP cluster_B
ON TABLES (INCLUDE 'db.table_X')
PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load");
```

### 5.8 模式匹配边界

| 场景 | 行为 |
|------|------|
| 无 ON TABLES 子句 | 集群级别全量预热 |
| `ON TABLES (INCLUDE '*.*')` | 显式全量匹配（所有表） |
| `ON TABLES (INCLUDE 'nonexistent_db.*')` | 创建时校验，当前无匹配表则报错。但若后续创建了 `nonexistent_db`，下次刷新会自动匹配 |
| 纯 EXCLUDE 无 INCLUDE | 语法错误，至少需要一条 INCLUDE 规则 |
| INCLUDE 和 EXCLUDE 交叉书写 | **允许**。系统内部自动分组、排序和去重，生成 canonical JSON |
| Glob 中 `*` `?` | `*` 匹配任意字符，`?` 匹配单个字符，其余按字面值匹配 |
| Glob 中 `.` | 字面值（库名和表名分隔符），无歧义 |
| 所有匹配表都被 DROP | FE 推送空 table_id 集合 → BE 不触发任何预热 → Job 保持 RUNNING（等待新匹配表出现） |

---

## 六、性能考量

### 6.1 过滤效率

在写入密集场景下，`commit_rowset()` 可能每秒被调用数百次。过滤逻辑必须高效：

```
commit_rowset()
  → warm_up_rowset()
    → rs_meta.tablet_schema()->table_id() — O(1)
    → get_replica_info(tablet_id, table_id, ...)
      → 遍历 _event_driven_filters (通常 1~5 个)
        → filter.should_warmup(table_id) — O(1) (unordered_set)
```

**结论**：过滤开销可忽略不计。`unordered_set<int64_t>::contains()` 是 O(1) 平均，单次过滤在纳秒级。

### 6.2 RPC 节省

假设集群有 200 张表，用户只关注 5 张表：

| 模式 | 每次 commit_rowset 的 FE RPC | 预热 RPC |
|------|------------------------------|----------|
| 集群级别 event-driven | 1 次/commit（200 张表均触发） | 200 张表均发送 |
| 表级别 event-driven（BE 过滤） | 仅 5 张表触发时才 RPC | 仅 5 张表 |
| 节省比例 | **97.5%** | **97.5%** |

### 6.3 内存开销

每个表级别 job 额外存储：
- **FE 侧**：
  - `tableFilterRules`（List\<PersistedTableFilterRule\>）：典型 2~5 条规则，每条 ≈ 50 bytes → 约 250 bytes
  - `onTablesFilter`（OnTablesFilter）：编译后的正则 + 规则列表 ≈ 1~2 KB
  - `currentTableIds`（Set\<Long\>）：典型 5~100 个表 → 约 800 bytes
- **BE 侧**：`EventDrivenJobFilter` — 集群级别为 `nullopt`（~1 byte），表级别典型 5~100 张表 ≈ 80~800 bytes
- **定期刷新**：`resolveTableIds` 遍历 catalog 不产生额外持久化开销
- 总计可忽略不计

---

## 七、兼容性

### 7.1 代码级兼容性审计

以下对本功能涉及的每个变更点进行逐一兼容性分析，基于对现有代码的审计结论。

#### 7.1.1 CloudWarmUpJob 序列化（✅ 安全）

**现状**：`CloudWarmUpJob` 通过 GSON JSON 序列化存入 EditLog（`CloudWarmUpJob.java:855-862`）：
```java
public void write(DataOutput out) throws IOException {
    String json = GsonUtils.GSON.toJson(this, CloudWarmUpJob.class);
    Text.writeString(out, json);
}
public static CloudWarmUpJob read(DataInput in) throws IOException {
    String json = Text.readString(in);
    return GsonUtils.GSON.fromJson(json, CloudWarmUpJob.class);
}
```

当前有 17 个 `@SerializedName` 字段（`jobId`, `jobState`, `srcClusterName`, `syncMode`, `syncEvent` 等）。

**新增字段**：
```java
@SerializedName(value = "tableFilterExpr")
protected String tableFilterExpr = "";

@SerializedName(value = "tableFilterRules")
protected List<PersistedTableFilterRule> tableFilterRules = new ArrayList<>();
```

**注意**：不再持久化 `tableIds`。table_id 集合由 FE 运行时动态计算。

**兼容性分析**：
| 场景 | 行为 | 安全 |
|------|------|------|
| **新 FE 读旧 EditLog**（旧 job 无新字段） | GSON 反序列化时缺少的字段使用 Java 字段初始值：`tableFilterExpr=""`, `tableFilterRules=[]` → 等同于集群级别 Job（不过滤） | ✅ |
| **旧 FE 读新 EditLog**（新 job 含新字段） | GSON 遇到未知 JSON key 默认忽略，不会报错 | ✅ |
| **新 FE 读新 EditLog** | 正常反序列化，然后调用 `rebuildOnTablesFilter()` 重建过滤器 + 立即评估 | ✅ |

> **参考先例**：代码中 `cloudClusterName` 字段注释（line 111-112）说明"serialized name is kept for compatibility reasons"，说明团队已有字段兼容性意识，GSON 方案经过验证。

#### 7.1.2 JobKey / repeatJobDetectionSet（✅ 安全）

**现状**：`repeatJobDetectionSet` 是 **纯内存数据结构**，`ConcurrentHashMap.newKeySet()`（`CacheHotspotManager.java:115-153`），**不持久化到 EditLog**。

当前 `JobKey` 结构：
```java
private static class JobKey {
    private final String srcName;
    private final String dstName;
    private final CloudWarmUpJob.SyncMode syncMode;
}
```

**FE 重启恢复**：`replayCloudWarmUpJob()`（line 885-905）中调用 `registerJobForRepeatDetection(job, true)`（replay=true 标志跳过重复校验），从 EditLog 中的 CloudWarmUpJob 对象重建 `repeatJobDetectionSet`。

**兼容性分析**：
| 场景 | 行为 | 安全 |
|------|------|------|
| **新增 `tableFilterExpr`（canonical JSON）字段到 JobKey** | 纯内存结构，无持久化 → 不存在跨版本兼容问题 | ✅ |
| **FE 重启后恢复** | 从 CloudWarmUpJob 的已持久化 `tableFilterExpr` 字段重建 → 正确恢复 | ✅ |
| **旧 Job 恢复**（无 `tableFilterExpr`） | 反序列化后 `tableFilterExpr=""`，JobKey 中为空串 → 等同于集群级别 | ✅ |
| **规范化保证 JobKey 一致性** | 不同书写顺序但语义相同的规则 → 相同 canonical JSON → 正确去重 | ✅ |

> **结论**：`repeatJobDetectionSet` 不持久化，因此 JobKey 结构变更是纯代码变更，不影响数据兼容性。canonical JSON 格式的 `tableFilterExpr` 确保了 JobKey 的规范性。

#### 7.1.3 Thrift TWarmUpTabletsRequest 扩展（✅ 安全）

**现状**：`BackendService.thrift` 中 `TWarmUpTabletsRequest` 使用 field 1-5：
```thrift
struct TWarmUpTabletsRequest {
    1: required i64 job_id
    2: required i64 batch_id
    3: optional list<TJobMeta> job_metas
    4: required TWarmUpTabletsRequestType type
    5: optional TWarmUpEventType event
}
```

**新增**：`6: optional list<i64> table_ids`

**兼容性分析**：
| 场景 | 行为 | 安全 |
|------|------|------|
| **新 FE → 旧 BE**（发送 field 6） | Thrift 协议中旧 BE 遇到未知 field 自动跳过忽略 → BE 不解析 `table_ids` → 降级为集群级别行为（不过滤） | ✅ |
| **旧 FE → 新 BE**（不发送 field 6） | `__isset.table_ids == false` → 新 BE 视为集群级别 Job → `EventDrivenJobFilter.table_ids = nullopt` | ✅ |
| **新 FE → 新 BE** | 正常传递和解析 `table_ids` | ✅ |

> **注意**：field 6 使用 `optional` 修饰，与 `std::optional<EventDrivenJobFilter>` 形成天然映射：
> - `__isset.table_ids == false` → `nullopt`（集群级别）
> - `__isset.table_ids == true` → `optional<set>{...}`（表级别）

#### 7.1.4 EditLog 操作类型（✅ 无需变更）

**现状**：使用 `OperationType.OP_MODIFY_CLOUD_WARM_UP_JOB = 1002`（`OperationType.java:431`），对整个 `CloudWarmUpJob` 对象做 JSON 序列化。

**分析**：不需要新增操作类型。新字段通过 GSON 自动序列化/反序列化，操作类型保持 `1002` 不变。

#### 7.1.5 Builder 模式（✅ 需小改）

**现状**：`CloudWarmUpJob.Builder`（`CloudWarmUpJob.java:154-206`）当前有 7 个字段（`jobId`, `srcClusterName`, `dstClusterName`, `jobType`, `syncMode`, `syncEvent`, `syncInterval`）。

**需要新增**：
```java
public static class Builder {
    // 新增
    private String tableFilterExpr = "";
    private List<PersistedTableFilterRule> tableFilterRules = new ArrayList<>();

    public Builder setTableFilterExpr(String expr) { this.tableFilterExpr = expr; return this; }
    public Builder setTableFilterRules(List<PersistedTableFilterRule> rules) { this.tableFilterRules = rules; return this; }
}
```

**兼容性分析**：Builder 是创建时的辅助类，不涉及持久化。新增 setter 不影响已有代码的调用路径。旧代码不调用新 setter，新字段使用默认值（空串/空列表），等同集群级别 → **安全**。

#### 7.1.6 WarmUpClusterCommand SQL 解析（⚠️ 需注意）

**现状**：`WarmUpClusterCommand.java` 的 `validate()` 方法（line 154-206）解析 `tables` 参数（ONCE 模式的 `db.table.partition` 三元组列表）和 `properties` 参数。

**新增**：`ON TABLES (...)` 子句需要在 FE Parser（Doris 使用 ANTLR4/Nereids 或旧版 CUP）中新增语法规则。

**兼容性分析**：
| 场景 | 行为 | 安全 |
|------|------|------|
| **旧 FE 收到带 ON TABLES 的 SQL** | Parser 不认识 `ON TABLES` 关键字 → 语法错误，拒绝执行 | ✅（合理行为） |
| **新 FE 收到不带 ON TABLES 的 SQL** | 与现有行为完全一致 | ✅ |
| **SQL 语法变更是否影响滚动升级** | 在滚动升级期间，只要用户不使用 ON TABLES 新语法，不会有任何问题。建议文档说明：在 FE 全部升级完成前，不要使用 ON TABLES 语法 | ✅ |

### 7.2 向后兼容总结

| 变更点 | 持久化？ | 旧版本兼容 | 风险 |
|--------|---------|-----------|------|
| `CloudWarmUpJob` 新增 `tableFilterRules` + `tableFilterExpr` | ✅ EditLog (GSON JSON) | GSON 忽略未知字段 / 缺失字段用默认值 | ✅ 无 |
| `JobKey` 新增 `tableFilterExpr`（canonical JSON） | ❌ 纯内存 | 不涉及持久化兼容，canonical JSON 保证规范性 | ✅ 无 |
| `TWarmUpTabletsRequest` 新增 field 6 | ❌ RPC 协议 | Thrift optional 字段向后兼容 | ✅ 无 |
| `OperationType` | 不变（仍为 1002） | — | ✅ 无 |
| `Builder` 新增 setter | ❌ 代码 | 不影响已有调用 | ✅ 无 |
| FE Parser 新增 `ON TABLES` 语法 | ❌ 代码 | 旧 FE 拒绝新语法（合理） | ⚠️ 低（滚动升级期间不用新语法即可） |
| FE 定期刷新调度器 | ❌ 代码 | 旧 FE 无此逻辑，无影响 | ✅ 无 |

### 7.3 滚动升级

| 升级状态 | FE 新 + BE 旧 | FE 旧 + BE 新 |
|----------|---------------|---------------|
| 带 ON TABLES 过滤的 job | FE 发送 `table_ids`（field 6），旧 BE 忽略新字段，**降级为集群级别行为**（不过滤） | 旧 FE 不认识 ON TABLES 语法，**不会创建**带过滤的 job |
| 不带 ON TABLES 的 job | 行为完全不变 | 行为完全不变 |
| **EditLog 混合** | 旧 FE 写的 EditLog 无新字段 → 新 FE 反序列化后 `tableFilterRules=[]`（集群级别）✅ | 新 FE 写的 EditLog 含新字段 → 旧 FE 反序列化时 GSON 忽略未知 key ✅ |
| **定期刷新** | 新 FE 的刷新推送被旧 BE 忽略（field 6 被跳过）→ 无害 | 旧 FE 不执行刷新 → 无影响 |

> **滚动升级建议**：
> 1. 先升级所有 BE，再升级所有 FE（或反过来均可，因为兼容性是双向的）
> 2. 在所有 FE 升级完成前，不要执行带 `ON TABLES` 子句的 SQL
> 3. 在所有 BE 升级完成前，带 `ON TABLES` 的 Job 会降级为集群级别行为（功能不全但不报错）

---

## 八、测试要点

### 8.1 功能测试

1. **精确匹配**：`ON TABLES (INCLUDE 'db1.table1')` → 写入 table1 被预热，写入 table2 不被预热
2. **前缀匹配**：`ON TABLES (INCLUDE 'db1.order_*')` → 写入 `order_2024` 被预热，写入 `user_info` 不被预热
3. **整库匹配**：`ON TABLES (INCLUDE 'db1.*')` → db1 下所有表被预热，db2 下的表不被预热
4. **跨库匹配**：`ON TABLES (INCLUDE '*.fact_*')` → 所有 DB 中 fact_ 开头的表被预热
5. **单字符通配**：`ON TABLES (INCLUDE 'db1.log_202?')` → 匹配 `db1.log_2023` 但不匹配 `db1.log_20234`
6. **INCLUDE + EXCLUDE**：`ON TABLES (INCLUDE 'db1.*', EXCLUDE 'db1.tmp_*')` → db1 下除 tmp_ 开头外的表被预热
7. **多规则组合**：`ON TABLES (INCLUDE 'db1.t1', INCLUDE 'db2.order_*')` → 验证并集行为
8. **一表多目标**：table_A 分别创建到 cluster_1 和 cluster_2 的 job → 两个集群都被预热
9. **混合共存**：集群级别 + 表级别 job 共存 → 验证去重正确性
10. **SHOW/CANCEL**：展示 TableFilter 和 MatchedTables，取消表级别 job

### 8.2 动态行为测试

9. **新建表自动纳入**：创建匹配模式的新表 → 等待刷新周期（≤60s）→ 验证新表的写入被预热
10. **新建表延迟窗口**：创建新表后立即写入 → 验证在刷新前**不被预热**，刷新后**被预热**
11. **删除表自动移除**：删除匹配表 → 等待刷新 → 验证 BE 过滤器已更新，其他表不受影响
12. **重命名表——仍匹配**：将 `order_2024` 重命名为 `order_2024_v2` → 模式 `order_*` 仍匹配 → 继续预热
13. **重命名表——不再匹配**：将 `order_2024` 重命名为 `archive_2024` → 模式 `order_*` 不再匹配 → 停止预热
14. **重命名外部表纳入**：将不匹配的 `other_table` 重命名为 `order_new` → 模式 `order_*` 匹配 → 自动纳入
15. **FE 主从切换后恢复**：切主后 → 新 Master 从 EditLog 恢复规则 → 立即刷新 → BE 收到最新 table_ids
16. **所有匹配表被 DROP**：删除所有匹配表 → 刷新后 table_id 集合为空 → Job 保持 RUNNING → 新建匹配表后自动恢复

### 8.3 规范化测试

17. **顺序无关**：`ON TABLES (INCLUDE 'b.*', EXCLUDE 'b.tmp_*', INCLUDE 'a.*')` 和 `ON TABLES (INCLUDE 'a.*', INCLUDE 'b.*', EXCLUDE 'b.tmp_*')` → 两者 canonical JSON 相同 → 第二个创建被判定为重复 Job
18. **重复规则去重**：`ON TABLES (INCLUDE 'db.*', INCLUDE 'db.*')` → canonical JSON 中只有一条 `"db.*"` → 不报错，正常去重
19. **canonical JSON 格式**：SHOW WARM UP JOB 的 TableFilter 列展示规范化后的 JSON → 验证格式正确、pattern 按字典序排列

### 8.4 边界测试

20. **无 ON TABLES**：不使用 ON TABLES → 等价于集群级别，全量预热
21. **无匹配**：`ON TABLES (INCLUDE 'nonexistent.*')` → 创建时报错
22. **纯 EXCLUDE**：`ON TABLES (EXCLUDE 'db1.*')` → 语法错误（至少需要一条 INCLUDE）
23. **Schema Change**：SC 过程中和之后的 rowset 都被正确预热
24. **Source BE 重启**：FE 重新注册触发器，包含 table_ids
25. **FE 主从切换**：job 恢复后继续工作

### 8.5 性能测试

26. **过滤效率**：大量不相关表写入时，验证 commit_rowset 延迟不受影响
27. **大量 job**：创建 50+ 个不同模式的 job → 验证系统稳定

---

## 九、总结

### 关键设计决策汇总

| # | 决策项 | 决定 |
|---|--------|------|
| 1 | SQL 语法 | 方案 F：`ON TABLES (INCLUDE/EXCLUDE '...')` 纯 Glob 子句 |
| 2 | 表匹配模式 | 纯 Glob 通配符（`*` `?`），精确匹配通过不使用通配符实现 |
| 3 | INCLUDE/EXCLUDE 语义 | 集合运算：先算 INCLUDE 集合，再减 EXCLUDE 集合 |
| 4 | 语法约束 | INCLUDE/EXCLUDE 可任意顺序，内部自动规范化为 canonical JSON |
| 5 | 过滤粒度 | DB + 表级别，不支持分区 |
| 6 | 模式匹配时机 | **创建时匹配一次 + 定期刷新**（默认每 60 秒），动态跟踪 catalog 变化 |
| 7 | 持久化内容 | 模式规则列表（tableFilterRules）+ canonical JSON（tableFilterExpr），**不持久化 table_id 集合** |
| 8 | 过滤位置 | BE 侧（减少 97.5% 无效 RPC） |
| 9 | table_id 获取 | `rs_meta.tablet_schema()->table_id()`，O(1) |
| 10 | BE 过滤器 | `std::optional<unordered_set<int64_t>>`：nullopt=集群级别，has_value=表级别 |
| 11 | 重复检测 | 按 (src, dst, syncMode, canonical JSON) 去重，书写顺序无关 |
| 12 | 集群+表共存 | 允许，目标 BE 侧去重 |
| 13 | RENAME | **跟随模式匹配**——新表名仍匹配则继续预热，不匹配则停止 |
| 14 | 新建表 | **自动纳入**——下次刷新时匹配到新表则自动加入预热范围 |
| 15 | DROP TABLE | **自动移除**——下次刷新时自动从过滤集合移除，Job 不取消 |
| 16 | 转义问题 | 无（glob 中 `.` 是字面量，无 REGEX 转义问题） |
| 17 | FE→BE 推送 | 复用 SET_JOB RPC + table_ids 字段，BE 侧支持幂等更新过滤器 |
