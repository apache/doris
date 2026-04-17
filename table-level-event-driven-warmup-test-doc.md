# 表级别 Event-Driven 预热功能 — 测试文档

本文档描述 `passive-warmup-table-dev` 分支上 **表级别 Event-Driven 预热（ON TABLES）** 功能的所有测试，包括回归测试（Docker 多集群）和单元测试（FE/BE）。

---

## 一、回归测试（Docker 多集群端到端测试）

### 1.1 测试框架与公共工具

所有回归测试位于 `regression-test/suites/cloud_p0/cache/multi_cluster/warm_up/on_tables/` 目录下，共 7 个独立的 Docker 测试文件。每个文件在独立的 Docker 环境中运行（1 个 BE 节点），确保 bvar 指标互不干扰。

**公共工具类：** `WarmupMetricsUtils.groovy`（位于 `regression-test/framework/src/main/groovy/org/apache/doris/regression/util/`），提供以下核心方法：

| 方法 | 功能 |
|------|------|
| `getBrpcMetric(ip, port, metricName)` | 从 BE 的 `/brpc_metrics` 端点获取单个指标值 |
| `getClusterMetricSum(sqlRunner, cluster, metric)` | 汇总集群内所有 BE 的指标 |
| `getWarmupMetrics(sqlRunner, src, dst)` | 获取四项指标：requested/submitted/finished/failed |
| `waitForWarmupFinish(sqlRunner, src, dst, expected, timeout)` | 轮询等待 finished ≥ expected |
| `waitForMatchedTables(sqlRunner, jobId, contains, notContains, timeout)` | 轮询 SHOW WARM UP JOB 直到 MatchedTables 符合预期 |
| `waitForMetricsStable(sqlRunner, src, dst, timeout)` | 双重检查模式确认指标已稳定 |
| `parseMatchedTables(jobInfo)` | 从 SHOW WARM UP JOB 结果解析 MatchedTables 列 |

**指标架构（bvar）：**

| 指标名 | 采集集群 | 含义 |
|--------|----------|------|
| `file_cache_event_driven_warm_up_requested_segment_num` | 源集群 | 源集群接收到的预热请求 segment 数 |
| `file_cache_event_driven_warm_up_submitted_segment_num` | 目标集群 | 目标集群已提交的预热 segment 数 |
| `file_cache_event_driven_warm_up_finished_segment_num` | 目标集群 | 目标集群已完成的预热 segment 数 |
| `file_cache_event_driven_warm_up_failed_segment_num` | 目标集群 | 目标集群预热失败的 segment 数 |

---

### 1.2 测试用例详解

#### Case 1：`test_warm_up_event_on_tables_include` — INCLUDE 通配符过滤

**测试目标：** 验证 `INCLUDE 'db.*'` 通配符模式能正确匹配数据库下的所有表，且不匹配其他数据库的表。

**测试步骤：**
1. 创建两个数据库：`test_on_tables_inc_db`（被包含）和 `test_on_tables_exc_db`（不被包含）
2. 在包含库中创建 `orders`、`customers` 两张表；在排除库中创建 `logs` 一张表
3. 执行 `WARM UP CLUSTER ... ON TABLES (INCLUDE 'test_on_tables_inc_db.*')` 创建预热任务
4. 记录基线指标

**验证方式：**
- **反向验证（负测试）：** 向排除库的 `logs` 表插入 5 行数据，检查 submitted 和 finished 增量均为 0 — 证明排除库的表不会触发预热
- **正向验证：** 向 `orders` 和 `customers` 各插入 5 行（共 10 个 segment），等待 finished ≥ 10，验证 requested ≥ 10、submitted ≥ 10、failed == 0
- **元数据验证：** 检查 SHOW WARM UP JOB 中 TableFilter 为 `{"include":["db.*"]}`（无 exclude 键），MatchedTables 包含 orders 和 customers，不包含 logs

---

#### Case 2：`test_warm_up_event_on_tables_include_exclude` — INCLUDE + EXCLUDE 组合过滤

**测试目标：** 验证 EXCLUDE 规则能在 INCLUDE 基础上进一步排除特定表，且多条 EXCLUDE 规则共同生效。

**测试步骤：**
1. 创建一个数据库，包含 4 张表：`orders`、`customers`（应被预热）、`tmp_staging`（匹配 `tmp_*` 排除规则）、`orders_bak`（匹配 `*_bak` 排除规则）
2. 执行带 `INCLUDE 'db.*'`、`EXCLUDE 'db.tmp_*'`、`EXCLUDE 'db.*_bak'` 的预热任务

**验证方式：**
- **反向验证：** 向 `tmp_staging` 和 `orders_bak` 各插入 5 行，submitted 和 finished 增量为 0
- **正向验证：** 向 `orders` 和 `customers` 各插入 5 行，等待 finished ≥ 10，验证全链路指标
- **元数据验证：** TableFilter JSON 包含 include 和 exclude 键，MatchedTables 只含 orders 和 customers

---

#### Case 3：`test_warm_up_event_on_tables_multi_include` — 多条 INCLUDE 跨库匹配

**测试目标：** 验证多条 INCLUDE 规则形成并集（union），可同时匹配不同数据库中的指定表。

**测试步骤：**
1. 创建两个数据库，在第一个库建 `orders` 和 `customers`，在第二个库建 `logs`
2. 执行带 `INCLUDE 'db1.orders'`、`INCLUDE 'db2.logs'` 的预热任务

**验证方式：**
- 向三张表各插入 5 行（共 15 个 segment），但只有 orders 和 logs 被匹配（10 个 segment）
- 等待 finished ≥ 10，同时验证 submitted < 15（customers 不应被预热）
- MatchedTables 包含 orders 和 logs，不包含 customers

---

#### Case 4：`test_warm_up_event_on_tables_canonicalization` — 规则标准化与去重

**测试目标：** 验证系统对 ON TABLES 规则进行标准化处理（排序 + 去重），使得规则顺序不同但内容相同的任务被视为重复。

**测试步骤：**
1. 创建第一个预热任务，规则顺序为：`INCLUDE 'db1.*'`, `EXCLUDE 'db1.tmp_*'`, `INCLUDE 'db2.*'`
2. 尝试创建第二个预热任务，规则内容相同但顺序不同：`INCLUDE 'db2.*'`, `INCLUDE 'db1.*'`, `EXCLUDE 'db1.tmp_*'`

**验证方式：**
- 第一个任务成功创建
- 第二个任务抛出异常，错误信息包含 "already has a runnable job"
- 证明系统对规则进行了标准化后比较，与用户输入的顺序无关

---

#### Case 5：`test_warm_up_event_on_tables_dynamic` — 动态表变更自动追踪

**测试目标：** 验证 MatchedTables 在 CREATE TABLE、DROP TABLE、RENAME TABLE 操作后自动更新。

**测试步骤与验证（4 个子测试）：**

**子测试 1 — 新建表自动纳入：**
- 创建 `fact_orders`，启动 `INCLUDE 'db.fact_*'` 预热任务
- 新建 `fact_sales`（匹配）和 `dim_product`（不匹配）
- 验证 MatchedTables 包含 `fact_orders` 和 `fact_sales`，不含 `dim_product`
- 向 `fact_sales` 插入 5 行，finished ≥ 5；向 `dim_product` 插入后指标无变化

**子测试 2 — 删除表自动移除：**
- DROP `fact_orders`
- 验证 MatchedTables 不再包含 `fact_orders`，任务仍为 RUNNING

**子测试 3 — 重命名触发重新匹配：**
- `fact_sales` → `archive_sales`（不再匹配 `fact_*`），MatchedTables 变空，任务仍 RUNNING
- `archive_sales` → `fact_revenue`（重新匹配），MatchedTables 包含 `fact_revenue`
- 插入 5 行数据，finished ≥ 5

**子测试 4 — `?` 单字符通配符：**
- 创建 `log_a`、`log_b`、`log_ab`，启动 `INCLUDE 'db.log_?'` 任务
- `?` 匹配恰好一个字符：`log_a` 和 `log_b` 匹配，`log_ab` 不匹配
- 各插入 3 行（共 9 segment），仅 6 个被预热，验证 finished ≥ 6 且 submitted < 9

---

#### Case 6：`test_warm_up_event_on_tables_multi_dst` — 多目标集群同步

**测试目标：** 验证同一张表可以同时被预热到多个不同的目标集群，且各目标集群的预热过程独立，过滤规则互不影响。

**测试步骤：**
1. 创建 3 个集群：`warmup_source`（源）、`warmup_target_1`（目标 1）、`warmup_target_2`（目标 2）
2. 创建数据库，包含 `orders` 和 `logs` 两张表
3. 创建 Job1：source → target1，`INCLUDE 'db.orders'`（仅匹配 orders）
4. 创建 Job2：source → target2，`INCLUDE 'db.*'`（匹配所有表）

**验证方式：**

**子测试 1 — orders 同时预热到两个目标：**
- 向 `orders` 插入 5 行
- 分别等待 target1 和 target2 的 finished 指标 ≥ 5
- 两个目标集群的 submitted、finished 增量独立验证，failed == 0

**子测试 2 — logs 仅预热到 target2（负测试）：**
- 等待 target1 指标稳定（`waitForMetricsStable`）后记录基线
- 向 `logs` 插入 5 行
- target2 的 finished ≥ 5（logs 被 `db.*` 匹配）
- target1 的 submitted 和 finished 增量为 0（logs 不被 `db.orders` 匹配）

**元数据验证：**
- Job1 的 TableFilter 为 `{"include":["db.orders"]}`，目标集群为 target1
- Job2 的 TableFilter 为 `{"include":["db.*"]}`，目标集群为 target2

---

#### Case 7：`test_warm_up_event_on_tables_error_and_lifecycle` — 错误处理与生命周期管理

**测试目标：** 覆盖所有错误场景、集群级与表级任务共存、重复检测、取消与重建。

**错误测试（5 个子场景）：**

| # | 场景 | 预期错误信息 |
|---|------|-------------|
| 1 | 仅有 EXCLUDE，无 INCLUDE | "at least one INCLUDE" |
| 2 | 模式格式错误（缺少 `db.` 前缀） | "db.table" format |
| 3 | ON TABLES + periodic 同步模式 | "event_driven" |
| 4 | 无匹配表（不存在的库名） | "no tables matched" / "no table" |
| 5 | ON TABLES + once 同步模式 | "event_driven" |

**生命周期测试：**

1. **共存测试：** 同时创建集群级 event-driven 任务（无 ON TABLES）和表级 event-driven 任务（有 ON TABLES），两者均成功。集群级任务 TableFilter 和 MatchedTables 为空，表级任务有完整的过滤规则和匹配表。

2. **重复检测：** 相同过滤规则创建第二个任务失败（"already has a runnable job"）；不同过滤规则可以成功创建。

3. **取消与重建：** 取消表级任务后状态变为 CANCELLED；使用相同规则重新创建成功，新任务 ID 不同，状态为 RUNNING。

4. **`?` 通配符指标验证：** 创建 `log_a`、`log_b`、`log_ab`，使用 `INCLUDE 'db.log_?'` 匹配，各插 3 行，验证 finished ≥ 6、failed == 0。

---

### 1.3 回归测试覆盖矩阵

| Case | INCLUDE | EXCLUDE | 多规则 | 跨库 | 通配符 * | 通配符 ? | 动态表变更 | 多目标集群 | 错误校验 | 生命周期 | 指标验证 |
|------|---------|---------|--------|------|----------|----------|-----------|-----------|----------|----------|----------|
| 1. include | ✅ | — | — | ✅ | ✅ | — | — | — | — | — | ✅ |
| 2. include_exclude | ✅ | ✅ | ✅ | — | ✅ | — | — | — | — | — | ✅ |
| 3. multi_include | ✅ | — | ✅ | ✅ | — | — | — | — | — | — | ✅ |
| 4. canonicalization | ✅ | ✅ | ✅ | ✅ | ✅ | — | — | — | ✅ | — | — |
| 5. dynamic | ✅ | — | — | — | ✅ | ✅ | ✅ | — | — | — | ✅ |
| 6. multi_dst | ✅ | — | ✅ | — | ✅ | — | — | ✅ | — | — | ✅ |
| 7. error_lifecycle | ✅ | ✅ | — | — | ✅ | ✅ | — | — | ✅ | ✅ | ✅ |

---

## 二、单元测试

### 2.1 FE 单元测试

本功能在 FE 侧共有 **7 个测试类，约 65 个测试方法**，覆盖从 SQL 解析到任务执行的完整链路。

---

#### 2.1.1 `OnTablesFilterTest.java`

**位置：** `fe/fe-core/src/test/java/org/apache/doris/cloud/OnTablesFilterTest.java`

**测试目标：** 验证 `OnTablesFilter` 类的 glob 模式匹配引擎。

| 测试方法 | 验证内容 |
|----------|---------|
| `testGlobWildcards()` | `*` 匹配任意字符，`?` 匹配恰好一个字符 |
| `testDotIsLiteral()` | `.` 作为字面量处理，不是正则的"任意字符" |
| `testRegexMetacharsEscaped()` | 正则元字符 `()[]` 等被正确转义 |
| `testIncludeOnlyMatchesTargetDb()` | 仅 INCLUDE 规则时正确匹配目标库 |
| `testExcludeOverridesInclude()` | EXCLUDE 规则优先级高于 INCLUDE |
| `testMultipleIncludesFormUnion()` | 多条 INCLUDE 形成并集 |
| `testExcludeOnlyNeverMatches()` | 仅有 EXCLUDE 规则时不匹配任何表 |
| `testEmptyRulesNeverMatches()` | 空规则不匹配任何表 |
| `testComplexScenario()` | 组合 INCLUDE/EXCLUDE 跨库复杂场景 |
| `testGetRulesPartition()` | 规则正确分组为 INCLUDE/EXCLUDE 两类 |

---

#### 2.1.2 `CloudWarmUpJobTableFilterTest.java`

**位置：** `fe/fe-core/src/test/java/org/apache/doris/cloud/CloudWarmUpJobTableFilterTest.java`

**测试目标：** 验证 `CloudWarmUpJob` 的表过滤规则标准化、序列化、SHOW 输出及动态跟踪。

| 测试方法 | 验证内容 |
|----------|---------|
| `testCanonicalizeIncludeOnly()` | INCLUDE-only 规则序列化为 JSON |
| `testCanonicalizeWithExclude()` | 混合规则排序后序列化 |
| `testCanonicalizeOrderIndependentAndDedup()` | 不同顺序 + 重复规则产生相同标准化结果 |
| `testBuilderNormalizesPersistedTableFilterRules()` | Builder 自动去重排序 |
| `testCanonicalizeExcludeKeyAbsentWhenNoExcludes()` | 无 EXCLUDE 时 JSON 不含 exclude 键 |
| `testCanonicalizeEmptyRules()` | 空规则标准化为 `{"include":[]}` |
| `testRebuildOnTablesFilter()` | 从持久化规则重建 OnTablesFilter 对象 |
| `testRebuildOnTablesFilterAlsoComputesExpr()` | 重建同时计算 JSON 表达式 |
| `testReadNormalizesPersistedTableFilterRules()` | 反序列化时自动标准化 |
| `testRebuildOnTablesFilterNoRules()` | 无规则时不创建 filter |
| `testHasTableFilter()` | 正确判断任务是否有表过滤器 |
| `testTableFilterExprDerivedFromRules()` | tableFilterExpr 从规则计算而非独立存储 |
| `testTableFilterExprEmptyWhenNoRules()` | 无规则时表达式为空 |
| `testGetJobInfoTableLevelJob()` | SHOW 输出包含 MatchedTables 和 TableFilter |
| `testGetJobInfoClusterLevelJob()` | 集群级任务的 TableFilter 和 MatchedTables 为空 |
| `testGetJobInfoMatchedTablesEmpty()` | 所有表被删除后 MatchedTables 为空 |
| `testDynamicTableIdTracking()` | 跟踪表 ID 变化（创建/删除/重命名） |
| `testBuilderMissingRequiredFieldsThrows()` | Builder 缺少必填字段时抛异常 |

---

#### 2.1.3 `CacheHotspotManagerTableFilterTest.java`

**位置：** `fe/fe-core/src/test/java/org/apache/doris/cloud/CacheHotspotManagerTableFilterTest.java`

**测试目标：** 验证 `CacheHotspotManager` 的表 ID 解析、动态刷新和任务管理逻辑。

| 测试方法 | 验证内容 |
|----------|---------|
| `testResolveTableIdsBasicMatching()` | INCLUDE 'ods.*' 匹配所有 ods 表 |
| `testResolveTableIdsWithExclude()` | INCLUDE + EXCLUDE 组合过滤 |
| `testResolveTableIdsMultipleDatabases()` | 跨库多 INCLUDE 解析 |
| `testResolveTableIdsNoMatch()` | 不匹配时返回空集合 |
| `testResolveTableIdsNullFilter()` | null filter 返回空集合 |
| `testResolveTableIdsDbNameWithPrefix()` | 处理 `default_cluster:` 前缀 |
| `testResolveTableIdsAfterNewTableCreated()` | 新建表后重新解析可发现 |
| `testResolveTableIdsAfterTableDropped()` | 删除表后重新解析不再包含 |
| `testResolveTableIdsAfterTableRenamed()` | 重命名后不再匹配原模式 |
| `testResolveTableIdsAfterAllTablesDropped()` | 全部删除后返回空集，任务仍 RUNNING |
| `testRefreshAllTableFiltersUpdatesJobTableIds()` | 批量刷新更新所有任务的表 ID |
| `testRefreshAllTableFiltersSkipsClusterLevelJob()` | 刷新跳过集群级任务 |
| `testRefreshAllTableFiltersHandlesTableDrop()` | 刷新时处理表删除 |
| `testRefreshAllTableFiltersUpdatesMatchedNamesAfterRenameStillMatches()` | 重命名后仍匹配时更新名称 |
| `testCreateJobRejectsOnTablesWithoutInitialMatches()` | 无初始匹配表时拒绝创建 |
| `testCreateJobRejectsEquivalentDuplicateTableFilter()` | 标准化后重复的过滤规则被拒绝 |
| `testCreateJobAllowsClusterLevelAndTableLevelToCoexist()` | 集群级和表级任务共存 |

---

#### 2.1.4 `WarmUpClusterOnTablesParseTest.java`

**位置：** `fe/fe-core/src/test/java/org/apache/doris/cloud/WarmUpClusterOnTablesParseTest.java`

**测试目标：** 验证 SQL 语法解析器对 `ON TABLES` 子句的解析和语义校验。

| 测试方法 | 验证内容 |
|----------|---------|
| `testOnTablesSingleInclude()` | 解析单条 INCLUDE 规则 |
| `testOnTablesMultipleRules()` | 解析多条 INCLUDE + EXCLUDE 规则 |
| `testWithoutOnTablesClause()` | 无 ON TABLES 时 filter 为 null |
| `testOnTablesWithForce()` | ON TABLES + FORCE 关键字组合 |
| `testOnTablesWithComputeGroup()` | ON TABLES + COMPUTE GROUP 语法 |
| `testOnTablesEmptyParensFails()` | `ON TABLES ()` 语法错误 |
| `testOnTablesMissingParensFails()` | 缺少括号语法错误 |
| `testOnTablesMissingPatternFails()` | `INCLUDE` 后缺少模式字符串 |
| `testOnTablesExcludeOnlyParsesButLacksInclude()` | 仅 EXCLUDE 可解析但标记无 INCLUDE |
| `testOnTablesNonEventDrivenSyncModeParses()` | 非 event_driven 模式可解析 |
| `testOnTablesExcludeOnlyValidateFails()` | 语义校验拒绝仅 EXCLUDE |
| `testOnTablesNonEventDrivenValidateFails()` | 语义校验拒绝非 event_driven + ON TABLES |
| `testOnTablesPatternWithoutDbTableFormatValidateFails()` | 校验模式必须为 `db.table` 格式 |

---

#### 2.1.5 `ModifyCloudWarmUpJobTest.java`

**位置：** `fe/fe-core/src/test/java/org/apache/doris/persist/ModifyCloudWarmUpJobTest.java`

**测试目标：** 验证 `CloudWarmUpJob` 对象的序列化/反序列化正确性。

- `testSerialization()` — 将 CloudWarmUpJob 写入文件后读回，验证 jobId、jobState、createTimeMs、errMsg 等所有字段一致。

---

#### 2.1.6 `WarmUpClusterCommandTest.java`

**位置：** `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/WarmUpClusterCommandTest.java`

**测试目标：** 验证 WarmUpClusterCommand 的基本校验逻辑。

- `testValidate()` — 验证 Cloud 模式下源/目标集群存在性检查，非 Cloud 模式抛错。

---

#### 2.1.7 `ShowWarmUpCommandTest.java`

**位置：** `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/ShowWarmUpCommandTest.java`

**测试目标：** 验证 SHOW WARM UP 命令的元数据结构。

| 测试方法 | 验证内容 |
|----------|---------|
| `testValidate()` | null WHERE 通过校验 |
| `testValidate2()` | WHERE 条件引用非 ID 列失败 |
| `testValidate3()` / `testValidate4()` | WHERE id = Integer/BigInt 通过 |
| `testMetaDataContainsTableFilterColumns()` | 元数据第 13、14 列为 TableFilter 和 MatchedTables |

---

### 2.2 BE 单元测试

#### 2.2.1 `cloud_warm_up_manager_filter_test.cpp`

**位置：** `be/test/cloud/cloud_warm_up_manager_filter_test.cpp`

**测试目标：** 验证 BE 侧 `CloudWarmUpManager` 的 event-driven 过滤器管理和副本过滤逻辑。

| 测试方法 | 验证内容 |
|----------|---------|
| `EventDrivenJobFilterNullopt()` | 集群级任务 filter 为 nullopt |
| `EventDrivenJobFilterWithTableIds()` | 表级任务 filter 包含指定表 ID |
| `EventDrivenJobFilterEmpty()` | 空表 ID 集合存储为空 filter |
| `SetEventWithoutTableIdsStoresClusterLevelFilter()` | `set_event()` 存储集群级 filter |
| `SetEventWithTableIdsStoresFilter()` | `set_event()` 存储表 ID filter |
| `SetEventWithEmptyTableIdsStoresEmptyFilter()` | `set_event()` 空表 ID 存储空 filter |
| `SetEventClearRemovesFilterAndCache()` | `set_event(clear=true)` 清除 filter 和缓存 |
| `SetEventUpdateTableIdsReplacesFilter()` | 更新表 ID 替换旧 filter |
| `SetEventUnsupportedType()` | 拒绝不支持的事件类型（如 QUERY） |
| `GetReplicaInfoAppliesTableFilter()` | `get_replica_info()` 按表 ID 过滤副本 |
| `GetReplicaInfoBypassesFilterWhenTableIdUnknown()` | 表 ID 为 0 时返回所有副本 |

---

#### 2.2.2 `bvar_windowed_adder_test.cpp`

**位置：** `be/test/util/bvar_windowed_adder_test.cpp`

**测试目标：** 验证 `MBvarWindowedAdder` 工具类的核心功能——滑动窗口多维度指标累加器，用于 warmup 进度观测的 BE 端指标采集。

| 测试方法 | 验证内容 |
|----------|---------|
| `PutAndGetTotal` | 基本的 put + get_window_value 读写流程 |
| `UnknownDimensionReturnsZero` | 查询不存在的维度返回 0 |
| `InvalidWindowIndexReturnsZero` | 越界的 window_idx 返回 0 |
| `MultipleDimensions` | 多个 job_id 维度共存，list_dimensions 返回正确列表 |
| `ListDimensionsEmpty` | 未 put 时 list_dimensions 返回空列表 |
| `MultipleWindowSizes` | 3 个窗口大小（300s/1800s/3600s）正确创建，越界 index 返回 0 |
| `GetWindowValueByStringKey` | 通过字符串 key（而非 initializer_list）查询 |
| `EnsureWindowsIdempotent` | 同维度多次 put 不创建重复 Window 对象 |
| `MakeKeyComposite` | 多维度值（如 {"a","b"}）生成逗号分隔的复合 key |

---

### 2.3 进度观测单元测试

#### 2.3.1 `WarmUpStatsTest.java`

**位置：** `fe/fe-core/src/test/java/org/apache/doris/cloud/WarmUpStatsTest.java`

**测试目标：** 验证 warmup 进度观测的 FE 侧数据模型——`TableWarmUpWindowedStats`（单 BE 指标解析/合并）和 `JobWarmUpStats`（多 BE 聚合/间隙计算/JSON 序列化）。

**TableWarmUpWindowedStats 测试：**

| 测试方法 | 验证内容 |
|----------|---------|
| `testFromJsonComplete` | 完整 JSON（requested/finish/fail + timestamps）解析所有字段 |
| `testFromJsonMissingSections` | 部分 JSON（仅 requested）解析，缺失部分默认为 0 |
| `testFromJsonEmptyObject` | 空 JSON（仅 job_id）解析，所有值为 0 |
| `testMergeAddsCounts` | merge() 累加计数、取 max 时间戳 |

**JobWarmUpStats 测试：**

| 测试方法 | 验证内容 |
|----------|---------|
| `testMergeRequestedAccumulates` | mergeRequested() 累加源集群指标，max(lastTriggerTs) |
| `testMergeFinishedAccumulates` | mergeFinished() 累加目标集群 finish/fail 指标 |
| `testComputeGap` | computeGap() 计算 gap = requested - finished |
| `testComputeGapNegative` | 窗口时序偏差导致 finished > requested 时 gap 为负 |
| `testToJsonStringStructure` | toJsonString() JSON 结构包含 seg_num/seg_size/idx_num/idx_size/timestamps |
| `testToJsonStringZeroTimestamps` | 零值时间戳输出为空字符串 |
| `testHumanReadableSizeInJson` | 大小字段使用 ByteSizeValue 格式化（500b/1.5kb/1mb/1gb） |
| `testEndToEndSourceAndTargetAggregation` | 端到端：2 个源 BE + 1 个目标 BE → 聚合 → 计算 gap，验证全流程 |

---

## 三、测试覆盖总结

### 3.1 按层级统计

| 层级 | 测试类 | 测试方法数 | 测试重点 |
|------|--------|-----------|---------|
| FE 单元测试 | 8 | ~77 | SQL 解析、模式匹配、规则标准化、动态刷新、序列化、元数据、进度观测数据模型 |
| BE 单元测试 | 2 | 20 | 过滤器管理、副本过滤、事件处理、滑动窗口指标 |
| 回归测试 (Docker) | 7 | ~22 个子场景 | 端到端功能验证、bvar 指标链路、多集群交互 |
| **合计** | **17** | **~119** | |

### 3.2 按功能覆盖

| 功能点 | FE UT | BE UT | 回归测试 |
|--------|-------|-------|---------|
| INCLUDE 通配符匹配 (`*`) | ✅ | — | ✅ |
| 单字符通配符 (`?`) | ✅ | — | ✅ |
| EXCLUDE 优先级 | ✅ | — | ✅ |
| 多条 INCLUDE 并集 | ✅ | — | ✅ |
| 跨库匹配 | ✅ | — | ✅ |
| 规则标准化/去重 | ✅ | — | ✅ |
| 动态表变更追踪 (CREATE/DROP/RENAME) | ✅ | — | ✅ |
| SQL 语法解析 | ✅ | — | — |
| 语义校验（错误场景） | ✅ | — | ✅ |
| 序列化/反序列化 | ✅ | — | — |
| SHOW WARM UP JOB 输出 | ✅ | — | ✅ |
| 集群级与表级任务共存 | ✅ | — | ✅ |
| 任务取消与重建 | — | — | ✅ |
| 多目标集群同步 | — | — | ✅ |
| BE 过滤器管理 | — | ✅ | — |
| BE 副本过滤 | — | ✅ | — |
| BE 滑动窗口指标（MBvarWindowedAdder） | — | ✅ | — |
| FE 进度观测数据模型（JSON 解析/合并/聚合） | ✅ | — | — |
| FE 进度观测 gap 计算与 JSON 序列化 | ✅ | — | — |
| bvar 指标端到端验证 | — | — | ✅ |
| 正则元字符转义 | ✅ | — | — |
| `default_cluster:` 前缀处理 | ✅ | — | — |

### 3.3 运行方式

**FE 单元测试：**
```bash
./run-fe-ut.sh --run org.apache.doris.cloud.OnTablesFilterTest
./run-fe-ut.sh --run org.apache.doris.cloud.CloudWarmUpJobTableFilterTest
./run-fe-ut.sh --run org.apache.doris.cloud.CacheHotspotManagerTableFilterTest
./run-fe-ut.sh --run org.apache.doris.cloud.WarmUpClusterOnTablesParseTest
./run-fe-ut.sh --run org.apache.doris.persist.ModifyCloudWarmUpJobTest
./run-fe-ut.sh --run org.apache.doris.nereids.trees.plans.commands.WarmUpClusterCommandTest
./run-fe-ut.sh --run org.apache.doris.nereids.trees.plans.commands.ShowWarmUpCommandTest
./run-fe-ut.sh --run org.apache.doris.cloud.WarmUpStatsTest
```

**BE 单元测试：**
```bash
./run-be-ut.sh --run --filter=CloudWarmUpManagerFilterTest.*
./run-be-ut.sh --run --filter=MBvarWindowedAdderTest.*
```

**回归测试：**
```bash
# 运行全部 6 个 case
./run-regression-test.sh --run -d cloud_p0/cache/multi_cluster/warm_up/on_tables -runMode cloud

# 运行单个 case
./run-regression-test.sh --run -d cloud_p0/cache/multi_cluster/warm_up/on_tables \
  -s test_warm_up_event_on_tables_include -runMode cloud
```
