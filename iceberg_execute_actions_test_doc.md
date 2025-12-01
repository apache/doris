# Iceberg Execute Actions 提测文档

## 一、背景

### 1.1 业务背景

随着数据湖架构在企业中的广泛应用，Apache Iceberg 作为新一代数据湖表格式，凭借其 ACID 事务、时间旅行（Time Travel）、分支管理（Branch/Tag）等核心能力，已成为数据湖存储的事实标准。Doris 作为高性能分析型数据库，通过 Iceberg Catalog 功能实现了对 Iceberg 表的查询和分析能力，但在表管理和维护操作方面仍存在能力缺口。

在实际生产环境中，用户需要对 Iceberg 表进行以下关键操作：

1. **快照管理**：当数据写入出现错误或需要回滚到历史状态时，需要快速回滚到指定快照或时间点
2. **分支管理**：在数据开发流程中，需要支持多分支并行开发，并通过 cherry-pick、fast-forward 等操作进行分支合并
3. **存储优化**：随着数据不断写入，表会产生大量小文件，影响查询性能，需要定期进行文件重写和快照清理
4. **元数据维护**：长期运行的表会积累大量历史快照和元数据，需要定期清理以降低存储成本和提升元数据访问效率

### 1.2 技术背景

Apache Iceberg 提供了丰富的 Procedure（存储过程）接口，用于执行表级别的管理和优化操作。在 Spark 生态中，这些操作通过 `CALL` 语句调用，例如：

```sql
CALL system.rollback_to_snapshot('db.table', 123456789);
CALL system.rewrite_data_files('db.table', 'target-file-size-bytes', '536870912');
```

然而，Doris 作为独立的分析型数据库，不依赖 Spark 运行时环境，因此无法直接使用 Spark 的 Procedure 实现。为了在 Doris 中提供完整的 Iceberg 表管理能力，需要：

1. **实现统一的 Execute Action 框架**：提供 `ALTER TABLE EXECUTE` 语法，支持通过 SQL 直接执行 Iceberg 表操作
2. **封装 Iceberg Core API**：基于 Iceberg Core API 重新实现各个 Procedure 的核心逻辑，避免对 Spark 的依赖
3. **支持参数化配置**：允许用户通过属性参数灵活配置操作行为，如文件大小、保留策略等

### 1.3 功能缺失

在实现 Iceberg Execute Actions 之前，Doris 缺少以下 Iceberg 表管理功能：

1. **快照管理功能**：无法在 Doris 中直接执行快照回滚、设置当前快照等操作
2. **分支管理功能**：不支持分支的 cherry-pick、fast-forward 等操作
3. **存储优化功能**：无法执行数据文件重写操作以优化存储布局
4. **元数据维护功能**：无法执行快照清理操作以降低存储成本和提升元数据访问效率

用户需要依赖 Spark 或其他外部工具来执行这些操作，增加了系统复杂度和运维成本。

### 1.4 与 Iceberg 生态的关系

Iceberg Execute Actions 功能是 Doris 与 Apache Iceberg 生态深度集成的关键组成部分：

1. **兼容 Iceberg 标准**：实现的功能与 Iceberg 官方 Procedure 语义保持一致，确保用户在不同工具间切换时行为一致
2. **基于 Iceberg Core API**：直接使用 Iceberg Core API 实现，不依赖 Spark 运行时，保证了实现的独立性和可维护性
3. **扩展 Doris 能力边界**：将 Doris 从"查询引擎"扩展为"查询+管理"一体化平台，减少用户对多工具链的依赖

### 1.5 用户价值

通过实现 Iceberg Execute Actions，为用户带来以下价值：

1. **统一操作界面**：在 Doris 中即可完成 Iceberg 表的查询、管理和优化，无需切换工具
2. **降低运维成本**：减少对 Spark 等外部工具的依赖，简化运维流程
3. **提升操作效率**：通过 SQL 直接执行表操作，比调用外部工具更便捷
4. **增强数据安全**：支持快照回滚和分支管理，提升数据操作的安全性和可控性
5. **优化存储成本**：支持文件重写和快照清理，帮助用户降低存储成本并提升查询性能

### 1.6 实现范围

本次实现涵盖了 7 个核心 Iceberg Execute Actions：

1. **rollback_to_snapshot**：回滚表到指定快照
2. **rollback_to_timestamp**：回滚表到指定时间点
3. **set_current_snapshot**：设置表的当前快照
4. **cherrypick_snapshot**：将指定快照合并到当前分支
5. **fast_forward**：将分支快进到指定快照
6. **expire_snapshots**：清理过期快照
7. **rewrite_data_files**：重写数据文件以优化存储布局

这些功能覆盖了 Iceberg 表管理的主要场景，为用户提供了完整的表生命周期管理能力。

---

## 二、实现方式

### 2.1 整体架构设计

Iceberg Execute Actions 采用分层架构设计，从 SQL 解析到 Iceberg API 调用，形成了清晰的职责分离：

```
┌─────────────────────────────────────────────────────────┐
│              SQL 语法层                                  │
│  ALTER TABLE table EXECUTE action("k"="v")             │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│          Nereids Parser (语法解析)                        │
│  解析 SQL 语句，生成 ExecuteActionCommand                 │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│       ExecuteActionCommand (命令执行)                     │
│  1. 验证表存在性和权限                                    │
│  2. 通过工厂创建 Action 实例                              │
│  3. 执行 Action 并返回结果                               │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│      ExecuteActionFactory (工厂模式)                      │
│  根据表类型路由到对应的 Action 工厂                       │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│   IcebergExecuteActionFactory (Iceberg 工厂)             │
│  根据 action 类型创建具体的 Iceberg Action 实例          │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│      BaseIcebergAction (基类)                            │
│  1. 参数注册和验证                                        │
│  2. 权限检查                                              │
│  3. 调用子类实现的具体逻辑                                │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│   具体 Action 实现 (如 IcebergRollbackToSnapshotAction)  │
│  调用 Iceberg Core API 执行具体操作                       │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│          Iceberg Core API                                │
│  Table.manageSnapshots(), RewriteDataFiles 等            │
└─────────────────────────────────────────────────────────┘
```

### 2.2 SQL 语法设计

Doris 采用 `ALTER TABLE EXECUTE` 语法来执行 Iceberg 表操作，语法格式如下：

```sql
ALTER TABLE [catalog_name.][database_name.]table_name 
EXECUTE action_name 
(property_key = property_value, ...)
[PARTITION (partition_name, ...)]
[WHERE condition]
```

**语法特点**：
- 使用标准的 `ALTER TABLE` 语句，保持与 Doris 其他 DDL 语法的一致性
- 支持通过属性参数传递配置，灵活支持不同 Action 的参数需求
- 可选的分区和 WHERE 条件支持，为未来扩展预留接口

**示例**：
```sql
回滚到指定快照
ALTER TABLE iceberg_db.my_table EXECUTE rollback_to_snapshot ("snapshot_id" = "123456789");

重写数据文件
ALTER TABLE iceberg_db.my_table EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "min-input-files" = "5"
);
```

### 2.3 命令执行流程

`ExecuteActionCommand` 负责整个执行流程的协调，主要步骤包括：

1. **表信息解析**：解析 catalog、database、table 信息
2. **表对象获取**：从 Catalog 中获取表对象，验证表存在性
3. **Action 创建**：通过工厂模式创建对应的 Action 实例
4. **权限验证**：检查用户是否有 ALTER 权限
5. **参数验证**：验证 Action 参数的有效性
6. **执行操作**：调用 Iceberg API 执行具体操作
7. **缓存失效**：操作成功后失效相关元数据缓存
8. **结果返回**：返回操作结果（如果有）

**关键代码流程**：
```java
public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
    // 1. 解析表信息
    tableNameInfo.analyze(ctx);
    
    // 2. 获取表对象
    TableIf table = database.getTableNullable(tableNameInfo.getTbl());
    
    // 3. 创建 Action 实例
    ExecuteAction action = ExecuteActionFactory.createAction(
        actionName, properties, partitionNamesInfo, whereCondition, table);
    
    // 4. 验证权限和参数
    action.validate(tableNameInfo, ctx.getCurrentUserIdentity());
    
    // 5. 执行操作
    ResultSet resultSet = action.execute(table);
    
    // 6. 返回结果
    if (resultSet != null) {
        executor.sendResultSet(resultSet);
    }
}
```

### 2.4 工厂模式设计

采用双重工厂模式，实现表类型和 Action 类型的解耦：

**工厂类的作用**：

工厂模式在 Execute Actions 框架中起到核心作用，主要职责包括：

1. **对象创建封装**：将 Action 实例的创建逻辑封装在工厂类中，调用方无需关心具体的创建细节，只需要提供表类型和 Action 类型即可获得对应的 Action 实例。

2. **类型解耦**：通过工厂模式实现了表类型（Iceberg、Hudi 等）与 Action 类型（rollback、rewrite 等）的解耦，使得系统可以灵活支持多种表引擎和多种操作类型。

3. **统一接口**：所有 Action 实例都通过统一的 `ExecuteAction` 接口返回，调用方可以以统一的方式处理不同类型的 Action。

4. **扩展性支持**：当需要添加新的表引擎或新的 Action 类型时，只需在对应的工厂类中注册即可，无需修改调用方代码，符合开闭原则。

5. **错误处理集中化**：在工厂类中集中处理不支持的 Action 类型或表类型，提供清晰的错误信息，提升用户体验。

#### 2.4.1 ExecuteActionFactory（通用工厂）

负责根据表类型路由到对应的表引擎工厂：

```java
public static ExecuteAction createAction(String actionType, ...) {
    if (table instanceof IcebergExternalTable) {
        return IcebergExecuteActionFactory.createAction(...);
    } else if (table instanceof ExternalTable) {
        // 未来支持其他外部表类型
    }
}
```

**设计优势**：
- 支持多表引擎扩展：未来可以支持 Hudi、Delta Lake 等其他表格式
- 类型安全：编译期即可确定表类型，避免运行时错误
- 职责清晰：通用工厂只负责路由，具体创建逻辑由引擎工厂负责

#### 2.4.2 IcebergExecuteActionFactory（Iceberg 工厂）

负责根据 Action 类型创建具体的 Iceberg Action 实例：

```java
public static ExecuteAction createAction(String actionType, ...) {
    switch (actionType.toLowerCase()) {
        case ROLLBACK_TO_SNAPSHOT:
            return new IcebergRollbackToSnapshotAction(...);
        case REWRITE_DATA_FILES:
            return new IcebergRewriteDataFilesAction(...);
        // ... 其他 Action
    }
}
```

**支持的 Action 类型**：
- `rollback_to_snapshot`：回滚到指定快照
- `rollback_to_timestamp`：回滚到指定时间点
- `set_current_snapshot`：设置当前快照
- `cherrypick_snapshot`：合并快照
- `fast_forward`：分支快进
- `expire_snapshots`：清理快照
- `rewrite_data_files`：重写数据文件

### 2.5 参数处理机制

采用 `NamedArguments` 机制进行参数注册、解析和验证，支持类型安全的参数处理：

#### 2.5.1 参数注册

每个 Action 在构造函数中通过 `registerIcebergArguments()` 注册所需参数：

```java
@Override
protected void registerIcebergArguments() {
    // 注册必需参数
    namedArguments.registerRequiredArgument(
        SNAPSHOT_ID,
        "Snapshot ID to rollback to",
        ArgumentParsers.positiveLong(SNAPSHOT_ID)
    );
    
    // 注册可选参数（带默认值）
    namedArguments.registerOptionalArgument(
        TARGET_FILE_SIZE_BYTES,
        "Target file size in bytes",
        536870912L,  // 默认值
        ArgumentParsers.positiveLong(TARGET_FILE_SIZE_BYTES)
    );
}
```

**参数类型支持**：
- `positiveLong`：正整数
- `positiveInt`：正整数（整数类型）
- `intRange`：整数范围
- `doubleRange`：浮点数范围
- `booleanValue`：布尔值
- `nonEmptyString`：非空字符串

#### 2.5.2 参数验证

参数验证分为两个层次：

1. **基础验证**：由 `NamedArguments` 自动完成，包括：
   - 必需参数是否存在
   - 参数类型是否正确
   - 数值范围是否合法

2. **业务验证**：由 `validateIcebergAction()` 完成，包括：
   - 参数之间的依赖关系
   - 业务逻辑约束
   - 分区和 WHERE 条件的支持情况

**示例**：
```java
@Override
protected void validateIcebergAction() throws UserException {
    // 验证文件大小参数的逻辑关系
    long targetFileSize = namedArguments.getLong(TARGET_FILE_SIZE_BYTES);
    if (minFileSizeBytes > maxFileSizeBytes) {
        throw new UserException("min-file-size-bytes must be <= max-file-size-bytes");
    }
    
    // 验证不支持分区
    validateNoPartitions();
}
```

### 2.6 Action 实现核心逻辑

各个 Action 基于 Iceberg Core API 实现，不依赖 Spark 运行时。下面详细描述每个 Action 的核心实现逻辑：

#### 2.6.1 快照管理操作

##### rollback_to_snapshot

**功能**：将表回滚到指定的快照 ID。

**核心实现逻辑**：
1. **参数获取**：从 `properties` 中获取必需的 `snapshot_id` 参数
2. **快照验证**：通过 `icebergTable.snapshot(targetSnapshotId)` 验证目标快照是否存在
3. **当前快照记录**：记录回滚前的当前快照 ID，用于返回结果
4. **执行回滚**：调用 `icebergTable.manageSnapshots().rollbackTo(targetSnapshotId).commit()` 执行回滚操作
5. **缓存失效**：操作成功后失效表级元数据缓存
6. **结果返回**：返回回滚前后的快照 ID

**关键代码**：
```java
Snapshot targetSnapshot = icebergTable.snapshot(targetSnapshotId);
if (targetSnapshot == null) {
    throw new UserException("Snapshot not found");
}
Snapshot previousSnapshot = icebergTable.currentSnapshot();
icebergTable.manageSnapshots().rollbackTo(targetSnapshotId).commit();
Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(table);
```

##### rollback_to_timestamp

**功能**：将表回滚到指定时间点的快照。

**核心实现逻辑**：
1. **时间戳解析**：支持两种格式的时间戳参数：
   - ISO 日期时间格式：`yyyy-MM-dd HH:mm:ss.SSS`
   - 毫秒时间戳：自 1970-01-01 以来的毫秒数
2. **时间戳转换**：将字符串时间戳转换为毫秒数
3. **执行回滚**：调用 `icebergTable.manageSnapshots().rollbackToTime(targetTimestamp).commit()` 回滚到指定时间点
4. **结果处理**：返回回滚前后的快照 ID

**关键代码**：
```java
long targetTimestamp = TimeUtils.msTimeStringToLong(timestampStr, TimeUtils.getTimeZone());
icebergTable.manageSnapshots().rollbackToTime(targetTimestamp).commit();
```

##### set_current_snapshot

**功能**：设置表的当前快照，支持通过快照 ID 或引用（branch/tag）设置。

**核心实现逻辑**：
1. **参数验证**：`snapshot_id` 和 `ref` 参数互斥，必须且仅能提供一个
2. **快照解析**：
   - 如果提供 `snapshot_id`：直接使用该快照 ID
   - 如果提供 `ref`：通过 `icebergTable.snapshot(ref)` 解析引用对应的快照 ID
3. **快照存在性验证**：验证目标快照是否存在
4. **执行设置**：调用 `icebergTable.manageSnapshots().setCurrentSnapshot(targetSnapshotId).commit()` 设置当前快照
5. **结果返回**：返回设置前后的快照 ID

**关键代码**：
```java
if (targetSnapshotId != null) {
    Snapshot targetSnapshot = icebergTable.snapshot(targetSnapshotId);
    icebergTable.manageSnapshots().setCurrentSnapshot(targetSnapshotId).commit();
} else if (ref != null) {
    Snapshot refSnapshot = icebergTable.snapshot(ref);
    icebergTable.manageSnapshots().setCurrentSnapshot(refSnapshot.snapshotId()).commit();
}
```

##### cherrypick_snapshot

**功能**：将指定快照的变更合并到当前表状态，创建一个新的快照。

**核心实现逻辑**：
1. **快照验证**：验证源快照是否存在
2. **执行合并**：调用 `icebergTable.manageSnapshots().cherrypick(sourceSnapshotId).commit()` 执行 cherry-pick 操作
3. **新快照获取**：操作完成后获取新创建的当前快照 ID
4. **结果返回**：返回源快照 ID 和新创建的快照 ID

**关键代码**：
```java
Snapshot targetSnapshot = icebergTable.snapshot(sourceSnapshotId);
if (targetSnapshot == null) {
    throw new UserException("Snapshot not found");
}
icebergTable.manageSnapshots().cherrypick(sourceSnapshotId).commit();
Snapshot currentSnapshot = icebergTable.currentSnapshot();
```

##### fast_forward

**功能**：将一个分支快进到另一个分支的最新快照。

**核心实现逻辑**：
1. **参数获取**：获取源分支名（`branch`）和目标分支名（`to`）
2. **分支快照记录**：记录源分支在快进前的快照 ID
3. **执行快进**：调用 `icebergTable.manageSnapshots().fastForwardBranch(sourceBranch, desBranch).commit()` 执行快进操作
4. **结果获取**：获取快进后源分支的新快照 ID
5. **结果返回**：返回分支名、快进前后的快照 ID

**关键代码**：
```java
Long snapshotBefore = icebergTable.snapshot(sourceBranch) != null 
    ? icebergTable.snapshot(sourceBranch).snapshotId() : null;
icebergTable.manageSnapshots().fastForwardBranch(sourceBranch, desBranch).commit();
long snapshotAfter = icebergTable.snapshot(sourceBranch).snapshotId();
```

##### expire_snapshots

**功能**：清理过期的快照及其关联的数据文件，释放存储空间。

**核心实现逻辑**：
1. **参数解析**：支持多个清理策略参数：
   - `older_than`：清理指定时间之前的快照（支持 ISO 日期时间或毫秒时间戳）
   - `retain_last`：保留最近的 N 个快照
   - `snapshot_ids`：指定要清理的快照 ID 列表
   - `max_concurrent_deletes`：并发删除的线程池大小
   - `clean_expired_metadata`：是否清理过期的元数据
2. **参数验证**：至少需要提供 `older_than` 或 `retain_last` 之一
3. **执行清理**：调用 Iceberg 的 `RemoveSnapshots` API 执行清理操作（当前实现中，核心逻辑待完成）
4. **结果统计**：返回清理的快照数量、删除的文件数量等统计信息

**当前状态**：框架和参数验证已实现，核心清理逻辑待完成。

#### 2.6.2 文件重写操作

##### rewrite_data_files

**目标**：通过拆分大文件、合并小文件、清理删除文件等方式，优化 Iceberg 表的数据文件布局和读取性能。

###### 组件与职责
1. **参数解析层**（`IcebergRewriteDataFilesAction`）  
   负责解析 SQL 中传入的参数，计算默认值，完成合法性校验，并构造 `RewriteDataFilePlanner.Parameters`。

2. **计划生成层**（`RewriteDataFilePlanner`）  
   扫描系统表/快照信息，筛选出需要被重写的文件，生成 `RewriteDataGroup` 列表。

3. **执行层**（`RewriteDataFileExecutor`）  
   并发执行每个 `RewriteDataGroup`，生成新的数据文件并提交 Iceberg 事务，最终返回 `RewriteResult`。

###### 参数处理逻辑

| 参数 | 是否必填 | 默认值 / 兜底策略 | 说明 |
| --- | --- | --- | --- |
| `target-file-size-bytes` | 是 | 用户显式传入 | 生成新文件的理想大小，也是其它参数推导的基准。 |
| `min-file-size-bytes` | 否 | `0.75 * target-file-size-bytes` | 过滤过小文件，避免频繁 rewite 微小文件。 |
| `max-file-size-bytes` | 否 | `1.8 * target-file-size-bytes` | 过滤过大的文件，触发拆分。 |
| `min-input-files` | 否 | Planner 自动兜底（≥2） | 单次重写至少包含的输入文件数量，防止频繁小任务。 |
| `delete-file-threshold` | 否 | 依据 SQL/默认 0 | position delete 文件数超过阈值即触发重写。 |
| `delete-ratio-threshold` | 否 | 依据 SQL/默认 0 | delete 文件占比达到阈值时触发重写。 |
| `rewrite-all` | 否 | `false` | 为 true 时跳过择优策略，对候选文件全部重写。 |
| `max-file-group-size-bytes` | 否 | Planner 根据目标大小推导 | 限制单个分组的输入数据量，避免单次任务过大。 |
| `output-spec-id` | 否 | 当前表的 `specId` | 允许在重写时写入新的 partition spec，兼容分区演进。 |

###### 规划与执行流程

```
┌─────────────────────────────────────────────────────────┐
│          规划阶段（RewriteDataFilePlanner）              │
│                                                          │
│  1. 扫描当前快照                                         │
│     读取所有 data files 及其统计信息（大小、记录数、     │
│     删除文件）                                           │
│                                                          │
│  2. 文件级筛选                                           │
│     - 文件大小不在 [min-file-size, max-file-size] 区间  │
│     - delete 文件数量超过 delete-file-threshold          │
│     - delete 文件占比超过 delete-ratio-threshold         │
│     - rewrite-all=true 时直接全部纳入                    │
│                                                          │
│  3. 分区/规格分组                                        │
│     按 specId + partitionKey 进行初步分桶，保证同组     │
│     文件具备相同的写入上下文                             │
│                                                          │
│  4. 组装 RewriteDataGroup                                │
│     - 累计文件数量/大小，直到满足 min-input-files        │
│       或接近 max-file-group-size-bytes                   │
│     - 记录每组的输入文件列表、删除文件列表及累积统计     │
│                                                          │
│  5. Where 条件裁剪（如果 SQL 带 WHERE）                  │
│     仅对符合过滤条件的分区/文件执行重写                  │
│                                                          │
│  输出：Iterable<RewriteDataGroup>                        │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│        执行阶段（RewriteDataFileExecutor）               │
│                                                          │
│  1. 上下文初始化                                         │
│     获取 ConnectContext，准备任务线程池                  │
│                                                          │
│  2. 并发执行                                             │
│     对每个 RewriteDataGroup 创建任务：                   │
│     - 读取原始文件，应用 Iceberg 的分区/排序策略         │
│     - 按 target-file-size-bytes 控制写入新文件           │
│     - 如果组内存在 delete files，在 rewrite 过程中       │
│       重新生成已过滤删除记录的新文件                     │
│                                                          │
│  3. 事务提交                                             │
│     每个组生成的文件通过 Iceberg Action API 提交，       │
│     替换原始文件并删除旧的 delete files                  │
│                                                          │
│  4. 结果聚合                                             │
│     RewriteResult 汇总所有组的统计数据：                 │
│     - rewritten_data_files_count（重写文件数）           │
│     - added_data_files_count（新增文件数）               │
│     - rewritten_bytes_count（重写字节数）               │
│     - removed_delete_files_count（移除的 delete files）  │
│                                                          │
│  输出：RewriteResult                                     │
└─────────────────────────────────────────────────────────┘
```

###### 保护机制与兜底
- **空表跳过**：若当前快照为空，直接返回 `0` 统计，避免无意义操作；
- **参数兜底**：min/max 文件大小为 0 时自动推导默认值，避免使用者漏填；
- **异常捕获**：任意阶段出现异常都会抛出 `UserException` 并记录日志，确保前端能获取错误信息；
- **缓存失效**：提交成功后会失效 Iceberg 表缓存，保证后续查询能感知最新文件布局。

###### 结果输出
Action 返回 4 个指标（`rewritten_data_files_count`, `added_data_files_count`, `rewritten_bytes_count`, `removed_delete_files_count`），可用于衡量一次重写的效果。

###### 关键代码引用
```java
// 构建参数
RewriteDataFilePlanner.Parameters parameters = buildRewriteParameters();

// 计划阶段
RewriteDataFilePlanner planner = new RewriteDataFilePlanner(parameters);
Iterable<RewriteDataGroup> groups = planner.planAndOrganizeTasks(icebergTable);

// 并发执行
RewriteDataFileExecutor executor = new RewriteDataFileExecutor((IcebergExternalTable) table, connectContext);
RewriteResult result = executor.executeGroupsConcurrently(Lists.newArrayList(groups), targetFileSizeBytes);

return result.toStringList();
```

###### 实践建议
- 搭配 `README_ICEBERG_REWRITE_ANALYSIS.md` 中的 SQL 分析工具，提前评估重写影响；
- 优先在低峰期或小范围分区测试，确保参数设置合理；
- 重写后可通过系统表验证新文件大小和分布是否达到预期。

### 2.7 结果返回机制

Action 可以返回操作结果，通过 `ResultSet` 返回给用户：

#### 2.7.1 结果 Schema 定义

每个 Action 通过 `getResultSchema()` 定义返回结果的列结构：

```java
@Override
protected List<Column> getResultSchema() {
    return Lists.newArrayList(
        new Column("previous_snapshot_id", Type.BIGINT, false, 
                   "ID of the snapshot before rollback"),
        new Column("current_snapshot_id", Type.BIGINT, false,
                   "ID of the snapshot after rollback")
    );
}
```

#### 2.7.2 结果数据返回

在 `executeAction()` 中返回结果数据：

```java
@Override
protected List<String> executeAction(TableIf table) throws UserException {
    // 执行操作...
    return Lists.newArrayList(
        String.valueOf(previousSnapshotId),
        String.valueOf(currentSnapshotId)
    );
}
```

**返回结果示例**：
```
+---------------------+-------------------+
| previous_snapshot_id| current_snapshot_id|
+---------------------+-------------------+
|          1234567890 |         1234567891|
+---------------------+-------------------+
```

### 2.8 缓存失效机制

执行 Iceberg 表操作后，需要失效相关的元数据缓存，确保后续查询能够获取到最新的表状态：

```java
// 操作成功后失效表缓存
Env.getCurrentEnv().getExtMetaCacheMgr()
    .invalidateTableCache((ExternalTable) table);
```

**缓存失效时机**：
- 快照操作（rollback、set_current、cherrypick）后：失效表级缓存
- 文件重写操作后：失效表级和分区级缓存
- 快照清理操作后：失效表级缓存

### 2.9 错误处理

采用分层错误处理机制：

1. **SQL 解析错误**：由 Nereids Parser 捕获，返回语法错误
2. **参数验证错误**：由 `NamedArguments` 捕获，返回参数错误
3. **权限验证错误**：由 `BaseExecuteAction.validate()` 捕获，返回权限错误
4. **Iceberg API 错误**：由 `executeAction()` 捕获，包装为 `UserException` 返回

**错误信息示例**：
```sql
参数错误
ALTER TABLE t EXECUTE rollback_to_snapshot ("snapshot_id" = "-1");
错误：snapshot_id must be positive, got: -1

快照不存在
ALTER TABLE t EXECUTE rollback_to_snapshot ("snapshot_id" = "999999");
错误：Snapshot 999999 not found in table t
```

### 2.10 扩展性设计

架构设计充分考虑了未来扩展需求：

1. **多表引擎支持**：通过工厂模式，可以轻松添加 Hudi、Delta Lake 等表引擎的支持
2. **新 Action 添加**：只需在 `IcebergExecuteActionFactory` 中注册新的 Action 类
3. **参数扩展**：通过 `NamedArguments` 机制，可以灵活添加新的参数类型和验证规则
4. **结果格式扩展**：通过 `getResultSchema()` 可以自定义返回结果的格式

这种设计使得系统具有良好的可维护性和可扩展性，为未来功能迭代提供了坚实的基础。

---

## 三、测试用例覆盖

### 3.1 快照管理操作测试用例

#### 3.1.1 rollback_to_snapshot 测试用例

##### TC-001: 正常回滚到历史快照
**测试目的**：验证能够成功回滚到指定的历史快照

**前置条件**：
- 表 `test_rollback` 已创建，包含多个快照（snapshot1, snapshot2, snapshot3）
- 当前快照为 snapshot3

**测试步骤**：
1. 查询当前快照 ID 和表数据
2. 执行回滚到 snapshot1
3. 查询回滚后的快照 ID 和表数据
4. 验证返回结果

**预期结果**：
- 回滚操作成功执行
- 当前快照 ID 变为 snapshot1
- 表数据恢复到 snapshot1 的状态
- 返回结果包含 previous_snapshot_id 和 current_snapshot_id

**测试 SQL**：
```sql
查询当前状态
SELECT * FROM test_db.test_rollback;

执行回滚
ALTER TABLE test_db.test_rollback 
EXECUTE rollback_to_snapshot ("snapshot_id" = "<snapshot1_id>");

验证回滚结果
SELECT * FROM test_db.test_rollback;
```

##### TC-002: 回滚到当前快照（无变化）
**测试目的**：验证回滚到当前快照时的处理逻辑

**前置条件**：
- 表当前快照为 snapshot2

**测试步骤**：
1. 执行回滚到 snapshot2（当前快照）
2. 验证表状态未改变

**预期结果**：
- 操作成功执行，不抛出异常
- 表状态保持不变
- 返回结果中 previous_snapshot_id 和 current_snapshot_id 相同

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback 
EXECUTE rollback_to_snapshot ("snapshot_id" = "<current_snapshot_id>");
```

##### TC-003: 参数验证 - snapshot_id 为空
**测试目的**：验证必需参数缺失时的错误处理

**测试步骤**：
1. 不提供 snapshot_id 参数执行回滚

**预期结果**：
- 抛出异常："Missing required argument: snapshot_id"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback 
EXECUTE rollback_to_snapshot ();
```

##### TC-004: 参数验证 - snapshot_id 为负数
**测试目的**：验证 snapshot_id 参数类型和范围校验

**测试步骤**：
1. 提供负数作为 snapshot_id

**预期结果**：
- 抛出异常："snapshot_id must be positive, got: -1"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback 
EXECUTE rollback_to_snapshot ("snapshot_id" = "-1");
```

##### TC-005: 参数验证 - snapshot_id 为 0
**测试目的**：验证 snapshot_id 必须为正整数的校验

**测试步骤**：
1. 提供 0 作为 snapshot_id

**预期结果**：
- 抛出异常："snapshot_id must be positive, got: 0"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback 
EXECUTE rollback_to_snapshot ("snapshot_id" = "0");
```

##### TC-006: 业务验证 - 快照不存在
**测试目的**：验证目标快照不存在时的错误处理

**测试步骤**：
1. 使用不存在的快照 ID 执行回滚

**预期结果**：
- 抛出异常："Snapshot <snapshot_id> not found in table <table_name>"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback 
EXECUTE rollback_to_snapshot ("snapshot_id" = "999999999");
```

##### TC-007: 不支持分区和 WHERE 条件
**测试目的**：验证 rollback_to_snapshot 不支持分区和 WHERE 条件

**测试步骤**：
1. 提供 PARTITION 子句执行回滚
2. 提供 WHERE 子句执行回滚

**预期结果**：
- 抛出异常："Action 'rollback_to_snapshot' does not support partition specification"
- 抛出异常："Action 'rollback_to_snapshot' does not support WHERE condition"

**测试 SQL**：
```sql
测试分区
ALTER TABLE test_db.test_rollback 
EXECUTE rollback_to_snapshot ("snapshot_id" = "123") 
PARTITION (p1);

测试 WHERE 条件
ALTER TABLE test_db.test_rollback 
EXECUTE rollback_to_snapshot ("snapshot_id" = "123") 
WHERE id > 10;
```

#### 3.1.2 rollback_to_timestamp 测试用例

##### TC-008: 正常回滚到指定时间点（ISO 格式）
**测试目的**：验证使用 ISO 日期时间格式回滚到指定时间点

**前置条件**：
- 表 `test_rollback_timestamp` 包含多个时间点的快照

**测试步骤**：
1. 使用 ISO 格式时间戳执行回滚
2. 验证回滚到指定时间点或之前最近的快照

**预期结果**：
- 回滚操作成功
- 当前快照为指定时间点或之前最近的快照
- 返回结果包含 previous_snapshot_id 和 current_snapshot_id

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback_timestamp 
EXECUTE rollback_to_timestamp ("timestamp" = "2025-01-02 11:00:00.000");
```

##### TC-009: 正常回滚到指定时间点（毫秒时间戳）
**测试目的**：验证使用毫秒时间戳格式回滚

**测试步骤**：
1. 使用毫秒时间戳执行回滚

**预期结果**：
- 回滚操作成功
- 时间戳正确解析并回滚到对应快照

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback_timestamp 
EXECUTE rollback_to_timestamp ("timestamp" = "1735804800000");
```

##### TC-010: 参数验证 - timestamp 为空
**测试目的**：验证必需参数缺失时的错误处理

**测试步骤**：
1. 不提供 timestamp 参数执行回滚

**预期结果**：
- 抛出异常："Missing required argument: timestamp"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback_timestamp 
EXECUTE rollback_to_timestamp ();
```

##### TC-011: 参数验证 - timestamp 格式错误
**测试目的**：验证时间戳格式校验

**测试步骤**：
1. 提供错误格式的时间戳

**预期结果**：
- 抛出异常："Invalid timestamp format. Expected ISO datetime (yyyy-MM-dd HH:mm:ss.SSS) or timestamp in milliseconds: <invalid_format>"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback_timestamp 
EXECUTE rollback_to_timestamp ("timestamp" = "invalid-format");
```

##### TC-012: 参数验证 - timestamp 为负数
**测试目的**：验证毫秒时间戳不能为负数

**测试步骤**：
1. 提供负数作为毫秒时间戳

**预期结果**：
- 抛出异常："Timestamp must be non-negative: -1"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback_timestamp 
EXECUTE rollback_to_timestamp ("timestamp" = "-1");
```

##### TC-013: 边界场景 - 回滚到最早快照之前的时间
**测试目的**：验证回滚到表创建之前的时间点

**测试步骤**：
1. 使用早于所有快照的时间戳执行回滚

**预期结果**：
- 操作可能失败或回滚到最早的快照（取决于 Iceberg 实现）

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rollback_timestamp 
EXECUTE rollback_to_timestamp ("timestamp" = "2020-01-01 00:00:00.000");
```

#### 3.1.3 set_current_snapshot 测试用例

##### TC-014: 通过快照 ID 设置当前快照
**测试目的**：验证使用快照 ID 设置当前快照

**前置条件**：
- 表 `test_current_snapshot` 包含多个快照

**测试步骤**：
1. 记录当前快照 ID
2. 使用历史快照 ID 设置当前快照
3. 验证表状态已改变

**预期结果**：
- 当前快照成功设置为目标快照
- 表数据反映目标快照的状态
- 返回结果包含 previous_snapshot_id 和 current_snapshot_id

**测试 SQL**：
```sql
ALTER TABLE test_db.test_current_snapshot 
EXECUTE set_current_snapshot ("snapshot_id" = "<target_snapshot_id>");
```

##### TC-015: 通过分支引用设置当前快照
**测试目的**：验证使用分支引用设置当前快照

**前置条件**：
- 表已创建分支 `dev_branch`

**测试步骤**：
1. 使用分支名称设置当前快照
2. 验证表状态与分支快照一致

**预期结果**：
- 当前快照设置为分支指向的快照
- 表数据与分支快照一致

**测试 SQL**：
```sql
ALTER TABLE test_db.test_current_snapshot 
EXECUTE set_current_snapshot ("ref" = "dev_branch");
```

##### TC-016: 通过标签引用设置当前快照
**测试目的**：验证使用标签引用设置当前快照

**前置条件**：
- 表已创建标签 `dev_tag`

**测试步骤**：
1. 使用标签名称设置当前快照
2. 验证表状态与标签快照一致

**预期结果**：
- 当前快照设置为标签指向的快照
- 表数据与标签快照一致

**测试 SQL**：
```sql
ALTER TABLE test_db.test_current_snapshot 
EXECUTE set_current_snapshot ("ref" = "dev_tag");
```

##### TC-017: 参数验证 - 同时提供 snapshot_id 和 ref
**测试目的**：验证参数互斥校验

**测试步骤**：
1. 同时提供 snapshot_id 和 ref 参数

**预期结果**：
- 抛出异常："snapshot_id and ref are mutually exclusive, only one can be provided"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_current_snapshot 
EXECUTE set_current_snapshot ("snapshot_id" = "123", "ref" = "main");
```

##### TC-018: 参数验证 - 既不提供 snapshot_id 也不提供 ref
**测试目的**：验证必填参数校验

**测试步骤**：
1. 不提供任何参数执行设置

**预期结果**：
- 抛出异常："Either snapshot_id or ref must be provided"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_current_snapshot 
EXECUTE set_current_snapshot ();
```

##### TC-019: 业务验证 - 快照 ID 不存在
**测试目的**：验证目标快照不存在时的错误处理

**测试步骤**：
1. 使用不存在的快照 ID 执行设置

**预期结果**：
- 抛出异常："Snapshot <snapshot_id> not found in table <table_name>"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_current_snapshot 
EXECUTE set_current_snapshot ("snapshot_id" = "999999999");
```

##### TC-020: 业务验证 - 引用不存在
**测试目的**：验证分支或标签不存在时的错误处理

**测试步骤**：
1. 使用不存在的分支或标签名称执行设置

**预期结果**：
- 抛出异常："Reference '<ref_name>' not found in table <table_name>"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_current_snapshot 
EXECUTE set_current_snapshot ("ref" = "non_existent_branch");
```

##### TC-021: 边界场景 - 设置到当前快照
**测试目的**：验证设置到当前快照时的处理

**测试步骤**：
1. 使用当前快照 ID 执行设置

**预期结果**：
- 操作成功，不创建新快照
- 返回结果中 previous_snapshot_id 和 current_snapshot_id 相同

**测试 SQL**：
```sql
ALTER TABLE test_db.test_current_snapshot 
EXECUTE set_current_snapshot ("snapshot_id" = "<current_snapshot_id>");
```

#### 3.1.4 cherrypick_snapshot 测试用例

##### TC-022: 正常合并快照
**测试目的**：验证能够成功将指定快照的变更合并到当前表状态

**前置条件**：
- 表 `test_cherrypick` 包含多个快照（snapshot1, snapshot2, snapshot3）
- 当前快照为 snapshot3

**测试步骤**：
1. 记录当前快照 ID
2. 执行 cherry-pick 操作，合并 snapshot1 的变更
3. 验证新快照已创建
4. 验证表数据包含合并后的变更

**预期结果**：
- Cherry-pick 操作成功
- 创建新的快照，包含源快照的变更
- 返回结果包含 source_snapshot_id 和 current_snapshot_id

**测试 SQL**：
```sql
ALTER TABLE test_db.test_cherrypick 
EXECUTE cherrypick_snapshot ("snapshot_id" = "<source_snapshot_id>");
```

##### TC-023: 参数验证 - snapshot_id 为空
**测试目的**：验证必需参数缺失时的错误处理

**测试步骤**：
1. 不提供 snapshot_id 参数执行 cherry-pick

**预期结果**：
- 抛出异常："Missing required argument: snapshot_id"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_cherrypick 
EXECUTE cherrypick_snapshot ();
```

##### TC-024: 业务验证 - 源快照不存在
**测试目的**：验证源快照不存在时的错误处理

**测试步骤**：
1. 使用不存在的快照 ID 执行 cherry-pick

**预期结果**：
- 抛出异常："Snapshot not found in table"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_cherrypick 
EXECUTE cherrypick_snapshot ("snapshot_id" = "999999999");
```

##### TC-025: 边界场景 - Cherry-pick 当前快照
**测试目的**：验证 cherry-pick 当前快照时的处理

**测试步骤**：
1. 对当前快照执行 cherry-pick

**预期结果**：
- 操作可能成功但不会产生新的变更（取决于 Iceberg 实现）

**测试 SQL**：
```sql
ALTER TABLE test_db.test_cherrypick 
EXECUTE cherrypick_snapshot ("snapshot_id" = "<current_snapshot_id>");
```

#### 3.1.5 fast_forward 测试用例

##### TC-026: 正常分支快进
**测试目的**：验证能够成功将一个分支快进到另一个分支的最新快照

**前置条件**：
- 表已创建分支 `feature_branch` 和 `main` 分支
- 两个分支指向不同的快照

**测试步骤**：
1. 记录源分支的当前快照 ID
2. 执行 fast-forward 操作，将 feature_branch 快进到 main 分支
3. 验证源分支已更新为目标分支的快照

**预期结果**：
- Fast-forward 操作成功
- 源分支的快照 ID 更新为目标分支的快照 ID
- 返回结果包含 branch_updated、previous_ref 和 updated_ref

**测试 SQL**：
```sql
ALTER TABLE test_db.test_fast_forward 
EXECUTE fast_forward ("branch" = "feature_branch", "to" = "main");
```

##### TC-027: 参数验证 - branch 参数缺失
**测试目的**：验证必需参数缺失时的错误处理

**测试步骤**：
1. 不提供 branch 参数执行 fast-forward

**预期结果**：
- 抛出异常："Missing required argument: branch"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_fast_forward 
EXECUTE fast_forward ("to" = "main");
```

##### TC-028: 参数验证 - to 参数缺失
**测试目的**：验证必需参数缺失时的错误处理

**测试步骤**：
1. 不提供 to 参数执行 fast-forward

**预期结果**：
- 抛出异常："Missing required argument: to"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_fast_forward 
EXECUTE fast_forward ("branch" = "feature_branch");
```

##### TC-029: 业务验证 - 源分支不存在
**测试目的**：验证源分支不存在时的错误处理

**测试步骤**：
1. 使用不存在的分支名称执行 fast-forward

**预期结果**：
- 抛出异常或操作失败（取决于 Iceberg 实现）

**测试 SQL**：
```sql
ALTER TABLE test_db.test_fast_forward 
EXECUTE fast_forward ("branch" = "non_existent_branch", "to" = "main");
```

##### TC-030: 业务验证 - 目标分支不存在
**测试目的**：验证目标分支不存在时的错误处理

**测试步骤**：
1. 使用不存在的目标分支名称执行 fast-forward

**预期结果**：
- 抛出异常或操作失败（取决于 Iceberg 实现）

**测试 SQL**：
```sql
ALTER TABLE test_db.test_fast_forward 
EXECUTE fast_forward ("branch" = "feature_branch", "to" = "non_existent_branch");
```

##### TC-031: 边界场景 - 快进到相同快照
**测试目的**：验证源分支和目标分支已指向相同快照时的处理

**测试步骤**：
1. 当两个分支已指向相同快照时执行 fast-forward

**预期结果**：
- 操作成功，但不会产生新的变更

**测试 SQL**：
```sql
假设两个分支已指向相同快照
ALTER TABLE test_db.test_fast_forward 
EXECUTE fast_forward ("branch" = "feature_branch", "to" = "main");
```

#### 3.1.6 expire_snapshots 测试用例

##### TC-032: 通过 older_than 清理快照
**测试目的**：验证使用 older_than 参数清理过期快照

**前置条件**：
- 表包含多个历史快照

**测试步骤**：
1. 使用 older_than 参数执行清理
2. 验证指定时间之前的快照已被清理

**预期结果**：
- 清理操作成功（当前实现中，核心逻辑待完成）
- 返回清理统计信息

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE expire_snapshots ("older_than" = "2024-01-01T00:00:00");
```

##### TC-033: 通过 retain_last 清理快照
**测试目的**：验证使用 retain_last 参数保留最近的 N 个快照

**测试步骤**：
1. 使用 retain_last 参数执行清理
2. 验证只保留了最近的 N 个快照

**预期结果**：
- 清理操作成功（当前实现中，核心逻辑待完成）
- 保留了最近的 N 个快照

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE expire_snapshots ("retain_last" = "5");
```

##### TC-034: 参数验证 - older_than 和 retain_last 都缺失
**测试目的**：验证至少需要提供一个清理策略参数

**测试步骤**：
1. 不提供 older_than 和 retain_last 参数执行清理

**预期结果**：
- 抛出异常："At least one of 'older_than' or 'retain_last' must be specified"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE expire_snapshots ();
```

##### TC-035: 参数验证 - older_than 格式错误
**测试目的**：验证 older_than 时间戳格式校验

**测试步骤**：
1. 提供错误格式的 older_than 参数

**预期结果**：
- 抛出异常："Invalid older_than format. Expected ISO datetime (yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: <invalid_format>"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE expire_snapshots ("older_than" = "invalid-format");
```

##### TC-036: 参数验证 - retain_last 小于 1
**测试目的**：验证 retain_last 必须至少为 1

**测试步骤**：
1. 提供小于 1 的 retain_last 值

**预期结果**：
- 抛出异常："retain_last must be at least 1"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE expire_snapshots ("retain_last" = "0");
```

##### TC-037: 组合参数 - older_than 和 retain_last 同时使用
**测试目的**：验证可以同时使用多个清理策略

**测试步骤**：
1. 同时提供 older_than 和 retain_last 参数

**预期结果**：
- 清理操作成功，同时应用两个策略（当前实现中，核心逻辑待完成）

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE expire_snapshots (
    "older_than" = "2024-01-01T00:00:00", 
    "retain_last" = "5"
);
```

#### 3.1.7 通用测试用例

##### TC-038: Action 名称大小写不敏感
**测试目的**：验证 Action 名称支持大小写不敏感

**测试步骤**：
1. 使用大写、小写、混合大小写的 Action 名称执行操作

**预期结果**：
- 所有格式的 Action 名称都能正确识别和执行

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE ROLLBACK_TO_SNAPSHOT ("snapshot_id" = "123");

ALTER TABLE test_db.test_table 
EXECUTE Rollback_To_Snapshot ("snapshot_id" = "123");
```

##### TC-039: 参数值前后空格处理
**测试目的**：验证参数值的前后空格处理

**测试步骤**：
1. 提供包含前后空格的参数值

**预期结果**：
- 参数值应正确解析（可能需要 trim 处理）

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE rollback_to_snapshot ("snapshot_id" = "  123456789  ");
```

##### TC-040: 并发执行测试
**测试目的**：验证多个快照操作并发执行时的正确性

**测试步骤**：
1. 并发执行多个快照操作（如回滚、设置当前快照等）

**预期结果**：
- 操作应正确执行，不出现数据不一致
- 或者正确检测并发冲突并抛出异常

**测试说明**：
- 需要多线程或并发测试框架支持

### 3.2 文件重写操作测试用例

#### 3.2.1 rewrite_data_files 测试用例

##### TC-041: 正常重写数据文件（基本参数）
**测试目的**：验证使用基本参数成功执行数据文件重写

**前置条件**：
- 表 `test_rewrite` 已创建，包含多个数据文件（大小不均或包含 delete 文件）

**测试步骤**：
1. 查询重写前的文件数量和大小分布
2. 执行重写操作，指定 target-file-size-bytes
3. 查询重写后的文件数量和大小分布
4. 验证返回结果

**预期结果**：
- 重写操作成功执行
- 文件大小更接近目标大小，分布更均匀
- 返回结果包含 rewritten_data_files_count、added_data_files_count、rewritten_bytes_count、removed_delete_files_count

**测试 SQL**：
```sql
查询重写前状态
SELECT * FROM test_db.test_rewrite;

执行重写
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "536870912");

验证重写结果
SELECT * FROM test_db.test_rewrite;
```

##### TC-042: 使用完整参数集重写
**测试目的**：验证使用所有可选参数执行重写操作

**测试步骤**：
1. 提供所有可选参数执行重写

**预期结果**：
- 重写操作成功执行
- 所有参数正确应用

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "min-file-size-bytes" = "402653184",
    "max-file-size-bytes" = "966367641",
    "min-input-files" = "5",
    "delete-file-threshold" = "10",
    "delete-ratio-threshold" = "0.5",
    "rewrite-all" = "false",
    "max-file-group-size-bytes" = "2147483648",
    "output-spec-id" = "0"
);
```

##### TC-043: 参数验证 - target-file-size-bytes 缺失
**测试目的**：验证必填参数 target-file-size-bytes 缺失时的错误处理

**测试步骤**：
1. 不提供 target-file-size-bytes 参数执行重写

**预期结果**：
- 抛出异常："Missing required argument: target-file-size-bytes"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ();
```

##### TC-044: 参数验证 - target-file-size-bytes 为 0 或负数
**测试目的**：验证 target-file-size-bytes 必须为正整数

**测试步骤**：
1. 提供 0 或负数作为 target-file-size-bytes

**预期结果**：
- 抛出异常："target-file-size-bytes must be positive, got: 0" 或类似错误

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "0");

ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "-1");
```

##### TC-045: 参数验证 - min-file-size-bytes 大于 max-file-size-bytes
**测试目的**：验证文件大小范围参数的逻辑校验

**测试步骤**：
1. 提供 min-file-size-bytes 大于 max-file-size-bytes 的参数

**预期结果**：
- 抛出异常："min-file-size-bytes must be less than or equal to max-file-size-bytes"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "min-file-size-bytes" = "1073741824",
    "max-file-size-bytes" = "536870912"
);
```

##### TC-046: 参数验证 - min-input-files 小于 1
**测试目的**：验证 min-input-files 必须至少为 1

**测试步骤**：
1. 提供小于 1 的 min-input-files 值

**预期结果**：
- 抛出异常："min-input-files must be at least 1"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "min-input-files" = "0"
);
```

##### TC-047: 参数验证 - delete-ratio-threshold 超出范围
**测试目的**：验证 delete-ratio-threshold 必须在 [0, 1] 范围内

**测试步骤**：
1. 提供小于 0 或大于 1 的 delete-ratio-threshold 值

**预期结果**：
- 抛出异常："delete-ratio-threshold must be between 0 and 1"

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "delete-ratio-threshold" = "1.5"
);
```

##### TC-048: 参数验证 - rewrite-all 布尔值格式
**测试目的**：验证 rewrite-all 参数支持布尔值格式

**测试步骤**：
1. 使用不同的布尔值格式（true/false, TRUE/FALSE, 1/0）

**预期结果**：
- 所有有效的布尔值格式都能正确解析

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "rewrite-all" = "true"
);

ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "rewrite-all" = "false"
);
```

##### TC-049: 默认值验证 - 仅提供 target-file-size-bytes
**测试目的**：验证仅提供必填参数时，其他参数使用默认值

**前置条件**：
- 表包含需要重写的文件

**测试步骤**：
1. 仅提供 target-file-size-bytes 执行重写
2. 验证 min-file-size-bytes 和 max-file-size-bytes 使用默认值（0.75 和 1.8 倍）

**预期结果**：
- 重写操作成功执行
- 默认值正确应用

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "536870912");
```

##### TC-050: WHERE 条件支持
**测试目的**：验证 rewrite_data_files 支持 WHERE 条件，仅重写符合条件的分区/文件

**前置条件**：
- 表包含多个分区

**测试步骤**：
1. 使用 WHERE 条件执行重写，限制重写范围
2. 验证只有符合条件的分区/文件被重写

**预期结果**：
- 重写操作成功执行
- 只有符合 WHERE 条件的分区/文件被重写

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "536870912")
WHERE dt = '2024-01-01';
```

##### TC-051: 空表处理
**测试目的**：验证对空表执行重写操作的处理

**前置条件**：
- 表 `test_rewrite_empty` 为空表（无数据文件）

**测试步骤**：
1. 对空表执行重写操作

**预期结果**：
- 操作成功执行，不抛出异常
- 返回结果中所有统计值为 0

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite_empty 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "536870912");
```

##### TC-052: 重写包含 delete 文件的数据
**测试目的**：验证重写包含 position delete 文件的数据文件

**前置条件**：
- 表包含数据文件和对应的 delete 文件

**测试步骤**：
1. 设置 delete-file-threshold 或 delete-ratio-threshold 触发重写
2. 执行重写操作
3. 验证 delete 文件被正确处理

**预期结果**：
- 重写操作成功执行
- delete 文件被过滤，生成干净的数据文件
- removed_delete_files_count 大于 0

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "delete-file-threshold" = "5"
);
```

##### TC-053: rewrite-all 强制重写所有文件
**测试目的**：验证 rewrite-all=true 时跳过筛选策略，重写所有文件

**前置条件**：
- 表包含多个数据文件

**测试步骤**：
1. 设置 rewrite-all=true 执行重写
2. 验证所有文件都被重写

**预期结果**：
- 重写操作成功执行
- 所有符合条件的文件都被重写，不受大小筛选限制

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "rewrite-all" = "true"
);
```

##### TC-054: 分区演进支持（output-spec-id）
**测试目的**：验证在重写时使用新的 partition spec

**前置条件**：
- 表支持分区演进，存在多个 partition spec

**测试步骤**：
1. 使用 output-spec-id 参数指定新的 partition spec
2. 执行重写操作

**预期结果**：
- 重写操作成功执行
- 新文件使用指定的 partition spec

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "output-spec-id" = "1"
);
```

##### TC-055: 大文件拆分
**测试目的**：验证重写操作能够拆分过大的文件

**前置条件**：
- 表包含大于 max-file-size-bytes 的文件

**测试步骤**：
1. 设置合适的 max-file-size-bytes
2. 执行重写操作
3. 验证大文件被拆分为多个小文件

**预期结果**：
- 重写操作成功执行
- 大文件被拆分为多个接近 target-file-size-bytes 的文件

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "max-file-size-bytes" = "1073741824"
);
```

##### TC-056: 小文件合并
**测试目的**：验证重写操作能够合并过小的文件

**前置条件**：
- 表包含多个小于 min-file-size-bytes 的小文件

**测试步骤**：
1. 设置合适的 min-file-size-bytes
2. 执行重写操作
3. 验证小文件被合并

**预期结果**：
- 重写操作成功执行
- 小文件被合并为接近 target-file-size-bytes 的文件

**测试 SQL**：
```sql
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "min-file-size-bytes" = "402653184"
);
```

##### TC-057: 结果验证 - 统计信息准确性
**测试目的**：验证返回的统计信息准确性

**前置条件**：
- 表包含需要重写的文件

**测试步骤**：
1. 记录重写前的文件数量和大小
2. 执行重写操作
3. 记录重写后的文件数量和大小
4. 对比返回的统计信息与实际变化

**预期结果**：
- 返回的统计信息与实际变化一致
- rewritten_data_files_count、added_data_files_count、rewritten_bytes_count 准确

**测试 SQL**：
```sql
查询重写前
SELECT COUNT(*) as file_count, SUM(file_size_in_bytes) as total_size 
FROM test_db.test_rewrite$files;

执行重写
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "536870912");

查询重写后
SELECT COUNT(*) as file_count, SUM(file_size_in_bytes) as total_size 
FROM test_db.test_rewrite$files;
```

##### TC-058: 缓存失效验证
**测试目的**：验证重写操作后缓存正确失效

**测试步骤**：
1. 执行重写操作
2. 立即查询表，验证能获取到最新的文件布局

**预期结果**：
- 重写后查询能立即看到新的文件布局
- 缓存已正确失效

**测试 SQL**：
```sql
执行重写
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "536870912");

立即查询验证
SELECT * FROM test_db.test_rewrite$files;
```

##### TC-059: 异常处理 - 表不存在
**测试目的**：验证对不存在的表执行重写的错误处理

**测试步骤**：
1. 对不存在的表执行重写操作

**预期结果**：
- 抛出异常："Table not found: <table_name>"

**测试 SQL**：
```sql
ALTER TABLE test_db.non_existent_table 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "536870912");
```

##### TC-060: 异常处理 - 权限验证
**测试目的**：验证无 ALTER 权限时执行重写的错误处理

**测试步骤**：
1. 使用无 ALTER 权限的用户执行重写操作

**预期结果**：
- 抛出权限异常："Access denied for user <user> to table <table_name>"

**测试 SQL**：
```sql
-- 使用无权限用户执行
ALTER TABLE test_db.test_rewrite 
EXECUTE rewrite_data_files ("target-file-size-bytes" = "536870912");
```

