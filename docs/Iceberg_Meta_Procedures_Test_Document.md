# Iceberg Meta Procedures 提测文档

## 一、功能概述

本文档描述了 Doris 中实现的 5 个 Iceberg Meta Procedure 功能，这些功能基于 PR [#56257](https://github.com/apache/doris/pull/56257) 实现，用于管理 Iceberg 表的快照（Snapshot）和分支（Branch）操作。

### 1.1 相关概念

#### Snapshot（快照）
- **定义**：Iceberg 表在某个时间点的完整状态，包含该时间点所有数据文件的元数据信息
- **特点**：
  - 每次写入操作（INSERT、UPDATE、DELETE）都会创建一个新的快照
  - 快照是不可变的，一旦创建不能修改
  - 每个快照都有一个唯一的 `snapshot_id`（长整型）
  - 快照之间通过 `parent_id` 形成链式结构，可以追踪历史变更

#### Branch（分支）
- **定义**：指向特定快照的命名引用，类似于 Git 的分支概念
- **特点**：
  - 默认分支名为 `main`，指向表的当前快照
  - 可以创建多个分支，每个分支可以独立演进
  - 分支可以用于并行开发、实验性功能测试等场景
  - 分支可以快进（fast-forward）到其他分支的最新快照

#### Tag（标签）
- **定义**：指向特定快照的不可变命名引用，类似于 Git 的标签
- **特点**：
  - 用于标记重要的版本点（如发布版本）
  - 一旦创建，标签指向的快照不会改变
  - 与分支不同，标签不能更新

#### Reference（引用）
- **定义**：分支和标签的统称，用于引用特定的快照
- **用途**：可以通过引用名称来访问特定快照的数据

### 1.2 语法格式

所有 Iceberg Meta Procedure 都通过 `ALTER TABLE EXECUTE` 语法执行：

```sql
ALTER TABLE [catalog_name.][database_name.]table_name 
EXECUTE procedure_name 
(property_key = property_value, ...)
```

## 二、Procedure 功能详解

### 2.1 rollback_to_snapshot

#### 功能描述
将 Iceberg 表回滚到指定的历史快照，使该快照成为表的当前快照。

#### 参数说明
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| snapshot_id | BIGINT | 是 | 要回滚到的目标快照 ID（必须为正整数） |

#### 返回值
| 列名 | 类型 | 说明 |
|------|------|------|
| previous_snapshot_id | BIGINT | 回滚操作前的当前快照 ID |
| current_snapshot_id | BIGINT | 回滚后的当前快照 ID（即目标快照 ID） |

#### 使用示例
```sql
-- 回滚到快照 ID 为 123456789 的快照
ALTER TABLE iceberg_catalog.db.table 
EXECUTE rollback_to_snapshot("snapshot_id" = "123456789");
```

#### 注意事项
- 不支持分区和 WHERE 条件
- 如果目标快照不存在，会抛出异常
- 如果当前快照已经是目标快照，操作会成功但不会创建新快照

---

### 2.2 rollback_to_timestamp

#### 功能描述
将 Iceberg 表回滚到指定时间戳对应的快照，即回滚到该时间点或之前最近的快照。

#### 参数说明
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| timestamp | STRING | 是 | 目标时间戳，支持两种格式：<br>1. ISO 格式：`yyyy-MM-dd HH:mm:ss.SSS`<br>2. 毫秒时间戳：从 1970-01-01 00:00:00 UTC 开始的毫秒数 |

#### 返回值
| 列名 | 类型 | 说明 |
|------|------|------|
| previous_snapshot_id | BIGINT | 回滚操作前的当前快照 ID |
| current_snapshot_id | BIGINT | 回滚后的当前快照 ID（指定时间点的快照） |

#### 使用示例
```sql
-- 使用 ISO 格式时间戳
ALTER TABLE iceberg_catalog.db.table 
EXECUTE rollback_to_timestamp("timestamp" = "2024-01-01 10:00:00.000");

-- 使用毫秒时间戳
ALTER TABLE iceberg_catalog.db.table 
EXECUTE rollback_to_timestamp("timestamp" = "1704096000000");
```

#### 注意事项
- 不支持分区和 WHERE 条件
- 时间戳格式必须正确，否则会抛出异常
- 如果指定时间点没有快照，会回滚到该时间点之前最近的快照

---

### 2.3 set_current_snapshot

#### 功能描述
将 Iceberg 表的当前快照设置为指定的快照 ID 或引用（分支/标签）。

#### 参数说明
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| snapshot_id | BIGINT | 否* | 要设置为当前快照的快照 ID（必须为正整数） |
| ref | STRING | 否* | 要设置为当前快照的引用名称（分支或标签） |

> **注意**：`snapshot_id` 和 `ref` 参数互斥，必须且只能提供其中一个。

#### 返回值
| 列名 | 类型 | 说明 |
|------|------|------|
| previous_snapshot_id | BIGINT | 设置前的当前快照 ID |
| current_snapshot_id | BIGINT | 设置后的当前快照 ID |

#### 使用示例
```sql
-- 通过快照 ID 设置
ALTER TABLE iceberg_catalog.db.table 
EXECUTE set_current_snapshot("snapshot_id" = "123456789");

-- 通过分支引用设置
ALTER TABLE iceberg_catalog.db.table 
EXECUTE set_current_snapshot("ref" = "feature_branch");

-- 通过标签引用设置
ALTER TABLE iceberg_catalog.db.table 
EXECUTE set_current_snapshot("ref" = "v1.0.0");
```

#### 注意事项
- 不支持分区和 WHERE 条件
- `snapshot_id` 和 `ref` 不能同时提供
- 如果引用不存在，会抛出异常
- 如果当前快照已经是目标快照，操作会成功但不会创建新快照

---

### 2.4 cherrypick_snapshot

#### 功能描述
从指定的快照中挑选（cherry-pick）变更并应用到当前表状态，创建一个新的快照。这个操作不会删除或修改原始快照，而是将指定快照的变更合并到当前状态。

#### 参数说明
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| snapshot_id | BIGINT | 是 | 要 cherry-pick 的源快照 ID（必须为正整数） |

#### 返回值
| 列名 | 类型 | 说明 |
|------|------|------|
| source_snapshot_id | BIGINT | 被 cherry-pick 的源快照 ID |
| current_snapshot_id | BIGINT | cherry-pick 操作后新创建的当前快照 ID |

#### 使用示例
```sql
-- Cherry-pick 快照 ID 为 123456789 的变更
ALTER TABLE iceberg_catalog.db.table 
EXECUTE cherrypick_snapshot("snapshot_id" = "123456789");
```

#### 使用场景
- 从历史快照中恢复特定的数据变更
- 将某个分支的变更应用到主分支
- 选择性合并历史快照的变更

#### 注意事项
- 不支持分区和 WHERE 条件
- 如果源快照不存在，会抛出异常
- Cherry-pick 操作会创建一个新的快照，不会修改原始快照

---

### 2.5 fast_forward

#### 功能描述
将一个分支快进到另一个分支的最新快照，使源分支指向目标分支的当前快照。

#### 参数说明
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| branch | STRING | 是 | 要快进的分支名称（不能为空） |
| to | STRING | 是 | 目标分支名称，源分支将快进到该分支的最新快照（不能为空） |

#### 返回值
| 列名 | 类型 | 说明 |
|------|------|------|
| branch_updated | STRING | 被快进的分支名称 |
| previous_ref | BIGINT | 快进操作前分支指向的快照 ID（可能为 NULL） |
| updated_ref | BIGINT | 快进操作后分支指向的快照 ID |

#### 使用示例
```sql
-- 将 feature 分支快进到 main 分支的最新快照
ALTER TABLE iceberg_catalog.db.table 
EXECUTE fast_forward("branch" = "feature", "to" = "main");
```

#### 使用场景
- 将功能分支同步到主分支的最新状态
- 分支间的同步和合并操作
- 保持分支与主分支的一致性

#### 注意事项
- 不支持分区和 WHERE 条件
- 如果源分支或目标分支不存在，会抛出异常
- 如果源分支已经是目标分支的最新状态，操作仍会成功

---

## 三、测试用例设计

### 3.1 测试环境准备

#### 前置条件
1. 已创建 Iceberg Catalog
2. 已创建测试数据库
3. 测试表已创建并包含初始数据

#### 测试数据准备脚本
```sql
-- 创建测试表
CREATE TABLE test_db.test_table (
    id BIGINT,
    name STRING,
    value INT
) ENGINE=iceberg;

-- 插入初始数据，创建多个快照
INSERT INTO test_db.test_table VALUES (1, 'record1', 100);
INSERT INTO test_db.test_table VALUES (2, 'record2', 200);
INSERT INTO test_db.test_table VALUES (3, 'record3', 300);

-- 创建分支和标签
ALTER TABLE test_db.test_table CREATE BRANCH feature_branch;
INSERT INTO test_db.test_table VALUES (4, 'record4', 400);
ALTER TABLE test_db.test_table CREATE TAG v1.0.0;
INSERT INTO test_db.test_table VALUES (5, 'record5', 500);
```

---

### 3.2 rollback_to_snapshot 测试用例

#### TC-001: 基本回滚功能
**测试目的**：验证回滚到指定快照的基本功能

**测试步骤**：
1. 查询当前表数据，记录当前快照 ID
2. 查询所有快照列表，获取历史快照 ID
3. 执行回滚到历史快照
4. 验证表数据已回滚到目标快照状态
5. 验证返回值包含正确的快照 ID

**预期结果**：
- 表数据回滚到目标快照的状态
- 返回 `previous_snapshot_id` 和 `current_snapshot_id`
- 当前快照 ID 等于目标快照 ID

**测试 SQL**：
```sql
-- 查询快照列表
SELECT snapshot_id, committed_at FROM test_db.test_table$snapshots ORDER BY committed_at;

-- 执行回滚（假设快照 ID 为 123456789）
ALTER TABLE test_db.test_table 
EXECUTE rollback_to_snapshot("snapshot_id" = "123456789");

-- 验证数据
SELECT * FROM test_db.test_table ORDER BY id;
```

#### TC-002: 回滚到当前快照
**测试目的**：验证回滚到当前快照时的行为

**测试步骤**：
1. 记录当前快照 ID
2. 执行回滚到当前快照
3. 验证表状态未改变

**预期结果**：
- 操作成功，但表状态不变
- 返回值中的 `previous_snapshot_id` 和 `current_snapshot_id` 相同

#### TC-003: 回滚到不存在的快照
**测试目的**：验证参数校验和错误处理

**测试步骤**：
1. 使用不存在的快照 ID 执行回滚

**预期结果**：
- 抛出异常："Snapshot [snapshot_id] not found in table [table_name]"

#### TC-004: 参数校验 - 负数快照 ID
**测试目的**：验证参数校验

**测试步骤**：
1. 使用负数快照 ID 执行回滚

**预期结果**：
- 抛出异常："snapshot_id must be positive, got: -123"

#### TC-005: 参数校验 - 零值快照 ID
**测试目的**：验证参数校验

**测试步骤**：
1. 使用 0 作为快照 ID 执行回滚

**预期结果**：
- 抛出异常："snapshot_id must be positive, got: 0"

---

### 3.3 rollback_to_timestamp 测试用例

#### TC-006: 基本时间戳回滚功能
**测试目的**：验证使用 ISO 格式时间戳回滚

**测试步骤**：
1. 查询快照列表，记录各快照的时间戳
2. 使用 ISO 格式时间戳执行回滚
3. 验证表数据已回滚到目标时间点的快照

**预期结果**：
- 表回滚到指定时间点或之前最近的快照
- 返回正确的快照 ID

**测试 SQL**：
```sql
-- 查询快照时间戳
SELECT snapshot_id, committed_at FROM test_db.test_table$snapshots ORDER BY committed_at;

-- 执行时间戳回滚
ALTER TABLE test_db.test_table 
EXECUTE rollback_to_timestamp("timestamp" = "2024-01-01 10:00:00.000");
```

#### TC-007: 使用毫秒时间戳回滚
**测试目的**：验证使用毫秒时间戳格式

**测试步骤**：
1. 获取目标快照的毫秒时间戳
2. 使用毫秒时间戳执行回滚

**预期结果**：
- 回滚成功
- 表状态正确

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE rollback_to_timestamp("timestamp" = "1704096000000");
```

#### TC-008: 无效时间戳格式
**测试目的**：验证时间戳格式校验

**测试步骤**：
1. 使用无效格式的时间戳执行回滚

**预期结果**：
- 抛出异常："Invalid timestamp format. Expected ISO datetime (yyyy-MM-dd HH:mm:ss.SSS) or timestamp in milliseconds"

#### TC-009: 缺失时间戳参数
**测试目的**：验证必填参数校验

**测试步骤**：
1. 不提供 timestamp 参数执行回滚

**预期结果**：
- 抛出异常："Missing required argument: timestamp"

---

### 3.4 set_current_snapshot 测试用例

#### TC-010: 通过快照 ID 设置当前快照
**测试目的**：验证使用快照 ID 设置当前快照

**测试步骤**：
1. 记录当前快照 ID
2. 查询历史快照列表
3. 使用快照 ID 设置当前快照
4. 验证表状态已改变

**预期结果**：
- 当前快照成功设置为目标快照
- 表数据反映目标快照的状态

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE set_current_snapshot("snapshot_id" = "123456789");
```

#### TC-011: 通过分支引用设置当前快照
**测试目的**：验证使用分支引用设置当前快照

**测试步骤**：
1. 创建分支并记录分支指向的快照
2. 使用分支名称设置当前快照
3. 验证表状态与分支快照一致

**预期结果**：
- 当前快照设置为分支指向的快照
- 表数据与分支快照一致

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE set_current_snapshot("ref" = "feature_branch");
```

#### TC-012: 通过标签引用设置当前快照
**测试目的**：验证使用标签引用设置当前快照

**测试步骤**：
1. 创建标签
2. 使用标签名称设置当前快照
3. 验证表状态与标签快照一致

**预期结果**：
- 当前快照设置为标签指向的快照
- 表数据与标签快照一致

**测试 SQL**：
```sql
ALTER TABLE test_db.test_table 
EXECUTE set_current_snapshot("ref" = "v1.0.0");
```

#### TC-013: 同时提供 snapshot_id 和 ref
**测试目的**：验证参数互斥校验

**测试步骤**：
1. 同时提供 snapshot_id 和 ref 参数

**预期结果**：
- 抛出异常："snapshot_id and ref are mutually exclusive, only one can be provided"

#### TC-014: 既不提供 snapshot_id 也不提供 ref
**测试目的**：验证必填参数校验

**测试步骤**：
1. 不提供任何参数执行设置

**预期结果**：
- 抛出异常："Either snapshot_id or ref must be provided"

#### TC-015: 引用不存在
**测试目的**：验证引用存在性校验

**测试步骤**：
1. 使用不存在的分支或标签名称

**预期结果**：
- 抛出异常："Reference '[ref_name]' not found in table [table_name]"

---

### 3.5 cherrypick_snapshot 测试用例

#### TC-016: 基本 Cherry-pick 功能
**测试目的**：验证从历史快照 cherry-pick 变更

**测试步骤**：
1. 记录当前表状态和快照 ID
2. 回滚到更早的快照
3. 执行 cherry-pick 操作，从较新的快照中挑选变更
4. 验证 cherry-pick 后的表状态

**预期结果**：
- Cherry-pick 成功，创建新快照
- 表数据包含源快照的变更
- 返回源快照 ID 和新创建的当前快照 ID

**测试 SQL**：
```sql
-- 回滚到早期快照
ALTER TABLE test_db.test_table 
EXECUTE rollback_to_snapshot("snapshot_id" = "early_snapshot_id");

-- Cherry-pick 较新的快照
ALTER TABLE test_db.test_table 
EXECUTE cherrypick_snapshot("snapshot_id" = "newer_snapshot_id");

-- 验证结果
SELECT * FROM test_db.test_table ORDER BY id;
```

#### TC-017: Cherry-pick 不存在的快照
**测试目的**：验证错误处理

**测试步骤**：
1. 使用不存在的快照 ID 执行 cherry-pick

**预期结果**：
- 抛出异常："Snapshot not found in table"

#### TC-018: Cherry-pick 参数校验
**测试目的**：验证参数校验

**测试步骤**：
1. 使用负数或零值快照 ID

**预期结果**：
- 抛出异常："snapshot_id must be positive"

---

### 3.6 fast_forward 测试用例

#### TC-019: 基本 Fast-forward 功能
**测试目的**：验证分支快进功能

**测试步骤**：
1. 创建两个分支，使它们指向不同的快照
2. 在主分支上插入新数据
3. 执行 fast-forward，将功能分支快进到主分支
4. 验证功能分支已更新到主分支的最新快照

**预期结果**：
- Fast-forward 成功
- 功能分支指向主分支的最新快照
- 返回正确的分支名称和快照 ID

**测试 SQL**：
```sql
-- 创建分支
ALTER TABLE test_db.test_table CREATE BRANCH feature_branch;

-- 在主分支插入数据
INSERT INTO test_db.test_table VALUES (6, 'record6', 600);

-- 执行 fast-forward
ALTER TABLE test_db.test_table 
EXECUTE fast_forward("branch" = "feature_branch", "to" = "main");

-- 验证分支状态
SELECT * FROM test_db.test_table@branch(feature_branch) ORDER BY id;
```

#### TC-020: Fast-forward 到不存在的分支
**测试目的**：验证错误处理

**测试步骤**：
1. 使用不存在的分支名称执行 fast-forward

**预期结果**：
- 抛出异常，提示分支不存在

#### TC-021: Fast-forward 参数校验
**测试目的**：验证必填参数校验

**测试步骤**：
1. 缺失 branch 或 to 参数

**预期结果**：
- 抛出异常："Missing required argument: branch" 或 "Missing required argument: to"

#### TC-022: Fast-forward 空字符串参数
**测试目的**：验证参数非空校验

**测试步骤**：
1. 使用空字符串作为分支名称

**预期结果**：
- 抛出异常："branch cannot be empty" 或类似错误

---

### 3.7 综合测试用例

#### TC-023: 快照操作链式测试
**测试目的**：验证多个操作的组合使用

**测试步骤**：
1. 创建表并插入数据，创建多个快照
2. 创建分支
3. 在主分支插入数据
4. 回滚到历史快照
5. Cherry-pick 较新的快照
6. 设置当前快照为分支引用
7. Fast-forward 分支到主分支

**预期结果**：
- 所有操作按顺序成功执行
- 每个操作后表状态正确

#### TC-024: 并发操作测试
**测试目的**：验证并发场景下的行为

**测试步骤**：
1. 同时执行多个快照操作（如果支持）

**预期结果**：
- 操作按顺序执行或正确处理并发冲突

#### TC-025: 系统表查询验证
**测试目的**：验证操作后系统表数据正确

**测试步骤**：
1. 执行各种快照操作
2. 查询 `$snapshots` 系统表验证快照列表
3. 查询 `$refs` 系统表验证引用信息

**预期结果**：
- 系统表数据与操作结果一致

**测试 SQL**：
```sql
-- 查询快照列表
SELECT snapshot_id, parent_id, committed_at, summary 
FROM test_db.test_table$snapshots 
ORDER BY committed_at;

-- 查询引用信息
SELECT name, type, snapshot_id 
FROM test_db.test_table$refs 
ORDER BY snapshot_id;
```

---

### 3.8 边界和异常测试用例

#### TC-026: 空表操作
**测试目的**：验证空表上的操作行为

**测试步骤**：
1. 在空表上执行各种快照操作

**预期结果**：
- 空表上某些操作可能失败，应有明确的错误提示

#### TC-027: 超大快照 ID
**测试目的**：验证大数值处理

**测试步骤**：
1. 使用接近 Long.MAX_VALUE 的快照 ID

**预期结果**：
- 正确处理或给出明确的错误提示

#### TC-028: 特殊字符处理
**测试目的**：验证分支/标签名称中的特殊字符

**测试步骤**：
1. 创建包含特殊字符的分支名称
2. 使用该分支执行操作

**预期结果**：
- 正确处理或给出明确的错误提示

---

## 四、测试执行计划

### 4.1 测试环境
- Doris 版本：支持 Iceberg Meta Procedures 的版本
- Iceberg Catalog 类型：REST Catalog（推荐）或 HMS Catalog
- 测试数据：使用上述测试数据准备脚本创建

### 4.2 测试顺序建议
1. **基础功能测试**：TC-001, TC-006, TC-010, TC-016, TC-019
2. **参数校验测试**：TC-003, TC-004, TC-005, TC-008, TC-009, TC-013, TC-014, TC-015, TC-017, TC-018, TC-020, TC-021, TC-022
3. **边界场景测试**：TC-002, TC-007, TC-011, TC-012, TC-026, TC-027, TC-028
4. **综合测试**：TC-023, TC-024, TC-025

### 4.3 验收标准
- ✅ 所有基础功能测试用例通过
- ✅ 所有参数校验测试用例正确抛出预期异常
- ✅ 边界场景测试用例行为符合预期
- ✅ 综合测试用例验证操作链正确性
- ✅ 系统表数据与操作结果一致
- ✅ 错误信息清晰明确

---

## 五、参考文档

- [PR #56257](https://github.com/apache/doris/pull/56257) - Iceberg Meta Procedure 实现
- [Iceberg Procedures 文档](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Doris Iceberg Catalog 文档](https://doris.apache.org/docs/3.x/lakehouse/catalogs/iceberg-catalog#iceberg-table-actions)

---

## 六、附录

### 6.1 快速测试脚本示例

```sql
-- 完整测试流程示例
-- 1. 创建表
CREATE TABLE test_db.test_meta_procedures (
    id BIGINT,
    name STRING,
    value INT,
    ts TIMESTAMP
) ENGINE=iceberg;

-- 2. 插入数据创建快照
INSERT INTO test_db.test_meta_procedures VALUES 
(1, 'v1', 100, '2024-01-01 10:00:00'),
(2, 'v2', 200, '2024-01-02 11:00:00'),
(3, 'v3', 300, '2024-01-03 12:00:00');

-- 3. 创建分支和标签
ALTER TABLE test_db.test_meta_procedures CREATE BRANCH dev;
INSERT INTO test_db.test_meta_procedures VALUES (4, 'v4', 400, '2024-01-04 13:00:00');
ALTER TABLE test_db.test_meta_procedures CREATE TAG release_v1;

-- 4. 查询快照
SELECT snapshot_id, committed_at FROM test_db.test_meta_procedures$snapshots;

-- 5. 执行各种操作
ALTER TABLE test_db.test_meta_procedures EXECUTE rollback_to_snapshot("snapshot_id" = "...");
ALTER TABLE test_db.test_meta_procedures EXECUTE set_current_snapshot("ref" = "dev");
ALTER TABLE test_db.test_meta_procedures EXECUTE fast_forward("branch" = "dev", "to" = "main");
```

### 6.2 常见问题排查

1. **快照 ID 如何获取？**
   ```sql
   SELECT snapshot_id, committed_at FROM table_name$snapshots ORDER BY committed_at;
   ```

2. **如何查看分支和标签？**
   ```sql
   SELECT name, type, snapshot_id FROM table_name$refs;
   ```

3. **操作失败如何排查？**
   - 检查快照 ID 是否存在
   - 检查分支/标签名称是否正确
   - 检查参数格式是否符合要求
   - 查看错误信息中的详细提示

