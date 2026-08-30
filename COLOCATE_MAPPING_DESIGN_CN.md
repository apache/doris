# COLOCATE MAPPING Constraint 设计文档

## 文档范围

本文描述 `COLOCATE MAPPING Constraint`（同分布映射约束）的用途、整体设计、模块划分、查询与 DDL 流程、生命周期行为、兼容策略、运维约束和测试范围。

本功能是 FE 侧的优化器与元数据能力，不新增 BE 算子，也不修改 FE-BE 执行协议。

## 术语说明

| 术语 | 含义 |
|---|---|
| `COLOCATE MAPPING Constraint` | 用户声明的一条“业务决定列可以确定物理分桶列”的约束。优化器可以用它证明某些 Join 无需 Shuffle。 |
| `determinant`（决定列） | Mapping 左侧的业务列或有序列组。它不要求唯一，但相同 determinant 值必须始终对应相同目标分桶键值。 |
| `mapping`（映射） | 一条 determinant 到目标分桶列位置的关系。持久化对象是 `DistributionMappingConstraint`，查询期对象是 `DistributionMapping`。 |
| `mapping ID`（映射标识） | 跨表匹配同一业务映射语义的标识。匹配还要求 determinant 数量和目标分桶位置相同。 |
| Constraint name（约束名） | 单表内用于 ADD、DROP 和 SHOW 的名称，不承担跨表语义匹配。 |
| `NOT ENFORCED` | Doris 不扫描或校验数据是否满足映射，正确性由用户保证。错误声明可能使优化器错误省略 Shuffle，并产生错误查询结果。 |
| Distribution key（分桶键） | `DISTRIBUTED BY HASH(...)` 中按顺序参与 Hash 分桶的列，列顺序属于分布语义。 |
| Target distribution position（目标分桶位置） | Mapping 所覆盖的分桶列在有序分桶键中的下标。最终证明必须覆盖全部位置。 |
| Natural distribution（自然分布） | 数据从 OLAP Scan 读取时保留的存储层 Bucket 布局。经过 Exchange 后不再是自然分布。 |
| Natural mapping proof（自然分布映射证明） | FE 内部的 `NaturalDistributionMappingSpec`。它记录物理表、所选索引、分区、Bucket 位置和可见 determinant，但不能用于构造 Exchange。 |
| Stable column unique ID（稳定列唯一 ID） | 列的持久化身份，用于识别同名列是否仍是原列。旧表没有稳定 ID 时，使用 base schema version 做更保守的替代保护。 |
| Table-local metadata（表内元数据） | Mapping 的完整 JSON 快照存放在 `OlapTable` 持有的 `TableProperty.properties` 内部保留键中并跟随物理表对象，而不是存放在按表名索引的全局 `ConstraintManager.constraintsMap` 中。 |
| Rolling-upgrade gate（滚动升级门禁） | ADD 和 Restore 前，所有已注册 FE 必须上报与当前 FE 完全一致的 `version-shortHash`。查询期版本不一致或未知时不使用 mapping，并回退普通规划。 |

## 功能概述

两张表使用相同的 Hash 分桶并处于稳定的 Colocate Group 时，Doris 原本要求 Join 等值条件直接覆盖全部分桶列。例如两表都按 `tenant_id` 分桶，通常需要：

```sql
ON orders.tenant_id = users.tenant_id
```

如果业务保证 `user_id -> tenant_id`，那么按 `user_id` Join 的匹配行也一定落在对应 Bucket：

```sql
ON orders.user_id = users.user_id
```

本功能允许用户显式声明该事实：

```sql
ALTER TABLE orders
ADD CONSTRAINT orders_user_mapping
COLOCATE MAPPING tenant_by_user (user_id)
DETERMINES DISTRIBUTION KEY (tenant_id)
NOT ENFORCED;
```

当两侧表声明兼容 mapping 且所有证明条件成立时，Nereids 可以选择已有的 `COLOCATE` Hash Join；否则仍使用原有 Shuffle 或 Broadcast 方案。

功能默认关闭：

```sql
SET enable_colocate_mapping_constraint = true;
```

### 代码组成与占比

以下数字按当前工作树相对 `upstream/master` merge-base 的 `git diff --numstat` 统计，并将最新的持久化与 Aggregate 屏障测试计入。一个文件只按其主要职责归类，因此比例用于说明修改重心，不代表复杂度或风险。

| 类别 | 文件数 | 增加/删除行 | 变更行占比 | 生产代码占比 |
|---|---:|---:|---:|---:|
| 核心功能逻辑 | 21 | 1,279 / 95 | 25.3% | 72.1% |
| 元数据与生命周期兼容 | 8 | 521 / 11 | 9.8% | 27.9% |
| UT、回归用例与结果 | 16 | 3,496 / 25 | 64.9% | 不适用 |
| 合计 | 45 | 5,296 / 131 | 100% | 不适用 |

核心逻辑主要位于：

- 语法与命令：`DorisLexer.g4`、`DorisParser.g4`、`LogicalPlanBuilder.java`、`Constraint.java`、`AddConstraintCommand.java`、`DropConstraintCommand.java`、`ShowConstraintsCommand.java`。
- 约束模型：`DistributionMappingConstraint.java` 和 catalog `Constraint.java`。
- Scan 与属性：`LogicalOlapScanToPhysicalOlapScan.java`、`DistributionMapping.java`、`DistributionSpecHash.java`、`NaturalDistributionMappingSpec.java`、`PhysicalProperties.java`。
- 传播与 Join 证明：`PhysicalHashAggregate.java`、`ChildOutputPropertyDeriver.java`、`RequestPropertyDeriver.java`、`ChildrenPropertiesRegulator.java`、`CostAndEnforcerJob.java`、`JoinUtils.java`。
- 开关：`SessionVariable.java`。

元数据与生命周期兼容主要位于：

- 表内存储和统一访问：`TableProperty.java`、`ConstraintManager.java`。
- Journal 与 Replay：`ModifyTablePropertyOperationLog.java`、`EditLog.java`、`Env.java`。
- DDL 防护与恢复：`SchemaChangeHandler.java`、`RestoreJob.java`。
- FE 版本可见性：`Frontend.java`。

与旧的分散补丁方案相比，当前设计不再修改外部 Catalog、HMS event、`RefreshManager`、MTMV rename、回收站或 Replace/Swap 的专用路径。生命周期正确性主要由“mapping 跟随物理表对象”这一条规则提供。

## 1. 功能用途

### 1.1 适用场景

- 多张内部 OLAP 表属于同一稳定 Colocate Group。
- 表使用兼容的 Hash 分桶布局。
- 查询经常使用业务键 Join，而业务键稳定确定物理分桶键。
- 用户能够从数据生产、约束校验或离线审计层面保证 mapping 的真实性。

### 1.2 支持范围

- 单列或复合 determinant。
- 一个 mapping 覆盖一个或多个按顺序排列的分桶位置。
- 多个 mapping 联合覆盖复合分桶键。
- 直接分桶列等值条件与 mapping 证明共同覆盖分桶键。
- 保持自然 Bucket 布局的 Project 和受限的普通非 DISTINCT Aggregate。
- Project 中的直接 Slot alias，以及不截断、Hash 值不变的字符类型 widening cast。
- 证明失败时回退到已有合法 Join 分布方案。

### 1.3 明确不支持

- 外部 Catalog、HMS 表、`RemoteOlapTable` 和临时表。
- 非 Hash 分桶表。
- 自动校验 `NOT ENFORCED` 数据不变量。
- 表达式 determinant、多跳映射推导或 mapping 闭包推导。
- 使用 mapping 构造 Exchange、Shuffle 或 Bucket Shuffle。
- 在 Union、Intersect、Except、Repeat/Grouping Sets 或已发生重分布的路径上传播自然 mapping 证明。
- 跨越 DISTINCT 聚合函数、MultiDistinct phase 或纯去重 Aggregate 传播 mapping 证明；这些查询仍可执行，但依赖证明跨越该边界的上层规划会回退。
- CREATE TABLE LIKE 或 CTAS 自动复制 mapping。

## 2. 整体设计

### 2.1 核心原则

1. **Mapping 属于物理表对象**：元数据以完整快照存放在 `OlapTable` 持有的 `TableProperty.properties` 内部保留键中，Rename、Drop/Recover、Replace/Swap 等操作无需维护第二套按名称索引。
2. **证明而非执行能力**：Mapping 只证明现有 Bucket 局部性，执行仍复用已有 Colocate Hash Join。
3. **双重保守校验**：每个 Join child 必须满足不可强制构造的 mapping request，最终还要重新校验跨表 mapping、等值条件和 Colocate Group。
4. **无法证明即不用**：普通证明失败、mapping 与 schema 不兼容或集群版本不一致时不构造 mapping proof，查询回退普通规划；元数据修改与 Restore 仍严格失败。
5. **生命周期规则集中化**：通过表内所有权、schema identity binding 和统一版本门禁解决一类问题，不在每个 Rename、Recover 或外部事件入口增加补丁。
6. **向后可读且可保留**：完整快照使用旧 FE 已支持的 table-property map 和 journal envelope；旧 FE 会把内部保留键当作不透明属性 replay 并写入 checkpoint，但不理解或使用 mapping，因此使用阶段仍由版本门禁阻止。

### 2.2 元数据模型

`DistributionMappingConstraint` 持久化以下内容：

- 单表约束名、跨表 mapping ID。
- 有序 determinant 列名和目标分桶列名。
- 创建时的 base schema version。
- determinant 与目标列的 stable unique ID。
- determinant 与目标列的类型签名。

创建时将用户声明绑定到当前表结构。规划和 Restore 时重新检查：

- 表仍为 Hash 分桶。
- 目标列仍是当前分桶列的有序子集。
- 列名、类型和 stable unique ID 仍匹配。
- 若列没有稳定 unique ID，则 base schema version 必须完全相同。

最后一条会让旧表上的无关 schema change 也使 mapping 暂时不可用，这是为了避免同名列被删除重建后误绑定。用户需要 DROP 并重新 ADD mapping。

### 2.3 持久化模型

- 持久化源：`TableProperty.properties["__distribution_mapping_constraints"]` 保存按约束名稳定排序的完整 JSON 快照；运行期 mapping map 由该属性反序列化得到，不单独持久化。
- ADD/DROP journal：每次操作都重写完整快照，并复用 `OP_MODIFY_TABLE_PROPERTIES` 的标准 properties 字段；删除最后一条 mapping 时显式写入 `[]`。
- 旧 FE replay/checkpoint：旧代码把该内部键当作不透明 table property 合并并继续序列化，因此能够保留 ADD 或 DROP 后的最新快照，但不能理解、展示或使用 mapping。
- 新 FE replay：按 table ID 找到 OLAP 表，在表写锁下应用完整快照并重建派生 mapping map。
- Binlog：支持该功能的 FE 不把 mapping snapshot 发布为普通 table-property binlog。

### 2.4 查询期证明模型

```text
Olap Scan
  -> 校验 FE 版本与 mapping/schema 绑定
  -> 把持久化 constraint 转换为 DistributionMapping
  -> 生成 NATURAL hash property + NaturalDistributionMappingSpec
  -> Project/普通非 DISTINCT Aggregate 保守重映射 proof
  -> DISTINCT/MultiDistinct/纯去重 Aggregate 丢弃 proof
  -> Join 生成 COLOCATE_MAPPING_REQUIRE 候选
  -> 两侧 satisfy + 最终跨表校验
  -> 复用现有 COLOCATE Hash Join
```

`NaturalDistributionMappingSpec` 独立记录隐藏的物理 Bucket 事实，即使 Project 或普通非 DISTINCT Aggregate 不再输出原分桶列，也不伪造可执行的 Hash distribution。它只服务 mapping-based Colocate proof，不能被 enforcer 转成 Exchange。

## 3. 模块设计

### 3.1 SQL 与命令层

语法形式：

```sql
ALTER TABLE <table>
ADD CONSTRAINT <constraint_name>
COLOCATE MAPPING <mapping_id> (<determinants>)
DETERMINES DISTRIBUTION KEY (<distribution_columns>)
NOT ENFORCED;
```

命令层完成权限检查、列解析、对象身份复核和 ADD/DROP/SHOW 分发。Mapping 仅接受内部、非临时 OLAP 表。

### 3.2 ConstraintManager 与锁

`ConstraintManager` 是访问入口，但 mapping 不进入其全局 `constraintsMap`。它负责：

- 统一校验和表内 map 读写。
- 防止 mapping 与既有 PK/FK/UNIQUE 使用相同约束名。
- 创建 schema identity binding。
- 统一执行版本门禁和规划期兼容检查。
- 将 SHOW 视图合并为“全局约束 + 表内 mapping”。

ADD/DROP 的锁顺序为：数据库读锁 -> 表写锁 -> ConstraintManager 锁。表内变更与 journal submit 在锁内完成，`EditLogItem.await()` 在所有元数据锁之外执行，避免等待持久化时长期持锁。

### 3.3 Schema Change

直接引用 mapping 的 determinant 或目标分桶列时：

- DROP COLUMN 被拒绝。
- RENAME COLUMN 被拒绝。
- MODIFY COLUMN 被拒绝。
- Hash 转 Random distribution 被拒绝。

仅修改 Rollup 的同名列不改变 base-table mapping，因此不被 mapping 防护误拦截。

如果旧 FE replay 了当前版本不知道的 schema 操作，导致上述前置拦截未执行，新 FE 仍会在规划期通过 identity binding 检测不兼容，忽略该表的 mapping，记录限频告警并回退普通规划，而不会继续使用陈旧证明。

### 3.4 Scan 与属性传播

Scan 只有在 session 开关开启、表存在 mapping 且 schema/版本校验通过时才构建 mapping proof。只有所有 determinant Slot 可见的 mapping 才进入物理属性。

Project 支持：

- 直接输出 Slot。
- `Alias(Slot)`。
- `Alias(Cast(Slot))`，但仅限字符类型不截断 widening，保证值和 Hash 字节不变。

任意一般表达式、narrowing cast 或 determinant 缺失都会丢弃对应 mapping。

Aggregate 仅在以下条件同时成立时传播：

- 不是 DISTINCT 聚合函数、MultiDistinct phase 或纯去重 Aggregate。
- child 仍携带自然 Bucket proof，没有 Exchange 截断。
- 不是 Repeat/Grouping Sets 来源。
- Group By 全部是直接 Slot。
- Group By 中的直接分桶列与完整 determinant 共同覆盖所有分桶位置。
- 输出保留父 Join 所需 determinant。

DISTINCT、MultiDistinct 和纯去重 Aggregate 会同时阻断向 child 请求 mapping property，并清除向 parent 输出的 mapping proof，避免错误传播只对原始行布局成立的证明。该限制只关闭 Mapping 优化，不影响查询本身执行。

### 3.5 Join 证明

Mapping candidate 使用不可 enforce 的 `COLOCATE_MAPPING_REQUIRE`：若 child 自身不能满足，优化器不能插入 Exchange“制造”成功，而是直接丢弃候选。

最终校验要求：

- 两侧分桶键数量相同。
- 两侧处于同一稳定 Colocate Group，或满足同表同索引单分区例外。
- Join hash conjunct 全部是 Slot-to-Slot 等值条件。
- 对应 mapping ID、determinant 数量和目标分桶位置一致。
- determinant 按声明顺序逐列由 Join 等值条件连接。
- 直接等值与 mapping 联合覆盖全部分桶位置。

任何条件不满足都不会产生 mapping-based Colocate Join。

### 3.6 Cache 与执行层

- 只有 Scan 实际构建出 mapping proof 时，当前 SQL cache context 才标记为不支持缓存。
- 不使用 mapping 的查询不额外关闭 SQL cache。
- 不新增 MTMV rename 或 rewrite-cache 生命周期补丁，因为 mapping 不按表名建立全局引用。
- BE 执行仍使用现有 Colocate Hash Join，不新增 runtime column、Thrift 字段或 BE 状态。

## 4. 关键执行流程

### 4.1 查询流程

1. Session 未开启功能时，不读取 mapping proof，行为与原版本一致。
2. Scan 发现 mapping 后检查所有 FE 的 `version-shortHash`；版本不一致或未知时忽略该表的 mapping 并回退普通规划。
3. Scan 校验表结构与 mapping binding；任一 mapping 不兼容时忽略该表的全部 mapping、记录限频告警并回退普通规划。
4. 只有版本和 schema 校验均通过时，Scan 才创建自然分布 proof，并仅携带当前输出可见的 determinant。
5. Project 和满足条件的普通非 DISTINCT Aggregate 按规则传播；DISTINCT/MultiDistinct/纯去重 Aggregate、Exchange 和 Set Operation 等边界丢弃 proof。
6. Join 额外生成 mapping candidate，同时保留原有 Shuffle/Broadcast candidate。
7. 两侧 child property 和最终跨表证明都成功时选择 Colocate；否则丢弃该 candidate。
8. 执行层运行原有 Colocate Hash Join。

### 4.2 ADD CONSTRAINT 流程

1. 解析 determinant 与目标列，并检查 ALTER 权限。
2. 复核分析得到的表对象仍是当前数据库中同一个对象。
3. 在数据库读锁和表写锁下检查表状态、表类型、列、Hash 分布、目标列顺序和约束名冲突。
4. 检查所有注册 FE 是否上报当前完整版本。
5. 将列名绑定到 schema version、unique ID 和类型签名。
6. 更新 `TableProperty.properties` 中按约束名排序的完整 mapping 快照，提交兼容 journal。
7. 释放元数据锁后等待 journal 持久化完成。

### 4.3 DROP CONSTRAINT 流程

1. 解析并定位当前表对象，检查 ALTER 权限。
2. 如果名称对应表内 mapping，进入 mapping DROP 路径；否则沿用 PK/FK/UNIQUE 路径。
3. 在表写锁和 manager 锁下移除 mapping，写入删除后的完整快照并提交 journal；删除最后一条时快照为 `[]`。
4. 释放锁后等待 journal。

DROP 不要求集群版本一致，目的是允许在降级前或混合版本期间清理 mapping。

### 4.4 Rename Table 与 Rename Database

Mapping 跟随物理 `OlapTable` 对象，不包含数据库名或表名引用：

```text
rename name binding
    -> same OlapTable object
    -> same TableProperty
    -> same mapping metadata
```

因此 Rename 不需要更新 mapping 索引，也不会出现旧名称残留。列 Rename 不同，因为 mapping 直接绑定列身份，所以被拒绝。

### 4.5 Truncate、Drop 与 Recover

- Truncate 的表元数据复制保留 `TableProperty`，因此 mapping 保留；新数据仍必须满足用户声明的不变量。
- Drop 将原物理表对象放入 recycle bin，mapping 随对象进入回收站。
- Recover 恢复同一对象，因此 mapping 恢复。
- Drop 后同名 CREATE 得到新对象，不继承旧 mapping。

### 4.6 Replace 与 Swap

Mapping 的归属按物理表对象而不是名称判断：

- `REPLACE ... swap=false`：替换表对象接管目标名称，最终保留替换表对象原有 mapping；旧对象及其 mapping 被丢弃。
- `REPLACE ... swap=true`：两个物理表对象交换名称，各自 mapping 继续跟随各自对象。

这避免了按旧名称搬运、清理或重建 mapping 的歧义。

### 4.7 CREATE TABLE LIKE 与 CTAS

这两类语句创建新的物理表对象，不复制 mapping。原因是 `NOT ENFORCED` mapping 是对已有数据生产约束的业务承诺，不能仅凭 schema 相似自动继承。

### 4.8 Backup 与 Restore

Backup 对 `OlapTable` 做深拷贝，`TableProperty.properties` 中的 mapping 快照一并进入备份元数据。

Restore 在修改目标表状态或创建副本之前统一预检备份中所有带 mapping 的 OLAP 表：

1. 所有 FE 版本必须完全一致。
2. 备份表中的 mapping 必须与备份表 schema 兼容。
3. 失败时 Restore 直接失败，目标表尚未进入 RESTORE 状态。

该规则有意偏保守：即使恢复到已有表、最终路径可能只恢复分区而不复制 mapping，只要备份对象中包含 mapping，仍要求版本一致。这样避免 Restore 分支差异演化成新的生命周期漏洞。

### 4.9 滚动升级

1. 升级期间，新旧 FE 并存或版本尚未通过 heartbeat 确认时，不能 ADD mapping。
2. 已有 mapping 在 session 关闭时不影响普通查询。
3. session 开启且查询访问带 mapping 的表时，版本不一致会忽略 mapping 并回退普通规划，不会导致查询失败。
4. 全部 FE 升级完成并上报相同 `version-shortHash` 后，ADD、Restore 恢复，查询自动重新启用 mapping planning。

该策略只牺牲升级窗口内的 mapping 优化收益，不影响普通查询可用性，同时避免在旧 FE 中实现完整 mapping 生命周期协议。

### 4.10 降级

推荐顺序：

1. 全局停止启用 `enable_colocate_mapping_constraint`。
2. 在仍运行新版本 FE 时 DROP 所有 mapping；DROP 在混合版本下也允许执行。
3. 使用 `SHOW CONSTRAINTS` 确认 mapping 已清理。
4. 再开始 FE 降级。

旧 FE 能读取新 journal/image，但会忽略未知 mapping 字段，也不会保留或执行该功能。因此“不先清理 mapping 直接降级”不受支持。

### 4.11 外部 Catalog 与 HMS 事件

外部表不支持 mapping，所以 Refresh、Catalog rename/drop、HMS alter/rename/drop、插件事件和远端 ID 变化都不进入本功能生命周期。这样从根源上消除了外部异步事件与内部 mapping 状态之间的双向同步问题。

## 5. 行为变化

### 5.1 查询行为

- 默认关闭时没有规划行为变化。
- 开启后，满足完整证明的 Join 可能从 Shuffle/Broadcast 变为 Colocate。
- 普通证明不成立时回退，不报错。
- 集群版本不一致或 mapping 元数据与 schema 不兼容时忽略该表的 mapping，记录限频告警并回退普通规划。
- 只有实际构建 mapping proof 的查询禁用 SQL cache。
- 错误的 `NOT ENFORCED` 声明可能导致错误结果，这是最重要的用户责任。

### 5.2 DDL 与生命周期行为

- mapping 引用列不能 Drop、Rename 或 Modify。
- 带 mapping 的表不能从 Hash distribution 转为 Random。
- 表/数据库 Rename、Truncate、Drop/Recover 保留 mapping。
- Replace/Swap 按物理表对象携带 mapping。
- CREATE TABLE LIKE 和 CTAS 不复制 mapping。
- 外部表和临时表拒绝 ADD mapping。
- Restore 对任何备份内含 mapping 的对象执行统一保守门禁。

### 5.3 兼容行为

- ADD 和 Restore 要求所有注册 FE 精确同版本；查询仅在版本一致时使用 mapping，否则回退普通规划。
- DROP 不受门禁限制。
- 旧 FE 可把内部 table-property 键作为不透明数据 replay 并保留到 checkpoint，但不能展示、使用 mapping，也不会执行 mapping 专用 DDL 防护。
- 没有 stable unique ID 的旧表在任意 base schema version 变化后，需要重建 mapping。

## 6. 运维注意事项

### 6.1 数据正确性责任

上线前必须独立验证以下不变量：

- 单表内相同 determinant 始终产生相同目标分桶键。
- 使用同一 mapping ID 的不同表具有完全相同的映射语义。
- 复合 determinant 的列顺序一致。
- 目标列位置与各表 Hash 分桶键位置一致。

Doris 不会在 INSERT、UPDATE、导入或 Compaction 时验证这些条件。

### 6.2 发布与升级

- 先升级全部 FE，再创建或 Restore mapping；滚动升级期间无需强制关闭 session 开关，查询会自动回退普通规划。
- 通过 `SHOW FRONTENDS` 检查版本字段已收敛；未知版本不会阻断查询，但会阻止 ADD、Restore 和 mapping 优化。
- 不要在 FE 滚动升级窗口内依赖 mapping 优化收益。

### 6.3 降级

- 降级前必须 DROP mapping；即使旧 FE 能保留不透明快照，它也不能展示、使用 mapping 或执行 mapping 专用 DDL 防护，仅关闭 session 开关不能把该功能变成受支持状态。
- 建议在降级变更单中加入 `SHOW CONSTRAINTS` 清理确认。

### 6.4 Schema 与生命周期操作

- 修改 mapping 列前先 DROP 约束，DDL 完成后重新 ADD，使 identity binding 绑定新 schema。
- 对没有 stable unique ID 的旧表，任何 schema version 变化后都应重建 mapping。
- Replace/Swap 后应按“物理表对象跟随”语义核对最终 mapping，而不是按操作前表名推断。
- Restore 前确保所有 FE 已升级并完成 heartbeat。

### 6.5 性能与排障

- 功能默认关闭；开启后每个可 Shuffle Join 会多一个不可 enforce 的 mapping candidate，最终仍由成本模型选择。
- 成功使用时主要收益是减少网络 Shuffle；BE 执行路径不变。
- 使用 `SHOW CONSTRAINTS` 检查元数据，使用 `EXPLAIN` 检查是否出现 `COLOCATE`。
- 版本或 schema 不兼容会产生限频告警，其中包含不兼容 FE、表名或约束名，可据此完成升级、清理或重建。
- 当前未新增专用 metric；告警、SHOW 和 EXPLAIN 覆盖主要诊断路径。

## 7. 测试与验证

覆盖范围包括：

- 语法、ADD/DROP/SHOW、权限与名称冲突。
- 单列、复合 determinant、多 mapping、直接键与 mapping 混合覆盖。
- Project alias、VARCHAR widening cast、普通非 DISTINCT Aggregate 的正向传播，以及 DISTINCT/MultiDistinct/纯去重 Aggregate 的保守回退。
- 不完整 determinant、表达式、narrowing cast、Repeat、Set Operation、Exchange 后的保守回退。
- Colocate Group 稳定性、索引与分区约束、mapping candidate 不可 enforce。
- schema identity、无 stable unique ID、列 DDL 和 Hash-to-Random 防护。
- `TableProperty` 完整快照在 journal/image 中的新旧 FE 可读与保留、Replay、failover 基本语义和 Binlog 隔离。
- Truncate、Drop/Recover、CREATE TABLE LIKE、Backup/Restore，以及混合 FE 版本和 schema 不兼容时的查询回退。
- SQL cache 仅在实际构建 proof 时关闭。

已完成的验证命令和结果以当前任务最终测试记录为准，包括 FE checkstyle、相关 FE UT、标准 `./build.sh --fe` 和 `query_p0/colocate/test_colocate_mapping_constraint` 回归。
