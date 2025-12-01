# Iceberg Procedures 实现计划

## 概述

本文档梳理了 Apache Iceberg 的 procedure 功能在 Doris 中的实现情况，并按照实现难度对未实现的 procedure 进行了排序。

参考文档：https://iceberg.apache.org/docs/latest/spark-procedures/#metadata-management

## 已实现的 Procedure（7个）

Doris 目前已实现以下 Iceberg procedure：

1. ✅ **rollback_to_snapshot** - 回滚到指定快照
2. ✅ **rollback_to_timestamp** - 回滚到指定时间戳
3. ✅ **set_current_snapshot** - 设置当前快照
4. ✅ **cherrypick_snapshot** - 选择快照
5. ✅ **fast_forward** - 快进分支
6. ✅ **expire_snapshots** - 过期快照清理
7. ✅ **rewrite_data_files** - 重写数据文件

实现位置：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/`

## 未实现的 Procedure（按实现难度排序）

### 🔵 简单难度（Low）

#### 1. `ancestors_of`
- **功能**：查询指定快照的所有祖先快照
- **难度**：⭐
- **说明**：主要是查询操作，通过 Iceberg API 获取快照的祖先链
- **参考实现**：已有快照相关操作，可参考 `IcebergSetCurrentSnapshotAction`
- **Iceberg API**：`Table.snapshots()`, `Snapshot.parentId()`

#### 2. `snapshot`
- **功能**：为表创建快照（用于表迁移场景）
- **难度**：⭐
- **说明**：创建表的快照，用于后续迁移操作
- **参考实现**：已有快照管理相关实现
- **Iceberg API**：`Table.newTransaction()`, `Transaction.commitTransaction()`

---

### 🟡 中等难度（Medium）

#### 3. `remove_orphan_files`
- **功能**：删除孤立文件（不在表元数据中引用的文件）
- **难度**：⭐⭐
- **说明**：需要扫描文件系统，识别并删除不在任何快照中引用的文件
- **关键点**：
  - 需要扫描表的数据目录
  - 对比文件系统中的文件与元数据中引用的文件
  - 支持 `older-than` 参数过滤文件
- **Iceberg API**：`Table.newRemoveOrphanFiles()`
- **参考实现**：可参考 `expire_snapshots` 的文件扫描逻辑

#### 4. `rewrite_manifests`
- **功能**：重写 manifest 文件，合并小文件以提高查询性能
- **难度**：⭐⭐
- **说明**：类似 `rewrite_data_files`，但操作的是 manifest 文件
- **关键点**：
  - 扫描并合并小的 manifest 文件
  - 更新 manifest list
- **Iceberg API**：`Table.newRewriteManifests()`
- **参考实现**：已有 `rewrite_data_files` 实现，逻辑类似

#### 5. `compute_table_stats`
- **功能**：计算表的 NDV（Number of Distinct Values）统计信息
- **难度**：⭐⭐
- **说明**：为表的所有列或指定列计算统计信息
- **关键点**：
  - 扫描数据文件并计算 NDV
  - 支持指定快照和列
  - 生成并写入统计文件
- **Iceberg API**：`Table.newAppend()`, `StatisticsFile`
- **参考实现**：已有 `StatisticsUtil.getIcebergColumnStats()` 相关代码

#### 6. `compute_partition_stats`
- **功能**：计算分区统计信息
- **难度**：⭐⭐
- **说明**：增量计算分区统计信息，从上次统计的快照到当前快照
- **关键点**：
  - 增量计算逻辑
  - 生成 `PartitionStatisticsFile`
  - 注册到表元数据
- **Iceberg API**：`PartitionStatisticsFile`
- **参考实现**：可参考 `compute_table_stats` 和 Doris 的分区统计实现

#### 7. `rewrite_position_delete_files`
- **功能**：重写 position delete 文件，合并小文件
- **难度**：⭐⭐
- **说明**：类似 `rewrite_data_files`，但针对 position delete 文件
- **关键点**：
  - 识别并合并小的 position delete 文件
  - 保持删除信息的正确性
- **Iceberg API**：`Table.newRewritePositionDeletes()`
- **参考实现**：已有 position delete 文件读取逻辑（`IcebergDeleteFileFilter`），可参考 `rewrite_data_files`

#### 8. `publish_changes`
- **功能**：将分支的更改发布到主分支
- **难度**：⭐⭐
- **说明**：将分支上的更改合并到主分支
- **关键点**：
  - 分支操作
  - 冲突检测和处理
- **Iceberg API**：`Table.manageSnapshots()`, `SnapshotRef`
- **参考实现**：已有 `fast_forward` 实现，逻辑类似

---

### 🔴 复杂难度（High）

#### 9. `create_changelog_view`
- **功能**：创建变更日志视图，用于 CDC（Change Data Capture）
- **难度**：⭐⭐⭐
- **说明**：计算两个快照之间的数据变更，支持 INSERT/DELETE/UPDATE 类型
- **关键点**：
  - 需要比较两个快照的数据差异
  - 支持 net changes（净变更）模式
  - 支持 pre/post update images
  - 处理 carry-over rows
  - 需要实现 CDC 元数据列（`_change_type`, `_change_ordinal`, `_commit_snapshot_id`）
- **Iceberg API**：`SparkChangelogTable`, `Table.changes()`
- **参考实现**：需要实现完整的 CDC 逻辑，复杂度较高

#### 10. `migrate`
- **功能**：将 Hive 表迁移到 Iceberg 表
- **难度**：⭐⭐⭐
- **说明**：将现有的 Hive 表转换为 Iceberg 表格式
- **关键点**：
  - Hive 元数据读取
  - 数据文件格式转换（如果需要）
  - 元数据迁移
  - 支持增量迁移
- **Iceberg API**：`SparkActions.get().migrateTable()`
- **参考实现**：需要了解 Hive 和 Iceberg 的元数据差异

#### 11. `add_files`
- **功能**：将外部文件添加到 Iceberg 表
- **难度**：⭐⭐⭐
- **说明**：将已存在的数据文件添加到表中，无需重新写入
- **关键点**：
  - 验证文件格式和 schema 兼容性
  - 生成正确的 DataFile 元数据
  - 更新表元数据
- **Iceberg API**：`Table.newAppend()`, `DataFile`
- **参考实现**：需要理解 Iceberg 的文件元数据结构

#### 12. `register_table`
- **功能**：在 catalog 中注册已存在的 Iceberg 表
- **难度**：⭐⭐⭐
- **说明**：将已存在的 Iceberg 表注册到 catalog 中
- **关键点**：
  - 验证表元数据
  - 在 catalog 中创建表记录
  - 支持不同的 catalog 类型
- **Iceberg API**：`Catalog.registerTable()`
- **参考实现**：需要了解 Doris catalog 的实现

#### 13. `rewrite_table_path`
- **功能**：重写表路径，用于表复制/迁移
- **难度**：⭐⭐⭐
- **说明**：将表中的所有路径前缀替换为新前缀，用于跨存储系统复制表
- **关键点**：
  - 扫描所有元数据文件（metadata.json, manifest lists, manifests, delete files）
  - 替换路径前缀
  - 生成文件复制清单
  - 支持全量和增量重写
  - 不支持有 partition statistics files 的表
- **Iceberg API**：需要手动处理元数据文件
- **参考实现**：需要深入理解 Iceberg 元数据结构

---

## 实现建议

### 优先级建议

1. **第一阶段（简单）**：实现 `ancestors_of` 和 `snapshot`，快速补齐基础功能
2. **第二阶段（中等）**：实现 `remove_orphan_files`、`rewrite_manifests`、`compute_table_stats`，这些是常用的维护操作
3. **第三阶段（复杂）**：根据业务需求实现复杂的 procedure，如 `create_changelog_view`（CDC 场景）、`migrate`（迁移场景）

### 实现模式

所有 procedure 应遵循现有的实现模式：

1. 在 `IcebergExecuteActionFactory` 中注册新的 procedure
2. 创建对应的 Action 类，继承 `BaseIcebergAction`
3. 实现 `registerIcebergArguments()` 注册参数
4. 实现 `executeAction()` 执行具体逻辑
5. 添加单元测试和集成测试

### 参考文件

- 工厂类：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergExecuteActionFactory.java`
- 基类：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/BaseIcebergAction.java`
- 示例实现：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergExpireSnapshotsAction.java`
- 测试文件：`regression-test/suites/external_table_p0/iceberg/action/test_iceberg_execute_actions.groovy`

---

## 总结

- **已实现**：7 个 procedure（快照管理和部分元数据管理）
- **待实现**：13 个 procedure
  - 简单：2 个
  - 中等：6 个
  - 复杂：5 个

建议按照难度和业务需求优先级逐步实现，优先完成简单和中等难度的 procedure，以快速提升 Iceberg 功能的完整性。

