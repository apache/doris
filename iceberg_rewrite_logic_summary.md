# Iceberg 回写逻辑总结

## 问题描述

在使用 Iceberg 表进行数据回写时，发现每个分桶（Bucket）中的文件数量过多：
- 当 `parallel_pipeline_task_num = 4` 时，每个分桶有 28 个文件
- 当 `parallel_pipeline_task_num = 2` 时，每个分桶有 14 个文件
- 规律：文件数 = `parallel_pipeline_task_num × BE节点数`（7个BE节点）

期望：控制小文件数量，每个文件大小约 1GB。

## Iceberg 回写整体流程

### 1. 回写入口（FE端）

**类：`IcebergRewriteDataFilesAction`**
- 触发 Iceberg 表的文件回写操作
- 调用 `RewriteDataFilePlanner` 规划需要回写的文件
- 调用 `RewriteDataFileExecutor` 执行回写任务

### 2. 文件规划阶段（FE端）

**类：`RewriteDataFilePlanner`**

主要步骤：
1. **扫描文件**：从 Iceberg 表中扫描所有数据文件（`FileScanTask`）
2. **按分区分组**：将文件按分区（Partition）进行分组
3. **Bin-Packing 分组**：在每个分区内，使用 bin-packing 策略将文件组织成文件组（`RewriteDataGroup`）
   - 目标：将多个小文件合并成接近目标大小的文件组
   - 参数：`maxFileGroupSizeBytes` 控制每个文件组的最大大小
4. **过滤文件组**：根据配置参数过滤需要回写的文件组
   - `minInputFiles`：最小输入文件数
   - `targetFileSizeBytes`：目标文件大小
   - `maxFileSizeBytes`：最大文件大小
   - `deleteFileThreshold`：删除文件阈值

**类：`RewriteDataGroup`**
- 表示一个需要回写的文件组
- 包含多个 `FileScanTask`（输入文件）
- 记录文件组的总大小和文件数量

### 3. 任务执行阶段（FE端）

**类：`RewriteDataFileExecutor`**

主要步骤：
1. **开启事务**：为回写操作开启 Iceberg 事务
2. **注册待删除文件**：将所有需要回写的原始文件注册到事务中
3. **创建回写任务**：为每个 `RewriteDataGroup` 创建一个 `RewriteGroupTask`
4. **并发执行**：将所有任务提交到 `TransientTaskManager` 并发执行
5. **等待完成**：等待所有任务完成
6. **提交事务**：提交 Iceberg 事务，完成文件替换

**类：`RewriteGroupTask`**

每个任务执行：
1. **创建独立的 ConnectContext**：为任务创建独立的执行上下文
2. **设置目标文件大小**：通过 `iceberg_write_target_file_size_bytes` 设置目标文件大小
3. **设置文件扫描任务**：将 `RewriteDataGroup` 中的文件扫描任务设置到上下文中
4. **构建逻辑计划**：构建 `INSERT INTO ... SELECT ...` 的逻辑计划
5. **执行插入操作**：执行插入操作，触发 BE 端的数据写入

### 4. 数据写入阶段（BE端）

**类：`VIcebergTableWriter`**

主要逻辑：
1. **接收数据块**：从上游接收数据块（Block）
2. **计算分区/Bucket**：
   - 如果有分区列，计算每行数据的分区值
   - 如果有 Bucket 列，通过 `PartitionColumnTransform` 计算每行数据的 Bucket ID
   - Bucket 计算：`hash_value % bucket_num`
3. **创建/获取 Writer**：
   - 根据分区路径（包含 Bucket ID）查找或创建 `VIcebergPartitionWriter`
   - 每个分区路径对应一个 Writer
   - Writer 使用 `query_id + uuid` 生成文件名
4. **文件大小控制**：
   - 当 Writer 写入的数据超过 `target_file_size_bytes` 时，关闭当前文件
   - 创建新文件（文件名索引递增：`file_name_index + 1`）
5. **写入数据**：将数据写入对应的 Writer

**类：`VIcebergPartitionWriter`**
- 负责实际的文件写入操作
- 支持 Parquet、ORC 等格式
- 文件路径格式：`{write_path}/{file_name}-{file_name_index}.{extension}`

## 文件数量过多的根本原因

### 问题分析

1. **并行任务分布**：
   - 每个 BE 节点上会启动 `parallel_pipeline_task_num` 个并行任务
   - 每个并行任务都会创建自己的 `VIcebergTableWriter` 实例
   - 每个 Writer 实例独立管理自己的文件写入

2. **文件创建机制**：
   - 每个 Writer 实例在首次写入某个 Bucket 时，会创建一个新文件
   - 文件名基于 `query_id + uuid`，不同 Writer 实例的文件名不同
   - 即使设置了 `target_file_size_bytes`，也只能控制单个 Writer 实例内的文件大小

3. **文件数量计算**：
   ```
   每个 Bucket 的文件数 = parallel_pipeline_task_num × BE节点数
   ```
   - 因为每个 BE 节点上的每个并行任务都会为每个 Bucket 创建至少一个文件
   - 这些文件无法跨 Writer 实例合并

### 示例

假设：
- 4 个 Bucket
- 7 个 BE 节点
- `parallel_pipeline_task_num = 4`

结果：
- 每个 Bucket 的文件数 = 4 × 7 = 28 个文件
- 总文件数 = 4 × 28 = 112 个文件

## 解决方案建议

### 方案1：调整并行度（临时方案）

通过调整 `parallel_pipeline_task_num` 来控制文件数量：
```sql
-- 如果希望每个 Bucket 只有 7 个文件（每个 BE 节点 1 个文件）
SET parallel_pipeline_task_num = 1;
```

**缺点**：
- 需要根据 BE 节点数手动调整
- 可能影响写入性能
- 不够灵活

### 方案2：增加目标文件大小（部分有效）

通过设置更大的 `target_file_size_bytes` 来减少单个 Writer 内的文件数：
```sql
-- 设置目标文件大小为 1GB
SET iceberg_write_target_file_size_bytes = 1073741824;
```

**缺点**：
- 只能减少单个 Writer 实例内的文件数
- 无法解决跨 Writer 实例的文件合并问题
- 如果数据量小，仍然会有很多小文件

### 方案3：优化代码实现（根本方案）

需要修改 BE 端的写入逻辑，实现跨 Writer 实例的文件合并：

1. **共享文件写入**：
   - 在 BE 节点级别或集群级别共享文件写入器
   - 同一 Bucket 的数据写入同一个文件，直到达到目标大小

2. **文件合并机制**：
   - 在写入完成后，对小文件进行合并
   - 或者在写入过程中，将小文件合并到大文件中

3. **动态调整并行度**：
   - 根据目标文件大小和数据量，自动调整并行度
   - 确保每个 Bucket 的文件数量在合理范围内

### 方案4：使用 Iceberg 的 Compaction 功能

在写入完成后，使用 Iceberg 的 Compaction 功能合并小文件：
```sql
-- 使用 rewrite_data_files 函数合并小文件
CALL rewrite_data_files('catalog.db.table', 
    'target_file_size_bytes' = '1073741824');
```

**优点**：
- 不需要修改代码
- 可以定期执行，保持文件大小合理

**缺点**：
- 需要额外的 Compaction 操作
- 不能实时控制文件大小

## 关键代码位置

### FE端
- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergRewriteDataFilesAction.java`：回写入口
- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/rewrite/RewriteDataFilePlanner.java`：文件规划
- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/rewrite/RewriteDataFileExecutor.java`：任务执行
- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/rewrite/RewriteGroupTask.java`：单个回写任务

### BE端
- `be/src/vec/sink/writer/iceberg/viceberg_table_writer.cpp`：表级写入器
- `be/src/vec/sink/writer/iceberg/viceberg_partition_writer.cpp`：分区级写入器
- `be/src/vec/sink/writer/iceberg/partition_transformers.h`：Bucket 计算逻辑

## 相关配置参数

### Session 变量
- `parallel_pipeline_task_num`：并行管道任务数，影响文件数量
- `iceberg_write_target_file_size_bytes`：Iceberg 写入目标文件大小（字节）

### 配置参数
- `iceberg_sink_max_file_size`：Iceberg Sink 最大文件大小（BE 配置）

## 总结

Iceberg 回写的文件数量问题主要源于：
1. **并行任务分布**：每个 BE 节点上的每个并行任务都会创建独立的文件
2. **缺乏跨实例合并**：不同 Writer 实例之间无法合并文件
3. **文件大小控制局限**：只能控制单个 Writer 实例内的文件大小

**建议**：
- 短期：调整 `parallel_pipeline_task_num` 或使用 Compaction 功能
- 长期：优化代码实现，支持跨 Writer 实例的文件合并或共享写入

