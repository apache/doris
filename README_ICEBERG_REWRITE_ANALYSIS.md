# Iceberg 文件重写分析工具

## 简介

这套工具允许你在执行 Iceberg 数据文件重写操作之前，通过 SQL 查询预先分析哪些文件会被重写。

这些 SQL 查询完全基于 `RewriteDataFilePlanner.java` 的逻辑实现，可以帮助你：
- 📊 **预估重写影响**：了解有多少文件和数据需要被重写
- 🎯 **识别问题文件**：找出文件大小不合理或删除文件过多的文件
- 📈 **优化重写参数**：根据实际情况调整重写参数
- ⏱️ **节省时间成本**：避免不必要的重写操作

## 文件说明

### 1. `iceberg_rewrite_quick_check.sql` ⭐ **推荐首选**

**适合场景**：日常使用、快速检查

**包含查询**：
- ✅ 查询 1：快速概览 - 识别需要重写的文件
- ✅ 查询 2：按分区汇总统计
- ✅ 查询 3：文件大小分布分析
- ✅ 查询 4：删除文件影响分析
- ✅ 查询 5：综合分析 - 所有重写条件
- ✅ 查询 6：总体统计摘要
- ✅ 查询 7：针对特定分区的详细分析
- ✅ 查询 8：快照历史查看

**使用方法**：
```sql
-- 1. 将脚本中的表名替换为你的实际表名
-- 2. 直接在 Doris 中运行你需要的查询部分
-- 3. 根据结果调整参数
```

### 2. `iceberg_rewrite_analysis_guide.md` 📚

**适合场景**：学习参考、深入理解

**内容包括**：
- Iceberg 系统表介绍
- 文件重写触发条件详解
- 参数配置参考
- 实际使用示例
- 与 Java 代码的对应关系

### 3. `iceberg_rewrite_file_analysis.sql` 🔧

**适合场景**：完整分析、自定义开发

**特点**：
- 完整的步骤化分析流程
- 详细的注释说明
- 可定制的参数
- 支持变量语法（需要数据库支持）

## 快速开始

### 步骤 1：确认表名

首先确认你的 Iceberg 表名，格式通常为：
```
catalog_name.database_name.table_name
```

例如：
```
iceberg_catalog.warehouse.orders
```

### 步骤 2：查看基本信息

运行最简单的查询，了解表的基本情况：

```sql
-- 查看总文件数和大小
SELECT 
    COUNT(*) AS total_files,
    ROUND(SUM(file_size_in_bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb
FROM your_catalog.your_db.your_table$files
WHERE content = 0;
```

### 步骤 3：运行快速检查

打开 `iceberg_rewrite_quick_check.sql`，找到**查询 6：总体统计摘要**，这会给你一个全面的概览：

```sql
-- 这个查询会告诉你：
-- - 总共有多少文件
-- - 需要重写多少文件
-- - 总数据量和需要重写的数据量
-- - 重写比例
-- - 受影响的分区数
```

### 步骤 4：查看详细信息

如果摘要显示有文件需要重写，运行**查询 5：综合分析**查看具体是哪些文件，以及为什么需要重写。

### 步骤 5：调整参数（可选）

如果发现需要重写的文件太多或太少，可以调整查询中的参数：

```sql
WITH parameters AS (
    SELECT 
        16 * 1024 * 1024 AS min_file_size_bytes,    -- 调整最小文件大小
        768 * 1024 * 1024 AS max_file_size_bytes,   -- 调整最大文件大小
        2 AS delete_file_threshold,                  -- 调整删除文件阈值
        0.10 AS delete_ratio_threshold               -- 调整删除比例阈值
)
```

## 使用示例

### 示例 1：检查特定表

```sql
-- 表名：iceberg_catalog.warehouse.orders

-- 查看总体情况
SELECT 
    COUNT(*) AS total_files,
    ROUND(SUM(file_size_in_bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb,
    ROUND(AVG(file_size_in_bytes) / 1024.0 / 1024.0, 2) AS avg_mb
FROM iceberg_catalog.warehouse.orders$files
WHERE content = 0;

-- 查看需要重写的文件
SELECT 
    file_path,
    ROUND(file_size_in_bytes / 1024.0 / 1024.0, 2) AS file_size_mb,
    CASE 
        WHEN file_size_in_bytes < 16*1024*1024 THEN '过小'
        WHEN file_size_in_bytes > 768*1024*1024 THEN '过大'
    END AS issue
FROM iceberg_catalog.warehouse.orders$files
WHERE content = 0
  AND (file_size_in_bytes < 16*1024*1024 OR file_size_in_bytes > 768*1024*1024);
```

### 示例 2：分区表分析

```sql
-- 查看每个分区的情况
SELECT 
    partition,
    COUNT(*) AS files,
    ROUND(SUM(file_size_in_bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS size_gb,
    COUNT(CASE 
        WHEN file_size_in_bytes < 16*1024*1024 OR file_size_in_bytes > 768*1024*1024 
        THEN 1 
    END) AS files_need_rewrite
FROM iceberg_catalog.warehouse.orders$files
WHERE content = 0
GROUP BY partition
HAVING files_need_rewrite > 0
ORDER BY files_need_rewrite DESC;
```

### 示例 3：只分析特定分区

```sql
-- 只看 2024-01 的数据
SELECT 
    file_path,
    ROUND(file_size_in_bytes / 1024.0 / 1024.0, 2) AS file_size_mb
FROM iceberg_catalog.warehouse.orders$files
WHERE content = 0
  AND partition LIKE '%2024-01%'
  AND (file_size_in_bytes < 16*1024*1024 OR file_size_in_bytes > 768*1024*1024);
```

## 参数说明

### 默认参数值

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `min_file_size_bytes` | 16 MB | 小于此值的文件需要重写 |
| `max_file_size_bytes` | 768 MB | 大于此值的文件需要重写 |
| `target_file_size_bytes` | 512 MB | 理想的文件大小 |
| `delete_file_threshold` | 2 | 删除文件数量超过此值需要重写 |
| `delete_ratio_threshold` | 0.10 (10%) | 删除比例超过此值需要重写 |
| `min_input_files` | 5 | 分区至少要有这么多文件才考虑重写 |

### 如何调整参数

根据你的场景调整：

**场景 1：小文件过多**
```sql
-- 增大 min_file_size_bytes，合并更多小文件
min_file_size_bytes = 64 * 1024 * 1024  -- 从 16MB 提高到 64MB
```

**场景 2：大文件过多**
```sql
-- 减小 max_file_size_bytes，分割更多大文件
max_file_size_bytes = 512 * 1024 * 1024  -- 从 768MB 降低到 512MB
```

**场景 3：删除操作频繁**
```sql
-- 降低阈值，更积极地处理删除文件
delete_file_threshold = 1           -- 从 2 降低到 1
delete_ratio_threshold = 0.05       -- 从 10% 降低到 5%
```

## 重写条件说明

文件会在以下任一条件满足时被重写：

### 文件级别条件
1. ❌ **文件过小**：`file_size < min_file_size_bytes`
2. ❌ **文件过大**：`file_size > max_file_size_bytes`
3. ❌ **删除文件过多**：`delete_file_count >= delete_file_threshold`
4. ❌ **删除比例过高**：`deleted_records / total_records >= delete_ratio_threshold`

### 分区组级别条件
1. ✅ **足够的输入文件**：`file_count > 1 AND file_count >= min_input_files`
2. ✅ **足够的内容**：`file_count > 1 AND total_size > target_file_size_bytes`
3. ⚠️ **内容过多**：`total_size > max_file_group_size_bytes`
4. ⚠️ **包含问题文件**：组内任意文件满足文件级别条件

## Iceberg 系统表参考

### files 表字段

常用字段说明：

| 字段 | 类型 | 说明 |
|------|------|------|
| `content` | int | 文件类型：0=数据文件, 1=位置删除, 2=等值删除 |
| `file_path` | string | 文件路径 |
| `file_format` | string | 文件格式（PARQUET, ORC 等） |
| `partition` | struct | 分区值 |
| `record_count` | long | 记录数 |
| `file_size_in_bytes` | long | 文件大小（字节） |
| `spec_id` | int | 分区规格 ID |

### 查询示例

```sql
-- 查看表结构
DESC your_catalog.your_db.your_table$files;

-- 查看前 10 个文件
SELECT * FROM your_catalog.your_db.your_table$files LIMIT 10;

-- 查看删除文件
SELECT * FROM your_catalog.your_db.your_table$files WHERE content IN (1, 2);
```

## 工作流程建议

```
1. 运行总体统计摘要
   ↓
2. 确认是否需要重写
   ↓ (是)
3. 查看分区级别统计
   ↓
4. 识别问题最严重的分区
   ↓
5. 查看具体文件列表
   ↓
6. 调整重写参数
   ↓
7. 执行重写操作
   ↓
8. 重新运行统计验证效果
```

## 常见问题

### Q1: 查询很慢怎么办？

**A:** Iceberg 的 `files` 表可能包含大量数据，建议：
- 先对单个分区测试
- 使用 `LIMIT` 限制结果数量
- 在非高峰时段运行

```sql
-- 限制分区范围
WHERE partition LIKE '2024-01%'

-- 限制结果数量
LIMIT 100
```

### Q2: 如何只分析特定分区？

**A:** 在 WHERE 子句中添加分区过滤：

```sql
WHERE content = 0 
  AND partition = 'year=2024/month=01'
```

### Q3: 删除文件的关联不准确？

**A:** 是的，这是已知限制。Iceberg 的删除文件可能影响多个数据文件，SQL 查询只能提供基于分区的估算。对于精确的影响，需要使用 Iceberg API。

### Q4: 参数应该设置多少？

**A:** 这取决于你的：
- **查询模式**：OLAP 类查询适合大文件（256MB - 1GB）
- **数据更新频率**：频繁更新需要更小的删除阈值
- **存储成本**：更大的文件可以减少元数据开销
- **并行度需求**：更多小文件提供更好的并行度

一般建议：
- 目标文件大小：256MB - 512MB
- 最小文件大小：目标的 10-20%
- 最大文件大小：目标的 150-200%
- 删除阈值：根据更新频率调整（1-5）

### Q5: 如何与实际重写操作结合使用？

**A:** 典型流程：

```sql
-- 1. 分析（使用本工具）
SELECT ... FROM table.files ...

-- 2. 确认需要重写后，执行实际重写
ALTER TABLE your_table 
REWRITE DATA 
WHERE partition = 'your_partition'
OPTIONS (
    'min-file-size-bytes' = '16777216',
    'max-file-size-bytes' = '805306368',
    'target-file-size-bytes' = '536870912'
);

-- 3. 验证重写效果（再次使用本工具）
SELECT ... FROM table.files ...
```

## 与 Java 代码的对应关系

| Java 类/方法 | SQL 实现 |
|-------------|----------|
| `RewriteDataFilePlanner` | 整套 SQL 查询 |
| `planFileScanTasks()` | `SELECT * FROM table$files WHERE content = 0` |
| `filterFiles()` | `WHERE` 条件过滤 |
| `outsideDesiredFileSizeRange()` | `file_size_in_bytes < min OR > max` |
| `tooManyDeletes()` | `delete_file_count >= threshold` |
| `tooHighDeleteRatio()` | `deleted_records / record_count >= ratio` |
| `groupTasksByPartition()` | `GROUP BY partition` |
| `shouldRewriteGroup()` | 分组后的 `WHERE`/`HAVING` 条件 |

## 最佳实践

### ✅ 建议

1. **定期检查**：每周或每月运行分析，了解表的健康状况
2. **先小后大**：先在小分区测试，验证效果后再扩大范围
3. **非高峰执行**：重写操作会消耗资源，建议在低峰时段执行
4. **备份快照**：重写前确保有可恢复的快照
5. **监控效果**：重写后对比查询性能和存储占用

### ❌ 避免

1. **盲目重写**：不是所有文件都需要重写，先分析再决策
2. **频繁重写**：给表一些稳定期，避免过于频繁的重写
3. **并发重写**：避免同时重写多个大分区
4. **忽略成本**：重写会产生新文件和删除旧文件，注意存储成本

## 相关文档

- [Apache Iceberg 官方文档](https://iceberg.apache.org/)
- [Doris Iceberg Catalog 文档](https://doris.apache.org/docs/lakehouse/multi-catalog/iceberg)
- [Iceberg 表维护](https://iceberg.apache.org/docs/latest/maintenance/)

## 反馈和贡献

如果你发现问题或有改进建议，请：
1. 在代码中添加注释
2. 分享你的使用经验
3. 提出优化方案

## 更新日志

- **v1.0** (2024-11-06)
  - 初始版本
  - 基于 RewriteDataFilePlanner.java 实现
  - 支持文件大小和删除文件分析
  - 提供 8 个常用查询模板

---

**开始使用**: 打开 `iceberg_rewrite_quick_check.sql`，将表名替换为你的表，运行**查询 6**查看总体摘要！

