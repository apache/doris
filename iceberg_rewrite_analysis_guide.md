# Iceberg 文件重写分析指南

## 概述

本指南提供了一套 SQL 查询方案，让你可以在实际执行 Iceberg 数据文件重写操作之前，预先分析哪些文件会被重写。这些查询完全基于 `RewriteDataFilePlanner.java` 的逻辑实现。

## 核心概念

### Iceberg 系统表

Iceberg 提供了多个系统表来访问元数据：

- **`<table>.files`**: 包含所有数据文件和删除文件的元数据
- **`<table>.entries`**: 包含清单条目信息
- **`<table>.snapshots`**: 快照历史信息
- **`<table>.partitions`**: 分区信息

### 文件重写触发条件

根据 `RewriteDataFilePlanner.java`，文件会在以下情况下被重写：

#### 文件级别条件（File-level）:
1. **文件大小超出范围** (`outsideDesiredFileSizeRange`)
   - 文件小于 `min_file_size_bytes`
   - 文件大于 `max_file_size_bytes`

2. **删除文件过多** (`tooManyDeletes`)
   - 关联的删除文件数量 >= `delete_file_threshold`

3. **删除比例过高** (`tooHighDeleteRatio`)
   - 已删除记录数 / 总记录数 >= `delete_ratio_threshold`

#### 分区组级别条件（Partition Group-level）:
1. **足够的输入文件** (`enoughInputFiles`)
   - 文件数 > 1 且 >= `min_input_files`

2. **足够的内容** (`enoughContent`)
   - 文件数 > 1 且总大小 > `target_file_size_bytes`

3. **内容过多** (`tooMuchContent`)
   - 总大小 > `max_file_group_size_bytes`

4. **包含需要删除的文件**
   - 分区组内任意文件满足文件级别条件

## 使用方法

### 方法 1: 使用 Doris 直接查询（推荐）

在 Doris 中，你可以直接查询 Iceberg 系统表：

```sql
-- 查询文件元数据
SELECT * FROM iceberg_catalog.db.table$files LIMIT 10;

-- 查询快照信息
SELECT * FROM iceberg_catalog.db.table$snapshots LIMIT 10;

-- 查询分区信息
SELECT * FROM iceberg_catalog.db.table$partitions LIMIT 10;
```

### 方法 2: 简化的查询模板

#### 查询 1: 识别大小不合适的文件

```sql
-- 参数设置
SET @min_file_size_bytes = 16 * 1024 * 1024;   -- 16 MB
SET @max_file_size_bytes = 768 * 1024 * 1024;  -- 768 MB

-- 查询需要重写的文件
SELECT 
    file_path,
    partition,
    file_size_in_bytes / 1024.0 / 1024.0 AS file_size_mb,
    record_count,
    CASE 
        WHEN file_size_in_bytes < @min_file_size_bytes THEN 'Too small'
        WHEN file_size_in_bytes > @max_file_size_bytes THEN 'Too large'
    END AS issue
FROM iceberg_catalog.database.table$files
WHERE content = 0  -- 只看数据文件
  AND (file_size_in_bytes < @min_file_size_bytes 
       OR file_size_in_bytes > @max_file_size_bytes)
ORDER BY partition, file_size_in_bytes;
```

#### 查询 2: 按分区统计需要重写的文件

```sql
SELECT 
    partition,
    COUNT(*) AS total_files,
    SUM(CASE 
        WHEN file_size_in_bytes < 16*1024*1024 
          OR file_size_in_bytes > 768*1024*1024 
        THEN 1 ELSE 0 
    END) AS files_need_rewrite,
    SUM(file_size_in_bytes) / 1024.0 / 1024.0 / 1024.0 AS total_size_gb,
    AVG(file_size_in_bytes) / 1024.0 / 1024.0 AS avg_file_size_mb,
    MIN(file_size_in_bytes) / 1024.0 / 1024.0 AS min_file_size_mb,
    MAX(file_size_in_bytes) / 1024.0 / 1024.0 AS max_file_size_mb
FROM iceberg_catalog.database.table$files
WHERE content = 0
GROUP BY partition
HAVING files_need_rewrite > 0
ORDER BY files_need_rewrite DESC;
```

#### 查询 3: 识别有删除文件的数据文件

```sql
-- 统计每个分区的删除文件情况
WITH delete_files AS (
    SELECT 
        partition,
        spec_id,
        COUNT(*) AS delete_file_count,
        SUM(record_count) AS total_deleted_records
    FROM iceberg_catalog.database.table$files
    WHERE content IN (1, 2)  -- 1=position deletes, 2=equality deletes
    GROUP BY partition, spec_id
),
data_files AS (
    SELECT 
        file_path,
        partition,
        spec_id,
        file_size_in_bytes,
        record_count
    FROM iceberg_catalog.database.table$files
    WHERE content = 0
)
SELECT 
    df.file_path,
    df.partition,
    df.file_size_in_bytes / 1024.0 / 1024.0 AS file_size_mb,
    df.record_count,
    COALESCE(del.delete_file_count, 0) AS delete_file_count,
    COALESCE(del.total_deleted_records, 0) AS deleted_records,
    ROUND(COALESCE(del.total_deleted_records, 0) * 100.0 / NULLIF(df.record_count, 0), 2) AS delete_ratio_pct,
    CASE 
        WHEN del.delete_file_count >= 2 THEN 'Too many delete files'
        WHEN (del.total_deleted_records * 100.0 / NULLIF(df.record_count, 0)) >= 10 THEN 'High delete ratio'
    END AS issue
FROM data_files df
LEFT JOIN delete_files del 
    ON df.partition = del.partition AND df.spec_id = del.spec_id
WHERE del.delete_file_count IS NOT NULL
ORDER BY delete_ratio_pct DESC;
```

#### 查询 4: 完整的重写候选分析

```sql
WITH data_files AS (
    SELECT 
        file_path,
        partition,
        spec_id,
        file_size_in_bytes,
        record_count,
        file_format
    FROM iceberg_catalog.database.table$files
    WHERE content = 0
),
delete_summary AS (
    SELECT 
        partition,
        spec_id,
        COUNT(*) AS delete_file_count,
        SUM(record_count) AS deleted_records
    FROM iceberg_catalog.database.table$files
    WHERE content IN (1, 2)
    GROUP BY partition, spec_id
),
file_analysis AS (
    SELECT 
        df.file_path,
        df.partition,
        df.file_size_in_bytes,
        df.record_count,
        COALESCE(ds.delete_file_count, 0) AS delete_file_count,
        COALESCE(ds.deleted_records, 0) AS deleted_records,
        
        -- 检查各种条件
        (df.file_size_in_bytes < 16*1024*1024 OR df.file_size_in_bytes > 768*1024*1024) AS size_issue,
        (COALESCE(ds.delete_file_count, 0) >= 2) AS delete_count_issue,
        (COALESCE(ds.deleted_records, 0) * 100.0 / NULLIF(df.record_count, 0) >= 10) AS delete_ratio_issue
        
    FROM data_files df
    LEFT JOIN delete_summary ds 
        ON df.partition = ds.partition AND df.spec_id = ds.spec_id
)
SELECT 
    file_path,
    partition,
    file_size_in_bytes / 1024.0 / 1024.0 AS file_size_mb,
    record_count,
    delete_file_count,
    deleted_records,
    ROUND(deleted_records * 100.0 / NULLIF(record_count, 0), 2) AS delete_ratio_pct,
    size_issue,
    delete_count_issue,
    delete_ratio_issue,
    (size_issue OR delete_count_issue OR delete_ratio_issue) AS should_rewrite,
    CONCAT(
        CASE WHEN size_issue THEN 'Size issue; ' ELSE '' END,
        CASE WHEN delete_count_issue THEN 'Too many deletes; ' ELSE '' END,
        CASE WHEN delete_ratio_issue THEN 'High delete ratio; ' ELSE '' END
    ) AS issues
FROM file_analysis
WHERE size_issue OR delete_count_issue OR delete_ratio_issue
ORDER BY partition, file_size_in_bytes DESC;
```

#### 查询 5: 总体统计摘要

```sql
WITH file_analysis AS (
    SELECT 
        partition,
        file_path,
        file_size_in_bytes,
        record_count,
        (file_size_in_bytes < 16*1024*1024 OR file_size_in_bytes > 768*1024*1024) AS needs_rewrite
    FROM iceberg_catalog.database.table$files
    WHERE content = 0
)
SELECT 
    '总文件数' AS metric, 
    COUNT(*) AS value
FROM file_analysis
UNION ALL
SELECT 
    '需要重写的文件数', 
    SUM(CASE WHEN needs_rewrite THEN 1 ELSE 0 END)
FROM file_analysis
UNION ALL
SELECT 
    '总大小 (GB)', 
    ROUND(SUM(file_size_in_bytes) / 1024.0 / 1024.0 / 1024.0, 2)
FROM file_analysis
UNION ALL
SELECT 
    '需要重写的大小 (GB)', 
    ROUND(SUM(CASE WHEN needs_rewrite THEN file_size_in_bytes ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, 2)
FROM file_analysis
UNION ALL
SELECT 
    '重写百分比 (%)', 
    ROUND(SUM(CASE WHEN needs_rewrite THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM file_analysis;
```

## 实际使用示例

### 示例 1: 分析特定表的重写需求

```sql
-- 假设表名为: iceberg_catalog.warehouse.orders

-- 1. 查看表的基本信息
SELECT 
    COUNT(*) AS total_files,
    SUM(file_size_in_bytes) / 1024.0 / 1024.0 / 1024.0 AS total_gb,
    AVG(file_size_in_bytes) / 1024.0 / 1024.0 AS avg_mb,
    MIN(file_size_in_bytes) / 1024.0 / 1024.0 AS min_mb,
    MAX(file_size_in_bytes) / 1024.0 / 1024.0 AS max_mb
FROM iceberg_catalog.warehouse.orders.files
WHERE content = 0;

-- 2. 按文件大小分布统计
SELECT 
    CASE 
        WHEN file_size_in_bytes < 10*1024*1024 THEN '< 10 MB'
        WHEN file_size_in_bytes < 100*1024*1024 THEN '10-100 MB'
        WHEN file_size_in_bytes < 500*1024*1024 THEN '100-500 MB'
        WHEN file_size_in_bytes < 1024*1024*1024 THEN '500 MB - 1 GB'
        ELSE '> 1 GB'
    END AS size_range,
    COUNT(*) AS file_count,
    SUM(file_size_in_bytes) / 1024.0 / 1024.0 / 1024.0 AS total_gb
FROM iceberg_catalog.warehouse.orders.files
WHERE content = 0
GROUP BY size_range
ORDER BY MIN(file_size_in_bytes);

-- 3. 识别需要重写的文件
SELECT 
    file_path,
    file_size_in_bytes / 1024.0 / 1024.0 AS file_size_mb,
    record_count
FROM iceberg_catalog.warehouse.orders.files
WHERE content = 0
  AND (file_size_in_bytes < 16*1024*1024 OR file_size_in_bytes > 768*1024*1024)
ORDER BY file_size_in_bytes;
```

### 示例 2: 分析分区表的重写需求

```sql
-- 按分区分析
SELECT 
    partition,
    COUNT(*) AS files,
    SUM(file_size_in_bytes) / 1024.0 / 1024.0 / 1024.0 AS size_gb,
    AVG(file_size_in_bytes) / 1024.0 / 1024.0 AS avg_mb,
    SUM(CASE WHEN file_size_in_bytes < 16*1024*1024 THEN 1 ELSE 0 END) AS small_files,
    SUM(CASE WHEN file_size_in_bytes > 768*1024*1024 THEN 1 ELSE 0 END) AS large_files,
    SUM(CASE 
        WHEN file_size_in_bytes < 16*1024*1024 OR file_size_in_bytes > 768*1024*1024 
        THEN 1 ELSE 0 
    END) AS files_need_rewrite
FROM iceberg_catalog.warehouse.orders.files
WHERE content = 0
GROUP BY partition
ORDER BY files_need_rewrite DESC, size_gb DESC
LIMIT 20;
```

### 示例 3: 结合 WHERE 条件的分析

如果你的重写操作有 WHERE 条件（比如只重写特定分区），可以在查询中添加相应的过滤：

```sql
-- 只分析特定日期范围的分区
SELECT 
    file_path,
    partition,
    file_size_in_bytes / 1024.0 / 1024.0 AS file_size_mb,
    record_count
FROM iceberg_catalog.warehouse.orders.files
WHERE content = 0
  AND partition LIKE '%2024-01%'  -- 过滤条件
  AND (file_size_in_bytes < 16*1024*1024 OR file_size_in_bytes > 768*1024*1024)
ORDER BY partition, file_size_in_bytes;
```

## 参数配置参考

以下是 `RewriteDataFilePlanner.java` 中使用的默认参数值：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `target_file_size_bytes` | 512 MB | 目标文件大小 |
| `min_file_size_bytes` | 计算得出 (约 16 MB) | 最小文件大小 (通常是 target * 0.75 / 24) |
| `max_file_size_bytes` | 768 MB | 最大文件大小 (通常是 target * 1.5) |
| `min_input_files` | 5 | 触发重写的最小文件数 |
| `delete_file_threshold` | 2 | 删除文件数量阈值 |
| `delete_ratio_threshold` | 0.10 | 删除比例阈值 (10%) |
| `max_file_group_size_bytes` | 100 GB | 单个重写组的最大大小 |

你可以根据实际情况调整这些参数。

## 注意事项

### 1. 删除文件的关联

Iceberg 的 `files` 系统表显示所有文件，但是：
- `content = 0` 表示数据文件
- `content = 1` 表示位置删除文件 (position deletes)
- `content = 2` 表示等值删除文件 (equality deletes)

删除文件可能影响多个数据文件，精确的关联关系需要更深入的分析。上述查询提供了基于分区的估算。

### 2. 性能考虑

- 对于大表，查询 `files` 系统表可能比较慢
- 建议先在小范围（如单个分区）测试查询
- 可以使用 `LIMIT` 限制结果数量

### 3. 实时性

- 系统表反映的是当前快照的状态
- 如果表正在被并发修改，结果可能会变化
- 建议在稳定时间窗口执行分析

## 与 Java 代码的对应关系

| Java 方法 | SQL 实现 |
|-----------|----------|
| `planFileScanTasks()` | `SELECT * FROM table.files WHERE content = 0` |
| `filterFiles()` | `WHERE` 子句中的条件过滤 |
| `outsideDesiredFileSizeRange()` | `file_size_in_bytes < @min OR file_size_in_bytes > @max` |
| `tooManyDeletes()` | `delete_file_count >= @threshold` |
| `tooHighDeleteRatio()` | `deleted_records / record_count >= @ratio` |
| `groupTasksByPartition()` | `GROUP BY partition` |
| `filterGroups()` | 在 `HAVING` 或 `WHERE` 子句中的分组条件 |

## 下一步

1. 使用这些查询分析你的表
2. 根据分析结果决定是否需要重写
3. 调整重写参数以优化效果
4. 执行实际的 `ALTER TABLE ... REWRITE DATA` 操作
5. 重新运行查询验证重写效果

## 完整脚本

完整的 SQL 脚本请参考：`iceberg_rewrite_file_analysis.sql`

