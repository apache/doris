# Iceberg Runtime Filter 分区裁剪测试覆盖分析

## 一、现有测试分析

### 1.1 `test_iceberg_runtime_filter_partition_pruning.groovy`

**测试内容**：
- 测试 **identity transform** 的分区裁剪
- 覆盖多种数据类型：decimal、int、string、date、timestamp、boolean、float、timestamp_ntz、binary
- 测试场景：
  - 等值查询（`=`）
  - IN 查询（`IN`）
  - 函数表达式（`abs()`）
  - NULL 值处理

**评估**：
- ✅ **合理**：覆盖了 identity transform 的基础场景
- ✅ **完整**：测试了多种数据类型和查询模式
- ✅ **有效**：能够验证 identity transform 的分区裁剪功能

### 1.2 `test_iceberg_runtime_filter_partition_pruning_transform.groovy`

**测试内容**：
- 测试 **bucket transform** 的分区裁剪
  - 数据类型：int、bigint、string、date、timestamp、timestamp_ntz、binary
  - 查询模式：等值查询（`=`）、IN 查询（`IN`）
- 测试 **truncate transform** 的分区裁剪
  - 数据类型：string、binary、int、bigint、decimal
  - 查询模式：等值查询（`=`）、IN 查询（`IN`）

**评估**：
- ⚠️ **存在问题**：我们刚刚实现的代码**不支持 bucket 和 truncate transform**
- ⚠️ **测试会失败**：这些测试可能会失败，因为 BE 端会跳过 bucket/truncate 的分区裁剪
- ⚠️ **需要调整**：需要说明不支持，或者调整测试预期

## 二、缺失的测试覆盖

### 2.1 缺失的转换函数测试

**我们刚刚实现的功能**：
- ✅ Day transform
- ✅ Year transform
- ✅ Month transform
- ✅ Hour transform

**现有测试覆盖**：
- ❌ **完全没有覆盖**：两个测试文件中都没有测试 day/year/month/hour transform

### 2.2 缺失的测试场景

1. **时间转换函数的基础测试**：
   - Day transform 的等值查询
   - Year transform 的等值查询
   - Month transform 的等值查询
   - Hour transform 的等值查询

2. **时间转换函数的范围查询**：
   - Day transform 的范围查询（`>=`, `<=`, `BETWEEN`）
   - Year transform 的范围查询
   - Month transform 的范围查询
   - Hour transform 的范围查询

3. **时间转换函数的 IN 查询**：
   - Day transform 的 IN 查询
   - Year transform 的 IN 查询
   - Month transform 的 IN 查询
   - Hour transform 的 IN 查询

4. **分区演进场景**：
   - 不同 specId 的分区裁剪
   - 分区演进前后的数据查询

## 三、测试建议

### 3.1 修改现有测试

**`test_iceberg_runtime_filter_partition_pruning_transform.groovy`**：

**选项1：添加注释说明不支持**
```groovy
// Note: Bucket and Truncate transforms are not supported for runtime filter partition pruning
// These tests verify that queries still work correctly (without partition pruning optimization)
def test_runtime_filter_partition_pruning_transform = {
    // Bucket partitions - not supported, but queries should still work
    qt_bucket_int_eq """
        select count(*) from bucket_int_4 where partition_key =
            (select partition_key from bucket_int_4
             group by partition_key having count(*) > 0
             order by partition_key desc limit 1);
    """
    // ... other bucket tests ...
    
    // Truncate partitions - not supported, but queries should still work
    qt_trunc_string_eq """
        select count(*) from truncate_string_3 where partition_key =
            (select partition_key from truncate_string_3
             group by partition_key having count(*) > 0
             order by partition_key desc limit 1);
    """
    // ... other truncate tests ...
}
```

**选项2：移除或跳过这些测试**
- 如果这些测试会失败，可以考虑暂时跳过或移除

### 3.2 新增测试用例

**建议创建新测试文件**：`test_iceberg_runtime_filter_partition_pruning_time_transforms.groovy`

```groovy
suite("test_iceberg_runtime_filter_partition_pruning_time_transforms", "p0,external,doris,external_docker,external_docker_doris") {
    // ... setup code ...
    
    def test_runtime_filter_partition_pruning_time_transforms = {
        // Day transform tests
        qt_day_eq """
            select count(*) from day_partitioned where ts =
                (select ts from day_partitioned
                 group by ts having count(*) > 0
                 order by ts desc limit 1);
        """
        qt_day_in """
            select count(*) from day_partitioned where ts in
                (select ts from day_partitioned
                 group by ts having count(*) > 0
                 order by ts desc limit 2);
        """
        qt_day_range """
            select count(*) from day_partitioned where ts >= '2024-01-15 00:00:00' 
                and ts < '2024-01-16 00:00:00';
        """
        qt_day_between """
            select count(*) from day_partitioned where ts between '2024-01-15 00:00:00' 
                and '2024-01-16 00:00:00';
        """
        
        // Year transform tests
        qt_year_eq """
            select count(*) from year_partitioned where ts =
                (select ts from year_partitioned
                 group by ts having count(*) > 0
                 order by ts desc limit 1);
        """
        qt_year_range """
            select count(*) from year_partitioned where ts >= '2024-01-01 00:00:00' 
                and ts < '2025-01-01 00:00:00';
        """
        
        // Month transform tests
        qt_month_eq """
            select count(*) from month_partitioned where ts =
                (select ts from month_partitioned
                 group by ts having count(*) > 0
                 order by ts desc limit 1);
        """
        qt_month_range """
            select count(*) from month_partitioned where ts >= '2024-01-01 00:00:00' 
                and ts < '2024-02-01 00:00:00';
        """
        
        // Hour transform tests
        qt_hour_eq """
            select count(*) from hour_partitioned where ts =
                (select ts from hour_partitioned
                 group by ts having count(*) > 0
                 order by ts desc limit 1);
        """
        qt_hour_range """
            select count(*) from hour_partitioned where ts >= '2024-01-15 10:00:00' 
                and ts < '2024-01-15 11:00:00';
        """
        
        // Partition evolution tests
        qt_partition_evolution_day """
            select count(*) from evolution_table where ts >= '2024-01-15 00:00:00' 
                and ts < '2024-01-16 00:00:00';
        """
    }
    
    try {
        sql """ set time_zone = 'Asia/Shanghai'; """
        sql """ set enable_runtime_filter_partition_prune = false; """
        test_runtime_filter_partition_pruning_time_transforms()
        sql """ set enable_runtime_filter_partition_prune = true; """
        test_runtime_filter_partition_pruning_time_transforms()
    } finally {
        sql """ unset variable time_zone; """
        sql """ set enable_runtime_filter_partition_prune = true; """
    }
}
```

### 3.3 测试表需求

需要在 `transform_partition_db` 数据库中创建以下测试表：

1. **day_partitioned**：`PARTITIONED BY (day(ts))`
2. **year_partitioned**：`PARTITIONED BY (year(ts))`
3. **month_partitioned**：`PARTITIONED BY (month(ts))`
4. **hour_partitioned**：`PARTITIONED BY (hour(ts))`
5. **evolution_table**：支持分区演进的表（用于测试不同 specId）

## 四、测试覆盖总结

### 4.1 当前覆盖情况

| 转换函数 | 测试覆盖 | 实现状态 | 测试状态 |
|---------|---------|---------|---------|
| Identity | ✅ 完整覆盖 | ✅ 已实现 | ✅ 测试通过 |
| Day | ❌ 未覆盖 | ✅ 已实现 | ⚠️ 需要添加测试 |
| Year | ❌ 未覆盖 | ✅ 已实现 | ⚠️ 需要添加测试 |
| Month | ❌ 未覆盖 | ✅ 已实现 | ⚠️ 需要添加测试 |
| Hour | ❌ 未覆盖 | ✅ 已实现 | ⚠️ 需要添加测试 |
| Bucket | ⚠️ 有测试但会失败 | ❌ 不支持 | ⚠️ 需要调整 |
| Truncate | ⚠️ 有测试但会失败 | ❌ 不支持 | ⚠️ 需要调整 |

### 4.2 优先级建议

1. **高优先级**：添加 Day/Year/Month/Hour 的测试用例
   - 这些是我们刚刚实现的功能
   - 需要验证功能正确性

2. **中优先级**：调整 Bucket/Truncate 的测试
   - 说明不支持，或者调整测试预期
   - 确保测试不会因为不支持而失败

3. **低优先级**：添加分区演进场景的测试
   - 验证不同 specId 的分区裁剪
   - 验证分区演进前后的数据查询

## 五、具体建议

### 5.1 立即行动项

1. **创建新的测试文件**：`test_iceberg_runtime_filter_partition_pruning_time_transforms.groovy`
   - 测试 Day/Year/Month/Hour transform
   - 覆盖等值查询、IN 查询、范围查询

2. **修改现有测试**：`test_iceberg_runtime_filter_partition_pruning_transform.groovy`
   - 添加注释说明 Bucket/Truncate 不支持
   - 或者调整测试预期（验证查询仍然正确，但不进行分区裁剪优化）

### 5.2 测试用例设计建议

**Day Transform 测试**：
```groovy
// 等值查询
qt_day_eq "select count(*) from day_partitioned where ts = '2024-01-15 10:30:00';"

// IN 查询
qt_day_in "select count(*) from day_partitioned where ts in ('2024-01-15 10:30:00', '2024-01-16 10:30:00');"

// 范围查询（应该能裁剪）
qt_day_range_ge "select count(*) from day_partitioned where ts >= '2024-01-15 00:00:00';"
qt_day_range_lt "select count(*) from day_partitioned where ts < '2024-01-16 00:00:00';"
qt_day_range_between "select count(*) from day_partitioned where ts between '2024-01-15 00:00:00' and '2024-01-16 00:00:00';"

// 跨分区范围查询（应该能裁剪多个分区）
qt_day_range_multi "select count(*) from day_partitioned where ts >= '2024-01-15 00:00:00' and ts < '2024-01-17 00:00:00';"
```

**Year/Month/Hour Transform 测试**：类似 Day，但范围更大

**分区演进测试**：
```groovy
// 测试不同 specId 的分区裁剪
qt_partition_evolution """
    select count(*) from evolution_table where ts >= '2024-01-15 00:00:00' 
        and ts < '2024-01-16 00:00:00';
"""
```

## 六、结论

### 6.1 测试合理性评估

1. **`test_iceberg_runtime_filter_partition_pruning.groovy`**：
   - ✅ **合理且完整**：覆盖了 identity transform 的各种场景

2. **`test_iceberg_runtime_filter_partition_pruning_transform.groovy`**：
   - ⚠️ **需要调整**：测试了不支持的 Bucket/Truncate transform
   - ⚠️ **可能失败**：需要说明不支持或调整预期

### 6.2 需要增加的测试

1. **必须添加**：Day/Year/Month/Hour transform 的测试用例
2. **建议添加**：分区演进场景的测试
3. **需要调整**：Bucket/Truncate 的测试说明或预期

### 6.3 推荐方案

1. **创建新测试文件**：专门测试时间转换函数
2. **修改现有测试**：添加注释说明 Bucket/Truncate 不支持
3. **确保测试通过**：验证查询正确性，即使不进行分区裁剪优化

