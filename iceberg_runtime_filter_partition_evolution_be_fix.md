# Iceberg Runtime Filter 分区裁剪 BE 端修复方案

## 一、问题分析

### 1.1 当前实现

**FE 端**（已修复）：
- 在 `IcebergScanNode.setIcebergParams()` 中，设置 `columns_from_path_keys` 和 `columns_from_path`
- `columns_from_path_keys` 是分区字段名（如 `ts_day`，通过 `day(ts)` 转换）
- `columns_from_path` 是分区值（如 `2024-01-15`）

**BE 端**（存在问题）：
- 在 `FileScanner._generate_partition_columns()` 中，通过 `columns_from_path_keys` 和 `columns_from_path` 获取分区值
- 在 `FileScanner._process_runtime_filters_partition_prune()` 中，将分区值反序列化为列，然后执行 Runtime Filter 的谓词来判断是否过滤

### 1.2 问题描述

对于非 identity transform 的分区字段（如 `day(ts)`），存在以下问题：

1. **分区字段名与源列名不匹配**：
   - 分区字段名：`ts_day`（Iceberg 自动生成）
   - 源列名：`ts`
   - BE 端通过 `_partition_slot_index_map` 查找分区字段名对应的 SlotDescriptor，但可能找不到对应的源列

2. **Runtime Filter 无法正确匹配**：
   - Runtime Filter 的谓词是基于源列（如 `ts >= '2024-01-15 00:00:00'`）
   - 但 BE 端只有分区值（如 `2024-01-15`），不知道如何将其转换回源列值范围

3. **分区演进支持不完整**：
   - 不同 `specId` 可能有不同的分区字段名
   - BE 端无法区分不同 `specId` 的分区字段

### 1.3 示例

**场景**：Iceberg 表使用 `day(ts)` 分区

```sql
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (day(ts));
```

**问题**：
- 分区字段名：`ts_day`
- 分区值：`2024-01-15`
- Runtime Filter 谓词：`ts >= '2024-01-15 00:00:00' AND ts < '2024-01-16 00:00:00'`
- BE 端需要知道：分区值 `2024-01-15` 对应的源列值范围是 `['2024-01-15 00:00:00', '2024-01-16 00:00:00')`

## 二、解决方案

### 2.1 方案概述

**核心思路**：在 FE 端传递分区字段到源列的映射信息，以及转换函数信息，让 BE 端能够正确进行 Runtime Filter 分区裁剪。

### 2.2 需要传递的信息

1. **分区字段到源列的映射**：
   - 分区字段名 → 源列名
   - 例如：`ts_day` → `ts`

2. **转换函数信息**：
   - 转换函数类型（identity、day、year、month、hour、bucket、truncate）
   - 转换函数参数（如 bucket 数量、truncate 宽度）

3. **分区规范 ID（specId）**：
   - 用于支持分区演进

### 2.3 实现方案

#### 方案1：扩展 TFileRangeDesc（推荐）

在 `TFileRangeDesc` 中添加新字段：

```thrift
struct TFileRangeDesc {
    // ... existing fields ...
    
    // 分区字段到源列的映射
    // key: 分区字段名, value: 源列名
    1: optional map<string, string> partition_field_to_source_column_map;
    
    // 分区字段的转换函数信息
    // key: 分区字段名, value: 转换函数字符串（如 "day", "bucket[16]", "truncate[4]"）
    2: optional map<string, string> partition_field_transforms;
    
    // 分区规范 ID（用于分区演进）
    3: optional i32 partition_spec_id;
}
```

**优点**：
- 信息完整，BE 端可以正确处理所有转换函数
- 支持分区演进
- 向后兼容（使用 optional 字段）

**缺点**：
- 需要修改 Thrift 定义
- 需要重新生成代码

#### 方案2：在现有字段中编码信息（临时方案）

在 `columns_from_path_keys` 中编码源列名和转换函数信息：

```
columns_from_path_keys = ["ts_day:ts:day"]
```

**优点**：
- 不需要修改 Thrift 定义
- 实现简单

**缺点**：
- 不够优雅
- 解析复杂
- 扩展性差

#### 方案3：仅支持 identity transform（当前限制）

如果只支持 identity transform，则不需要修改，因为分区字段名就是源列名。

**优点**：
- 不需要修改代码
- 实现简单

**缺点**：
- 功能受限，不支持常见的转换函数（如 day、year、month、hour、bucket、truncate）

### 2.4 推荐实现（方案1）

#### 2.4.1 FE 端修改

在 `IcebergScanNode.setIcebergParams()` 中，添加分区字段映射信息：

```java
private void setIcebergParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit) {
    // ... existing code ...
    
    Map<String, String> partitionValues = icebergSplit.getIcebergPartitionValues();
    if (partitionValues != null) {
        // ... existing code to set columns_from_path_keys and columns_from_path ...
        
        // 获取 specId 和 PartitionSpec
        int specId = icebergSplit.getSpecId(); // 需要在 IcebergSplit 中添加 specId 字段
        PartitionSpec partitionSpec = icebergTable.specs().get(specId);
        
        // 构建分区字段到源列的映射
        Map<String, String> partitionFieldToSourceColumnMap = new HashMap<>();
        Map<String, String> partitionFieldTransforms = new HashMap<>();
        
        for (PartitionField partitionField : partitionSpec.fields()) {
            String partitionFieldName = partitionField.name();
            String sourceColumnName = icebergTable.schema().findColumnName(partitionField.sourceId());
            String transform = partitionField.transform().toString();
            
            partitionFieldToSourceColumnMap.put(partitionFieldName, sourceColumnName);
            partitionFieldTransforms.put(partitionFieldName, transform);
        }
        
        rangeDesc.setPartitionFieldToSourceColumnMap(partitionFieldToSourceColumnMap);
        rangeDesc.setPartitionFieldTransforms(partitionFieldTransforms);
        rangeDesc.setPartitionSpecId(specId);
    }
}
```

#### 2.4.2 BE 端修改

在 `FileScanner._process_runtime_filters_partition_prune()` 中，使用映射信息进行转换：

```cpp
Status FileScanner::_process_runtime_filters_partition_prune(bool& can_filter_all) {
    // ... existing code to get partition values ...
    
    // 1. 获取分区字段到源列的映射
    std::map<std::string, std::string> partition_field_to_source_column_map;
    std::map<std::string, std::string> partition_field_transforms;
    
    if (range.__isset.partition_field_to_source_column_map) {
        partition_field_to_source_column_map = range.partition_field_to_source_column_map;
    }
    if (range.__isset.partition_field_transforms) {
        partition_field_transforms = range.partition_field_transforms;
    }
    
    // 2. 对于每个分区字段，根据转换函数将分区值转换回源列值范围
    for (auto const& partition_col_desc : _partition_col_descs) {
        const auto& [partition_value, partition_slot_desc] = partition_col_desc.second;
        std::string partition_field_name = partition_slot_desc->col_name();
        
        // 2.1 获取源列名和转换函数
        auto source_col_it = partition_field_to_source_column_map.find(partition_field_name);
        auto transform_it = partition_field_transforms.find(partition_field_name);
        
        if (source_col_it == partition_field_to_source_column_map.end() ||
            transform_it == partition_field_transforms.end()) {
            // 如果没有映射信息，假设是 identity transform
            // 使用分区字段名作为源列名
            continue;
        }
        
        std::string source_column_name = source_col_it->second;
        std::string transform = transform_it->second;
        
        // 2.2 根据转换函数将分区值转换回源列值范围
        // 例如：day transform，分区值 "2024-01-15" -> 源列值范围 ['2024-01-15 00:00:00', '2024-01-16 00:00:00')
        // 这个转换逻辑需要根据不同的 transform 类型实现
        // ...
    }
    
    // ... existing code to execute runtime filter ...
}
```

#### 2.4.3 转换函数实现

需要在 BE 端实现各种转换函数的反向转换逻辑：

1. **identity**：分区值 = 源列值
2. **day**：分区值 `2024-01-15` → 源列值范围 `['2024-01-15 00:00:00', '2024-01-16 00:00:00')`
3. **year**：分区值 `2024` → 源列值范围 `['2024-01-01 00:00:00', '2025-01-01 00:00:00')`
4. **month**：分区值 `2024-01` → 源列值范围 `['2024-01-01 00:00:00', '2024-02-01 00:00:00')`
5. **hour**：分区值 `2024-01-15-10` → 源列值范围 `['2024-01-15 10:00:00', '2024-01-15 11:00:00')`
6. **bucket**：无法反向转换，需要特殊处理
7. **truncate**：无法反向转换，需要特殊处理

## 三、实施步骤

### 3.1 第一阶段：Thrift 定义修改

1. 修改 `gensrc/thrift/PlanNodes.thrift`，添加新字段
2. 重新生成 Thrift 代码

### 3.2 第二阶段：FE 端实现

1. 在 `IcebergSplit` 中添加 `specId` 字段
2. 在 `IcebergScanNode.createIcebergSplit()` 中设置 `specId`
3. 在 `IcebergScanNode.setIcebergParams()` 中构建映射信息并设置到 `TFileRangeDesc`

### 3.3 第三阶段：BE 端实现

1. 在 `FileScanner` 中读取映射信息
2. 实现各种转换函数的反向转换逻辑
3. 在 `_process_runtime_filters_partition_prune()` 中使用转换后的值进行过滤

### 3.4 第四阶段：测试

1. 测试 identity transform
2. 测试时间转换（day、year、month、hour）
3. 测试 bucket 和 truncate（如果支持）
4. 测试分区演进场景

## 四、注意事项

1. **向后兼容**：使用 optional 字段，确保向后兼容
2. **性能考虑**：映射信息的构建和转换可能影响性能，需要优化
3. **边界情况**：处理 NULL 值、边界值等特殊情况
4. **bucket 和 truncate**：这两个转换函数无法完全反向转换，可能需要特殊处理或限制支持

## 五、总结

BE 端的 Runtime Filter 分区裁剪需要知道分区字段到源列的映射以及转换函数信息，才能正确进行过滤。推荐使用方案1（扩展 TFileRangeDesc），虽然需要修改 Thrift 定义，但信息完整、扩展性好，能够支持分区演进。


