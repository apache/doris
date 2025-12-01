# Doris 查询 Iceberg 分区表逻辑分析与改进建议

## 一、当前实现分析

### 1.1 分区谓词转换流程

**当前实现位置**：
- `IcebergUtils.convertToIcebergExpr()`: 将 Doris 的列谓词转换为 Iceberg Expression
- `IcebergScanNode.createTableScan()`: 通过 `TableScan.filter()` 下推谓词

**实现逻辑**：
```java
// IcebergScanNode.java:326-354
TableScan scan = icebergTable.newScan();
List<Expression> expressions = new ArrayList<>();
for (Expr conjunct : conjuncts) {
    Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema());
    if (expression != null) {
        expressions.add(expression);
    }
}
for (Expression predicate : expressions) {
    scan = scan.filter(predicate);
}
```

**优点**：
- ✅ 利用了 Iceberg 的隐藏分区（Hidden Partitioning）特性
- ✅ 用户只需写列谓词（如 `WHERE ts >= '2024-01-01'`），Iceberg 自动转换为分区谓词
- ✅ 支持多种转换函数（identity、day、year、month、hour、bucket、truncate）

**问题**：
- ⚠️ 没有显式利用分区规范（Partition Spec）信息进行优化
- ⚠️ 对于复杂转换（如 `bucket(16, id)`），可能无法充分利用分区裁剪

### 1.2 多分区规范（Partition Spec）处理

**当前实现位置**：
- `IcebergUtils.getMatchingManifest()`: 处理多个 specId 的 manifest 过滤
- `IcebergUtils.generateIcebergPartition()`: 根据 specId 获取对应的 PartitionSpec

**实现逻辑**：
```java
// IcebergUtils.java:949-975
public static CloseableIterable<ManifestFile> getMatchingManifest(
        List<ManifestFile> dataManifests,
        Map<Integer, PartitionSpec> specsById,
        Expression dataFilter) {
    LoadingCache<Integer, ManifestEvaluator> evalCache = Caffeine.newBuilder()
            .build(specId -> {
                PartitionSpec spec = specsById.get(specId);
                return ManifestEvaluator.forPartitionFilter(
                        Expressions.and(
                                Expressions.alwaysTrue(),
                                Projections.inclusive(spec, true).project(dataFilter)),
                        spec, true);
            });
    // ...
}
```

**优点**：
- ✅ 支持分区演化（Partition Evolution），可以处理多个不同的分区规范
- ✅ 为每个 specId 创建独立的 ManifestEvaluator

**问题**：
- ⚠️ 在 `IcebergScanNode.createIcebergSplit()` 中，只使用了 `fileScanTask.file().partition()`，没有考虑 specId
- ⚠️ `getPartitionInfoMap()` 方法没有接收 specId 参数，可能无法正确处理不同规范的分区值

### 1.3 分区信息获取

**当前实现位置**：
- `IcebergUtils.getPartitionInfoMap()`: 从 PartitionData 中提取分区信息
- `IcebergScanNode.createIcebergSplit()`: 为每个 split 设置分区值

**实现逻辑**：
```java
// IcebergScanNode.java:384-397
if (isPartitionedTable) {
    PartitionData partitionData = (PartitionData) fileScanTask.file().partition();
    if (sessionVariable.isEnableRuntimeFilterPartitionPrune()) {
        Map<String, String> partitionInfoMap = partitionMapInfos.computeIfAbsent(
                partitionData, k -> {
                    return IcebergUtils.getPartitionInfoMap(partitionData, 
                            sessionVariable.getTimeZone());
                });
        if (partitionInfoMap != null) {
            split.setIcebergPartitionValues(partitionInfoMap);
        }
    }
}
```

**问题**：
- ❌ **关键问题**：`getPartitionInfoMap()` 只从 `PartitionData` 中提取分区值，但没有考虑：
  1. **分区字段的转换函数（Transform）**：对于 `day(ts)` 转换，分区值是日期，但无法直接映射回源列 `ts`
  2. **分区字段的源列信息**：不知道分区字段对应哪个源列
  3. **specId**：不同 specId 可能有不同的分区规范

### 1.4 分区裁剪优化

**当前实现**：
- 依赖 Iceberg 的 `TableScan.filter()` 进行分区裁剪
- 通过 `Projections.inclusive()` 将列谓词投影到分区谓词

**问题**：
- ⚠️ 没有充分利用分区规范信息进行额外的优化
- ⚠️ 对于某些转换函数（如 `bucket`、`truncate`），可能无法进行精确的分区裁剪

## 二、与 Iceberg 分区规范文档的对比

### 2.1 分区规范的核心概念

根据文档，分区规范包含：
1. **规范 ID（Spec ID）**：唯一标识一个分区规范
2. **分区字段列表**：每个字段包含：
   - 源列 ID（Source Column ID）
   - 分区字段 ID（Partition Field ID）
   - 转换函数（Transform）
   - 分区名称（Partition Name）

### 2.2 Doris 当前实现与文档的差距

| 文档要求 | Doris 当前实现 | 差距 |
|---------|--------------|------|
| 支持多分区规范 | ✅ 支持 | 无 |
| 利用转换函数进行分区裁剪 | ⚠️ 部分支持 | 依赖 Iceberg API，没有显式优化 |
| 分区值到源列的映射 | ❌ 缺失 | 无法从分区值反推源列范围 |
| 分区字段信息获取 | ⚠️ 部分支持 | `getPartitionInfoMap()` 缺少 specId 和转换函数信息 |

## 三、需要改进的地方

### 3.1 改进分区信息获取（高优先级）

**问题**：
`getPartitionInfoMap()` 方法缺少分区规范信息，无法正确处理转换函数。

**改进方案**：
```java
// 改进后的方法签名
public static Map<String, String> getPartitionInfoMap(
        PartitionData partitionData, 
        PartitionSpec partitionSpec,  // 新增：分区规范
        String timeZone) {
    Map<String, String> partitionInfoMap = new HashMap<>();
    List<PartitionField> partitionFields = partitionSpec.fields();
    
    for (int i = 0; i < partitionFields.size(); i++) {
        PartitionField partitionField = partitionFields.get(i);
        String fieldName = partitionField.name();
        String transform = partitionField.transform().toString();
        int sourceColumnId = partitionField.sourceId();
        
        // 获取分区值
        Object partitionValue = partitionData.get(i, partitionSpec.javaClasses()[i]);
        
        // 根据转换函数类型，可能需要特殊处理
        // 例如：对于 day(ts)，分区值是日期，但需要记录源列信息
        String partitionString = serializePartitionValue(
                partitionSpec.partitionType().fieldType(fieldName),
                partitionValue, timeZone);
        
        partitionInfoMap.put(fieldName, partitionString);
        
        // 可选：添加源列信息，用于分区裁剪优化
        // partitionInfoMap.put(fieldName + "_source_column", 
        //     table.schema().findColumnName(sourceColumnId));
    }
    
    return partitionInfoMap;
}
```

**调用处修改**：
```java
// IcebergScanNode.java:384-397
if (isPartitionedTable) {
    PartitionData partitionData = (PartitionData) fileScanTask.file().partition();
    int specId = fileScanTask.file().specId();  // 获取 specId
    PartitionSpec partitionSpec = icebergTable.specs().get(specId);  // 获取对应的规范
    
    if (sessionVariable.isEnableRuntimeFilterPartitionPrune()) {
        Map<String, String> partitionInfoMap = partitionMapInfos.computeIfAbsent(
                partitionData, k -> {
                    return IcebergUtils.getPartitionInfoMap(
                            partitionData, partitionSpec,  // 传入 partitionSpec
                            sessionVariable.getTimeZone());
                });
        // ...
    }
}
```

### 3.2 增强分区谓词转换（中优先级）

**问题**：
对于某些转换函数（如 `bucket`、`truncate`），可能无法充分利用分区裁剪。

**改进方案**：
1. **显式利用分区规范信息**：
   - 在转换列谓词时，检查是否有对应的分区字段
   - 对于 identity 转换，可以直接使用列谓词
   - 对于时间转换（day、year、month、hour），可以显式转换谓词

2. **优化 bucket 转换的分区裁剪**：
   ```java
   // 对于 bucket(16, id) 分区，如果查询条件是 id = 100
   // 可以计算 bucket_id = hash(100) % 16，然后过滤对应的分区
   ```

### 3.3 改进多分区规范处理（中优先级）

**问题**：
在 `createIcebergSplit()` 中，没有充分利用 specId 信息。

**改进方案**：
1. **在 Split 中保存 specId**：
   ```java
   // IcebergSplit.java
   private int specId;  // 新增字段
   
   // 在 createIcebergSplit() 中设置
   split.setSpecId(fileScanTask.file().specId());
   ```

2. **根据 specId 获取正确的分区规范**：
   ```java
   // 在需要分区信息的地方，使用正确的 specId
   PartitionSpec partitionSpec = icebergTable.specs().get(specId);
   ```

### 3.4 优化分区裁剪逻辑（低优先级）

**问题**：
完全依赖 Iceberg API，没有额外的优化。

**改进方案**：
1. **在 FE 端进行分区裁剪预检查**：
   - 对于简单的分区谓词（如 `ts_day = '2024-01-01'`），可以在 FE 端预先过滤
   - 减少发送到 BE 端的 split 数量

2. **利用分区统计信息**：
   - 如果 Iceberg 表有分区统计信息，可以利用这些信息进行更精确的分区裁剪

### 3.5 支持分区演化场景（低优先级）

**问题**：
虽然代码中处理了多个 specId，但在某些场景下可能不够完善。

**改进方案**：
1. **在查询时显式处理分区演化**：
   - 对于不同 specId 的文件，使用对应的分区规范进行处理
   - 确保分区裁剪逻辑对每个 specId 都正确

2. **优化分区信息缓存**：
   - 当前 `partitionMapInfos` 使用 `PartitionData` 作为 key，但不同 specId 可能有相同的 PartitionData
   - 建议使用 `(specId, PartitionData)` 作为 key

## 四、具体改进建议

### 4.1 立即改进（高优先级）

1. **修改 `getPartitionInfoMap()` 方法**：
   - 添加 `PartitionSpec` 参数
   - 在方法内部利用分区规范信息
   - 可选：添加源列信息到返回的 Map 中

2. **修改 `createIcebergSplit()` 方法**：
   - 获取 `fileScanTask.file().specId()`
   - 获取对应的 `PartitionSpec`
   - 将 `PartitionSpec` 传递给 `getPartitionInfoMap()`

### 4.2 中期改进（中优先级）

1. **在 `IcebergSplit` 中添加 specId 字段**：
   - 保存每个 split 使用的 specId
   - 在需要时使用正确的分区规范

2. **优化分区谓词转换**：
   - 显式利用分区规范信息
   - 对于某些转换函数，进行额外的优化

### 4.3 长期改进（低优先级）

1. **FE 端分区裁剪优化**：
   - 在 FE 端进行分区裁剪预检查
   - 减少发送到 BE 端的 split 数量

2. **分区信息缓存优化**：
   - 使用 `(specId, PartitionData)` 作为缓存 key
   - 避免不同 specId 之间的冲突

## 五、代码示例

### 5.1 改进后的 `getPartitionInfoMap()` 方法

```java
public static Map<String, String> getPartitionInfoMap(
        PartitionData partitionData,
        PartitionSpec partitionSpec,
        String timeZone) {
    Map<String, String> partitionInfoMap = new HashMap<>();
    List<PartitionField> partitionFields = partitionSpec.fields();
    
    for (int i = 0; i < partitionFields.size(); i++) {
        PartitionField partitionField = partitionFields.get(i);
        String fieldName = partitionField.name();
        String transform = partitionField.transform().toString();
        
        // 获取分区值的类型
        Types.NestedField partitionTypeField = partitionSpec.partitionType()
                .asNestedType().fields().get(i);
        
        // 获取分区值
        Object partitionValue = partitionData.get(i, partitionSpec.javaClasses()[i]);
        
        // 序列化分区值
        String partitionString = serializePartitionValue(
                partitionTypeField.type(), partitionValue, timeZone);
        
        partitionInfoMap.put(fieldName, partitionString);
        
        // 可选：添加转换函数信息，用于调试和优化
        partitionInfoMap.put(fieldName + "_transform", transform);
    }
    
    return partitionInfoMap;
}
```

### 5.2 改进后的 `createIcebergSplit()` 方法

```java
private Split createIcebergSplit(FileScanTask fileScanTask) {
    // ... 现有代码 ...
    
    if (isPartitionedTable) {
        PartitionData partitionData = (PartitionData) fileScanTask.file().partition();
        int specId = fileScanTask.file().specId();  // 获取 specId
        PartitionSpec partitionSpec = icebergTable.specs().get(specId);  // 获取对应的规范
        
        if (sessionVariable.isEnableRuntimeFilterPartitionPrune()) {
            // 使用 (specId, PartitionData) 作为缓存 key，避免不同 specId 之间的冲突
            String cacheKey = specId + "_" + partitionData.toString();
            Map<String, String> partitionInfoMap = partitionMapInfos.computeIfAbsent(
                    partitionData, k -> {
                        return IcebergUtils.getPartitionInfoMap(
                                partitionData, partitionSpec,  // 传入 partitionSpec
                                sessionVariable.getTimeZone());
                    });
            if (partitionInfoMap != null) {
                split.setIcebergPartitionValues(partitionInfoMap);
            }
        } else {
            partitionMapInfos.put(partitionData, null);
        }
    }
    
    return split;
}
```

## 六、总结

### 6.1 当前实现的优点

1. ✅ 利用了 Iceberg 的隐藏分区特性
2. ✅ 支持多分区规范（分区演化）
3. ✅ 基本的谓词下推和分区裁剪功能正常

### 6.2 主要问题

1. ❌ `getPartitionInfoMap()` 缺少分区规范信息，无法正确处理转换函数
2. ⚠️ 没有充分利用分区规范信息进行优化
3. ⚠️ 分区信息缓存可能在不同 specId 之间产生冲突

### 6.3 改进优先级

1. **高优先级**：改进 `getPartitionInfoMap()` 方法，添加 `PartitionSpec` 参数
2. **中优先级**：在 `IcebergSplit` 中添加 specId 字段，优化分区谓词转换
3. **低优先级**：FE 端分区裁剪优化，分区信息缓存优化

### 6.4 预期效果

改进后，Doris 将能够：
1. 更准确地处理不同分区规范的分区值
2. 更好地利用分区规范信息进行查询优化
3. 正确处理分区演化场景
4. 提供更精确的分区裁剪

