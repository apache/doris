# Iceberg 分区表删除分区键后查询问题分析

## 一、问题场景

对于一张 Iceberg 分区表，如果后面通过 `DROP PARTITION FIELD` 将分区键删除，查询时会发生什么问题？

### 1.1 场景示例

```sql
-- 初始分区规范（specId = 0）
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (day(ts));

-- 插入一些数据（使用 specId = 0）
INSERT INTO events VALUES (1, '2024-01-15 10:00:00');

-- 分区演进：删除分区字段（specId = 1）
ALTER TABLE events DROP PARTITION FIELD day(ts);

-- 插入新数据（使用 specId = 1，无分区）
INSERT INTO events VALUES (2, '2024-01-16 10:00:00');

-- 查询数据
SELECT * FROM events WHERE ts >= '2024-01-15' AND ts < '2024-01-17';
```

## 二、问题分析

### 2.1 分区规范演进

**删除分区键后的状态**：
- **旧数据（specId = 0）**：使用 `day(ts)` 分区规范，数据文件存储在分区目录中（如 `ts_day=2024-01-15/`）
- **新数据（specId = 1）**：使用无分区规范（unpartitioned），数据文件存储在表根目录

**关键点**：
- Iceberg 支持分区演进，表可以同时存在多个分区规范（specId）
- 删除分区键后，新数据使用新的分区规范，但历史数据保持不变

### 2.2 查询时的问题

#### 问题1：分区裁剪失效（部分数据）

**场景**：删除分区键后，查询使用源列谓词

```sql
-- 查询条件基于源列 ts
SELECT * FROM events WHERE ts >= '2024-01-15' AND ts < '2024-01-17';
```

**问题分析**：

1. **旧数据（specId = 0）的分区裁剪**：
   - 查询谓词：`ts >= '2024-01-15' AND ts < '2024-01-17'`
   - Iceberg 通过 `Projections.inclusive()` 将列谓词投影到分区谓词
   - 投影结果：`ts_day >= '2024-01-15' AND ts_day < '2024-01-17'`
   - ✅ **分区裁剪有效**：可以过滤掉不相关的分区，只扫描 `ts_day=2024-01-15` 和 `ts_day=2024-01-16` 的分区

2. **新数据（specId = 1）的分区裁剪**：
   - 新数据使用无分区规范（unpartitioned）
   - 查询谓词：`ts >= '2024-01-15' AND ts < '2024-01-17'`
   - ❌ **分区裁剪无效**：由于没有分区字段，Iceberg 无法进行分区裁剪
   - ⚠️ **需要全表扫描**：必须扫描所有无分区数据文件，然后在读取时应用谓词过滤

**影响**：
- 查询性能下降：新数据无法利用分区裁剪，需要扫描更多数据
- 资源消耗增加：需要读取和过滤更多数据文件

#### 问题2：多分区规范处理

**代码位置**：`IcebergUtils.getMatchingManifest()`

```java
public static CloseableIterable<ManifestFile> getMatchingManifest(
        List<ManifestFile> dataManifests,
        Map<Integer, PartitionSpec> specsById,
        Expression dataFilter) {
    LoadingCache<Integer, ManifestEvaluator> evalCache = Caffeine.newBuilder()
            .build(
                    specId -> {
                        PartitionSpec spec = specsById.get(specId);
                        return ManifestEvaluator.forPartitionFilter(
                                Expressions.and(
                                        Expressions.alwaysTrue(),
                                        Projections.inclusive(spec, true).project(dataFilter)),
                                spec,
                                true);
                    });
    // ...
}
```

**问题分析**：
- 对于每个 `specId`，Iceberg 会创建一个 `ManifestEvaluator`
- 对于 `specId = 0`（有分区字段），`Projections.inclusive()` 可以正确投影列谓词到分区谓词
- 对于 `specId = 1`（无分区字段），`Projections.inclusive()` 可能返回 `alwaysTrue()`，导致无法过滤 manifest

**影响**：
- 可能扫描不必要的 manifest 文件
- 对于无分区规范的数据，无法利用 manifest 级别的过滤

#### 问题3：Runtime Filter 分区裁剪失效

**场景**：启用 Runtime Filter 分区裁剪时

**代码位置**：`IcebergScanNode.createIcebergSplit()`

```java
if (isPartitionedTable) {
    PartitionData partitionData = (PartitionData) fileScanTask.file().partition();
    if (sessionVariable.isEnableRuntimeFilterPartitionPrune()) {
        Map<String, String> partitionInfoMap = partitionMapInfos.computeIfAbsent(partitionData, k -> {
            return IcebergUtils.getPartitionInfoMap(partitionData, sessionVariable.getTimeZone());
        });
        if (partitionInfoMap != null) {
            split.setIcebergPartitionValues(partitionInfoMap);
        }
    }
}
```

**问题分析**：
- 对于 `specId = 0` 的数据，`PartitionData` 包含分区值（如 `ts_day=2024-01-15`）
- 对于 `specId = 1` 的数据，`PartitionData` 可能为空或只包含默认值
- Runtime Filter 需要知道分区字段的转换函数和源列信息，才能正确进行分区裁剪
- 如果分区字段被删除，Runtime Filter 可能无法正确匹配

**影响**：
- Runtime Filter 分区裁剪可能失效
- 无法利用 Runtime Filter 优化查询性能

#### 问题4：分区键对应的列也被删除

**场景**：删除分区键后，如果源列也被删除

```sql
-- 初始分区规范（specId = 0）
CREATE TABLE events (
    id BIGINT,
    ts TIMESTAMP
) PARTITIONED BY (day(ts));

-- 删除分区键
ALTER TABLE events DROP PARTITION FIELD day(ts);

-- 删除源列
ALTER TABLE events DROP COLUMN ts;

-- 查询（会报错）
SELECT * FROM events WHERE ts >= '2024-01-15';  -- ❌ 列 ts 不存在
```

**问题分析**：
- 如果源列被删除，查询时使用该列的谓词会直接报错
- 但是，旧数据（specId = 0）仍然使用该列的分区规范
- 查询旧数据时，Iceberg 需要知道分区字段对应的源列，但源列已经不存在

**代码位置**：`IcebergUtils.convertToIcebergExpr()`

```java
String colName = slotRef.getColumnName();
Types.NestedField nestedField = schema.caseInsensitiveFindField(colName);
if (nestedField == null) {
    // 列不存在，无法转换谓词
    return null;
}
```

**影响**：
- 查询时无法使用已删除列的谓词
- 如果旧数据使用该列的分区规范，可能无法正确查询

## 三、具体问题总结

### 3.1 查询性能问题

| 问题 | 影响 | 严重程度 |
|------|------|----------|
| 新数据无法分区裁剪 | 需要全表扫描，性能下降 | ⚠️ 中等 |
| Manifest 过滤失效 | 扫描不必要的 manifest 文件 | ⚠️ 中等 |
| Runtime Filter 失效 | 无法利用 Runtime Filter 优化 | ⚠️ 中等 |

### 3.2 功能问题

| 问题 | 影响 | 严重程度 |
|------|------|----------|
| 源列删除后无法查询 | 查询报错，无法访问数据 | ❌ 严重 |
| 多分区规范兼容性 | 不同 specId 的数据处理方式不同 | ⚠️ 中等 |

## 四、解决方案建议

### 4.1 查询优化

1. **显式处理多分区规范**：
   - 在查询时，识别不同 `specId` 的数据
   - 对于有分区字段的 `specId`，使用分区裁剪
   - 对于无分区字段的 `specId`，使用其他优化策略（如文件级别过滤）

2. **改进 Runtime Filter**：
   - 在 FE 端传递分区字段到源列的映射信息
   - 在 BE 端正确处理不同 `specId` 的分区值
   - 支持分区字段被删除后的降级处理

### 4.2 用户建议

1. **谨慎删除分区键**：
   - 删除分区键前，评估对查询性能的影响
   - 考虑使用 `REPLACE PARTITION FIELD` 而不是 `DROP PARTITION FIELD`

2. **数据重写**：
   - 如果删除分区键后性能下降严重，考虑重写数据文件，统一使用新的分区规范
   - 使用 `REWRITE DATA FILES` 操作重写数据

3. **避免删除源列**：
   - 如果分区键被删除，建议保留源列，以便查询时可以使用列谓词
   - 如果必须删除源列，需要先重写数据，统一分区规范

## 五、代码实现细节

### 5.1 查询流程

**当前实现**：
```java
// IcebergScanNode.createTableScan()
TableScan scan = icebergTable.newScan();
List<Expression> expressions = new ArrayList<>();
for (Expr conjunct : conjuncts) {
    Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema());
    if (expression != null) {
        expressions.add(expression);
    }
}
for (Expression predicate : expressions) {
    scan = scan.filter(predicate);  // 下推给 Iceberg API
}
```

**问题**：
- 使用当前 schema（可能不包含已删除的分区字段）
- 对于不同 `specId` 的数据，Iceberg 会分别处理，但可能无法充分利用分区裁剪

### 5.2 分区规范处理

**当前实现**：
```java
// IcebergUtils.getMatchingManifest()
public static CloseableIterable<ManifestFile> getMatchingManifest(
        List<ManifestFile> dataManifests,
        Map<Integer, PartitionSpec> specsById,
        Expression dataFilter) {
    LoadingCache<Integer, ManifestEvaluator> evalCache = Caffeine.newBuilder()
            .build(
                    specId -> {
                        PartitionSpec spec = specsById.get(specId);
                        return ManifestEvaluator.forPartitionFilter(
                                Expressions.and(
                                        Expressions.alwaysTrue(),
                                        Projections.inclusive(spec, true).project(dataFilter)),
                                spec,
                                true);
                    });
    // ...
}
```

**问题**：
- 对于无分区规范（unpartitioned）的 `specId`，`Projections.inclusive()` 可能返回 `alwaysTrue()`
- 导致无法过滤 manifest，需要扫描所有 manifest 文件

## 六、测试建议

### 6.1 测试场景

1. **删除分区键后查询**：
   ```sql
   -- 创建表并插入数据
   CREATE TABLE test (id BIGINT, ts TIMESTAMP) PARTITIONED BY (day(ts));
   INSERT INTO test VALUES (1, '2024-01-15 10:00:00');
   
   -- 删除分区键
   ALTER TABLE test DROP PARTITION FIELD day(ts);
   
   -- 插入新数据
   INSERT INTO test VALUES (2, '2024-01-16 10:00:00');
   
   -- 查询（应该能正常返回数据，但性能可能下降）
   SELECT * FROM test WHERE ts >= '2024-01-15' AND ts < '2024-01-17';
   ```

2. **删除分区键和源列后查询**：
   ```sql
   -- 删除源列
   ALTER TABLE test DROP COLUMN ts;
   
   -- 查询（应该报错）
   SELECT * FROM test WHERE ts >= '2024-01-15';  -- ❌ 列不存在
   ```

3. **性能测试**：
   - 对比删除分区键前后的查询性能
   - 检查分区裁剪是否生效
   - 检查 Runtime Filter 是否生效

### 6.2 验证点

1. ✅ **功能正确性**：查询应该能返回正确的结果
2. ⚠️ **性能影响**：新数据无法利用分区裁剪，性能可能下降
3. ⚠️ **兼容性**：不同 `specId` 的数据应该都能正确查询

## 七、总结

删除分区键后，查询时主要会发生以下问题：

1. **分区裁剪失效**：新数据（使用新的分区规范）无法利用分区裁剪，需要全表扫描
2. **性能下降**：查询性能可能显著下降，特别是对于大数据量的表
3. **Runtime Filter 失效**：Runtime Filter 分区裁剪可能无法正常工作
4. **源列删除问题**：如果源列也被删除，查询时无法使用该列的谓词

**建议**：
- 谨慎删除分区键，评估对查询性能的影响
- 如果必须删除，考虑重写数据文件，统一使用新的分区规范
- 避免删除源列，以便查询时可以使用列谓词

