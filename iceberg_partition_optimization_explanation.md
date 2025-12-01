# Iceberg 分区优化问题详细解释

## 问题1：没有显式利用分区规范（Partition Spec）信息进行优化

### 当前实现方式

**代码位置**：`IcebergScanNode.createTableScan()`

```java
// 当前实现：简单地将列谓词转换为 Iceberg Expression，然后交给 Iceberg API 处理
TableScan scan = icebergTable.newScan();
List<Expression> expressions = new ArrayList<>();
for (Expr conjunct : conjuncts) {
    Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema());
    if (expression != null) {
        expressions.add(expression);
    }
}
for (Expression predicate : expressions) {
    scan = scan.filter(predicate);  // 直接下推给 Iceberg API
}
```

### 重要澄清：显式优化 vs Iceberg API 的区别

**关键问题**：显式优化和调用 Iceberg API 进行裁剪有什么区别？

**实际情况**：
1. ✅ **Iceberg API 已经能够正确处理分区裁剪**：
   - Iceberg 的 `TableScan.filter()` 会通过 `Projections.inclusive()` 将列谓词投影到分区谓词
   - 对于大多数转换函数（`identity`、`day`、`year`、`month`、`hour`），Iceberg 能够正确进行分区裁剪
   - 对于 `bucket` 转换，Iceberg 也能处理等值查询（`id = 100`）

2. ⚠️ **显式优化的实际价值有限**：
   - 如果 Iceberg API 已经能够正确处理分区裁剪，那么 Doris 显式利用分区规范信息进行优化，**可能确实没有太大区别**
   - 显式优化主要是重复 Iceberg 已经做的工作

### 真正的问题：不是"显式优化"，而是"正确获取和使用分区信息"

**实际需要改进的地方**：

1. **`getPartitionInfoMap()` 缺少分区规范信息**：
   - 当前实现只从 `PartitionData` 中提取分区值
   - 没有考虑分区字段的转换函数（Transform）和源列信息
   - **这会影响 Runtime Filter 和其他优化功能**，而不是分区裁剪本身

2. **多分区规范处理不完善**：
   - 在 `createIcebergSplit()` 中，没有考虑 specId
   - 可能导致不同分区规范之间的冲突

3. **查询计划优化**：
   - 如果 Doris 知道分区信息，可以在查询计划阶段进行优化
   - 比如估算扫描的数据量、优化 join 顺序等
   - 但这些优化不依赖于"显式分区裁剪"，而是依赖于"分区信息获取"

### 实际区别分析

**场景**：表按 `day(ts)` 分区，查询条件是 `WHERE ts >= '2024-01-01' AND ts < '2024-01-05'`

**当前实现（调用 Iceberg API）**：
```java
// 1. 将列谓词转换为 Iceberg Expression
Expression expr = Expressions.and(
    Expressions.greaterThanOrEqual("ts", "2024-01-01"),
    Expressions.lessThan("ts", "2024-01-05")
);

// 2. 直接下推给 Iceberg
scan = scan.filter(expr);

// 3. Iceberg 内部处理
// Iceberg 会通过 Projections.inclusive() 将列谓词投影到分区谓词
// 最终过滤出 ts_day 在 ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'] 的分区
// ✅ Iceberg 已经能够正确处理，分区裁剪是有效的
```

**显式优化（Doris 自己实现）**：
```java
// 1. 获取分区规范
PartitionSpec spec = icebergTable.spec();
// 2. 检查查询列是否有对应的分区字段
// 3. 显式计算分区值范围
// 4. 构建分区谓词
// ❌ 但这只是重复 Iceberg 已经做的工作，没有实际价值
```

### 结论：显式优化对于分区裁剪本身没有太大区别

**原因**：
1. ✅ Iceberg API 已经能够正确处理分区裁剪
2. ✅ 对于大多数转换函数，Iceberg 的分区裁剪是有效的
3. ❌ Doris 显式实现分区裁剪，只是重复 Iceberg 的工作，没有实际价值

**但是，显式获取分区信息仍然有价值**：
1. **Runtime Filter 优化**：需要知道分区字段的转换函数和源列信息
2. **查询计划优化**：需要知道分区信息来估算扫描的数据量
3. **统计信息利用**：需要分区信息来结合统计信息进行优化

---

## 问题2：对于复杂转换（如 `bucket(16, id)`），可能无法充分利用分区裁剪

### Bucket 转换的特点

**Bucket 转换**：`bucket(16, id)` 表示将 `id` 列的值通过哈希函数映射到 0-15 的桶中。

**转换公式**：
```
bucket_id = hash(id) % 16
```

**关键特性**：
- ✅ **单向映射**：从 `id` 值可以计算出 `bucket_id`
- ❌ **不可逆**：从 `bucket_id` **无法**反推出 `id` 的值范围
- ❌ **多对一映射**：多个不同的 `id` 值可能映射到同一个 `bucket_id`

### 重要澄清：Iceberg 对 bucket 转换的支持

**实际情况**：
1. ✅ **Iceberg 能够处理 bucket 转换的等值查询**：
   - 对于 `id = 100`，Iceberg 的 `Projections.inclusive()` 能够计算出 `bucket_id = hash(100) % 16`
   - 能够正确进行分区裁剪，只扫描对应的 bucket

2. ⚠️ **Iceberg 对 bucket 转换的范围查询支持有限**：
   - 对于 `id >= 100 AND id < 200`，由于 bucket 转换的不可逆性，无法精确确定 bucket_id 范围
   - 可能需要扫描所有 bucket，或者进行保守估计

3. ❌ **Doris 显式优化对 bucket 转换的价值有限**：
   - 对于等值查询，Iceberg 已经能够正确处理
   - 对于范围查询，Doris 显式优化也无法解决 bucket 转换的不可逆性问题
   - **显式优化对 bucket 转换没有太大区别**

### 当前实现的问题（重新审视）

**场景**：表按 `bucket(16, id)` 分区，查询条件是 `WHERE id = 100`

**当前实现**：
```java
// 1. 将列谓词转换为 Iceberg Expression
Expression expr = Expressions.equal("id", 100);

// 2. 下推给 Iceberg
scan = scan.filter(expr);

// 3. Iceberg 内部处理
// ✅ Iceberg 的 Projections.inclusive() 能够处理 bucket 转换的等值查询
// ✅ 能够计算出 bucket_id = hash(100) % 16，并进行精确的分区裁剪
// ✅ 分区裁剪是有效的，不需要 Doris 显式优化
```

**真正的问题**：
1. ❌ **不是分区裁剪本身**：Iceberg 已经能够正确处理
2. ❌ **而是分区信息获取**：`getPartitionInfoMap()` 缺少分区规范信息，无法正确处理转换函数
3. ❌ **影响 Runtime Filter**：需要知道分区字段的转换函数和源列信息

### 结论：bucket 转换的问题不是分区裁剪，而是分区信息获取

**实际情况**：
1. ✅ **Iceberg 能够处理 bucket 转换的等值查询**：不需要 Doris 显式优化
2. ⚠️ **Iceberg 对 bucket 转换的范围查询支持有限**：但这是 bucket 转换本身的限制，Doris 显式优化也无法解决
3. ❌ **真正的问题**：`getPartitionInfoMap()` 缺少分区规范信息，无法正确处理 bucket 转换的分区值

**改进方向**：
1. **改进 `getPartitionInfoMap()` 方法**：
   - 添加 `PartitionSpec` 参数
   - 利用分区规范信息正确处理转换函数
   - **这主要影响 Runtime Filter，而不是分区裁剪本身**

2. **查询计划优化**：
   - 利用分区信息估算扫描的数据量
   - 优化 join 顺序等
   - **这些优化不依赖于"显式分区裁剪"，而是依赖于"分区信息获取"**

### 其他复杂转换的问题

**Truncate 转换**：`truncate(4, name)` 表示截取字符串的前 4 个字符
- 对于 `name = 'abcde'`，可以确定分区值是 `'abcd'`
- 但对于 `name LIKE 'abc%'`，无法精确确定分区值范围

**Year/Month/Day 转换**：
- 相对简单，可以精确转换
- 但对于跨年/跨月的范围查询，可能需要扫描多个分区

---

## 总结：重新审视问题

### 重要澄清

**用户提出的问题**：显式优化和调用 Iceberg API 进行裁剪有什么区别？

**答案**：
1. ✅ **对于分区裁剪本身，显式优化和调用 Iceberg API 没有太大区别**
   - Iceberg API 已经能够正确处理分区裁剪
   - 对于大多数转换函数，Iceberg 的分区裁剪是有效的
   - Doris 显式实现分区裁剪，只是重复 Iceberg 的工作，没有实际价值

2. ❌ **真正的问题不是"显式优化"，而是"正确获取和使用分区信息"**
   - `getPartitionInfoMap()` 缺少分区规范信息，无法正确处理转换函数
   - 这主要影响 Runtime Filter 和其他优化功能，而不是分区裁剪本身

### 真正需要改进的地方

1. **改进 `getPartitionInfoMap()` 方法**（高优先级）：
   - 添加 `PartitionSpec` 参数
   - 利用分区规范信息正确处理转换函数
   - **主要影响 Runtime Filter，而不是分区裁剪**

2. **多分区规范处理**（中优先级）：
   - 在 `createIcebergSplit()` 中考虑 specId
   - 避免不同分区规范之间的冲突

3. **查询计划优化**（低优先级）：
   - 利用分区信息估算扫描的数据量
   - 优化 join 顺序等
   - **这些优化不依赖于"显式分区裁剪"，而是依赖于"分区信息获取"**

### 结论

**显式优化对于分区裁剪本身没有太大区别**，因为：
- ✅ Iceberg API 已经能够正确处理分区裁剪
- ✅ 对于大多数转换函数，Iceberg 的分区裁剪是有效的
- ❌ Doris 显式实现分区裁剪，只是重复 Iceberg 的工作

**但是，正确获取和使用分区信息仍然有价值**，因为：
- ✅ 影响 Runtime Filter 优化
- ✅ 影响查询计划优化
- ✅ 影响统计信息利用

**因此，改进的重点应该是"正确获取和使用分区信息"，而不是"显式分区裁剪"**

