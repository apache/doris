# Iceberg Runtime Filter 分区裁剪可行性分析

## 一、当前 BE 端实现原理

### 1.1 Runtime Filter 分区裁剪流程

**代码位置**：`FileScanner._process_runtime_filters_partition_prune()`

**工作流程**：
1. **获取分区值**：从 `columns_from_path` 中获取分区值的字符串表示
2. **反序列化分区值**：根据分区字段的数据类型，将字符串反序列化为列
3. **填充 Block**：将分区值填充到 `_runtime_filter_partition_prune_block` 中，使用**源列的 SlotDescriptor**
4. **执行谓词**：执行 Runtime Filter 的谓词表达式，判断是否过滤

**关键代码**：
```cpp
// 1. 反序列化分区值
for (auto const& partition_col_desc : _partition_col_descs) {
    const auto& [partition_value, partition_slot_desc] = partition_col_desc.second;
    // partition_slot_desc 是源列的 SlotDescriptor
    // partition_value 是分区值的字符串表示
    // 反序列化为列...
}

// 2. 填充到 Block（使用源列的 SlotDescriptor）
if (partition_slot_id_to_column.find(slot_desc->id()) !=
    partition_slot_id_to_column.end()) {
    // slot_desc 是源列的 SlotDescriptor
    _runtime_filter_partition_prune_block.insert(
        index, ColumnWithTypeAndName(std::move(partition_value_column), 
                                     data_type, slot_desc->col_name()));
}

// 3. 执行 Runtime Filter 谓词
VExprContext::execute_conjuncts(_runtime_filter_partition_prune_ctxs, nullptr,
                                &_runtime_filter_partition_prune_block,
                                &result_filter, &can_filter_all);
```

### 1.2 当前实现的假设

**关键假设**：
- 分区字段名 = 源列名（identity transform）
- 分区值的数据类型 = 源列的数据类型
- Runtime Filter 的谓词直接基于源列

**问题**：
- 对于非 identity transform（如 `day(ts)`），分区字段名是 `ts_day`，但源列名是 `ts`
- 分区值的数据类型可能不同（如 `day(ts)` 的分区值是 DATE，源列是 TIMESTAMP）
- BE 端无法找到对应的源列 SlotDescriptor

## 二、转换函数特性分析

### 2.1 转换函数分类

| 转换函数 | 可逆性 | 分区值类型 | 源列类型 | 裁剪可行性 |
|---------|--------|-----------|---------|-----------|
| `identity` | ✅ 完全可逆 | 与源列相同 | 与源列相同 | ✅ **完全支持**：分区值 = 源列值 |
| `day` | ✅ 可转换为范围 | DATE | TIMESTAMP/DATE | ✅ **支持**：分区值 `2024-01-15` → 源列范围 `['2024-01-15 00:00:00', '2024-01-16 00:00:00')` |
| `year` | ✅ 可转换为范围 | INT | TIMESTAMP/DATE | ✅ **支持**：分区值 `2024` → 源列范围 `['2024-01-01 00:00:00', '2025-01-01 00:00:00')` |
| `month` | ✅ 可转换为范围 | INT | TIMESTAMP/DATE | ✅ **支持**：分区值 `202401` → 源列范围 `['2024-01-01 00:00:00', '2024-02-01 00:00:00')` |
| `hour` | ✅ 可转换为范围 | INT | TIMESTAMP | ✅ **支持**：分区值 `2024011510` → 源列范围 `['2024-01-15 10:00:00', '2024-01-15 11:00:00')` |
| `bucket[N]` | ❌ 不可逆 | INT | 任意 | ⚠️ **部分支持**：无法反向转换，但可以判断是否可能匹配 |
| `truncate[W]` | ❌ 不可逆 | 与源列相同 | 与源列相同 | ⚠️ **部分支持**：无法反向转换，但可以判断是否可能匹配 |

### 2.2 转换函数详细分析

#### 2.2.1 Identity Transform

**特性**：
- 分区值 = 源列值
- 分区值类型 = 源列类型
- 完全可逆

**裁剪方式**：
- 直接使用分区值作为源列值
- 当前实现已经支持

**示例**：
- 分区字段：`category`（identity transform）
- 分区值：`"electronics"`
- Runtime Filter：`category = 'electronics'`
- 裁剪：直接匹配 ✅

#### 2.2.2 Day Transform

**特性**：
- 分区值：DATE 类型（如 `2024-01-15`）
- 源列：TIMESTAMP 或 DATE 类型
- 可转换为源列值的范围

**裁剪方式**：
1. **将分区值转换为源列值范围**：
   - 分区值：`2024-01-15`
   - 源列范围：`['2024-01-15 00:00:00', '2024-01-16 00:00:00')`

2. **判断 Runtime Filter 是否与范围重叠**：
   - Runtime Filter：`ts >= '2024-01-15 10:00:00' AND ts < '2024-01-16 00:00:00'`
   - 分区范围：`['2024-01-15 00:00:00', '2024-01-16 00:00:00')`
   - 结果：重叠，不能过滤 ✅

3. **如果 Runtime Filter 与范围不重叠，可以过滤**：
   - Runtime Filter：`ts >= '2024-01-16 00:00:00'`
   - 分区范围：`['2024-01-15 00:00:00', '2024-01-16 00:00:00')`
   - 结果：不重叠，可以过滤 ✅

**实现复杂度**：中等
- 需要实现范围转换逻辑
- 需要实现范围重叠判断

#### 2.2.3 Year/Month/Hour Transform

**特性**：类似 Day Transform，但范围更大

**裁剪方式**：
- Year：分区值 `2024` → 源列范围 `['2024-01-01 00:00:00', '2025-01-01 00:00:00')`
- Month：分区值 `202401` → 源列范围 `['2024-01-01 00:00:00', '2024-02-01 00:00:00')`
- Hour：分区值 `2024011510` → 源列范围 `['2024-01-15 10:00:00', '2024-01-15 11:00:00')`

**实现复杂度**：中等（与 Day 类似）

#### 2.2.4 Bucket Transform

**特性**：
- 分区值：INT 类型（桶号，如 `5`）
- 源列：任意类型
- **不可逆**：无法从桶号反推出源列值

**裁剪方式**：
1. **等值查询**：
   - Runtime Filter：`id = 100`
   - 计算：`bucket(16, id)` 对 `100` 的哈希值，得到桶号（如 `5`）
   - 分区值：`5`
   - 结果：匹配，不能过滤 ✅

2. **范围查询**：
   - Runtime Filter：`id >= 100 AND id < 200`
   - 问题：无法确定哪些桶号可能包含这个范围的值
   - 结果：**无法精确判断**，只能保守处理（不过滤）⚠️

**实现复杂度**：高
- 需要实现哈希函数（与 Iceberg 一致）
- 对于范围查询，无法精确判断

#### 2.2.5 Truncate Transform

**特性**：
- 分区值：与源列类型相同（截断后的值）
- 源列：字符串或数字
- **不可逆**：无法从截断值反推出完整值

**裁剪方式**：
1. **前缀匹配查询**：
   - Runtime Filter：`code LIKE 'ABC%'`
   - 分区值：`"ABC"`（truncate(3, code)）
   - 结果：可能匹配，不能过滤 ✅

2. **等值查询**：
   - Runtime Filter：`code = 'ABC123'`
   - 分区值：`"ABC"`（truncate(3, code)）
   - 结果：可能匹配，不能过滤 ✅

3. **范围查询**：
   - Runtime Filter：`code >= 'ABC' AND code < 'ABD'`
   - 分区值：`"ABC"`（truncate(3, code)）
   - 结果：可能匹配，不能过滤 ✅

**实现复杂度**：中等
- 主要是前缀匹配判断
- 对于范围查询，可以判断前缀是否在范围内

## 三、理论可行性评估

### 3.1 完全支持的转换函数

✅ **Identity**：当前已支持
✅ **Day/Year/Month/Hour**：理论上完全可行，需要实现范围转换和重叠判断

### 3.2 部分支持的转换函数

⚠️ **Bucket**：
- 等值查询：可行（需要实现哈希函数）
- 范围查询：不可行（无法精确判断）

⚠️ **Truncate**：
- 前缀匹配：可行
- 等值查询：可行（保守处理）
- 范围查询：部分可行（前缀范围判断）

### 3.3 实现方案

#### 方案1：将分区值转换为源列值范围（推荐）

**适用于**：Day、Year、Month、Hour

**实现步骤**：
1. 根据转换函数类型，将分区值转换为源列值的范围
2. 将范围转换为源列类型的列值（使用范围的边界值）
3. 填充到 Block 中，执行 Runtime Filter 谓词

**优点**：
- 逻辑清晰
- 可以精确判断

**缺点**：
- 需要为每个转换函数实现范围转换逻辑
- 对于范围查询，需要判断范围重叠

#### 方案2：将 Runtime Filter 谓词转换为分区值谓词

**适用于**：所有转换函数

**实现步骤**：
1. 解析 Runtime Filter 谓词
2. 根据转换函数，将源列谓词转换为分区值谓词
3. 使用分区值执行转换后的谓词

**优点**：
- 统一处理所有转换函数
- 逻辑更直接

**缺点**：
- 需要实现谓词转换逻辑（复杂）
- 对于不可逆转换，转换后的谓词可能不精确

#### 方案3：混合方案（推荐）

**实现策略**：
1. **Identity**：直接使用（当前已支持）
2. **Day/Year/Month/Hour**：使用方案1（范围转换）
3. **Bucket/Truncate**：
   - 等值查询：实现哈希/前缀匹配
   - 范围查询：保守处理（不过滤）

## 四、实现复杂度评估

### 4.1 时间转换函数（Day/Year/Month/Hour）

**复杂度**：⭐⭐⭐（中等）

**需要实现**：
1. 分区值到源列值范围的转换
2. 范围重叠判断
3. 边界值处理（NULL、边界情况）

**工作量**：约 2-3 天

### 4.2 Bucket Transform

**复杂度**：⭐⭐⭐⭐（较高）

**需要实现**：
1. 哈希函数（与 Iceberg 一致）
2. 等值查询的桶号计算
3. 范围查询的保守处理

**工作量**：约 3-5 天

### 4.3 Truncate Transform

**复杂度**：⭐⭐⭐（中等）

**需要实现**：
1. 前缀匹配判断
2. 范围查询的前缀范围判断

**工作量**：约 2-3 天

## 五、结论

### 5.1 理论可行性

✅ **完全可行**：Identity、Day、Year、Month、Hour
⚠️ **部分可行**：Bucket（等值查询）、Truncate（前缀匹配）

### 5.2 推荐实现策略

1. **第一阶段**：实现 Identity（已支持）和 Day/Year/Month/Hour
   - 覆盖最常见的场景
   - 实现复杂度适中
   - 可以精确判断

2. **第二阶段**：实现 Bucket 和 Truncate 的基础支持
   - Bucket：支持等值查询
   - Truncate：支持前缀匹配

3. **第三阶段**：优化和扩展
   - 范围查询的优化
   - 性能优化

### 5.3 关键挑战

1. **分区字段到源列的映射**：需要从 FE 端传递映射信息 ✅（已实现）
2. **转换函数的反向转换**：需要实现各种转换函数的反向逻辑
3. **范围重叠判断**：需要实现高效的范围判断算法
4. **性能优化**：避免影响查询性能

### 5.4 总结

**BE 端理论上可以通过转换函数进行分区裁剪**，但需要：

1. ✅ **FE 端支持**：传递分区字段映射和转换函数信息（已完成）
2. ⚠️ **BE 端实现**：实现转换函数的反向转换逻辑（待实现）
3. ⚠️ **范围判断**：实现范围重叠判断（待实现）

**推荐优先级**：
1. **高优先级**：Day/Year/Month/Hour（最常见，实现相对简单）
2. **中优先级**：Bucket 等值查询、Truncate 前缀匹配
3. **低优先级**：Bucket/Truncate 范围查询优化

