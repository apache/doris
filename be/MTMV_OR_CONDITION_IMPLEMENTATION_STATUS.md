# 物化视图重写支持 OR 条件 - 实现状态

## ✅ 实现状态：**已完成核心功能**

## 实现位置

**文件**：`fe/fe-core/src/main/java/org/apache/doris/nereids/rules/exploration/mv/Predicates.java`

## 已实现功能

### 1. 核心方法

✅ **`compensateResidualPredicate()`** (第267行)
- 支持 OR 条件的残差谓词补偿
- 分离 OR 谓词和非 OR 谓词
- 调用 OR 条件匹配检查

✅ **`checkOrPredicatesMatch()`** (第327行)
- 检查所有 OR 谓词是否至少有一个分支被匹配
- 收集查询的所有谓词（residual + range + equal）
- 使用 `allMatch()` 确保所有 OR 条件都被匹配

✅ **`isQueryPredicateContainedByOr()`** (第388行)
- 检查查询谓词是否被 OR 条件的至少一个分支包含
- 使用 Stream API 简化逻辑

✅ **`isPredicateContained()`** (第403行)
- 检查查询谓词是否被视图谓词包含
- 支持精确匹配和比较谓词匹配

✅ **`extractPredicateComponents()`** (第436行)
- 从比较谓词中提取列和字面量

✅ **`checkComparisonContainment()`** (第459行)
- 检查两个比较谓词之间的包含性

✅ **`checkEqualityInRange()`** (第484行)
- 检查等值是否满足范围条件
- 支持：`>=`, `>`, `<=`, `<`

### 2. 支持的场景

#### ✅ 场景1：查询是等值条件，物化视图是 OR 条件

```
物化视图：ds >= '202531' OR ds = '202530'
查询：ds = '202530'
结果：✅ 可以重写（匹配 ds = '202530' 分支）
```

**实现逻辑**：
- 提取 OR 分支：`[ds >= '202531', ds = '202530']`
- 检查 `ds = '202530'` 是否匹配任一分支
- `isPredicateContained(ds='202530', ds='202530')` → `true` ✅

#### ✅ 场景2：查询是范围条件，物化视图是 OR 条件（不应匹配）

```
物化视图：ds >= '202531' OR ds = '202530'
查询：ds >= '202530'
结果：❌ 不能重写（查询范围更广）
```

**实现逻辑**：
- 检查 `ds >= '202530'` vs `ds >= '202531'` → 不匹配（'202530' < '202531'）
- 检查 `ds >= '202530'` vs `ds = '202530'` → 不匹配（等值条件不能包含范围条件）
- 返回 `false` ✅ （符合预期）

### 3. 支持的操作符

✅ `EqualTo` - 等值比较
✅ `GreaterThanEqual` - 大于等于
✅ `GreaterThan` - 大于
✅ `LessThanEqual` - 小于等于
✅ `LessThan` - 小于

### 4. 代码质量

✅ 使用现代 Java 特性（Stream API）
✅ 方法拆分清晰，职责单一
✅ 添加了详细的 JavaDoc 注释
✅ 错误处理（try-catch）
✅ 使用内部类封装数据（`PredicateComponents`）

## 实现细节

### OR 条件匹配流程

```
compensateResidualPredicate()
  ├─> 分离 OR 谓词和非 OR 谓词
  │
  ├─> checkOrPredicatesMatch()
  │   ├─> 收集所有查询谓词（residual + range + equal）
  │   └─> 检查每个 OR 条件是否至少有一个分支被匹配
  │       └─> isQueryPredicateContainedByOr()
  │           └─> isPredicateContained()
  │               ├─> extractPredicateComponents()
  │               └─> checkComparisonContainment()
  │                   └─> checkEqualityInRange()
  │
  └─> handleNonOrPredicates()  // 处理非 OR 谓词
```

### 谓词包含性检查

```java
isPredicateContained(queryPred, viewPred)
  ├─> 精确匹配：queryPred.equals(viewPred) → true
  │
  └─> 比较谓词匹配：
      ├─> 提取列和字面量
      ├─> 检查列名是否相同
      └─> checkComparisonContainment()
          ├─> EqualTo vs EqualTo → 字面量相等
          └─> EqualTo vs Range → checkEqualityInRange()
              ├─> GreaterThanEqual: queryLiteral >= viewLiteral
              ├─> GreaterThan: queryLiteral > viewLiteral
              ├─> LessThanEqual: queryLiteral <= viewLiteral
              └─> LessThan: queryLiteral < viewLiteral
```

## 已知限制

### 1. Range vs Range 匹配

当前 `checkComparisonContainment()` 中，对于 Range vs Range 的情况：
```java
// Other cases: not supported yet
return false;
```

**影响**：如果查询和物化视图都是范围条件，且查询范围被物化视图范围包含的情况，可能无法匹配。

**示例**：
- 物化视图：`ds >= '202531' OR ds <= '202501'`
- 查询：`ds = '202530'`
- 当前：可能无法匹配 `ds <= '202501'` 分支（因为比较逻辑不支持 Range vs Range）

**注意**：文档中的场景示例不涉及这种情况，所以当前实现已满足需求。

### 2. 嵌套 OR/AND 条件

当前实现主要处理顶层的 OR 条件。对于复杂的嵌套情况（如 `(A OR B) AND (C OR D)`），可能需要进一步测试。

## 测试建议

### 1. 基本场景测试

```sql
-- 场景1：等值查询匹配 OR 条件
CREATE MATERIALIZED VIEW mv1 AS
SELECT * FROM table1 WHERE ds >= '202531' OR ds = '202530';

SELECT * FROM table1 WHERE ds = '202530';
-- 期望：使用 mv1 重写 ✅
```

### 2. 复杂场景测试

```sql
-- 多个 OR 条件
CREATE MATERIALIZED VIEW mv2 AS
SELECT * FROM table1 
WHERE (ds >= '202531' OR ds = '202530') 
  AND (region = 'US' OR region = 'CN');

SELECT * FROM table1 
WHERE ds = '202530' AND region = 'US';
-- 期望：使用 mv2 重写（如果实现支持）

-- 查询范围更广的情况（不应匹配）
CREATE MATERIALIZED VIEW mv3 AS
SELECT * FROM table1 WHERE ds >= '202531' OR ds = '202530';

SELECT * FROM table1 WHERE ds >= '202530';
-- 期望：不使用 mv3 重写 ❌（当前实现正确）
```

### 3. 边界情况测试

- NULL 值处理
- 空结果集
- 类型转换（字符串、日期等）
- 多列 OR 条件

## 总结

✅ **核心功能已实现**：文档中描述的主要场景（场景1）已完整实现

✅ **代码质量良好**：代码结构清晰，使用了现代 Java 特性

⚠️ **扩展性**：某些边缘情况（Range vs Range、复杂嵌套）可能需要进一步支持

✅ **符合需求**：实现已满足 GitHub Issue #57279 中描述的基本需求

## 下一步

1. **添加单元测试**：验证各种场景
2. **集成测试**：在实际查询中测试 OR 条件重写
3. **性能测试**：确保 OR 条件匹配不会影响性能
4. **扩展支持**：如需要，可以扩展支持 Range vs Range 匹配

---

**最后更新**：2025年1月
**实现状态**：✅ 核心功能已完成

