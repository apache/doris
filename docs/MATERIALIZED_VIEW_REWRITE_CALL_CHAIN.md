# Apache Doris 物化视图重写调用链文档

## 目录
1. [概述](#概述)
2. [整体架构](#整体架构)
3. [调用链详解](#调用链详解)
4. [关键组件说明](#关键组件说明)
5. [OR 条件支持实现](#or-条件支持实现)

---

## 概述

Apache Doris 的物化视图（Materialized View）重写功能允许查询优化器自动识别并使用物化视图来加速查询。本文档详细说明了物化视图重写的完整调用链和实现机制。

### 核心概念

- **查询（Query）**：用户提交的 SQL 查询
- **物化视图（Materialized View）**：预计算的查询结果
- **重写（Rewrite）**：将查询转换为使用物化视图的等价查询
- **谓词匹配（Predicate Matching）**：检查查询的过滤条件是否可以使用物化视图

---

## 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Query (用户查询)                      │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Nereids Planner (查询优化器)                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  PreMaterializedViewRewriter (预重写阶段)            │   │
│  │  - 记录临时计划                                        │   │
│  │  - 执行 RBO 优化                                       │   │
│  └──────────────────────────────────────────────────────┘   │
│                        │                                      │
│                        ▼                                      │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  MaterializedViewRule (CBO 阶段)                    │   │
│  │  - AbstractMaterializedViewRule                      │   │
│  │  - MaterializedViewFilterScanRule                    │   │
│  │  - MaterializedViewAggregateRule                     │   │
│  │  - MaterializedViewJoinRule                          │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              重写后的查询计划 (使用物化视图)                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 调用链详解

### 1. 入口：查询优化器

**位置**：`fe/fe-core/src/main/java/org/apache/doris/nereids/NereidsPlanner.java`

```java
// 查询优化器在优化过程中会调用物化视图重写规则
Optimizer.optimize()
  └─> ExplorationRuleFactory.apply()
      └─> AbstractMaterializedViewRule.rewrite()
```

### 2. 预重写阶段（Pre-Rewrite）

**位置**：`PreMaterializedViewRewriter.java`

#### 2.1 判断是否需要预重写

```
PreMaterializedViewRewriter.needRecordTmpPlanForRewrite()
  ├─> 检查是否有候选物化视图
  ├─> 检查重写策略配置
  └─> 返回是否需要记录临时计划
```

#### 2.2 执行预重写

```
PreMaterializedViewRewriter.rewrite()
  ├─> Optimizer.execute()                    // 执行优化
  ├─> NereidsPlanner.chooseBestPlan()        // 选择最佳物理计划
  ├─> MaterializedViewUtils.getChosenMaterializationAndUsedTable()
  │   └─> 获取被选中的物化视图和使用的表
  └─> StructInfo.getOriginalPlan()           // 提取逻辑计划
```

### 3. CBO 阶段：物化视图规则匹配

**位置**：`AbstractMaterializedViewRule.java`

#### 3.1 主重写流程

```
AbstractMaterializedViewRule.rewrite()
  ├─> 遍历所有 MaterializationContext
  │   ├─> checkIfRewritten()                  // 检查是否已重写
  │   ├─> isMaterializationValid()            // 检查物化视图是否有效
  │   └─> getValidQueryStructInfos()         // 获取有效的查询结构信息
  │       └─> MaterializedViewUtils.extractStructInfo()
  │           └─> 提取查询的 StructInfo
  └─> doRewrite()                            // 执行重写
```

#### 3.2 核心重写逻辑

```
AbstractMaterializedViewRule.doRewrite()
  ├─> decideMatchMode()                       // 决定匹配模式
  │   └─> COMPLETE / QUERY_PARTIAL / VIEW_PARTIAL / NOT_MATCH
  │
  ├─> RelationMapping.generate()              // 生成表映射关系
  │   └─> 建立查询表到物化视图表的映射
  │
  ├─> SlotMapping.generate()                  // 生成列映射关系
  │   └─> 建立查询列到物化视图列的映射
  │
  ├─> LogicalCompatibilityContext.from()      // 构建兼容性上下文
  │
  ├─> StructInfo.isGraphLogicalEquals()      // 检查图逻辑等价性
  │   └─> ComparisonResult                  // 返回比较结果
  │
  ├─> predicatesCompensate()                 // ⭐ 谓词补偿（关键步骤）
  │   ├─> Predicates.compensateEquivalence()  // 等值谓词补偿
  │   ├─> Predicates.compensateRangePredicate() // 范围谓词补偿
  │   └─> Predicates.compensateResidualPredicate() // ⭐ 残差谓词补偿（OR 条件处理）
  │       ├─> checkOrPredicatesMatch()        // 检查 OR 条件匹配
  │       │   └─> isQueryPredicateContainedByOr() // 检查查询谓词是否被 OR 包含
  │       │       └─> isPredicateContained() // 检查单个谓词包含性
  │       └─> handleNonOrPredicates()        // 处理非 OR 谓词
  │
  ├─> materializationContext.getScanPlan()   // 获取物化视图扫描计划
  │
  ├─> rewriteExpression()                    // 重写表达式
  │   └─> 将查询表达式转换为物化视图表达式
  │
  ├─> rewriteQueryByViewPreCheck()           // 重写前检查
  │
  ├─> rewriteQueryByView()                   // 使用物化视图重写查询
  │   └─> 子类实现（FilterScanRule, AggregateRule, JoinRule 等）
  │
  └─> 返回重写后的计划
```

### 4. 谓词补偿详细流程

**位置**：`Predicates.java`

#### 4.1 残差谓词补偿（支持 OR 条件）

```
Predicates.compensateResidualPredicate()
  ├─> 转换物化视图谓词为查询基准表达式
  │   └─> ExpressionUtils.replace() + SlotMapping
  │
  ├─> 分离 OR 谓词和非 OR 谓词
  │   ├─> viewOrPredicates      // OR 条件集合
  │   └─> viewNonOrPredicates   // 非 OR 条件集合
  │
  ├─> checkOrPredicatesMatch()              // ⭐ 检查 OR 条件匹配
  │   ├─> 收集所有查询谓词（residual + range + equal）
  │   └─> 检查每个 OR 条件是否至少有一个分支被匹配
  │       └─> isQueryPredicateContainedByOr()
  │           └─> isPredicateContained()
  │               ├─> extractPredicateComponents()  // 提取列和字面量
  │               └─> checkComparisonContainment() // 检查比较包含性
  │                   └─> checkEqualityInRange()   // 检查等值是否在范围内
  │
  ├─> handleNonOrPredicates()              // 处理非 OR 谓词
  │   └─> 确保查询包含所有非 OR 谓词
  │
  └─> buildCompensationMap()                // 构建补偿映射
      └─> 返回需要补偿的谓词
```

#### 4.2 OR 条件匹配逻辑

```
isQueryPredicateContainedByOr(queryPred, orBranches)
  └─> 遍历 OR 的每个分支
      └─> isPredicateContained(queryPred, branch)
          ├─> 精确匹配：queryPred.equals(branch)
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

### 5. 具体规则实现

#### 5.1 Filter Scan Rule

**位置**：`MaterializedViewFilterScanRule.java`

```
MaterializedViewFilterScanRule.buildRules()
  └─> logicalFilter(LogicalCatalogRelation)
      └─> AbstractMaterializedViewScanRule.rewrite()
          └─> AbstractMaterializedViewRule.rewrite()
              └─> 调用上述主流程
```

#### 5.2 Aggregate Rule

**位置**：`MaterializedViewAggregateRule.java`

```
MaterializedViewAggregateRule.buildRules()
  └─> logicalAggregate(...)
      └─> 检查聚合函数匹配
          └─> 调用主重写流程
```

#### 5.3 Join Rule

**位置**：`MaterializedViewJoinRule.java`

```
MaterializedViewJoinRule.buildRules()
  └─> logicalJoin(...)
      └─> 检查连接条件匹配
          └─> 调用主重写流程
```

---

## 关键组件说明

### 1. StructInfo

**位置**：`StructInfo.java`

**作用**：封装查询或物化视图的结构信息

**关键字段**：
- `originalPlan`: 原始查询计划
- `topPlan`: 顶层计划（包含 Project、Filter）
- `bottomPlan`: 底层计划（包含 Join、Scan）
- `relations`: 涉及的表
- `predicates`: 谓词信息
- `equivalenceClass`: 等价类
- `splitPredicate`: 分割后的谓词（equal/range/residual）

**关键方法**：
- `getSplitPredicate()`: 获取分割后的谓词
- `isGraphLogicalEquals()`: 检查图逻辑等价性

### 2. Predicates

**位置**：`Predicates.java`

**作用**：处理谓词的包含性检查和补偿

**关键方法**：
- `compensateEquivalence()`: 补偿等值谓词
- `compensateRangePredicate()`: 补偿范围谓词
- `compensateResidualPredicate()`: ⭐ 补偿残差谓词（支持 OR 条件）
- `splitPredicates()`: 分割谓词为 equal/range/residual

**SplitPredicate 结构**：
```java
class SplitPredicate {
    Map<Expression, ExpressionInfo> equalPredicateMap;    // 等值谓词
    Map<Expression, ExpressionInfo> rangePredicateMap;    // 范围谓词
    Map<Expression, ExpressionInfo> residualPredicateMap; // 残差谓词（包含 OR）
}
```

### 3. SlotMapping

**位置**：`SlotMapping.java`

**作用**：建立查询列到物化视图列的映射关系

**关键方法**：
- `generate()`: 生成映射
- `inverse()`: 获取反向映射
- `toSlotReferenceMap()`: 转换为 SlotReference 映射

### 4. MaterializationContext

**位置**：`MaterializationContext.java`

**作用**：封装物化视图的上下文信息

**关键方法**：
- `getStructInfo()`: 获取物化视图的 StructInfo
- `getScanPlan()`: 获取物化视图扫描计划
- `recordFailReason()`: 记录失败原因

### 5. ComparisonResult

**位置**：`ComparisonResult.java`

**作用**：存储查询和物化视图的比较结果

**关键字段**：
- `queryExpressions`: 查询中独有的表达式
- `viewExpressions`: 物化视图中独有的表达式
- `isInvalid()`: 是否无效

---

## OR 条件支持实现

### 问题场景

**物化视图定义**：
```sql
CREATE MATERIALIZED VIEW mv1 AS
SELECT * FROM table1 
WHERE ds >= '202531' OR ds = '202530';
```

**查询**：
```sql
SELECT * FROM table1 
WHERE ds = '202530';
```

**期望**：查询应该能够使用物化视图进行重写

### 实现位置

**主要修改**：`Predicates.compensateResidualPredicate()`

### 实现逻辑

#### 1. OR 条件识别

```java
// 分离 OR 谓词和非 OR 谓词
Set<Expression> viewOrPredicates = new HashSet<>();
Set<Expression> viewNonOrPredicates = new HashSet<>();
for (Expression viewExpr : viewResidualQueryBasedSet) {
    if (viewExpr instanceof Or) {
        viewOrPredicates.add(viewExpr);
    } else {
        viewNonOrPredicates.add(viewExpr);
    }
}
```

#### 2. OR 条件匹配检查

```java
checkOrPredicatesMatch(querySplitPredicate, viewOrPredicates)
  ├─> 收集所有查询谓词（residual + range + equal）
  └─> 检查每个 OR 条件是否至少有一个分支被匹配
      └─> 使用 allMatch() 确保所有 OR 条件都被匹配
```

#### 3. 谓词包含性检查

```java
isPredicateContained(queryPred, viewPred)
  ├─> 精确匹配：queryPred.equals(viewPred)
  └─> 比较谓词匹配：
      ├─> extractPredicateComponents()  // 提取列和字面量
      └─> checkComparisonContainment()  // 检查包含性
          └─> checkEqualityInRange()   // 检查等值是否在范围内
              ├─> ds = '202530' vs ds >= '202531' → false
              └─> ds = '202530' vs ds = '202530' → true ✅
```

#### 4. 补偿逻辑

```java
if (orPredicatesMatched) {
    // OR 条件已满足，只需处理非 OR 谓词
    handleNonOrPredicates(queryResidualSet, viewNonOrPredicates);
} else {
    // 使用原始逻辑：查询必须包含所有物化视图谓词
    if (!queryResidualSet.containsAll(viewNonOrPredicates)) {
        return null; // 无法重写
    }
}
```

### 关键代码片段

```java
/**
 * 检查查询谓词是否被 OR 条件的至少一个分支包含
 */
private static boolean isQueryPredicateContainedByOr(
        Expression queryPred, List<Expression> orBranches) {
    return orBranches.stream()
        .anyMatch(branch -> isPredicateContained(queryPred, branch));
}

/**
 * 检查等值是否满足范围条件
 * 例如：ds = '202530' 满足 ds >= '202531' OR ds = '202530'
 */
private static boolean checkEqualityInRange(
        Literal equalityLiteral,
        ComparisonPredicate rangePredicate,
        Literal rangeLiteral) {
    if (rangePredicate instanceof GreaterThanEqual) {
        return equalityCompLit.compareTo(rangeCompLit) >= 0;
    } else if (rangePredicate instanceof EqualTo) {
        return equalityLiteral.equals(rangeLiteral);
    }
    // ... 其他比较操作符
}
```

---

## 调用链流程图

```
用户查询
  │
  ▼
NereidsPlanner.optimize()
  │
  ├─> PreMaterializedViewRewriter.needRecordTmpPlanForRewrite()
  │   └─> 判断是否需要记录临时计划
  │
  ├─> PreMaterializedViewRewriter.rewrite() [RBO 阶段]
  │   ├─> Optimizer.execute()
  │   ├─> chooseBestPlan()
  │   └─> extractStructInfo()
  │
  └─> AbstractMaterializedViewRule.rewrite() [CBO 阶段]
      │
      ├─> 遍历 MaterializationContext
      │   │
      │   ├─> getValidQueryStructInfos()
      │   │   └─> MaterializedViewUtils.extractStructInfo()
      │   │
      │   └─> doRewrite()
      │       │
      │       ├─> decideMatchMode()              // 决定匹配模式
      │       ├─> RelationMapping.generate()     // 表映射
      │       ├─> SlotMapping.generate()         // 列映射
      │       ├─> StructInfo.isGraphLogicalEquals() // 图逻辑等价性
      │       │
      │       ├─> predicatesCompensate()        // ⭐ 谓词补偿
      │       │   ├─> compensateEquivalence()    // 等值补偿
      │       │   ├─> compensateRangePredicate() // 范围补偿
      │       │   └─> compensateResidualPredicate() // ⭐ 残差补偿（OR 条件）
      │       │       ├─> checkOrPredicatesMatch()
      │       │       │   └─> isQueryPredicateContainedByOr()
      │       │       │       └─> isPredicateContained()
      │       │       │           ├─> extractPredicateComponents()
      │       │       │           └─> checkComparisonContainment()
      │       │       │               └─> checkEqualityInRange()
      │       │       └─> handleNonOrPredicates()
      │       │
      │       ├─> getScanPlan()                 // 获取物化视图扫描计划
      │       ├─> rewriteExpression()           // 重写表达式
      │       ├─> rewriteQueryByViewPreCheck()  // 重写前检查
      │       └─> rewriteQueryByView()          // 使用物化视图重写
      │           └─> 子类实现（FilterScanRule, AggregateRule 等）
      │
      └─> 返回重写后的计划列表
```

---

## 关键数据结构

### 1. SplitPredicate

```java
class SplitPredicate {
    // 等值谓词：col1 = col2
    Map<Expression, ExpressionInfo> equalPredicateMap;
    
    // 范围谓词：col1 > 100, col1 IN (1,2,3)
    Map<Expression, ExpressionInfo> rangePredicateMap;
    
    // 残差谓词：复杂表达式，包括 OR 条件
    // 例如：ds >= '202531' OR ds = '202530'
    Map<Expression, ExpressionInfo> residualPredicateMap;
}
```

### 2. ExpressionInfo

```java
class ExpressionInfo {
    Literal literal;  // 谓词中使用的字面量值
}
```

### 3. ComparisonResult

```java
class ComparisonResult {
    List<Expression> queryExpressions;  // 查询独有的表达式
    List<Expression> viewExpressions;  // 物化视图独有的表达式
    boolean isInvalid();                // 是否无效
}
```

---

## 示例：OR 条件重写流程

### 场景

**物化视图**：
```sql
CREATE MATERIALIZED VIEW mv1 AS
SELECT * FROM table1 
WHERE ds >= '202531' OR ds = '202530';
```

**查询**：
```sql
SELECT * FROM table1 
WHERE ds = '202530';
```

### 执行流程

1. **提取 StructInfo**
   - 查询：`ds = '202530'` → rangePredicateMap
   - 物化视图：`ds >= '202531' OR ds = '202530'` → residualPredicateMap

2. **谓词补偿**
   ```
   compensateResidualPredicate()
     ├─> 分离 OR 谓词：viewOrPredicates = {ds >= '202531' OR ds = '202530'}
     ├─> checkOrPredicatesMatch()
     │   ├─> 提取 OR 分支：[ds >= '202531', ds = '202530']
     │   └─> 检查查询谓词 ds = '202530'
     │       ├─> isPredicateContained(ds='202530', ds>='202531') → false
     │       └─> isPredicateContained(ds='202530', ds='202530') → true ✅
     │
     └─> orPredicatesMatched = true
   ```

3. **构建补偿映射**
   ```
   buildCompensationMap()
     └─> 返回空映射（无需额外补偿）
   ```

4. **重写查询**
   ```
   rewriteQueryByView()
     └─> 返回使用物化视图的查询计划
   ```

---

## 调试和日志

### 关键日志点

1. **PreMaterializedViewRewriter**
   - `needPreRewrite()`: 记录是否需要预重写
   - `rewrite()`: 记录重写结果

2. **AbstractMaterializedViewRule**
   - `rewrite()`: 记录每个物化视图的处理结果
   - `doRewrite()`: 记录重写失败原因

3. **Predicates**
   - `compensateResidualPredicate()`: 记录 OR 条件匹配情况

### 失败原因记录

```java
materializationContext.recordFailReason(
    queryStructInfo,
    "失败原因描述",
    () -> "详细错误信息"
);
```

---

## 相关文件清单

### 核心文件

1. **重写入口**
   - `PreMaterializedViewRewriter.java` - 预重写阶段
   - `AbstractMaterializedViewRule.java` - 抽象重写规则

2. **具体规则**
   - `MaterializedViewFilterScanRule.java` - Filter + Scan 规则
   - `MaterializedViewAggregateRule.java` - Aggregate 规则
   - `MaterializedViewJoinRule.java` - Join 规则

3. **数据结构**
   - `StructInfo.java` - 结构信息
   - `Predicates.java` - ⭐ 谓词处理（包含 OR 条件支持）
   - `SlotMapping.java` - 列映射
   - `MaterializationContext.java` - 物化视图上下文

4. **工具类**
   - `MaterializedViewUtils.java` - 工具方法
   - `PredicatesSplitter.java` - 谓词分割器
   - `PartitionCompensator.java` - 分区补偿器

---

## 学习建议

1. **从入口开始**：先理解 `PreMaterializedViewRewriter.rewrite()` 的整体流程
2. **理解数据结构**：重点理解 `StructInfo` 和 `SplitPredicate` 的作用
3. **跟踪一个简单查询**：选择一个简单的 Filter 查询，跟踪完整的重写流程
4. **理解谓词匹配**：深入理解 `isPredicateContained()` 的实现逻辑
5. **OR 条件支持**：重点学习 `compensateResidualPredicate()` 中的 OR 条件处理

---

## 参考资料

- [GitHub Issue #57279](https://github.com/apache/doris/issues/57279) - OR 条件支持需求
- `be/MTMV_OR_CONDITION_IMPLEMENTATION.md` - OR 条件实现方案文档

---

**最后更新**：2025年1月

