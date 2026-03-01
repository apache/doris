# 物化视图重写支持 OR 条件实现方案

## 问题描述

根据 GitHub Issue #57279，需要实现物化视图重写支持 OR 条件。

**场景**：
- 物化视图定义：`where ds >= '202531' or ds = '202530'`
- 查询：`where ds = '202530'`
- 期望：查询应该能够使用物化视图进行重写

## 核心问题

物化视图重写需要判断：**查询的谓词是否被物化视图的谓词包含**（即查询结果是否在物化视图结果集内）。

对于 OR 条件，需要检查查询的谓词是否满足 OR 条件中的任何一个分支。

## 实现位置

物化视图重写的逻辑主要在 **FE（Frontend）** 中实现，具体应该在优化器（Optimizer）的查询重写阶段。

## 实现方案

### 1. 谓词包含性检查（Predicate Containment Check）

需要实现一个函数来判断查询谓词是否被物化视图谓词包含。

#### 1.1 基本规则

对于 OR 条件 `P1 OR P2 OR ... OR Pn`，查询谓词 Q 被包含当且仅当：
- Q 被至少一个 Pi 包含，或者
- Q 可以推导出至少一个 Pi 为真

#### 1.2 具体实现逻辑

```java
// 伪代码示例（FE 中通常是 Java 代码）
boolean isQueryPredicateContained(Predicate queryPred, Predicate mvPred) {
    // 如果物化视图谓词是 OR 条件
    if (mvPred.isOrPredicate()) {
        // 检查查询谓词是否被 OR 的任何一个分支包含
        for (Predicate branch : mvPred.getOrBranches()) {
            if (isPredicateContained(queryPred, branch)) {
                return true;
            }
        }
        return false;
    }
    
    // 如果物化视图谓词是 AND 条件
    if (mvPred.isAndPredicate()) {
        // 所有 AND 分支都必须包含查询谓词
        for (Predicate branch : mvPred.getAndBranches()) {
            if (!isPredicateContained(queryPred, branch)) {
                return false;
            }
        }
        return true;
    }
    
    // 单个谓词的包含性检查
    return checkSinglePredicateContainment(queryPred, mvPred);
}

// 单个谓词的包含性检查
boolean checkSinglePredicateContainment(Predicate queryPred, Predicate mvPred) {
    // 必须是同一列
    if (!queryPred.getColumn().equals(mvPred.getColumn())) {
        return false;
    }
    
    // 根据操作符类型判断
    switch (queryPred.getOp()) {
        case EQ:
            // ds = '202530' 被 ds >= '202531' OR ds = '202530' 包含
            // 需要检查是否满足 OR 的任何一个分支
            return checkEqualityContainment(queryPred, mvPred);
        case GE:
        case GT:
        case LE:
        case LT:
            return checkRangeContainment(queryPred, mvPred);
        // ... 其他操作符
    }
    return false;
}
```

### 2. 具体场景处理

#### 场景 1：查询是等值条件，物化视图是 OR 条件

```
物化视图：ds >= '202531' OR ds = '202530'
查询：ds = '202530'
```

检查逻辑：
1. 检查 `ds = '202530'` 是否满足 `ds >= '202531'` → 否
2. 检查 `ds = '202530'` 是否满足 `ds = '202530'` → 是
3. 返回 true（可以重写）

#### 场景 2：查询是范围条件，物化视图是 OR 条件

```
物化视图：ds >= '202531' OR ds = '202530'
查询：ds >= '202530'
```

检查逻辑：
1. 检查 `ds >= '202530'` 是否被 `ds >= '202531'` 包含 → 否（'202530' < '202531'）
2. 检查 `ds >= '202530'` 是否被 `ds = '202530'` 包含 → 否（等值条件不能包含范围条件）
3. 返回 false（不能重写）

**注意**：这种情况下，查询结果可能包含 `ds = '202530'` 和 `ds >= '202531'` 的数据，但查询范围更广，不能简单重写。

### 3. FE 代码修改位置

需要在 FE 的以下位置进行修改：

1. **物化视图重写器**（MaterializedViewRewriter）
   - 位置：`fe/fe-core/src/main/java/org/apache/doris/rewrite/mvrewrite/`
   - 修改：添加对 OR 条件的支持

2. **谓词匹配逻辑**（PredicateMatching）
   - 位置：可能在 `fe/fe-core/src/main/java/org/apache/doris/rewrite/`
   - 修改：实现 OR 条件的包含性检查

3. **优化器规则**（Optimizer Rules）
   - 位置：`fe/fe-core/src/main/java/org/apache/doris/nereids/rules/`
   - 修改：如果是 Nereids 优化器，需要在相应的规则中添加支持

### 4. 实现步骤

1. **分析现有代码结构**
   - 找到物化视图重写的入口点
   - 理解当前的谓词匹配逻辑
   - 确定需要修改的文件

2. **实现 OR 条件支持**
   - 添加 OR 条件的解析和处理
   - 实现谓词包含性检查函数
   - 处理各种边界情况

3. **添加测试用例**
   - 测试基本场景：`ds = '202530'` 匹配 `ds >= '202531' OR ds = '202530'`
   - 测试复杂场景：多个 OR 分支、嵌套 OR/AND 条件
   - 测试边界情况：空结果集、全量匹配等

4. **性能优化**
   - 优化谓词匹配的性能
   - 避免不必要的计算

### 5. 注意事项

1. **正确性**：确保查询结果与直接查询基表的结果一致
2. **性能**：OR 条件的检查可能比 AND 条件复杂，需要优化
3. **兼容性**：确保不影响现有的 AND 条件重写逻辑
4. **边界情况**：处理 NULL 值、空结果集等情况

### 6. 参考实现

可以参考其他数据库系统（如 PostgreSQL、Oracle）的物化视图重写实现，它们通常都有对 OR 条件的支持。

## 总结

实现物化视图重写支持 OR 条件的关键是：
1. 在 FE 的优化器中实现谓词包含性检查
2. 对于 OR 条件，检查查询谓词是否被至少一个分支包含
3. 确保正确性和性能

建议先查看 FE 代码中的物化视图重写相关代码，理解现有实现后再进行修改。



