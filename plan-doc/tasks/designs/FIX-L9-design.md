# FIX-L9 — maxcompute 谓词下推「全有全无」（一个不可转 conjunct 丢整个 filter）

> 来源：reverify §1 表 L9（原 P4-4）。🟡 低（perf-only；BE 重过滤保正确）。范围：`MaxComputePredicateConverter.convert`。
> HEAD 复核基线：`f62ccf25fe9`。

## Problem / Root Cause

`convert(expr)`（:87-97）把整棵表达式树包在**一个** try/catch 里：树中**任一**子表达式抛（不可转列/类型/会话 TZ）
→ 整个 filter 退化为 `NO_PREDICATE` → **完全不下推** → ODPS 读全表数据。`convertAnd`（:117-123）逐 conjunct 调
`doConvert`,任一抛即向上冒泡到 `convert` 的 catch。

**perf-only**：下推谓词经 `TableReadSessionBuilder.withFilterPredicate`（源端读优化）,BE 仍重评估完整 conjuncts
（现「丢整个 filter」本就靠 BE 重过滤保正确）→ 部分下推与丢全部同样正确,只是少读数据。

## Design（仅顶层 AND 逐 conjunct 容错；OR/NOT/嵌套 AND 保持全有全无）

公有入口 `convert` 特判**根**是 `ConnectorAnd`：逐 top-level conjunct 独立转换,丢失败者、AND 幸存者:
```java
public Predicate convert(ConnectorExpression expr) {
    if (expr == null) {
        return Predicate.NO_PREDICATE;
    }
    if (expr instanceof ConnectorAnd) {
        List<Predicate> survivors = new ArrayList<>();
        for (ConnectorExpression conjunct : ((ConnectorAnd) expr).getConjuncts()) {
            Predicate p = convertOne(conjunct);            // all-or-nothing per conjunct
            if (p != Predicate.NO_PREDICATE) {
                survivors.add(p);
            }
        }
        if (survivors.isEmpty()) {
            return Predicate.NO_PREDICATE;
        }
        return survivors.size() == 1 ? survivors.get(0)
                : new CompoundPredicate(CompoundPredicate.Operator.AND, survivors);
    }
    return convertOne(expr);
}

private Predicate convertOne(ConnectorExpression expr) {   // = 旧 convert 的 try/catch doConvert
    try {
        return doConvert(expr);
    } catch (Exception e) {
        LOG.warn("Failed to convert expression to ODPS predicate: {}", e.getMessage());
        return Predicate.NO_PREDICATE;
    }
}
```

**为何只顶层 AND（正确性核心）**：谓词下推须发**超集**（可多读、BE 再滤;不可漏行）。
- 顶层根是隐式合取,处**正/单调位置**：丢一个 conjunct = 放松 AND = 超集 → 安全（且既然丢全部已正确,丢部分必正确）。
- **OR 不容错**：丢一个 disjunct = `a OR b`→`a` = **子集** → 漏行,**不安全** → OR 整体 all-or-nothing。
- **NOT/嵌套 AND 不逐子容错**：NOT 下放松子式 = 子集不安全;嵌套 AND 一律整体转（其失败使所属顶层 conjunct 整条被丢,仍安全）。
  → 内部递归全走 `doConvert`（`convertAnd`/`convertOr`/`convertNot` 不变,保持整体转换）。`convert` 无内部递归调用,只公有入口
  → 特判只作用于根。已核。
- `NO_PREDICATE` 是单例哨兵（既有测试用 `assertSame`/`assertNotSame` 佐证）→ `!=` 引用比较正确。`doConvert` 失败抛异常
  （不返回 NO_PREDICATE）→ 幸存者判定无假阴。

## Risk

- **正确性**：与现「丢整个 filter」等价安全（都靠 BE 重过滤）,只多下推幸存者。核实入口非递归、内部保持全有全无。
- 既有 `testMixedAndTreeNotDropped`（两 conjunct 均转成功）结构不变（2 幸存者→AND(两者)）→ 仍绿。
- 连接器局部,不碰 fe-core,import 门禁不适用。

## Test Plan

### Unit（`MaxComputePredicateConverterTest` 加 5 例,converter 是纯函数、可直接驱动，RED-able）

- **RED 主证** `topLevelAndKeepsConvertibleWhenOneConjunctFails`：`id=5 AND ghost=3`（ghost 不在 schema）→ 非 NO_PREDICATE、
  含 `id`、不含 `ghost`（改前：整条 NO_PREDICATE）。
- `topLevelAndAllConjunctsFailDropsToNoPredicate`：`ghost1=3 AND ghost2=4` → NO_PREDICATE。
- `topLevelAndSingleSurvivorReturnedDirectly`：`id=5 AND ghost=3` → 返回单个 `id` 谓词（非包 AND）。
- `nestedAndStaysAllOrNothing`：`id=5 AND (amount=6 AND ghost=3)` → 仅 `id`；`amount` 也被丢（证嵌套 AND 整体转,不逐子）。
- `topLevelOrNotTolerated`：`id=5 OR ghost=3` → NO_PREDICATE（证 OR 不容错,避免漏行）。

（typeMap 补一个已知列 `amount` 供嵌套测试。）

### E2E — live-gated（真 ODPS）

mixed filter（可转 + 不可转 conjunct）下,ODPS 读端仍下推可转部分（少读数据）,结果与全 BE 过滤一致。
