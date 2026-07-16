# FIX-L6 — trino guard 字段先于依赖字段发布 → 并发首访瞬时 NPE

> 来源：reverify §1 表 L6（原 P2-5）。🟡 低（并发时序，volatile 自愈的瞬时窗口）。范围：`TrinoDorisConnector.doInitialize` 字段赋值顺序。
> HEAD 复核基线：`4cd63c6911a`。

## Problem / Root Cause

`ensureInitialized`（:133-141）用 **`trinoConnector` 作 guard** 做 double-checked locking；`getMetadata`（:67-68）等
在 guard 通过后立即读 `trinoSession`/`trinoCatalogHandle`。但 `doInitialize`（:176-179）先赋 **guard `trinoConnector`**（:176）
再赋 `trinoSession`/`trinoCatalogHandle`/`trinoConnectorName`（:177-179）。

字段虽全 `volatile`，问题是**发布顺序**：线程 B 见 `trinoConnector != null`（A 在 :176 写的）→ 跳过 synchronized →
`new TrinoConnectorDorisMetadata(trinoConnector, trinoSession, trinoCatalogHandle)`，而 A 可能尚未执行 :177/:178
→ B 读到 `trinoSession==null` → NPE。volatile happens-before 只保证「见 guard 写 ⟹ 见 guard 写**之前**的所有写」;
`trinoSession` 写在 guard **之后**,故不被覆盖。

## Design（4 行重排：guard 字段最后赋值）

把 `this.trinoConnector = result.getConnector();` 从 :176 **移到 4 个赋值的最后**：
```java
this.trinoProperties  // 已在 :145 更早赋(guard 前),getTrinoProperties 安全,不动
...
this.trinoSession = result.getSession();
this.trinoCatalogHandle = result.getCatalogHandle();
this.trinoConnectorName = result.getConnectorName();
this.trinoConnector = result.getConnector();   // guard 最后发布 → 见它非空即见其前所有依赖写
```
则 B 见 `trinoConnector != null` ⟹（volatile release/acquire）见 `trinoSession`/`trinoCatalogHandle`/
`trinoConnectorName` 的写 → 无半初始化。经典「guard 字段最后发布」安全发布法;全字段已 volatile,重排即足,无需引 holder。

## Risk

- 单线程语义完全不变（4 个赋值无相互依赖,顺序对结果无影响,仅影响并发可见性）。
- 所有 guard 检查点（:88 testConnection、:96 close、:134/:136 ensureInitialized）**统一用 `trinoConnector`** → 移它最后即全覆盖。已核。
- 连接器局部,不碰 fe-core。

## Test Plan

- build-compile。
- 并发竞态的行为 UT 不切实际（需精确时序触发瞬时窗口）+ 同 `io.trino.Session`/重构造墙（见 FIX-L3）→ 不加,登记。
  修法是教科书安全发布惯用法,以设计推理 + 并发对抗复审兜底。
- e2e：并发首访不再偶发 NPE（难稳定复现,live/压测观察）。

## 备注

与 L4 同属 trino 并发/单例条,合并一次并发对抗复审。
