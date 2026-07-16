# Problem

CI 996541 中 `paimon_system_table` 用例失败（issue 文档 §B 触发器 2）。

用例 `regression-test/suites/external_table_p0/paimon/paimon_system_table.groovy:154-167` 有三个 block，期望 paimon system table 拒绝时间旅行：

| block | 行 | SQL | 期望 exception 子串 |
|---|---|---|---|
| 1 | :155-158 | `select * from ts_scale_orc$snapshots FOR VERSION AS OF 1` | `system tables do not support time travel` |
| 2 | :159-162 | `select * from ts_scale_orc$snapshots FOR TIME AS OF "2024-07-11 16:01:57.425"` | `system tables do not support time travel` |
| 3 | :163-166 | `select * from ts_scale_orc$snapshots@incr('startSnapshotId'=1, 'endSnapshotId'=2)` | `system tables do not support scan params` |

实际：block 1 抛出的是 `Failed to pin MVCC snapshot for plugin-driven scan`（`PluginDrivenScanNode.java:1742` 的包装文案），不含期望子串 ⇒ 用例红。

## 这不是陈旧用例，是产品 bug（已用 git 核实）

`git diff master -- regression-test/suites/external_table_p0/paimon/paimon_system_table.groovy` 显示本分支只改了 3 行，且**全是放宽**：

```
-            exception "Paimon system tables do not support time travel"
+            exception "system tables do not support time travel"
-            exception "Paimon system tables do not support time travel"
+            exception "system tables do not support time travel"
-            exception "Paimon system tables do not support scan params"
+            exception "system tables do not support scan params"
```

（改动理由：连接器无关化后引擎文案由 `Paimon system tables ...` 变为 `Plugin system tables ...`，用例去掉源名前缀以匹配两者。）

`test { exception "..." }` 是**子串**匹配。新期望 `"system tables do not support time travel"` 是旧期望 `"Paimon system tables do not support time travel"` 的**真子串** ⇒ 本分支期望**严格弱于 master**。一个被放宽的断言仍然挂 ⇒ 产品行为回归，非用例陈旧。**结论：修产品，不修用例。**

# Root Cause

## 事实链（行号均已对照 HEAD 核实）

1. `PluginDrivenScanNode.pinMvccSnapshot():868` → `:885` 调 `resolveSysTableSnapshotPin()`。
2. `resolveSysTableSnapshotPin():1044` 现有实现只有三个 early-return：

```java
Optional<MvccSnapshot> resolveSysTableSnapshotPin() throws UserException {
    if (!(getTargetTable() instanceof PluginDrivenSysExternalTable)) {   // :1045
        return Optional.empty();
    }
    if (getQueryTableSnapshot() == null && getScanParams() == null) {    // :1048
        return Optional.empty();
    }
    PluginDrivenExternalTable source = ((PluginDrivenSysExternalTable) getTargetTable()).getSourceTable();
    if (!(source instanceof MvccTable)) {                                // :1052
        return Optional.empty();
    }
    return Optional.of(((MvccTable) source).loadSnapshot(               // :1055
            Optional.ofNullable(getQueryTableSnapshot()),
            Optional.ofNullable(getScanParams())));
}
```

**从不查 `sysTableSupportsTimeTravel()`**。paimon（能力位 `false`）与 iceberg（`true`）在此**同路**：都去拿**源表**的 pin。

3. `PluginDrivenMvccExternalTable.loadSnapshot():338` 的 point-in-time 分支在 `:396` 解析出**非 null** 的源表 `pinnedSchema`，`:406` 带着它返回。
4. 回到 `pinMvccSnapshot():899-909`：snapshot 非空且是 `PluginDrivenMvccSnapshot` ⇒ 调 `assertBoundColumnsResolveInPinnedSchema()`，拿**sys 表**的 bound 列（`snapshot_id`/`schema_id`/`commit_time`…）去比**源表** schema（`ts_scale_orc` 的数据列）。两个 schema 天然毫无交集 ⇒ **必然**抛 `UserException`。
5. 该 `UserException` 在 `getOrLoadPropertiesResult():1725` 内的 `:1739-1743` 被包装：

```java
try {
    pinMvccSnapshot();
} catch (UserException e) {
    throw new RuntimeException("Failed to pin MVCC snapshot for plugin-driven scan", e);
}
```

⇒ 客户端看到的就是观测到的文案。

## 正确的 guard 存在且正确，但**不可达**

`checkSysTableScanConstraints():1073` 文案正确（`:1083` `"Plugin system tables do not support scan params."`、`:1087` `"Plugin system tables do not support time travel."`，均含用例期望子串），逻辑也正确（`:1077` 查 `sysTableSupportsTimeTravel()`）。

但它只被 `getSplits():1106`、`startSplit():1454`、`startStreamingSplit():1551` 调用——**全部晚于** `getOrLoadPropertiesResult():1725`（该私有方法由 init/explain 路径经 `getNodeExplainString` 等触发）。第 4 步的 `UserException` 在 guard 有机会跑之前就炸了。**这是纯粹的顺序 bug。**

## 能力位与短路（已核实）

- `ConnectorScanPlanProvider.supportsSystemTableTimeTravel():89` 默认 `false`（paimon 未 override ⇒ 继承 `false`；`grep` 确认 fe-connector-paimon 零命中）。
- `IcebergScanPlanProvider.supportsSystemTableTimeTravel():340-341` = `true`。
- `PluginDrivenScanNode.sysTableSupportsTimeTravel():1096`（package-private，可 override，Mockito 可 stub）：

```java
boolean sysTableSupportsTimeTravel() {
    ConnectorScanPlanProvider scanProvider = resolveScanProvider();
    if (scanProvider == null) {
        return false;
    }
    return onPluginClassLoader(scanProvider, scanProvider::supportsSystemTableTimeTravel);
}
```

- `PaimonConnectorMetadata.applySnapshot():715-719` 对 `paimonHandle.isSystemTable()` **直接短路返回、不抛** ⇒ 跳过 pin 后 init 一定能走完到 `getSplits():1106` 的 `checkSysTableScanConstraints()`。

# Design

**在 `resolveSysTableSnapshotPin()` 取源表 pin 之前，若 `!sysTableSupportsTimeTravel()` 直接 `return Optional.empty()`。**

即：能力位 `false` 的连接器，其 sys 表**根本不进入 pin 解析**；scan 带着未 pin 的 handle 走完 init，到 `getSplits()` 时由既有的、正确的 `checkSysTableScanConstraints()` 抛出期望文案。

## 为什么优于 "assert carve-out"（reviewer 已推翻后者）

carve-out 方案 = 在 `pinMvccSnapshot():899` 的 assert 前加「sys 表跳过 guard」。缺陷：

1. **仍会调 `loadSnapshot()`** ⇒ 仍可能撞 `PluginDrivenMvccExternalTable.java:368` 的 `throw new RuntimeException(notFoundMessage(spec))`。该异常**不是** `UserException`，**不被** `:1740` 的 `catch (UserException e)` 拦截，会带着 paimon 自己的 not-found 文案裸奔到客户端 ⇒ 仍不含期望子串。
2. **block 2 变成掷硬币**：`FOR TIME AS OF "2024-07-11 16:01:57.425"` 的成败取决于 fixture 里是否存在 at-or-before 快照——存在则 `loadSnapshot` 成功返回、carve-out 跳过 assert、继续走到 guard（绿）；不存在则走 1. 的 `RuntimeException`（红）。**测试结果依赖数据**，不可接受。
3. 白白多一次 `getTableSchema` 跨插件往返。

能力位门控下，三个 block 全部**数据无关地**在 `resolveSysTableSnapshotPin` 就返回 empty，不碰 `loadSnapshot`，确定性地拿到期望文案。

## connector-agnostic 合规

分支建立在通用 fe-core 类型（`PluginDrivenSysExternalTable`）+ **既有能力 SPI**（`sysTableSupportsTimeTravel()` → `supportsSystemTableTimeTravel()`）之上，**不按源名/类型分支**，不新增 SPI，不在 fe-core 解析任何属性。iceberg（`true`）行为逐字节不变。符合「fe-core 通用 SPI 层禁 source-specific 代码」铁律。

本轮改的是**本分支自己引入**的 `resolveSysTableSnapshotPin`（P6.6-C1 WS-PIN），属「修本分支自己加的代码」，不触发「fe-core 源只出不进」律。

## 门控放置位置（关键）

必须放在**两个便宜的 early-return 之后**：

- 放在 `instanceof PluginDrivenSysExternalTable` 检查**之前** ⇒ 普通表路径也会调 `sysTableSupportsTimeTravel()` → `resolveScanProvider()` → 跨 classloader 调用，普通读路径不再字节等价，且平白增加开销。**不可**。
- 放在 selector null 检查**之后** ⇒ 无时间旅行的普通 sys 表扫描（绝大多数）仍在 `:1048` 返回，不付能力位查询的代价。

# Implementation Plan

## 1. `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java`

### 1.1 产品改动：`resolveSysTableSnapshotPin()` 加能力位门控

在 `:1048-1050` 的 selector 检查块**之后**、`:1051` 的 `PluginDrivenExternalTable source = ...` **之前**插入：

```java
        // Capability gate: a connector whose sys tables do NOT honor a pin (paimon — the default
        // supportsSystemTableTimeTravel()==false) must not resolve one. Resolving the SOURCE table's pin
        // for it hands back a non-null pinnedSchema whose columns are the SOURCE's, which the
        // pinMvccSnapshot bound-column assert then compares against the SYS table's columns -> always
        // throws, masking the real (and correct) checkSysTableScanConstraints rejection that runs later in
        // getSplits(). Skipping the pin lets init complete (the connector's applySnapshot is a no-op for a
        // sys handle anyway) so that guard emits its proper message. Generic: keyed on the existing
        // capability SPI, not on the connector's identity.
        if (!sysTableSupportsTimeTravel()) {
            return Optional.empty();
        }
```

（插入后原 `:1051` 起整体下移 ~10 行。）

### 1.2 注释改动 A：`:896-897` —— 已证伪

现文（`:896-897`）：
```java
        // latest / @incr / sys-table / hive scan carries a null pinnedSchema -> no-op.
```
（完整句自 `:896` 起：`// ... A` + `:897`）

证伪：sys-table **不**恒为 null pinnedSchema——本 bug 的整条链就是 sys-table 拿到了源表的**非 null** pinnedSchema（`PluginDrivenMvccExternalTable.java:396/406`）。改后（1.1 落地后此句才为真，因为门控后 sys 表不再进 `loadSnapshot`）：

```java
        // A latest / @incr / hive scan carries a null pinnedSchema -> no-op. A sys-table scan of a
        // time-travel-INCAPABLE connector never reaches here (resolveSysTableSnapshotPin gates on the
        // capability and returns empty); a CAPABLE one (iceberg) resolves the SOURCE table's pin, whose
        // pinnedSchema is the source's -> the assert below is meaningful for it and must run.
```

### 1.3 注释改动 B：`:923` —— 同一处证伪的**第 4 个实例**（任务未点名，本设计一并修）

`assertBoundColumnsResolveInPinnedSchema` 的 javadoc 现文（`:923`）：
```java
     * pinnedSchema (latest / {@code @incr} / sys-table / hive reference) is a no-op.</p>
```
同样错误地宣称 sys-table 恒 null。改为：

```java
     * pinnedSchema (latest / {@code @incr} / hive reference, and any sys-table reference on a
     * time-travel-incapable connector) is a no-op.</p>
```

### 1.4 注释改动 C：`:1039` —— 已证伪

现文（`:1038-1039`，`resolveSysTableSnapshotPin` javadoc 尾）：
```java
     * or — defensively — when the source is not MVCC-capable (the guard
     * {@link #checkSysTableScanConstraints} already rejects that case for the relevant connectors).
```

证伪**两重**：(a) `checkSysTableScanConstraints` 压根不检查「源是否 MVCC-capable」，它检查的是时间旅行能力位；(b) 更要命的是它在此处**尚未运行**（只在 `getSplits():1106` / `startSplit():1454` / `:1551` 调用，全都晚于 `getOrLoadPropertiesResult():1725` 触发的 `pinMvccSnapshot`）——「already rejects」是彻头彻尾的假设。改为：

```java
     * <p>Returns empty (no pin) when the target is not a sys table, when there is no time-travel selector,
     * when the connector's sys tables do not honor a pin ({@link #sysTableSupportsTimeTravel()} — paimon and
     * the default; its later {@link #checkSysTableScanConstraints} rejection is the user-visible error, and
     * that guard runs only at split time, AFTER this method, so it cannot be relied on here), or —
     * defensively — when the source is not MVCC-capable.
```

（同时删掉原 `:1035-1039` 中「the guard ... already rejects that case」的表述。）

## 2. `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenScanNodeSysTablePinTest.java` —— **必改，否则现有测试变红**

见 Risk Analysis §1。

### 2.1 `sysPinNode():56-62` 默认 stub 能力位为 `true`

```java
    private static PluginDrivenScanNode sysPinNode() {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        // Default: no time travel (plain scan). Time-travel cases override these.
        Mockito.doReturn(null).when(node).getQueryTableSnapshot();
        Mockito.doReturn(null).when(node).getScanParams();
        // Default: a connector whose sys tables DO honor a pin (iceberg-like). The pin-feed only runs for
        // such connectors; the incapable-connector gate is pinned by its own test below.
        Mockito.doReturn(true).when(node).sysTableSupportsTimeTravel();
        return node;
    }
```

### 2.2 类 javadoc `:49-52` —— 已证伪的**第 5 个实例**（任务未点名，一并修）

现文：
```
 * exactly to enable this (mirrors the sibling guard/pin tests). The guard
 * {@code checkSysTableScanConstraints} (tested separately) has already rejected this for connectors
 * whose sys tables do not honor a pin (paimon / {@code @incr}), so the fallback only ever runs for a
 * pin-capable connector.
```
与 §1.4 同一个假设（「guard 已经拒了」），同样为假。改为：
```
 * exactly to enable this (mirrors the sibling guard/pin tests). The pin-feed itself gates on
 * {@code sysTableSupportsTimeTravel()}: {@code checkSysTableScanConstraints} cannot be relied on here
 * because it runs only at split time, LONG AFTER this method (CI 996541 — the ungated pin resolved the
 * SOURCE table's schema for paimon and the bound-column assert then masked the guard's proper message).
```

### 2.3 `sysTableWithNonMvccSourceDoesNotPin():138-152` —— 防恒真化

门控位于 `instanceof MvccTable` 检查**之前**。该测试若不显式 stub `true`，会在门控处返回 empty，`assertFalse` 依然通过 ⇒ **测试变恒真**，不再验证它自称验证的 `instanceof MvccTable` 分支（违反 Rule 9）。§2.1 的 helper 默认 `true` 恰好保住它，但**必须显式加注释锁死这个依赖**，否则后人改 helper 会静默掏空该测试：

在 `:143` 前补一行：
```java
        // Capability MUST stay true here (helper default): with false the capability gate returns empty
        // first and this test would pass vacuously without ever exercising the instanceof MvccTable branch.
```

### 2.4 新增两个测试（见 Test Plan §Unit Tests）

## 3. `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenScanNodeMvccSchemaGuardTest.java`

`nullPinnedSchemaIsNoOp():116-118` 的注释（`:117-118`）现文：
```java
        // A latest / @incr / sys-table / hive reference carries a null pinnedSchema -> the guard is a no-op
        // (no version-at-snapshot schema to skew against), regardless of the bound columns.
```
同 §1.2 证伪。改为：
```java
        // A latest / @incr / hive reference — and a sys-table reference on a time-travel-incapable
        // connector, which never resolves a pin at all — carries a null pinnedSchema -> the guard is a
        // no-op (no version-at-snapshot schema to skew against), regardless of the bound columns.
```
（仅注释，断言不动。）

# Risk Analysis

## 1. 会打挂现有单测吗？—— **会，2 个，已定位并在 Implementation Plan §2.1 修掉**

`PluginDrivenScanNodeSysTablePinTest`（`fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenScanNodeSysTablePinTest.java`）直接测 `resolveSysTableSnapshotPin()`，helper `sysPinNode():56` **未** stub `sysTableSupportsTimeTravel()`。该 mock 是 `CALLS_REAL_METHODS` ⇒ 加门控后会跑**真** `sysTableSupportsTimeTravel():1096` → `resolveScanProvider():216-218` → `connector.getScanPlanProvider(...)`，而 mock 的 `connector` 字段为 `null`（无构造）⇒ **NPE**。`resolveScanProvider()` 是 `private`，Mockito **无法** stub 它，只能 stub package-private 的 `sysTableSupportsTimeTravel()`。

逐个方法判定：

| 类 | 方法 | 行 | 不修 helper 的结果 | 修后 |
|---|---|---|---|---|
| `PluginDrivenScanNodeSysTablePinTest` | `sysTableForTimeAsOfDelegatesToSourceLoadSnapshot` | :65 | **NPE 红** | 绿（能力位 true = iceberg 语义） |
| `PluginDrivenScanNodeSysTablePinTest` | `sysTableBranchTagDelegatesScanParams` | :86 | **NPE 红** | 绿 |
| `PluginDrivenScanNodeSysTablePinTest` | `sysTablePlainScanDoesNotPin` | :107 | 绿（`:1048` selector 检查先返回，门控在其后） | 绿 |
| `PluginDrivenScanNodeSysTablePinTest` | `normalTableNeverUsesSysFallback` | :124 | 绿（`:1045` instanceof 先返回） | 绿 |
| `PluginDrivenScanNodeSysTablePinTest` | `sysTableWithNonMvccSourceDoesNotPin` | :138 | 绿但**恒真**（门控先返回，不再走 `instanceof MvccTable`） | 绿且非恒真（§2.3） |

`PluginDrivenScanNodeSysTableGuardTest`（同目录）：全部测 `checkSysTableScanConstraints()`，本改动不碰该方法 ⇒ **不受影响**。其 helper `guardOnlyNode():55` 已 stub `sysTableSupportsTimeTravel()`，是 §2.1 的先例。

`PluginDrivenScanNodeMvccSchemaGuardTest`：全部测 `static assertBoundColumnsResolveInPinnedSchema(...)`，签名与逻辑均不动 ⇒ **不受影响**（只改 §3 注释）。

`IcebergScanPlanProviderTest.supportsSystemTableTimeTravelIsTrue():187-196`：只断言 provider 返 `true`，不碰 ⇒ 不受影响。

`PluginDrivenMvccExternalTableTest`：测 `loadSnapshot` 侧，不碰 ⇒ 不受影响。

## 2. 会影响那 550 个通过的用例吗？—— **不会**

- **普通（非 sys）表**：`resolveSysTableSnapshotPin():1045` 的 `instanceof PluginDrivenSysExternalTable` 先返回，门控在其后 ⇒ 路径**字节等价**，连 `sysTableSupportsTimeTravel()` 都不调（这是把门控放在两个 early-return 之后的直接理由）。所有普通表时间旅行用例零影响。
- **无时间旅行的 sys 表扫描**（`select * from t$snapshots`，占 sys 表用例绝大多数，含 `paimon_system_table` 前 2.5 节全部 `qt_*`）：`:1048` selector 检查先返回 ⇒ 零影响。
- **iceberg sys 表 + pin**：`supportsSystemTableTimeTravel()==true` ⇒ 门控放行，行为**逐字节不变**。
- **paimon sys 表 + pin**：唯一改变的路径，且当前就是**红**的。改后：跳过 pin → `PaimonConnectorMetadata.applySnapshot():715-719` 对 sys handle 短路不抛（已核实）→ init 走完 → `getSplits():1106` 的 `checkSysTableScanConstraints()` 抛出正确文案。
- 三个 block 的确定性：block 1/2（`getQueryTableSnapshot() != null`）走 `:1087`；block 3（`@incr` ⇒ `getScanParams() != null` 且 `incrementalRead()==true`）走 `:1083`。**均不依赖 fixture 数据**。

## 3. 残余风险

- **风险 R1（中）**：`checkSysTableScanConstraints()` 只在 `getSplits()` / `startSplit()` / `startStreamingSplit()` 上挂。若某条 sys 表查询路径（如纯 `EXPLAIN`、或 pruned-to-zero 短路）在这三处**之前/之外**结束，跳过 pin 后将不再抛任何错，退化为「静默忽略 time travel 读 latest」——比现在的「抛错但文案不对」更差（静默错 > 吵闹错）。当前用例走 `getSplits()` 故会绿，但**未证实**所有路径都过 guard。缓解：本设计不移动 guard（Rule 3 最小改动）；若要根治应把 guard 上提到 init/analyze，那是独立改动，风险面大得多，建议单开。
- **风险 R2（低）**：`sysTableSupportsTimeTravel()` 会触发 `resolveScanProvider()` → 跨 classloader 调用。仅发生在「sys 表 + 有 selector」这条冷路径，且 `checkSysTableScanConstraints()` 本就已在调它 ⇒ 无新增类加载语义。

# Test Plan

## Unit Tests

全部加在既有 `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenScanNodeSysTablePinTest.java`（Rule 3：该类已是 `resolveSysTableSnapshotPin` 的归属测试类，且已有 mock 基建；不新建类）。

### 新增 1 — `incapableSysTablePinIsSkippedAndSourceUntouched`

```java
    @Test
    public void incapableSysTablePinIsSkippedAndSourceUntouched() throws Exception {
        PluginDrivenScanNode node = sysPinNode();
        Mockito.doReturn(false).when(node).sysTableSupportsTimeTravel();   // paimon = the default
        PluginDrivenSysExternalTable sysTable = Mockito.mock(PluginDrivenSysExternalTable.class);
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        Mockito.doReturn(sysTable).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();
        Mockito.doReturn(source).when(sysTable).getSourceTable();

        // WHY (CI 996541): resolving the SOURCE table's pin for a connector whose sys tables do not
        // time-travel hands back a non-null SOURCE pinnedSchema; pinMvccSnapshot's bound-column assert then
        // compares the SYS table's columns against it -> always throws -> masks the real
        // checkSysTableScanConstraints message the user must see ("Plugin system tables do not support time
        // travel"). MUTATION: removing the !sysTableSupportsTimeTravel() gate -> non-empty + loadSnapshot
        // invoked -> red.
        Optional<MvccSnapshot> result = node.resolveSysTableSnapshotPin();
        Assertions.assertFalse(result.isPresent(), "a time-travel-incapable connector's sys table must not pin");
        // Load-bearing: proves we never touch the source at all -> no getTableSchema round-trip and, above
        // all, no RuntimeException(notFoundMessage) from loadSnapshot (which is NOT a UserException and so
        // would escape the catch in getOrLoadPropertiesResult with the wrong message).
        Mockito.verifyNoInteractions(source);
    }
```

断言意图：能力位 `false` ⇒ ①返 empty ②**完全不碰 source**（`verifyNoInteractions` 覆盖「不调 `loadSnapshot`」的要求，并同时锁死「不做无用 `getTableSchema` 往返」）。去掉门控即红。

### 新增 2 — `normalTableNeverConsultsSysTimeTravelCapability`

```java
    @Test
    public void normalTableNeverConsultsSysTimeTravelCapability() throws Exception {
        PluginDrivenScanNode node = sysPinNode();
        Mockito.doReturn(Mockito.mock(TableIf.class)).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();

        // WHY: the capability probe crosses into the plugin classloader (resolveScanProvider ->
        // getScanPlanProvider). The normal-table path must stay byte-identical and pay nothing for the sys
        // gate. MUTATION: hoisting the gate above the instanceof PluginDrivenSysExternalTable check -> the
        // probe fires for every normal scan -> red.
        Assertions.assertFalse(node.resolveSysTableSnapshotPin().isPresent());
        Mockito.verify(node, Mockito.never()).sysTableSupportsTimeTravel();
    }
```

断言意图：锁死门控的**放置位置**（普通表零成本）。把门控上提即红。

### 既有（`sysPinNode()` 加默认 `true` 后即覆盖 iceberg 不回归）

- `sysTableForTimeAsOfDelegatesToSourceLoadSnapshot():65` —— 能力位 `true` ⇒ 仍 `assertSame(resolved, ...)` + `verify(source).loadSnapshot(Optional.of(ts), Optional.empty())`。断言意图：**iceberg 的 sys 表 `FOR TIME AS OF` 仍拿到 pin，无回归**。门控若写成无条件 `return Optional.empty()` 即红。
- `sysTableBranchTagDelegatesScanParams():86` —— 同上，覆盖 `@branch`/`@tag`。
- `sysTableWithNonMvccSourceDoesNotPin():138` —— 依赖 helper 的 `true`（§2.3 已加注释锁死），继续验 `instanceof MvccTable` 防御分支。

## E2E Tests

**无需新增。** 理由：目标用例 `regression-test/suites/external_table_p0/paimon/paimon_system_table.groovy:154-167` 的三个 block **本身就是**本修复的 E2E 断言，且已存在于本分支——它们当前是红的，修复后应转绿。新写一个 e2e 只会复刻它。

- block 1/2 → `PluginDrivenScanNode.java:1087` `"Plugin system tables do not support time travel."` ⊃ 期望子串 `"system tables do not support time travel"`。
- block 3 → `:1083` `"Plugin system tables do not support scan params."` ⊃ 期望子串 `"system tables do not support scan params"`。

iceberg 侧：`grep` 显示 `regression-test/suites/external_table_p0/iceberg/` 下**当前没有** sys 表 + 时间旅行的 e2e（`t$snapshots FOR VERSION/TIME AS OF`、`t$files@branch`），故本改动对 iceberg 的不回归**只有单测覆盖**。补 iceberg sys 表时间旅行 e2e 是既有欠账（对齐 memory「iceberg-on-HMS 新增能力须补 e2e」的同类精神），**不在本 T3 范围**，建议单开跟踪。

# 仍未证实

1. **（继承自 issue 文档）** block 2 `FOR TIME AS OF "2024-07-11 16:01:57.425"` 在 fixture 中是否存在 at-or-before 快照——**未证实**（未跑 e2e）。本设计的能力位门控使该问题**不再影响结果**（数据无关地在 pin 解析前返回），但也因此**未证实**原假设的真伪。
2. **风险 R1**：是否**所有** paimon sys 表查询路径都必经 `getSplits()` / `startSplit()` / `startStreamingSplit()` 三者之一（从而必过 `checkSysTableScanConstraints()`）——**未证实**。已确认目标用例的三个 block 走 `getSplits()`；纯 `EXPLAIN` 等路径是否也过 guard 未验。若不过，跳过 pin 会退化为静默读 latest。
3. `getOrLoadPropertiesResult():1725` 是本用例中**第一个**触发 `pinMvccSnapshot()` 的调用点（早于 `getSplits():1157`）—— 由「观测到的异常文案 = `:1742` 的包装文案」**反推**，未用断点/日志直接证实调用顺序。另三个调用点（`:1157` / `:1483` / `:1570`）均在 guard 之后，故不影响结论。
4. 本设计**未编译、未跑任何 maven**（按任务约束：另一 session 正在同一工作树跑 maven）。所有行号、类型关系（`PluginDrivenMvccExternalTable` IS-A `PluginDrivenExternalTable` 且 implements `MvccTable`）、Mockito 可 stub 性（package-private 非 final）均由**静态阅读**核实，`sysTablePlainScanDoesNotPin` 等「不受影响」判定亦为静态推演，**未经执行验证**。
5. §1「`connector` 字段为 null ⇒ NPE」是基于「Mockito mock 不跑构造函数」的推演；也可能因其他原因抛别的异常——但**测试变红**这一结论不变（无论 NPE 还是返 false 导致断言失败，两个测试都必须修）。
