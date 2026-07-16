# FIX-L3 — trino 元数据方法开事务却从不 commit/close

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §1 表 L3（原 P2-1）。
> 严重度：🟡 低（连接器局部资源泄漏；与 master parity,翻闸后量级放大）。范围：`fe-connector-trino` 一文件。
> HEAD 复核基线：`88aa55b831b`。

## Problem

`TrinoConnectorDorisMetadata` 的 **6 个** FE 侧元数据方法各自
`trinoConnector.beginTransaction(READ_UNCOMMITTED, true, true)` 开一个 Trino 事务、用它取 `ConnectorMetadata`、
映射结果为 Doris 类型后**直接 return，从不 commit/rollback**：
- `listDatabaseNames`:86、`listTableNames`:102、`getTableHandle`:127、`getTableSchema`:165、
  `applyFilter`:239、`applyProjection`:300。

每次规划都会走 listTableNames/getTableHandle/getTableSchema/applyFilter/applyProjection → Trino connector 的
transaction manager 里累积未释放的事务句柄（部分连接器 per-txn 持资源/状态）→ 长期内存增长 / 资源泄漏。

## Root Cause

FE 侧把 Trino connector 当「开事务→取 metadata→用完丢」，缺 try/finally 释放。Trino 引擎正常路径由
`TransactionManager` 负责 commit/rollback;这里直接驱动 connector,释放责任落在调用方,却漏了。

## Design（6 站就地 try/finally；scan 站不动）

**只修 6 个 FE-only 元数据站**。每站把「beginTransaction 之后到所有 return」包进 `try`,`finally` 里经小 helper 释放事务:
```java
ConnectorTransactionHandle txn = trinoConnector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true, true);
try {
    ConnectorMetadata metadata = trinoConnector.getMetadata(connSession, txn);
    ... 原方法体(含各 early-return)不变 ...
    return result;
} finally {
    releaseQuietly(txn);
}
```
小 helper（集中「masking-safe 释放」,避免 6 站各 4 行 try/catch 重复）:
```java
private void releaseQuietly(io.trino.spi.connector.ConnectorTransactionHandle txn) {
    try {
        trinoConnector.commit(txn);
    } catch (RuntimeException e) {
        LOG.warn("Failed to release Trino metadata transaction", e);  // 不 mask body 异常(Rule 12)
    }
}
```

**决策要点(已核 + 对抗复审 `agent a182a049f` SOUND_WITH_CHANGES 折入)**：
- **commit + swallow-log(masking-safe)**：事务 **read-only READ_UNCOMMITTED**,无写入 → commit≡rollback,只为「从 txn
  manager 释放」。取 `commit`(对齐 reverify + 成功语义)。**复审命中(minor)**：「read-only commit 不会抛」是**未证的连接器相关假设**
  (某连接器 commit 可能关 JDBC 连接/metastore client 而抛),裸 finally-commit 会 mask body 的真异常(Rule 12 fail-loud)。
  → 释放包在 `releaseQuietly` 的 `try/catch(RuntimeException)+LOG.warn`,既释放又不 mask。Trino 引擎本身是 rollback-on-error/
  commit-on-success;严格镜像须每个 return 前插 commit(方法多 early-return→太侵入),`releaseQuietly` 是保留单-finally 简洁的最小安全形。
- **就地 try/finally 而非抽 lambda helper**：镜像**同文件 scan 站已有的 `try{...}finally{metadata.cleanupQuery(...)}`
  per-site 惯用法**(Rule 11 conform)+ 保持 surgical(Rule 3),不重构 6 个方法为 lambda。
- **条件开事务保持**：`applyFilter`(`tupleDomain.isAll()` early-return 在 beginTransaction 前)、
  `applyProjection`(`projections.isEmpty()`/`trinoProjections.isEmpty()` 在前)——`try` 从 **beginTransaction 之后**起,
  不改「满足前置才开事务」的现状,不新增无谓事务。
- **commit 后句柄仍可用(安全性核实)**：6 方法返回的 `TrinoTableHandle`/列句柄是 Trino **不可变 opaque 值对象**,
  scan 时 `TrinoScanPlanProvider.planScan` 会**另开自己的事务**(:111)复用它们——今日事务虽泄漏但 return 后即无用,
  且句柄跨事务复用本就成立 → **commit 元数据事务不影响任何后续使用**。已核。

**scan 站不动**（`TrinoScanPlanProvider.planScan:111`）：它 `beginQuery` 后把 **txnHandle 序列化成 JSON 发给 BE**
（`.transactionHandle(txnHandleJson)`,:223/:252,BE JNI scanner 用它读数据）→ 事务**必须保持打开**,故只 `finally
{ metadata.cleanupQuery }` 不 commit。此为设计,L3 不碰。

## Risk Analysis

- **破坏 scan / 句柄**：无（见上「commit 后句柄仍可用」+「scan 站不动」核实）。
- **异常 mask**：read-only commit 不抛 → finally-commit 不 mask（见上）。
- **行为变更**：仅新增事务释放,不改任何返回值/映射逻辑。字节级对返回结果不变。
- **铁律**：连接器局部,不碰 fe-core,不解析属性,无 source-name 分支。import 门禁不受影响（不新增 fe-core import）。

## Implementation Plan

改 `fe-connector-trino/.../TrinoConnectorDorisMetadata.java` 6 站,各加 try/finally。不动 scan provider。

## Test Plan

### Unit Tests — ⚠ 受阻,登记

驱动这 6 个方法需构造 `io.trino.Session`(连接器构造入参,方法内 `trinoSession.toConnectorSession(...)` 必用)。
`io.trino.Session` 是重量级具体类(大 builder,无既有测试夹具;本模块现有 4 个 UT 均不驱动 `TrinoConnectorDorisMetadata`,
正因此墙)。为一个低危 parity 泄漏 fabricate 全套 Trino SPI(`Connector`+`ConnectorMetadata`+`Session`+`CatalogHandle`)
夹具**不成比例**。→ **本条不加行为 UT**,以 build-compile(验 `commit(txn)` 签名 + 无 checkstyle)+ 对抗复审 + e2e 兜底。
**不静默**：显式登记 UT-wall（Rule 12）。

### E2E — live-gated（trino 真集群）

trino-connector 目录跑一批查询(listTables / desc / 带谓词 filter / 投影),事务不再累积;功能结果与修前一致。
需真 trino 插件 + 后端,live-gated(memory `hms-iceberg-delegation-needs-e2e`)。

## 备注

L4/L5/L6 亦在本连接器,随后各自独立 commit（L5 顺带在 `listTableNames` 加 `.distinct()`,与本条同方法但分开提交）。
