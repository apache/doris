# FIX-PERF-11 (C14 slice #3) — memoize resolveScanProvider() per scan

> **范围**：仅 C14 的第 3 个子项——**扫描 provider 每分片重复解析/分配**。C14 的其余三项（路径重复解析 #1/#2、build-then-discard 分区列 #4）**不在本次范围**（#1/#2 有跨连接器 Hadoop Path 字节风险、#4 需新增 SPI 能力位给 fe-core 加面，均按 recon 结论暂缓）。用户 2026-07-19 拍板：只做这个安全小赢。
> 归属：`PERF-11` 的一个独立 commit（连同 C12+C15a+C13-plan 的 `10b7d29423f` 之后的第二块）。

## Problem

`PluginDrivenScanNode.resolveScanProvider()`（`fe-core`，第 224-226 行）= `connector.getScanPlanProvider(currentHandle)`。SPI 契约（`Connector.java:74-83`）明确 "providers are built fresh/stateless per call"。**多数连接器每次调用 new 一个 provider 实例**（iceberg `IcebergConnector.getScanPlanProvider:670` / paimon `:283` / hive `:204`）；jdbc（`:114`）/maxcompute（`:218`）则**字段缓存**复用同一实例、es/trino 做**幂等惰性 init**——故本 memo 对前者去掉每分片重复分配、对后者是**严格 no-op**，**任何连接器都不回退**（红队跨 8 连接器逐一核实）。

该方法在扫描热路径被**每分片**重复调用：
- `getFileCompressType(FileSplit)`（第 603-609 行）每分片调 `resolveScanProvider()` 再 `onPluginClassLoader(...)` 做**两次类加载器切换**跑 `adjustFileCompressType`（iceberg 是 identity 空操作）。
- `getDeleteFiles(TFileRangeDesc)`（第 673-679 行）每 range 亦调 `resolveScanProvider()`。
- 三条枚举路径均命中：eager（`FileQueryScanNode:435` 协程内逐 split）、partition-batch（`startSplit:1551-1590` 多个 `CompletableFuture` **并发** batch 线程经 `SplitAssignment.appendBatch:114` 逐 split 回调）、streaming（`startStreamingSplit:1630` 单泵线程逐 split）。

**成本量级**（recon + verify 双确认）：纯本地 CPU + 短命对象分配，**无远程 IO**；每分片新建 provider（~100-200ns）+ 两次 TCCL swap。10 万 split 约几百毫秒 + GC 压力，百万 split 秒级；1 万 split 以下不可见。是审计问题类里量级最低的一档（variant D：循环不变量未 hoist、无缓存、默认配置）。

## Root Cause

provider 是 `currentHandle` 的**纯函数**：SPI 契约保证 "the selected provider does not change across a scan"，单格式连接器默认忽略 handle（`Connector.java:85-86` 直接委派 no-arg），唯一按 handle 选 provider 的是 hive 异构网关（`HiveConnector.getScanPlanProvider(handle):222`，按 handle **类型/格式**选 sibling，格式在一次扫描内不变；且 hms 尚未进 `SPI_READY_TYPES`，休眠）。而 `currentHandle` 只在规划早期被 pushdown/pin 换新对象（filter `:779`/limit `:817`/projection `:836`/mvcc pin `:919`/rewrite scope `:1023`/topn `:1042`），**全部发生在 split 枚举之前**；进入逐 split 阶段后 `currentHandle` 恒定。

所以一次扫描内 `resolveScanProvider()` 本应至多解析**几次**（每个不同的 handle 对象一次），实际却**每分片一次**——纯重复分配。

**既有先例证明"共享一个 provider 实例"是安全且已在用的模式**：`startSplit:1542` / `startStreamingSplit:1628` 已在协调线程 `resolveScanProvider()` 一次，把该**单一实例**捕获进 `final scanProvider`，再在多个**并发** async 任务里共享它做 `planScanForPartitionBatch`（`:1562`）/`streamSplits`（`:1633`）。即"一次扫描共享一个 provider（含并发线程）"已是重活路径的现状。本修复只是把这份共享延伸到当前仍在每分片重解析的 `getFileCompressType`/`getDeleteFiles`。

## Design

在 `PluginDrivenScanNode` 内对 `resolveScanProvider()` 做**按 `currentHandle` 身份记忆**的 memo，用**单个不可变 holder + 一次 volatile 发布**，使 partition-batch 的并发 appendBatch 线程始终读到自洽的 `(handle, provider)` 对。

```java
// 字段（挂在其它 cache 字段附近，如 cachedPropertiesResult 旁）
private volatile ResolvedScanProvider resolvedScanProvider;

// 私有静态不可变 holder
private static final class ResolvedScanProvider {
    private final ConnectorTableHandle handle;
    private final ConnectorScanPlanProvider provider;   // 可为 null（无扫描能力的连接器）
    ResolvedScanProvider(ConnectorTableHandle handle, ConnectorScanPlanProvider provider) {
        this.handle = handle;
        this.provider = provider;
    }
}

private ConnectorScanPlanProvider resolveScanProvider() {
    ResolvedScanProvider cached = resolvedScanProvider;
    if (cached == null || cached.handle != currentHandle) {   // 身份比较（==）
        cached = new ResolvedScanProvider(currentHandle,
                connector.getScanPlanProvider(currentHandle));
        resolvedScanProvider = cached;   // 单次 volatile 写，原子发布 (handle, provider)
    }
    return cached.provider;
}
```

**为何这样安全且字节不变**：
- **身份键（`==`）**：每次都以当前 `currentHandle` 为键，命中才复用。`currentHandle` 一换对象（pushdown/pin）即 miss→重解析——与"每次都 `connector.getScanPlanProvider(currentHandle)`"**逐字节等价**，无需依赖"格式不随 pushdown 变"这类连接器语义推断，纯机械不变式。**无需**在 `convertPredicate` 等处显式失效（身份 miss 自动失效）。
- **null 正确缓存**：无扫描能力连接器 provider=null，holder 携带 `(handle, null)`，下次同 handle 直接返回 null 不重解析。
- **并发安全**：两个字段合成一个不可变 holder，经**单次 volatile 写**发布——读者要么见旧 holder、要么见新 holder，绝不会读到"新 key + 旧 provider"的撕裂组合（这正是裸双字段 memo 的数据竞争）。并发下多线程可能各建一个 holder（冗余但无害，因同一 `currentHandle`→等价 provider），每个返回值自洽。`currentHandle` 在并发阶段不再变更（pushdown/pin 都在 `CompletableFuture.runAsync` 之前），且 executor 提交建立 happens-before，async 线程读到稳定 currentHandle。
- **共享 provider 实例安全**：本路径调用的 provider 方法（`adjustFileCompressType` iceberg=identity/hive=纯 LZ4 重映射、`getDeleteFiles` 只读 params）**皆纯函数/只读**，provider 的缓存（manifest/format/table）都挂在**长生命周期 connector 上并注入**（非 provider 自身状态）。且既有 async planScan 已并发共享一个 provider 实例（见 Root Cause 先例），本改动与现状一致。

**改动面**：仅 `PluginDrivenScanNode.java`——加 1 个 volatile 字段 + 1 个私有静态 holder 类 + 改 `resolveScanProvider()` 方法体（约 15 行）。12 个 `resolveScanProvider()` 调用点全部自动受益，**无一处签名变更**。

## Constraints check（铁律）

- **铁律 A（fe-core 只减不增）**：本改动给 fe-core **净增** ~15 行（1 字段 + 1 holder + 方法体）。**但**：本任务空间 README 已**预先授权** PERF-09/C14 类"fe-core 通用框架层性能改动"（"允许改——惠及所有连接器、非 source-specific"），用户 2026-07-19 亦已选定本项。这是"通用框架 perf memo"，非"删旧代码期把源逻辑搬进 fe-core 的 scaffolding relocation"，属被授权的例外。**已向用户点明净增性质**。
- **铁律 B（connector-agnostic，禁按源名分支）**：memo 纯机械（缓存一个既有 connector-agnostic 调用的结果），**零源名分支**。✓
- **铁律 C（对所有连接器 byte+cost 双不变）**：byte——每 handle 返回等价 provider，下游 thrift 不变；cost——严格更少或相等的分配（memo），**任何连接器都不回退**。✓
- **铁律 D（BE/thrift 协议）**：**不触碰** BE/thrift。✓ 无需该轴签字。

## Implementation Plan

1. `PluginDrivenScanNode.java`：加 `private volatile ResolvedScanProvider resolvedScanProvider;` 字段（放 `cachedPropertiesResult` 附近）+ 私有静态 `ResolvedScanProvider` holder 类（放文件内既有私有静态类约定位置）+ 改 `resolveScanProvider()` 方法体为上述 memo。方法 javadoc 补一句"memoized per currentHandle"。
2. 不改任何调用点、不改任何 SPI、不改任何连接器。

## Risk

- **低**。唯一微妙点是并发（partition-batch），已由不可变 holder + volatile 发布消解，且与既有 async 共享 provider 的现状一致。
- 反向验证锚点：既有测试 `PluginDrivenScanNodeScanProviderSelectionTest.resolvesProviderForCurrentHandle` 断言"handle 变则重解析、不永久缓存首个"——本 memo 身份键**保住**该行为（handle 变→miss→重解析），故该测试是天然的 parity 守门，必须继续绿。

## Test Plan（含度量守门）

- **新守门测试**（扩 `PluginDrivenScanNodeScanProviderSelectionTest`）：`memoizesProviderForStableHandleAndReResolvesOnHandleChange`——用计数 Mock connector：
  - 固定 `currentHandle=icebergHandle`，调 `resolveScanProvider()` 3 次 → `Mockito.verify(connector, times(1)).getScanPlanProvider(icebergHandle)`（证每分片重分配已消除，修前会是 3 次）。
  - 换 `currentHandle=hiveHandle`，调 2 次 → `verify(connector, times(1)).getScanPlanProvider(hiveHandle)` + `assertSame(hiveProvider, ...)`（证 handle 变则重解析、且再命中缓存）。
  - 镜像既有测试的 `Mockito.CALLS_REAL_METHODS` 部分 mock + `Deencapsulation.setField/invoke` 构造法（对齐 `catalog-spi-fe-core-test-infra` 记忆）。
- **既有测试全绿**（byte-parity）：`PluginDrivenScanNodeScanProviderSelectionTest` + `PluginDrivenScanNode*Test`（DeleteFiles/ClassifyColumn/BatchMode/... 全套）+ 相关连接器 UT 不需改一行。
- **两段验**（本项碰 fe-core）：iceberg 连接器不依赖 fe-core，故 fe-core 改动 + 其单测须**单独** `mvn test -pl fe-core -am -Dtest=PluginDrivenScanNode*Test -DfailIfNoTests=false -Dmaven.build.cache.enabled=false -f <abs>/fe/pom.xml`；另跑一遍 iceberg 全模块 `install` 确认无回归。
- **度量口径**：守门测试的 `verify(times(1))` 即"每 handle 解析 1 次"的直接断言（对齐 PERF-01/03 的 `loadCountForTest()==1` 模式）；并发正确性由构造保证（volatile 不可变 holder），不写 flaky 压测。
