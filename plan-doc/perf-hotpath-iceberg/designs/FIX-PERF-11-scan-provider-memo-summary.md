# FIX-PERF-11 (C14 slice #3) — 小结：memoize resolveScanProvider() per scan

> 设计见 [`FIX-PERF-11-scan-provider-memo-design.md`](./FIX-PERF-11-scan-provider-memo-design.md)。仅 C14 第 3 子项（provider 每分片重复分配），其余 C14 子项按 recon 结论暂缓（用户 2026-07-19 定）。

## Problem

`PluginDrivenScanNode.resolveScanProvider()`（fe-core 通用扫描节点）= `connector.getScanPlanProvider(currentHandle)`，SPI 契约"providers are built fresh/stateless per call"。它在**每分片/每 range** 热路径被重复调用（`getFileCompressType:603` 每 split、`getDeleteFiles:673` 每 range，另 ~10 个规划调用点），对 new-per-call 连接器（iceberg/paimon/hive）每分片**重新分配一个 provider + 两次 TCCL 类加载器切换**（对 iceberg 是 identity 空操作）。三条枚举路径全中：eager 协调线程逐 split、partition-batch 多并发 batch 线程、streaming 单泵线程。量级为审计问题类最低档（~100-200ns/split、无远程 IO，10 万 split 约几百毫秒 + GC 压力）。

## Root Cause

provider 是 `currentHandle` 的纯函数（单格式连接器忽略 handle；唯一按 handle 选的 hive 网关按**格式**选、格式一次扫描内不变，且 hms 休眠）。`currentHandle` 只在规划早期 pushdown/pin 换新对象，进入 split 枚举后恒定 → 一次扫描本应至多解析几次，实际每分片一次。且"整次扫描共享一个 provider 实例"**已是**重活路径（`startSplit:1542`/`startStreamingSplit:1628` 捕获一次、并发 async planScan 共享）的现状。

## Fix

对 `resolveScanProvider()` 做**按 `currentHandle` 身份键**的 memo：单个不可变 `ResolvedScanProvider{handle, provider}` holder，经**一次 volatile 写**发布。命中（`holder.handle == currentHandle`）复用；miss（首次或 handle 被 pushdown/pin 换新对象）重解析并重发布。null provider（无扫描能力连接器）正确缓存返回。

- **逐字节不变**：身份键使其与"每次都 `connector.getScanPlanProvider(currentHandle)`"机械等价，不依赖任何连接器语义假设；下游 thrift 不变。
- **并发安全**：final 字段 + 单次 volatile 写 = 安全发布，并发 appendBatch 线程绝不读到"新键配旧值"的撕裂；冗余并发构建无害（同 handle→等价 provider）。与既有 async 共享 provider 现状一致。
- **改动面**：仅 `PluginDrivenScanNode.java`——1 个 volatile 字段 + 1 个私有静态 holder 类 + 改 `resolveScanProvider()` 方法体，约 15 行。零调用点/SPI/连接器改动。

## 铁律核对

- A（fe-core 只减不增）：净增 ~15 行；本任务空间 README 预授权"通用框架层 perf 改动"，用户 2026-07-19 选定，已向用户明示净增。属被授权例外（非 scaffolding relocation）。
- B（禁按源名分支）：纯机械 memo，零源名分支。✓
- C（对所有连接器 byte+cost 双不变）：byte 等价；cost 严格更少或相等（memo），无连接器回退。✓
- D（BE/thrift）：不触碰。✓

## 设计红队（3 视角对抗，clean-room）

并发/JMM、跨 8 连接器字节等价、生命周期与必要性——**三者全 SURVIVES、0 缺陷、一致"照方案实现"**。红队额外核实：所有连接器的 `getScanPlanProvider` 要么 new-per-call（本改动省分配）、要么已字段缓存（jdbc/maxcompute，本改动为严格 no-op），无一回退；provider 实例除 iceberg `scanProfileStash`（仅经 getSplits 单一捕获 local 走、不经 per-split resolve）外皆 final/不可变；per-split 方法（`adjustFileCompressType`/`getDeleteFiles`/`classifyColumn`）皆纯函数。修正设计文档一处笔误（jdbc/maxcompute 实为字段缓存非 new-per-call，方向安全）。

## Tests

- **TDD 守门（新）**：扩 `PluginDrivenScanNodeScanProviderSelectionTest.memoizesProviderForStableHandleAndReResolvesOnHandleChange`——固定 handle 连调 3 次断言底层 `getScanPlanProvider` **恰 1 次**（修前 3 次，先观察 RED：`Wanted 1 time ... but was 3` 确证）；换 handle 后再 2 次断言重解析恰 1 次 + provider 匹配。镜像既有测试的 `CALLS_REAL_METHODS`+`Deencapsulation` 构造。
- **既有 parity 全绿**：`PluginDrivenScanNode*Test` 全 19 类 **103 pass / 0 fail / 0 error / 0 skip**（含天然守门 `resolvesProviderForCurrentHandle`：换 handle 须重解析）。连接器单测不需改一行。
- **两段验（fe-core）**：`mvn test -pl fe-core -am -Dtest='PluginDrivenScanNode*Test'` **BUILD SUCCESS + fe-core Checkstyle 0 违规**。change 为 fe-core-only、iceberg 连接器不依赖 fe-core → iceberg 模块不受影响（无需另跑，无编译/测试耦合）。

## Result

`87ff73b1a95`：fe-core `PluginDrivenScanNode` scan-provider 每分片重分配 O(splits)→ 每 handle 1 次。度量守门 `verify(times(1))`。全 `PluginDrivenScanNode*Test` 103 绿 + 0 checkstyle，0 回归。

## 度量口径

守门测试 `verify(connector, times(1)).getScanPlanProvider(handle)` 直接断言"每 handle 解析 1 次"（对齐 PERF-01/03 `loadCountForTest()==1`）；并发正确性由构造（不可变 holder + volatile）保证，不写 flaky 压测。
