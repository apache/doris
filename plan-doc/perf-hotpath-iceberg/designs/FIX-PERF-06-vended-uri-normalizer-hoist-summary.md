# FIX-PERF-06 小结 — vended-credentials URI 归一化的 scan 级提升

> 覆盖审计发现 **C3**（P1）。commit `6294edf2833`。路线由用户 2026-07-18 拍板：连接器侧「准备一次、逐文件套用」。

## Problem

REST catalog + `iceberg.rest.vended-credentials-enabled=true` 下，扫描规划期每处理**一个 data file、每一个 delete file**都把整个 scan 内不变的 vended token 重新翻成一整套 `StorageProperties`（`StorageProperties.createAll` + 新建 hadoop `Configuration` + 逐 key set）。50k 文件 ≈ 数十秒纯 FE CPU，MOR 翻倍。

## Root Cause

贵活 `buildVendedStorageMap(token)` 只依赖 token、与 per-file URI 无关；token 每 scan 只提取一次且同一 Map 实例穿过所有归一化调用。即：一份完全相同的输入被逐文件重复派生 O(N_files + N_deletes) 次。`DefaultConnectorContext` 是 per-catalog、跨查询共享的长生命周期对象，故**不**在其上做跨查询 memo（并发冲刷退化 + 凭证跨查询保留贴近红线）。

## Fix（route A）

- **SPI**（`ConnectorContext`）新增 default `UnaryOperator<String> newStorageUriNormalizer(token)`：默认逐文件折回 `normalizeStorageUri`（无提升、行为不变，其它连接器零影响）。
- **fe-core**（`DefaultConnectorContext`）override：把 token→effective 存储配置的贵活**惰性做一次并 memo**，逐文件只做廉价 `LocationPath.of(rawUri, effective)`。逐字对齐原逐文件版：空 URI 短路、vended 覆盖 static、坏路径 fail-loud、异常时机（惰性→首个非空 URI 才派生）全部不变。normalizer 是 scan 局部对象、单线程套用、scan 结束即回收——无跨查询状态、无凭证保留。
- **iceberg**（`TcclPinningConnectorContext`）override 透传到真 delegate（拿到 fe-core 的提升；此路径纯 fe-core、无需 TCCL pin）。
- **iceberg**（`IcebergScanPlanProvider`）三处 scan-start（同步 data / 流式 / position_deletes）在 extractVendedToken 后构造一次 normalizer；六个 per-file seam（buildRangeForTask/buildRange/buildDeleteFiles/convertDelete/buildPositionDeleteRange/planCountPushdown + streaming source 字段）参数由 token 换成 normalizer；旧 `normalizeUri` helper 删除。
- 写路径 `getBackendFileType`（每写一次、非 per-file）不在范围。

## Tests

- **iceberg 全模块 974 UT 绿 / 0 fail / 1 skip**（含迁移的 8 处 convertDelete 签名对齐 + 两处路径归一化 parity 断言保留）。
- **减负守门（新增）**：`planScanDerivesUriNormalizerOncePerScanNotPerFile` —— 一次扫 3 文件断言 `newNormalizerCount==1`（token→config 派生每 scan 一次）且 `normalizeCount==3`（路径归一化 N 次）。`RecordingConnectorContext` 未 override 前继承 SPI 默认逐文件折回，现存 recording 断言零改动。
- **fe-core parity（新增，`DefaultConnectorContextNormalizeUriTest` 9→13 UT 绿）**：normalizer.apply 逐字等于 `normalizeStorageUri(uri, token)`（vended 覆盖 / static 回退 / 空URI短路 / 坏路径 fail-loud）+ 一个 normalizer 服务多个 URI。

## Result

一次 vended scan 内 `buildVendedStorageMap`/`StorageProperties.createAll` 从 O(N_files+N_deletes) 次降到 1 次；50k 文件的数十秒 FE CPU 空耗消除。行为对 vended 与非 vended 目录均不变。

## 可复用产物

- **gate 判据延伸**：scan 内复用（token 恒定）非跨查询，不涉凭证泄漏——与 PERF-05 的跨查询凭证 gate 正交（此处安全因作用域是 scan 而非 catalog）。
- **SPI「准备一次、套用多次」范式**：`newStorageUriNormalizer` 可被其它 vended 连接器（paimon 每文件亦 normalize）按需 opt-in；默认无副作用。
