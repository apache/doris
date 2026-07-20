# FIX-PERF-06 设计 — vended-credentials 每文件重建 StorageProperties 的 scan 级提升

> 覆盖审计发现 **C3**（P1）。路线由用户 2026-07-18 拍板：**连接器侧「准备一次、逐文件套用」**（新增一个通用归一化接口），非 fe-core 框架 memo。
> 行号基线 = HEAD `27903c9e5d1`（复核已重 grep，见下）。

---

## Problem

REST catalog + `iceberg.rest.vended-credentials-enabled=true` 时，扫描规划期每处理**一个 data file、每一个 delete file**，都要把整个 scan 内不变的 vended token 重新翻成一整套 `StorageProperties`（含遍历所有 provider + 新建 hadoop `Configuration` + 逐 key `set` + `StorageProperties.createAll` 推导 region/endpoint）。50k 文件 ≈ 数十秒纯 FE CPU，MOR（带 delete）翻倍。

## Root Cause（复核确认，HEAD 行号）

- 贵活 `DefaultConnectorContext.buildVendedStorageMap(token)`（`DefaultConnectorContext.java:225-242`）**只依赖 token，与 `rawUri` 无关**；`rawUri` 仅用于其后廉价的 `LocationPath.of(rawUri, effective)`（`normalizeStorageUri:392-409`）。
- vended token 一次 scan 只提取一次，且是**同一个 Map 实例**穿过所有归一化调用：
  - 同步数据路径 `planScanInternal:647` → `buildRangeForTask:750` → `buildRange:1131` → `normalizeUri(rawDataPath, vendedToken):1191` + `buildDeleteFiles:1215` → `convertDelete:1242` → `normalizeUri(delete.path, vendedToken):1243`。
  - 流式路径 `streamSplits:496` → `IcebergStreamingSplitSource` 字段 `vendedToken:543` → `buildRangeForTask:578`（同上）。
  - COUNT(*) 下推 `planCountPushdown:1084` → `buildRange`（同上）。
  - 位置删除系统表 `:904` → `buildPositionDeleteRange:929` → `normalizeUri(originalPath, vendedToken):935`。
- 在这些 seam 里 `vendedToken` **只服务于 `normalizeUri`**（props 生成走另一条 `getScanNodeProperties → vendStorageCredentials`，每 scan 一次，非本病灶）。
- `DefaultConnectorContext` 是 **per-catalog、跨查询（含并发）共享**的长生命周期对象（`PluginDrivenExternalCatalog.java:197`）——这正是本设计**不**在其上做跨查询 memo 的原因（并发冲刷退化 + 凭证跨查询保留贴近红线）。

## Design（route A：scan 级 normalizer）

把「token → effective 存储配置」的贵活从 per-file 循环里提升到 scan 开始一次，封装成一个**只做廉价路径归一化**的 `UnaryOperator<String>`，逐文件复用。

### 新增 SPI seam（连接器无关，默认无副作用）

`fe-connector-spi / ConnectorContext.java` 新增 default 方法：

```java
default UnaryOperator<String> newStorageUriNormalizer(Map<String, String> rawVendedCredentials) {
    // 默认：无引擎机制（离线测试 / 无 context）→ 每次 apply 折回 normalizeStorageUri(rawUri, token)，
    // 行为与逐文件调用完全一致，不做提升。任何未 override 的连接器不受影响。
    return rawUri -> normalizeStorageUri(rawUri, rawVendedCredentials);
}
```

`fe-core / DefaultConnectorContext.java` override（**唯一做提升的真实现**）：

```java
@Override
public UnaryOperator<String> newStorageUriNormalizer(Map<String, String> rawVendedCredentials) {
    // 贵活一次：token → effective map（vended 覆盖 static，与 normalizeStorageUri 同一 precedence）。
    Map<StorageProperties.Type, StorageProperties> vended = buildVendedStorageMap(rawVendedCredentials);
    Map<StorageProperties.Type, StorageProperties> effective =
            vended != null ? vended : storagePropertiesSupplier.get();
    // 逐文件只做廉价归一化（保留 empty-uri 短路 + fail-loud LocationPath），与 normalizeStorageUri 逐字对齐。
    return rawUri -> Strings.isNullOrEmpty(rawUri)
            ? rawUri
            : LocationPath.of(rawUri, effective).toStorageLocation().toString();
}
```

`fe-connector-iceberg / TcclPinningConnectorContext.java` 增 override，透传到真 delegate（否则 SPI 默认会用本 wrapper 的 `normalizeStorageUri` 逐文件折回，拿不到 `DefaultConnectorContext` 的提升）：

```java
@Override
public UnaryOperator<String> newStorageUriNormalizer(Map<String, String> rawVendedCredentials) {
    return delegate.newStorageUriNormalizer(rawVendedCredentials);
}
```

> paimon / hive 的 `TcclPinningConnectorContext` **不动**：它们的连接器不调用新方法，继承 SPI 默认（逐文件折回其自身 `normalizeStorageUri`）= 零回归。

### 连接器侧线路改造（`IcebergScanPlanProvider.java`）

- 新增 package-private helper（供三处 scan-start 与单测统一构造）：
  ```java
  UnaryOperator<String> newUriNormalizer(Map<String, String> vendedToken) {
      return context != null ? context.newStorageUriNormalizer(vendedToken) : UnaryOperator.identity();
  }
  ```
- 把 per-file seam 的参数 `Map<String,String> vendedToken` **替换为** `UnaryOperator<String> uriNormalizer`：
  `buildRangeForTask` / `buildRange` / `buildDeleteFiles` / `convertDelete` / `buildPositionDeleteRange` / `planCountPushdown`，以及 `IcebergStreamingSplitSource` 的字段 `vendedToken`。
- 三处 scan-start（`planScanInternal:647`、`streamSplits:496`、位置删除 `:904`）在 `extractVendedToken(...)` 之后立刻 `UnaryOperator<String> uriNormalizer = newUriNormalizer(vendedToken);`，往下传 `uriNormalizer`。
- 终点 `normalizeUri(path, vendedToken)` → `uriNormalizer.apply(path)`；旧私有 helper `normalizeUri`（1308）随之删除（无其它引用）。

**为何是替换而非新增参数**：这些 seam 里 `vendedToken` 的唯一用途就是 `normalizeUri`；替换后不变量在类型上显式化（seam 携带的是"已准备好的归一化器"），也不留悬空的 token 参数。

## Parity 论证（byte 不变）

1. 逐文件输出：`normalizeStorageUri(rawUri, token)` 与 `newStorageUriNormalizer(token).apply(rawUri)` 对**同一 (rawUri, token)** 逐字相同——两者都是 `empty→原样` 否则 `LocationPath.of(rawUri, effective).toStorageLocation().toString()`，`effective` precedence 相同（vended 非空→vended，否则 static）。
2. `buildVendedStorageMap` fail-soft（catch 返回 null，永不抛）；fail-loud 只在 per-uri 的 `LocationPath.of` —— 提升后 fail-loud 时机不变（仍逐文件）。唯一差异：空文件 scan 下也会跑一次 `buildVendedStorageMap`（fail-soft、廉价、罕见），无可观测行为差。
3. `storagePropertiesSupplier.get()`（catalog 静态 map）scan 内稳定，capture 一次正确（且顺带省掉逐文件重取）。
4. 离线测试（`RecordingConnectorContext` 未 override 新方法）继承默认逐文件折回 → `normalizeCount` / `normalizedUris` / `lastVendedToken` 全部照旧触发，现存 recording 断言零改动。

## Implementation Plan

1. SPI：`ConnectorContext.java` 加 default `newStorageUriNormalizer` + import `UnaryOperator`。
2. fe-core：`DefaultConnectorContext.java` override（复用现成 `buildVendedStorageMap` / `storagePropertiesSupplier` / `LocationPath` / `Strings`）+ import `UnaryOperator`。
3. iceberg：`TcclPinningConnectorContext.java` override 透传 + import。
4. iceberg：`IcebergScanPlanProvider.java` 加 `newUriNormalizer` helper、三处 scan-start 构造、六个 seam 换参、删 `normalizeUri` helper、import `UnaryOperator`。
5. 测试见下。

## Test Plan（闸门 = parity + 证明减负）

- **现存全绿**（parity）：iceberg 全模块 UT（含 `convertDelete*` / vended-token / streaming / count-pushdown / position_deletes 那批）。
- **改签名的单测**（机械对齐）：`convertDelete(delete, Collections.emptyMap())` → `convertDelete(delete, UnaryOperator.identity())`（仅读 bounds/format/fieldId 的 6 处）；两处断言路径归一化的（`convertDeleteNormalizesDeletePathViaContext`、`convertDeleteNormalizesDeletePathViaVendedToken`）→ `convertDelete(delete, provider.newUriNormalizer(token))`，断言（路径归一化 + `lastVendedToken==token`）不变。
- **减负守门（新增，SPI 结构层）**：`RecordingConnectorContext` 加 `int newNormalizerCount` 并 override `newStorageUriNormalizer`（自增 + 返回逐文件折回 `normalizeStorageUri` 的 normalizer 以保留现存 recording）。新测：一次 `planScan`（多 data + 多 delete 文件）后断言 `newNormalizerCount == 1` 且 `normalizeCount == N_data + N_delete`——直接编码"token→map 派生每 scan 一次、路径归一化 N 次"。
- **fe-core parity 单测（新增）**：`DefaultConnectorContext` 的 `newStorageUriNormalizer(token).apply(uri)` 对多个 uri 与 `normalizeStorageUri(uri, token)` 逐字相等（vended 覆盖 + static 回退 + bad-path fail-loud + empty-uri 短路）。

## Risk

- **低**。SPI 加一个默认无副作用方法（其它连接器不受影响）；fe-core override 是把现成 `normalizeStorageUri` 的 body 拆成"准备一次 + 逐文件套用"，无新解析、无新缓存、无跨查询状态、无凭证保留（normalizer 是 scan 局部变量，scan 结束即回收）。
- 铁律核对：非 source-specific（默认惠及所有 vended 连接器）；fe-core 不新增属性解析（`buildVendedStorageMap` 本就在 fe-core）；不塞 Table 缓存进 fe-core。用户已就"动 fe-core 边界"签字。
