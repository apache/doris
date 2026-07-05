# P6.2-T09 — iceberg scan 凭据下发（静态 `location.*` + vended overlay + 2-arg URI 归一化）设计文档

> 分支 `catalog-spi-10-iceberg`。**0 新 SPI、0 fe-core 改、0 paimon 改、iceberg 仍不在 `SPI_READY_TYPES`**（零行为变更，P6.6 才翻闸）。
> 复用既有引擎中立接缝 `ConnectorContext.{getStorageProperties, vendStorageCredentials, normalizeStorageUri(uri,token)}`（`DefaultConnectorContext` 已实现）。连接器只写 iceberg SDK 的 `extractVendedToken`。镜像 paimon `PaimonScanPlanProvider` 的 vended/静态凭据链。

## 0. Scope（用户裁定 2026-06-23，AskUserQuestion 两问）

T09 原列「vended 凭据（仅 REST，复用接缝）」。本轮 code-grounded recon（主线直读 legacy `IcebergScanNode`/`IcebergVendedCredentialsProvider`/`VendedCredentialsFactory` + paimon 模板 + `DefaultConnectorContext` + `PluginDrivenScanNode.getLocationProperties`）暴露一个**比 vended 更基础的缺口**，用户拍板：

| 问题 | 决策 | 理由 |
|---|---|---|
| **静态 `location.*` 凭据从没发过** | **纳入 T09，单 commit**（用户选项 1） | 现 iceberg `getScanNodeProperties` 一个 `location.*` 都不发（grep 实证：全连接器零 `"location.` 发射）。`PluginDrivenScanNode.getLocationProperties()` **只**从 `getScanNodeProperties` 的 `location.*` 取 BE 存储凭据（无第二接缝）→ 翻闸后 iceberg **任何** native 读（含非 REST 的 HMS/Glue 静态 AK/SK catalog）BE 拿不到凭据 → 403，过不了 P6.2 验收门（native·JNI / SELECT*）。paimon 在同一 `getScanNodeProperties` 发「静态 `location.*` + vended overlay」两块；iceberg 镜像。静态 + vended 共用同一 `location.*` 机制 + 同一 `getScanNodeProperties`，逻辑内聚 → 单 commit。 |
| **vended 启用 gate 取哪种** | **catalog flag `iceberg.rest.vended-credentials-enabled`**（用户选「legacy 忠实」） | legacy `IcebergVendedCredentialsProvider.isVendedCredentialsEnabled` = `IcebergRestProperties.isIcebergRestVendedCredentialsEnabled()`（即该 flag）。连接器已读该 flag（`IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED`），且 T05 `IcebergCatalogFactory:394-398` 已用它注入 REST delegation header → gate 与 header 注入条件一致。**有别于** paimon 的 FileIO-类型 gate（paimon 无 per-flavor flag）。 |

**净 0 新 SPI**：静态/vended/URI 全走既有 `ConnectorContext` 接缝；连接器只加私有方法 + 读既有属性键。

## 1. old → new 映射（vs legacy `IcebergScanNode` / `IcebergVendedCredentialsProvider`）

| legacy（fe-core） | T09（连接器） |
|---|---|
| `IcebergScanNode.doInitialize:230` `storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(metastoreProps, catalogStaticMap, table)`（vended 可用→替换静态；否→静态） | `getScanNodeProperties`：静态 `location.*`（`context.getStorageProperties()`→`toBackendProperties().toMap()`）+ vended overlay `location.*`（`context.vendStorageCredentials(extractVendedToken(table, flag))`，vended 覆盖静态键） |
| `IcebergScanNode.getLocationProperties:1137` 返回 `backendStorageProperties`（= `CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap)`） | `PluginDrivenScanNode.getLocationProperties()` 剥 `location.` 前缀（通用，已就绪） |
| `IcebergVendedCredentialsProvider.isVendedCredentialsEnabled:44` = REST flag | `restVendedCredentialsEnabled()` = `Boolean.parseBoolean(properties.get("iceberg.rest.vended-credentials-enabled"))` |
| `IcebergVendedCredentialsProvider.extractRawVendedCredentials:52` = `table.io().properties()` + `SupportsStorageCredentials.credentials().config()` | `extractVendedToken(Table, boolean enabled)`（自包含移植，仅 iceberg SDK） |
| `AbstractVendedCredentialsProvider` tail：`filterCloudStorageProperties`→`StorageProperties.createAll`→`getBackendPropertiesFromStorageMap` | `DefaultConnectorContext.vendStorageCredentials`（既有接缝，逐字同源 tail） |
| `IcebergScanNode.createIcebergSplit:852` 数据路径 `LocationPath.of(path, storagePropertiesMap)`（vended→替换静态） | `buildRange` 数据路径 `normalizeUri(rawPath, vendedToken)` = `context.normalizeStorageUri(rawPath, vendedToken)`（2-arg，T04 现为 1-arg 静态） |
| delete 路径同上经 `LocationPath.of(path, storagePropertiesMap)` | `convertDelete` delete 路径 `normalizeUri(deletePath, vendedToken)`（2-arg） |

## 2. 实现（`IcebergScanPlanProvider`）

### 2.1 `extractVendedToken`（新静态方法，忠实移植 legacy `extractRawVendedCredentials` + flag gate）
```java
static Map<String, String> extractVendedToken(Table table, boolean vendedEnabled) {
    if (!vendedEnabled || table == null || table.io() == null) {
        return Collections.emptyMap();
    }
    FileIO fileIO = table.io();
    Map<String, String> ioProps = new HashMap<>(fileIO.properties());   // 无条件（legacy parity）
    if (fileIO instanceof SupportsStorageCredentials) {
        for (StorageCredential sc : ((SupportsStorageCredentials) fileIO).credentials()) {
            ioProps.putAll(sc.config());                                 // 合并 server 下发的 vended 凭据
        }
    }
    return ioProps;
}
```
- gate（`vendedEnabled`）在**提取前**短路 = legacy `getStoragePropertiesMapWithVendedCredentials` 先查 `isVendedCredentialsEnabled` 再 extract 的等价。
- 读 `table.io()`（iceberg SDK，**非** paimon 的 `table.fileIO()`/`RESTTokenFileIO`）；iceberg 无 `validToken()` 刷新概念，凭据随 table 加载新鲜（REST 每查询 `loadTable` 重取 → 解 ~1h TTL）。

### 2.2 flag 读取（**两段式 gate**，对抗复核更正）
```java
private boolean restVendedCredentialsEnabled() {
    return IcebergConnectorProperties.TYPE_REST.equals(IcebergCatalogFactory.resolveFlavor(properties))
            && Boolean.parseBoolean(properties.get(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED));
}
```
**对抗复核（workflow `wf_a6996983-799`）抓的真 bug（low，已修）**：legacy `IcebergVendedCredentialsProvider.isVendedCredentialsEnabled` 是**两段式**——`metastoreProperties instanceof IcebergRestProperties` **且** flag。初版只移植 flag（单段），故一个 flavor=hms/glue/… 的 catalog 若误带 `iceberg.rest.vended-credentials-enabled=true`（连接器不 reject 未知属性，可创建），单段 gate 会触发 vended 提取/归一化，而 legacy 因 `instanceof` 不成立而**抑制**（回落静态）。修=AND 上 `TYPE_REST.equals(resolveFlavor(props))`（flag 仅声明在 `IcebergRestProperties`，故 flavor==rest ⟺ instanceof）。正确配置的 catalog（REST flag 只在 REST、非 REST 无该键 → null≠rest → false）字节不变。

### 2.3 `planScanInternal` 每 scan 取一次 token + 线程进归一化
```java
Map<String, String> vendedToken =
        context != null ? extractVendedToken(table, restVendedCredentialsEnabled()) : Collections.emptyMap();
```
- 线程进 `buildRange(..., vendedToken)` 与 `planCountPushdown(..., vendedToken)`。
- `buildRange` 数据路径：`normalizeUri(rawDataPath, vendedToken)`（替换 T04 的 `normalizePath`）。
- `buildDeleteFiles(task, vendedToken)` → `convertDelete(delete, vendedToken)` → delete 路径 `normalizeUri(delete.path(), vendedToken)`。
- 新 helper `normalizeUri(rawPath, vendedToken)` = `context != null ? context.normalizeStorageUri(rawPath, vendedToken) : rawPath`（删旧 `normalizePath`；空 token 时 2-arg 接缝折回静态 map = 与 1-arg 同结果，非 REST 路径字节不变）。

### 2.4 `getScanNodeProperties` 发静态 + vended `location.*`（镜像 paimon :661-681）
```java
// 静态存储凭据（所有 flavor）：catalog 绑定的 fe-filesystem StorageProperties → BE 规范键（AWS_*/hadoop）。
// BLOCKER：BE native 读只认规范键，raw 别名（s3.access_key…）→ 403。REST catalog 的静态 map 为空 → 此块空。
if (context != null) {
    Map<String, String> backendStorageProps = new HashMap<>();
    for (StorageProperties sp : context.getStorageProperties()) {
        sp.toBackendProperties().ifPresent(b -> backendStorageProps.putAll(b.toMap()));
    }
    backendStorageProps.forEach((k, v) -> props.put("location." + k, v));
}
// vended overlay（REST per-table token）：覆盖静态（legacy precedence，vended 赢冲突键）。
// gate = catalog flag；空 token（flag off / 非 REST）→ no-op。
if (context != null) {
    Map<String, String> vendedBeProps =
            context.vendStorageCredentials(extractVendedToken(table, restVendedCredentialsEnabled()));
    vendedBeProps.forEach((k, v) -> props.put("location." + k, v));
}
```
（`StorageProperties` = `org.apache.doris.filesystem.properties.StorageProperties`，连接器允许 import。`b.toMap()` 的 `b` = `BackendStorageProperties`，lambda 推断类型无需 import。）

## 3. 精度 / parity 要点

- **precedence 偏差（与 paimon 同款，已登记）**：legacy `VendedCredentialsFactory` 在 vended 可用时**整体替换**静态 map；SPI 走「静态基底 + vended overlay（vended 赢冲突）」。**实践等价**：REST catalog 的静态 map 为空（`DefaultConnectorContext.getStorageProperties` 对 REST-vended 返回空）→ overlay == 替换；非 REST catalog vended token 空 → overlay no-op，只剩静态。差异仅在「REST catalog 同时配了静态存储 prop 且 vended token 缺某键」的边缘态保留静态键（vended token 实践自包含 → benign）。
- **URI 归一化 precedence**：`normalizeStorageUri(uri, token)` 2-arg 接缝对 vended **替换**静态（`buildVendedStorageMap(token)!=null ? vended : static`）= legacy `LocationPath.of(path, vendedMap)` parity；与 creds overlay 的「overlay」语义不同但各自对 legacy 忠实（creds 走 `getBackendPropertiesFromStorageMap` 合并、URI 走 LocationPath 单 map）。paimon 同此双语义。
- **`original_file_path` 保持 raw**：T04 已拆 `path`（归一化）/`originalPath`（raw，BE 匹配 position-delete）；T09 只把归一化从 1-arg 升 2-arg，`originalPath` 仍发 raw `dataFile.path()`，不变。

## 4. Deviation（UT 不可见，仅 P6.6 docker plugin-zip e2e 真验，登记）

1. **overlay vs 替换**（§3）：vended 覆盖静态而非整体替换；REST 静态空 → 实践等价。
2. **gate = catalog flag vs paimon FileIO-类型**：iceberg 忠实 legacy flag；与 T05 header 注入条件一致。
3. **iceberg 无 `validToken()` 刷新**：凭据随 `loadTable` 新鲜（legacy `IcebergVendedCredentialsProvider` 也无显式刷新，每次 doInitialize 重读 `table.io()`）。
4. **`extractVendedToken` 无条件读 `table.io().properties()`**（legacy parity）：对非 SupportsStorageCredentials FileIO 也读其 properties（经 `filterCloudStorageProperties` 过滤，非 cloud 键丢弃 → benign）。
5. **REST PROVIDER_CHAIN 非-DEFAULT signing-cred gap**（T05 记录的同族）：与 T09 无关（T09 走 vended cloud-storage 凭据，不碰 REST signing）。
6. **`extractVendedToken` 非 fail-soft**（对抗复核 low，**与 paimon 同款，不修，登记**）：legacy 在**提取**外层 fail-soft（`VendedCredentialsFactory:39-45` catch → 回落静态 map）；SPI 设计把 fail-soft 边界移到 `vendStorageCredentials`（对**已提取**的 map），提取本身（`table.io().properties()` / `credentials().config()`）无 try/catch。一个 properties()/credentials() 抛异常的病态 REST FileIO 会让 scan 崩，而 legacy 回落静态。**不修**因：①paimon 模板（`PaimonScanPlanProvider:677`）同样无 guard，单修 iceberg 会引入 iceberg/paimon 不一致；②真实 REST FileIO（S3FileIO/ResolvingFileIO）的 `properties()`/`credentials()` 是内存访问器，生产几乎不抛；③仅 P6.6 翻闸后可触。若要修应 paimon+iceberg 一并（超 T09 scope）。

## 5. 测试计划（无 Mockito，fail-loud fake；真 InMemoryCatalog + FakeIcebergTable + 手写 ConnectorContext double）

测试基建扩：
- `RecordingConnectorContext`：加 `vendStorageCredentials(token)`（token 非空→返回配置的 BE map，空→空，**忠实 `DefaultConnectorContext`**）+ 2-arg `normalizeStorageUri(uri, token)`（记 `lastVendedToken` + 计数，scheme 归一化同 1-arg）。
- `FakeIcebergTable`：`io()` 改为可设（默认仍抛 = 保 fail-loud 契约；vended 测试注入 fake FileIO）。
- 新增 test-local fake `FileIO`（plain）+ `FileIO implements SupportsStorageCredentials`（properties + credentials）。

测试（断**装配后** `location.*` 值 / token 流向 vs legacy 期望，非只断类名）：
- `extractVendedToken`：①enabled+SupportsStorageCredentials → io props ∪ credentials.config 合并；②enabled+plain FileIO → 仅 io props（不崩）；③disabled → 空（gate）；④null table → 空。
- `getScanNodeProperties` 静态：`getStorageProperties()` 返回 fake backend → `location.AWS_ACCESS_KEY=…`、raw 别名缺（B-9）。
- `getScanNodeProperties` vended overlay：静态 + vended 冲突键 → vended 赢。
- `getScanNodeProperties` flag gate 端到端：flag on（FakeIcebergTable+fake SSC FileIO，context vend token-aware）→ vended `location.*` present；flag off → absent。
- `getScanNodeProperties` 无 context → 无 `location.*`。
- `getScanNodeProperties` 跳过无 BE 模型的 StorageProperties + 合并其余（`.ifPresent` 边）。
- `planScan` 数据 + delete 路径走 2-arg `normalizeStorageUri`（`lastVendedToken` 记录 / 2-arg 计数）。
- 改 T04 的 5 处 `convertDelete(delete)` → `convertDelete(delete, emptyMap())`；新增 `convertDeleteNormalizesDeletePathViaVendedToken`（非空 token 到 2-arg）。

## 6. 验收门

- fe-connector-iceberg UT 绿（258 → 预计 +12~14）、1 skip（env-gated live）。
- checkstyle 0 + `tools/check-connector-imports.sh` 净（仅 `org.apache.doris.{thrift,connector,extension,filesystem}` + iceberg SDK）。
- iceberg **不在** `SPI_READY_TYPES`（零行为变更）。
- **0 SPI / fe-core / paimon / pom 改**（iceberg SDK + fe-filesystem-api 均已是依赖）。
- 对抗 parity 复核（多维 workflow，每发现独立 skeptic verify）vs legacy。
