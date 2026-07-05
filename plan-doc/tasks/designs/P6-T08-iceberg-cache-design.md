# P6.2-T08 — iceberg 连接器内部 cache（设计文档）

> 分支 `catalog-spi-10-iceberg`。**0 新 SPI、0 fe-core 改、iceberg 仍不在 `SPI_READY_TYPES`**（零行为变更，P6.6 才翻闸）。
> 镜像 paimon 连接器 cache 层；manifest 缓存从 fe-core 移植（连接器不能 import fe-core，故是 PORT，非依赖）。

## 0. Scope（用户裁定 2026-06-23，AskUserQuestion）

T08 原列三个 cache。本轮经 code-grounded recon（workflow `wf_57863154-e49`，4 路）逐个复核 + 用户拍板：

| Cache | 决策 | 理由 |
|---|---|---|
| ① `IcebergLatestSnapshotCache` | **做** | `beginQuerySnapshot` 现每查询 live `loadTable()`+`currentSnapshot()`；缓存命中省 I/O + 跨查询钉稳定快照（paimon FIX-4 / CI 973411 语义） |
| ② `IcebergSchemaAtMemo` | **跳过**（用户选「跳过」） | iceberg 所有历史 schema 内嵌 table metadata；`table.schemas().get(id)` 是 O(1) 内存 map 查找**无 I/O**（T07 该行未包 auth 即证），且 `getTableSchema(@snapshot)` 仍须 `loadTable`（I/O 省不掉）。memo 对 iceberg **零收益**（≠ paimon `schemaAt` 读 schema 文件有 I/O）。`getTableSchema(@snapshot)` 保持 T07 现状（live map 查找） |
| ③ `IcebergManifestCache` | **做 + 扩 scope**（用户选「扩 scope：scan 改 manifest 级」） | connector scan（T02 起）走 SDK `planFiles()`/`splitFiles`，**不消费** Doris manifest cache → 单移植 cache = 死代码。用户裁定本轮**一并移植 legacy manifest 级 planning**（`planFileScanTaskWithManifestCache`），让 cache 真被消费；gate `meta.cache.iceberg.manifest.enable`（默认 false）→ 默认仍走 SDK `splitFiles`，opt-in 才走 manifest 级 |

**净 0 新 SPI**：cache 全连接器内部；失效用既有 `Connector.invalidateTable`/`invalidateAll` 默认方法 override（paimon 已 override 过）；新增的只是连接器读的**属性键**（非接口），与 paimon `meta.cache.paimon.table.ttl-second` 同性质。

## 1. Cache ① — `IcebergLatestSnapshotCache`（镜像 `PaimonLatestSnapshotCache`）

**与 paimon 的唯一结构性偏差**：paimon 缓存单 `long`（snapshotId）；iceberg `beginQuerySnapshot` 须**原子地**钉 `snapshotId` **和** `schemaId`（`table.schema().schemaId()` = 最新 schema id，**非** `currentSnapshot().schemaId()`，legacy `getLatestIcebergSnapshot` parity）。两次 live 读之间 schemaId 可能漂移（schema-only ALTER 不产生新 snapshot）→ 必须一起缓存。故缓存值 = `CachedSnapshot{long snapshotId; long schemaId}`（移植 legacy `IcebergSnapshot` 形态）。

- 结构镜像 paimon：`ConcurrentHashMap<TableIdentifier, Entry>`，`Entry{CachedSnapshot value; volatile long expireAtNanos}`，access-based TTL，best-effort `maxSize` 溢出整清，可注入 `LongSupplier nanoClock`（确定性测试）。
- **key** = `org.apache.iceberg.catalog.TableIdentifier.of(dbName, tableName)`（镜像 paimon `Identifier.create`）。`beginQuerySnapshot`（handle 的 remote db/table）与 `invalidateTable`（RefreshManager 传 remote db/table）用**同一 2-string 形态**构 key → put/remove 对称（dotted-namespace 即便结构不同也对称，无碍）。
- `getOrLoad(TableIdentifier, Supplier<CachedSnapshot> loader)`：`ttlNanos<=0` → 每次跑 loader 不缓存（no-cache catalog）；命中且未过期 → 刷新 expiry 返回缓存值；否则跑 loader（**锁外**，并发同 key 双载无害=值即当前 live）+ put。
- `isEnabled()`/`invalidate(id)`/`invalidateAll()`/`size()` 同 paimon。
- **TTL 配置**：连接器属性 `meta.cache.iceberg.table.ttl-second`（新键，镜像 paimon `meta.cache.paimon.table.ttl-second`），默认 `86400`（24h = legacy `Config.external_cache_expire_time_seconds_after_access`，legacy iceberg table-cache 默认 ON），capacity `1000`（legacy `Config.max_external_table_cache_num`）。`<=0` 禁缓存（always live）；不可解析 → 默认（best-effort，不破建目录）。

**`beginQuerySnapshot` 接线**（`IcebergConnectorMetadata`）：
```
TableIdentifier id = TableIdentifier.of(handle.getDbName(), handle.getTableName());
CachedSnapshot pin = latestSnapshotCache.getOrLoad(id, () -> {
    Table table = loadTable(handle);
    Snapshot cur = table.currentSnapshot();
    return new CachedSnapshot(cur == null ? -1L : cur.snapshotId(), table.schema().schemaId());
});
return Optional.of(ConnectorMvccSnapshot.builder().snapshotId(pin.snapshotId).schemaId(pin.schemaId).build());
```
命中时**完全跳过 `loadTable`**（省 I/O + 稳定钉）。

**ctor 策略（镜像 paimon，保现有 MVCC 测试不回归）**：`IcebergConnectorMetadata` 现 3-arg ctor（测试直建用）→ 委派 4-arg 并传**禁用** cache（`new IcebergLatestSnapshotCache(0L, 1)`）→ 现有 `IcebergConnectorMetadataMvccTest` 仍 always-live，零回归；4-arg ctor（生产，`getMetadata` 注入连接器 cache）。

## 2. Cache ③ — manifest 缓存 + manifest 级 planning（移植 legacy）

### 2.1 移植件（连接器内 `org.apache.doris.connector.iceberg`，仅 iceberg-SDK 类型，零 fe-core import）

- **`ManifestCacheValue`**（逐字移植 fe-core `cache/ManifestCacheValue`）：`List<DataFile> dataFiles` + `List<DeleteFile> deleteFiles`，`forDataFiles`/`forDeleteFiles` 工厂，null→empty。
- **`IcebergManifestEntryKey`**（逐字移植 fe-core）：`(String manifestPath, ManifestContent content)`，`of(ManifestFile)=new(manifest.path(), manifest.content())`，equals/hashCode by (path, content)。**path-keyed**（跨表共享同 manifest 路径，故 invalidateTable 不清它）。
- **`IcebergManifestCache`**（移植 fe-core `IcebergExternalMetaCache` 的 manifest 部分 + loader）：
  - `ConcurrentHashMap<IcebergManifestEntryKey, ManifestCacheValue>`，**无 TTL**，capacity `DEFAULT_MANIFEST_CACHE_CAPACITY = 100_000`（= legacy 实测 `getCapacity()==100000`，best-effort 溢出整清）。
  - `getManifestCacheValue(ManifestFile manifest, Table table)`：key=of(manifest)，命中返回；否则 `loadManifestCacheValue`（content==DELETES→`loadDeleteFiles` 否则 `loadDataFiles`）+ put。loader **锁外**（无 I/O under lock）。
  - `loadDataFiles` = `ManifestFiles.read(manifest, table.io())` 迭代 `dataFile.copy()`；`loadDeleteFiles` = `ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())` 迭代 `deleteFile.copy()`（**`.copy()` 必须**：reader 迭代器复用对象）。
  - **失效语义**：manifest cache **不**被 `invalidateTable`/`invalidateAll` 清（legacy `testInvalidateTableKeepsManifestCache`/`testInvalidateDbAndStats` parity：db/table 失效清 table/view/schema、**留** manifest）；只在 **REFRESH CATALOG = 连接器整体重建**时随 connector final 字段 GC 清掉（recon 实证 fe-core 无 `Connector.invalidateAll()` 调用点，REFRESH CATALOG 走 `PluginDrivenExternalCatalog.onClose` null+rebuild）。

### 2.2 manifest 级 planning（移植 legacy `IcebergScanNode.planFileScanTaskWithManifestCache:662-782`）

`IcebergScanPlanProvider` 把 `planScanInternal:201` 的 `splitFiles(scan, session)` 换成 gated `planFileScanTask(scan, session, table, filter)`：

```
private CloseableIterable<FileScanTask> planFileScanTask(scan, session, table, filter):
    if (!isManifestCacheEnabled()) return splitFiles(scan, session);     // 默认路径不变
    try { return planFileScanTaskWithManifestCache(scan, session, table, filter); }
    catch (Exception e) { LOG.warn(fallback); return splitFiles(scan, session); }  // legacy 同款兜底
```

`planFileScanTaskWithManifestCache`（逐字移植，仅替换 cache 来源 + filterExpr 来源）：
1. `Snapshot snapshot = scan.snapshot()`；null → empty。（`scan` 已带 T07 MVCC pin，故 snapshot 是 pinned）
2. `Expression filterExpr` = `filter` 经 `IcebergPredicateConverter(table.schema(), zone).convert(...)`（T02 件）reduce `Expressions.and`（起 `alwaysTrue()`）；空 filter → `alwaysTrue()`。（legacy `conjuncts.stream().map(convertToIcebergExpr).filter(nonNull).reduce(alwaysTrue, and)` parity；用 **current** schema = T07/legacy `:679` parity）
3. `specsById = table.specs()`；`caseSensitive = true`。
4. `residualEvaluators`：per spec `ResidualEvaluator.of(spec, filterExpr, true)`。
5. `metricsEvaluator = new InclusiveMetricsEvaluator(table.schema(), filterExpr, true)`。
6. **Phase 1 deletes**：遍历 `snapshot.deleteManifests(table.io())`，skip 非 DELETES、spec==null、`ManifestEvaluator.forPartitionFilter(filterExpr, spec, true).eval(manifest)==false`；命中 → `manifestCache.getManifestCacheValue(manifest, table).getDeleteFiles()` 加入 `deleteFiles`。
7. `deleteIndex = DeleteFileIndex.builderFor(deleteFiles).specsById(specsById).caseSensitive(true).build()`。
8. **Phase 2 data**：`getMatchingManifest(snapshot.dataManifests(table.io()), specsById, filterExpr)` → 遍历，skip 非 DATA、spec==null、residualEvaluator==null；`manifestCache.getManifestCacheValue(manifest, table).getDataFiles()`；每 DataFile：skip `!metricsEvaluator.eval(dataFile)`、`residualEvaluator.residualFor(dataFile.partition()).equals(alwaysFalse())`；deletes = `deleteIndex.forDataFile(dataFile.dataSequenceNumber(), dataFile)`；`tasks.add(new BaseFileScanTask(dataFile, deletes, SchemaParser.toJson(table.schema()), PartitionSpecParser.toJson(spec), residualEvaluator))`。
9. `targetSplitSize = determineTargetFileSplitSize(tasks, session)`（T02 件）；`return TableScanUtil.splitFiles(withNoopClose(tasks), targetSplitSize)`。

**输出类型 = `CloseableIterable<FileScanTask>`**（同 `splitFiles`）→ 下游 `buildRange`（T03-T07：path/format/delete/分区/field-id）**逐 task 处理不变**。manifest 路径仅替换枚举源；delete(T04)/count(T05,独立分支不走此路)/field-id(T06)/MVCC(T07) 全不受影响。

### 2.3 `getMatchingManifest` 移植（fe-core `IcebergUtils:1265`）

connector 无 Caffeine（latest-snapshot cache 用 ConcurrentHashMap）→ 用 `HashMap<Integer, ManifestEvaluator>` `computeIfAbsent` 替 `LoadingCache`（语义等价，per-call 本地 memo）：per specId `ManifestEvaluator.forPartitionFilter(Projections.inclusive(spec, true).project(dataFilter), spec, true)`；`CloseableIterable.filter`（SDK）双重过滤：eval(manifest) + `hasAddedFiles()||hasExistingFiles()`。

### 2.4 gate（移植 fe-core `IcebergUtils.isManifestCacheEnabled` + `CacheSpec.isCacheEnabled`）

连接器读 3 属性 + 公式（逐字）：
- `meta.cache.iceberg.manifest.enable`（默认 `false`）
- `meta.cache.iceberg.manifest.ttl-second`（默认 `172800`=48h）
- `meta.cache.iceberg.manifest.capacity`（默认 `1024`）
- 启用 iff `enable && ttlSecond != 0 && capacity != 0`（= `CacheSpec.isCacheEnabled`，逐字）。

**legacy quirk（保真）**：上述 `.ttl-second`/`.capacity` **仅喂 gate 公式**，不决定实际 cache 大小——实际 manifest entry 固定 `no-TTL / capacity 100000`（fe-core `IcebergExternalMetaCache:100` 硬编码 `CacheSpec.of(false, CACHE_NO_TTL, 100_000)`，两测试 `getCapacity()==100000` 实证）。连接器 `IcebergManifestCache` 同样固定 100000/no-TTL，gate 读 ttl/capacity 仅用于公式（用户可经 ttl=0 或 capacity=0 显式禁用一个 enable=true 的 cache，保真）。`null`/不可解析 enable → false。

### 2.5 wiring

- `IcebergConnector`：`manifestCache` = `new IcebergManifestCache()`（connector final 字段，REFRESH CATALOG 重建时随连接器 GC）。`getScanPlanProvider()` 注入（4-arg `IcebergScanPlanProvider(properties, catalogOps, context, manifestCache)`，镜像 paimon 注 `schemaAtMemo` 进 scan provider）。
- `IcebergScanPlanProvider`：新 4-arg ctor；现有 2-arg/3-arg ctor 委派 `null` manifestCache（直建测试默认 gate-off 不触；gate-on 但 cache==null → fallback splitFiles，安全）。

## 3. 失效矩阵（`IcebergConnector` override，镜像 paimon）

| 触发 | fe-core 派发 | iceberg override 动作 |
|---|---|---|
| REFRESH TABLE | `RefreshManager:254` `getConnector().invalidateTable(remoteDb, remoteTable)` | `latestSnapshotCache.invalidate(TableIdentifier.of(db, table))`。**不**清 manifest（legacy parity）、**不**清 schema（无 memo） |
| REFRESH CATALOG | `PluginDrivenExternalCatalog.onClose` → connector null+rebuild（**无** `invalidateAll()` 调用） | 连接器整体重建 → 所有 final cache 字段（latest-snapshot + manifest）随 GC 清空 |
| `invalidateAll()`（SPI，**fe-core 无调用点**，dead/defensive） | — | `latestSnapshotCache.invalidateAll()`（与 paimon 对称；manifest 不动，靠重建清） |

## 4. Deviation（UT 不可见，P6.6 docker 验，登记）

1. **latest-snapshot 缓存值 = `(snapshotId, schemaId)` 二元组** vs paimon 单 `long`（iceberg 须原子钉 schema-only-ALTER 漂移；移植 legacy `IcebergSnapshot` 形态）。
2. **schema memo 跳过**（iceberg schema 内存即得，memo 零 I/O 收益；§0②）。
3. **manifest cache hit/miss EXPLAIN/profile 统计丢弃**（连接器不传 `cacheHitRecorder`；同 T02 profile drop 族，P6.6 后 EXPLAIN VERBOSE 的 `manifest cache: hits=...` 行缺失，纯可观测性）。
4. **`getMatchingManifest` 用 HashMap 替 Caffeine LoadingCache**（per-call 本地 evaluator memo，语义等价，避 Caffeine 依赖）。
5. **manifest 级 filterExpr / metricsEvaluator / SchemaParser 用 current schema**（`table.schema()`），time-travel pin 下与 legacy `:679/:695/:772` 一致（潜伏的 rename+time-travel 分歧是 legacy parity，T07 已登记同族）。
6. **manifest gate 的 `.ttl-second`/`.capacity` 属性仅喂启用公式、不 size cache**（固定 100000/no-TTL）= legacy quirk 保真（§2.4）。
7. **batch mode**：manifest 路径 `determineTargetFileSplitSize` 复用 T02 件（已含 batch 延后决策，同 splitFiles 路径）。
8. **manifest-cache 三键不进 `validateProperties`**（对抗复核 LOW#1，REFUTED-as-bug）：legacy `IcebergExternalCatalog.checkProperties` 对 `enable`/`ttl-second`/`capacity` 做 `checkBoolean/LongProperty` 建目录期校验；连接器 `validateProperties` 只做 flavor+`bindForType().validate()`，cache 键不校验。恶值在 scan 期被 `propLong`/`getOrDefault` 静默回落默认 → 不改结果/不崩（gate 翻转只换枚举算法，两路对同一 `scan` 产相同 range，有 fallback 兜底）。纯校验严格度 gap，非正确性。
9. **不调 `ManifestFiles.dropCache`**（对抗复核 LOW#2，REFUTED-as-bug）：legacy `invalidateCatalog` 遍历 tableEntry 调 `ManifestFiles.dropCache(io)` 释放 iceberg SDK 自身 per-FileIO `ContentCache`；连接器无 table cache（不持有 FileIO 集合）故不调。REFRESH CATALOG 重建连接器 → 新 FileIO → 新 ContentCache key，旧条目 GC 前驻留 → **内存/资源释放 gap，非 stale-read**（SDK ContentCache 有界）。不可在不引入 table cache 下干净移植，故有意保留。

## 5. 风险

- **manifest 级 planning 仅 gate-on 才走**：默认 false → 现有 scan 路径（SDK splitFiles）字节不变，T02-T07 全部 scan 测试不回归。gate-on 路径靠新 TDD 测试（真 InMemoryCatalog，partition/delete/metrics 裁剪 + cache 命中）守。
- **SDK 1.10.1 API 已逐一 javap 实证**（`BaseFileScanTask` 5-arg ctor / `DeleteFileIndex.builderFor(Iterable<DeleteFile>).specsById().caseSensitive().build()`+`forDataFile(long,DataFile)` / `ManifestEvaluator.forPartitionFilter` / `InclusiveMetricsEvaluator(Schema,Expr,bool).eval(ContentFile)` / `ResidualEvaluator.of().residualFor(StructLike)` / `Projections.inclusive(spec,bool).project()` / `ManifestFiles.read|readDeleteManifest` / `Snapshot.dataManifests|deleteManifests` / `ManifestFile.hasAddedFiles|hasExistingFiles`）= 与 legacy 1.4.3 无漂移。
- **fallback 兜底**：manifest 路径任何异常 → `splitFiles`（legacy `:610-613` parity），不破查询。

## 6. 测试计划（无 Mockito，真 InMemoryCatalog / 注入时钟）

- `IcebergLatestSnapshotCacheTest`（镜像 `PaimonLatestSnapshotCacheTest`）：TTL 内服务稳定 (snapshotId,schemaId)、ttl=0 always-live、invalidate 强制重载、过期重载、invalidateAll、**二元组两字段都钉**（MUTATION：只钉 snapshotId 漏 schemaId → red）。
- `IcebergManifestCacheTest`：path-keyed 命中（同路径同 content 命中、二次不重读）、DATA→dataFiles / DELETES→deleteFiles、capacity 溢出整清、`.copy()` 隔离。
- `IcebergManifestEntryKeyTest`：(path,content) equals/hashCode；同路径不同 content 不等。
- `IcebergConnectorCacheTest`（镜像 `PaimonConnectorCacheTest`）：`resolveTableCacheTtlSecond` 解析（unset→86400 / 0→禁 / 正值 / 不可解析→默认）、`isManifestCacheEnabled` gate（默认 off / enable=true on / ttl=0 或 capacity=0 → off）、`invalidateTable` 清 latest 留 manifest、`invalidateAll`。
- `IcebergScanPlanProviderTest` +：manifest gate-off→SDK 路径（range 数同 baseline）；gate-on→manifest 级 planning 等价 range（partition 裁剪 / metrics 裁剪 / delete 关联 / 空快照→空 / fallback）。
- `IcebergConnectorMetadataMvccTest`：`beginQuerySnapshot` 经 cache 钉稳定（注入 enabled cache 验跨调用同 pin + loadTable 仅一次）；现有 3-arg 直建测试（禁用 cache）always-live 不回归。

## 7. 验收门

fe-connector-iceberg UT 全绿（235 → 新增）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`、**无 SPI/fe-core/pom 改**（iceberg-core/api 已 compile 依赖；ManifestFiles/DeleteFileIndex/expressions 全在 iceberg-api/core 闭包内）。
