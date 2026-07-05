# P6.2-T11 — iceberg scan 路径迁移：汇总设计 + deviation 注册 + validation gate 收口

> **任务**：P6.2-T11（P6.2 最后一个 task）。**完成 = P6.2 DONE**（scan + MVCC + cache + vended 全实现）。
> **本文不重述各 task 细节**（见各 `P6-T0x-iceberg-scan-*-design.md`），只做 **P6.2 整体收口**：①架构总览 + 逐 task 索引；②连接器 CREATE-CATALOG validation gate 核对；③UT 不可见 deviation 中央注册（[deviations-log.md](../../deviations-log.md) DV-038/039/040）；④翻闸（P6.6）阻塞项；⑤验收门状态 + 下一阶段。
> **工作分支** `catalog-spi-10-iceberg`（off `branch-catalog-spi`）。**全程未碰 `SPI_READY_TYPES`，零行为变更**（翻闸只在 P6.6）。

---

## 1. P6.2 范围与架构总览

P6.2 把 legacy fe-core 的 iceberg **scan 路径**（`IcebergScanNode` 1228 + `IcebergUtils` scan 半 + `cache/` + `IcebergMvccSnapshot`/`IcebergSnapshot` + `IcebergVendedCredentialsProvider`）迁进 `fe-connector-iceberg`，走通用 `PluginDrivenScanNode`（SPI 点 E3，已就绪）+ `PluginDrivenMvccExternalTable`（E5，已就绪）。

**关键架构结论**（recon 签字，全程坚持）：**P6.2 净 0 个新 SPI 接口、0 处 SPI 破坏**，唯一例外 = T03 为修真 query-crash 加的**非破坏** `ConnectorScanRange.isPartitionBearing()` 默认方法（默认 false，paimon 字节不变；用户签字）。

```
                       ┌─────────────────────────── 通用 fe-core（已就绪，未改）───────────────────────────┐
  CREATE CATALOG ──▶ IcebergConnectorProvider.validateProperties ──▶ (per-flavor validate, 见 §3)
  SELECT ──────────▶ PluginDrivenScanNode.getSplits(numBackends)
                         │  getScanPlanProvider(handle) ──▶ IcebergScanPlanProvider
                         │      planScan(4-arg / 7-arg COUNT-aware)
                         │        ├─ predicate 下推：IcebergPredicateConverter（自包含移植 convertToIcebergExpr）
                         │        ├─ table.newScan() + useSnapshot/useRef（MVCC pin）+ per-conjunct filter
                         │        ├─ split 枚举：SDK splitFiles（默认）/ gated manifest 级 planFileScanTaskWithManifestCache
                         │        ├─ 每 FileScanTask ─▶ IcebergScanRange（typed carriers）
                         │        │      populateRangeParams ─▶ TIcebergFileDesc（format-version/spec/data-json/v3 row-lineage/delete-files/count）
                         │        ├─ COUNT 下推：getCountFromSnapshot ─▶ 单 count range（table_level_row_count）
                         │        └─ delete files：position / DV-PUFFIN / equality（typed DeleteFile carrier）
                         │  getScanNodeProperties ──▶ path_partition_keys(#968880) + file_format_type=jni
                         │                           + iceberg.schema_evolution（field-id 字典 history_schema_info）
                         │                           + 静态 location.*（AWS_*/hadoop）+ vended overlay（仅 REST）
                         │  getLocationProperties ──▶ location.*（凭据）
                       MVCC: IcebergConnectorMetadata.{beginQuerySnapshot, resolveTimeTravel(5 kinds), applySnapshot, getTableSchema(@snapshot)}
                       cache（连接器内部 D6）: IcebergLatestSnapshotCache(snapshotId,schemaId) + IcebergManifestCache + vendored DeleteFileIndex
                       └──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 逐 task 索引（T01–T10 全 ✅）

| task | 主题 | 关键产物 | commit | 设计文档 |
|---|---|---|---|---|
| T01 | scan provider 骨架 | `IcebergScanPlanProvider`/`IcebergScanRange` + `getScanPlanProvider` 接线 + `ignorePartitionPruneShortCircuit()=true` | `78b6f988bc4` | （骨架，无独立设计） |
| T02 | 谓词下推 + split 枚举 | `IcebergPredicateConverter`（移植 `convertToIcebergExpr`）+ `createTableScan` + `splitFiles` + `determineTargetFileSplitSize` | `90f5474fcf5` | `P6-T02-iceberg-scan-pushdown-design.md` |
| T03 | typed range-params | `IcebergPartitionUtils` + typed carriers + `populateRangeParams`→`TIcebergFileDesc` + native fileFormat + `path_partition_keys` | `626dd933dcf` | `P6-T03-iceberg-scan-rangeparams-design.md` |
| T04 | merge-on-read delete | typed `DeleteFile` carrier（position/DV/equality）+ `convertDelete` + 数据路径归一化跟修 | `d249a4a9dea`+`54ecac35e3d` | `P6-T04-iceberg-scan-deletefiles-design.md` |
| T05 | COUNT(*) 下推 | `getCountFromSnapshot` + 塌缩单 count range + `pushDownRowCount`；batch mode 延后 | `2cc510608f6` | `P6-T05-iceberg-scan-countpushdown-design.md` |
| T06 | **field-id 字典** | `IcebergSchemaUtils`（单 -1 entry + name-mapping）+ `IcebergColumnHandle` + `getColumnHandles` + `iceberg.schema_evolution` | `933171826a9` | `P6-T06-iceberg-scan-fieldid-design.md` |
| T07 | MVCC / time-travel | `getCapabilities`(MVCC+TIME_TRAVEL) + typed pin + 4 MVCC 方法 + Option A 全 pinned-schema 字典 + `IcebergTimeUtils` | `31b51b6d4d1` | `P6-T07-iceberg-scan-mvcc-design.md` |
| T08 | cache（D6）+ manifest 级 | `IcebergLatestSnapshotCache` + `IcebergManifestCache` + **vendored `DeleteFileIndex`** + gated manifest planning | `1c71c61b2dc` | `P6-T08-iceberg-cache-design.md` |
| T09 | vended + 静态凭据 | `extractVendedToken`（两段式 gate）+ 静态 `location.*` + vended overlay + 2-arg `normalizeUri` | `b7af9312977` | `P6-T09-iceberg-scan-vended-design.md` |
| T10 | parity-UT 审计 | 10 维审计 + 8 缺口补测（PRED/PP/NF/G1/MVCC/VC/G2-E2E）+ mutation-verify | `cfd55d46e2c` | `P6-T10-iceberg-scan-parity-audit-design.md` |

> 各 task DONE 块（含逐项 deviation + 对抗复核结论）见 [HANDOFF.md](../../HANDOFF.md)「✅ P6.2-T0x = DONE」。

---

## 3. 连接器 CREATE-CATALOG validation gate（核对 = 已接线，无 gap）

T11 子目标之一 = 核对连接器 validation gate 接线齐。**结论：已接线，0 gap**（T10 Phase B 落地，本 task 仅核实）。

**调用链**（`IcebergConnectorProvider.java:60-64`）：
```
validateProperties(props)
  └─ IcebergCatalogFactory.resolveFlavor(props)        // 读 iceberg.catalog.type；blank/unknown→null，否则 toLowerCase(Locale.ROOT)
  └─ MetaStoreProviders.bindForType(flavor, props, Collections.emptyMap()).validate()
       └─ ServiceLoader 首命中 supportsType(flavor) 的 provider（fe-connector-metastore-iceberg，7 flavor）
       └─ per-flavor MetaStoreProperties.validate()：REST(security/creds 枚举/OAuth2/signing/AK&SK)·Glue(AK/SK-together/endpoint https/at-least-one-cred)·JDBC(uri/catalog_name/warehouse)·HMS/DLF(共享连接校验)·hadoop/s3tables(no-op，storage 上游已校验)
```
- blank/unknown flavor（null / `nessie`…）→ 无 provider 认 → `bindForType` 抛 `IllegalArgumentException("No MetaStoreProvider supports...")` → `PluginDrivenExternalCatalog.checkProperties` wrap 成 `DdlException`。
- **校验全 prop-map 驱动**，传空 storage map（storage 半边已在 fe-filesystem bind 时校验）。
- **inert until P6.6**（iceberg 未进 `SPI_READY_TYPES`）。

**测试** `IcebergConnectorValidatePropertiesTest`（7/0/0，驱动**生产入口** `PROVIDER.validateProperties(Map)` 真实链非 stub）：REST(security=bogus reject / uri accept)·Glue(endpoint-only reject)·JDBC(no-uri reject)·HMS(no-warehouse accept)·hadoop+s3tables(no-op accept)·unknown(`nessie` reject)·missing-catalog-type(reject)。逐字报错串与 metastore-iceberg `validate()` throw 串对齐（per-flavor 报错/fire-order 由 metastore-iceberg 模块自有测试钉）。

---

## 4. UT 不可见 deviation 中央注册（[deviations-log.md](../../deviations-log.md)）

P6.2 全部 UT 不可见 deviation 此前只散落在各 task 设计文档 + HANDOFF，**从未进中央 deviations-log**。T11 经审计 workflow（9 reader 逐设计文档抽取 + completeness-critic 交叉核对，共 75 项 → 去重批化）统一登记为 **3 条**：

| DV | 层级 | 含义 | 翻闸影响 |
|---|---|---|---|
| **DV-038** | 🔴 BLOCKER | 共享 fe-core field-id 路径 BE StructNode DCHECK 崩溃（1 主题/2 面） | **P6.6 前必修**，否则 BE 崩 |
| **DV-039** | HIGH/MEDIUM | parity-忠实 correctness-bearing 但 UT 不可见（**已连接器内缓解**） | 单项不阻塞，但翻闸前必逐项 docker 验 |
| **DV-040** | perf/cosmetic | EXPLAIN/profile drop + lenient-validation + benign superset（~36 项，镜像 DV-035 批 style） | 无正确性影响，P6.6 确认无害 |

**审计两个非显然产出**（Rule 12 诚实记录）：
1. **blocker 计数 = 2 面非 1**：critic 实证 `isGateFlipBlocker=true` 有两项——GLOBAL_ROWID（T06）+ `getColumnHandles` 无 snapshot 重载（T07）。两者是**同一** BE DCHECK 崩溃类、同需 holistic 共享 fe-core 修。合并 DV-038 单条但**显式记两面**，不静默丢面 2（面 2 暴露既有 **paimon 潜伏**：snapshot-id time-travel + rename）。
2. **category (a) classloader 漏报补登**：T08 extractor 漏报了 **split-package shadowing**（vendored `org.apache.iceberg.DeleteFileIndex` 与 iceberg-core jar 包私有副本共存），critic 标 category (a) 缺失 → 主线据 HANDOFF T08 §dev⑦ 补登进 DV-040，并跨引用 P6.1 同类 classloader 风险（[risks.md R-004]、CI #973270 ServiceLoader-empty、hive-catalog-shade + direct iceberg-core 共存）。

---

## 5. 🔴🔴 P6.6 翻闸阻塞项（= DV-038，写在显眼处）

**翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）前必修，否则整 BE 崩**：

- **面 1（GLOBAL_ROWID）**：top-N 延迟物化合成列被通用 `PluginDrivenScanNode.classifyColumn` 归 REGULAR → field-id 字典让 BE 走 field-id 路径 → 该列不在字典 → `iceberg_reader.cpp` `children_column_exists` StructNode DCHECK。修在共享 fe-core（GLOBAL_ROWID→SYNTHESIZED），但 `paimon_reader.cpp` 无 SYNTHESIZED-GLOBAL_ROWID 处理器 → **须 paimon 影响分析 + 可能 BE 协同**。
- **面 2（`getColumnHandles` 无 snapshot 重载）**：rename + time-travel pin 下从 current schema 建字典丢被重命名 slot field-id → 同一 DCHECK。iceberg 侧 T07 Option A 已闭合，但**共享 seam 仍潜伏 paimon**。

翻闸是**全有或全无**（`CatalogFactory:104-113`），须等 **P6.1–P6.5 全部实现完**（P6.3 写 / P6.4 procedure / P6.5 sys-table 都还没做）。现在翻闸会让所有 iceberg 查询走只有读元数据+scan 的连接器、write/procedure/sys-table 全断。

---

## 6. 验收门状态

| 项 | 状态 | 证据 |
|---|---|---|
| fe-connector-iceberg UT | ✅ **278/0/0**（1 skip = env-gated `IcebergLiveConnectivityTest`） | `mvn -pl :fe-connector-iceberg -am test`（cache off）BUILD SUCCESS（2026-06-23 本 session 重跑） |
| validation gate test | ✅ **7/0/0** | `IcebergConnectorValidatePropertiesTest` surefire |
| checkstyle | ✅ 0 | validate phase（build 内） |
| import-gate | ✅ 净 | `tools/check-connector-imports.sh`（连接器零 fe-core import） |
| `SPI_READY_TYPES` | ✅ iceberg **缺席** | `CatalogFactory:50`（零行为变更，翻闸只在 P6.6） |
| SPI / fe-core / paimon / pom 改 | ✅ T11 = **0**（纯文档） | git diff 仅 plan-doc/ |

**P6.2 验收门（migration line 90）**：scan parity（谓词下推 / 分区裁剪行数 / native·JNI / position+equality delete / SELECT* 无谓词）+ MVCC time-travel（AS OF / VERSION）+ vended REST round-trip——**FE 离线 UT 全覆盖逻辑/wiring**（278 测，T10 审计断 value-parity 非类名）；**真 BE/live 行为留 P6.6 docker**（DV-038/039/040 真值闸）。

---

## 7. 下一阶段 = P6.3 写路径（先写 RFC 过 PMC）

P6.2 DONE ⇒ 进入 **P6.3 写路径**。**前置硬约束**：写路径深度耦合 nereids（`IcebergTransaction` 966 + delete/merge sink + conflict-detection），master plan 注明**须先写 `plan-doc/06-iceberg-write-path-rfc.md` 评审方案**（过 PMC）再实现。P6.3 折进新「写路径 SPI」（recon 已确认 E11/W-phase 面在 P4 部分就绪）。

此后：P6.4 procedures（`ConnectorProcedureOps` E2，10 个 action）→ P6.5 sys-table + 元数据列 → **P6.6 才翻闸**（加 `SPI_READY_TYPES` + GSON compat + SHOW-CREATE 渲染 + **DV-038 翻闸阻塞修**）→ P6.7 删 legacy → P6.8 docker 回归（届时首验 DV-038/039/040 全部 UT 不可见 deviation）。
