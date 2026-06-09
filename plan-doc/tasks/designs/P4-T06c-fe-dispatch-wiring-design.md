# P4-T06c — FE 分发接线：DDL / 分区内省 → 已有 SPI（[D-028]）

> 状态：**DESIGN（待批准）** · 分支 `catalog-spi-05` · 前置：T06b flip 已落（`2b135899411`）
> 关联：[P4-T05-T06 cutover design](./P4-T05-T06-cutover-design.md)（Batch C，已落）· [Batch D 移除设计](./P4-batchD-maxcompute-removal-design.md)（前置门 = 本任务落 + live 绿）
> 决策：[D-028]（翻闸前全补 FE 分发接线，通用 PluginDriven 实现，非 MC 专有）

---

## 1. 背景与问题

T06b 翻闸后，`max_compute` catalog 实例化为 `PluginDrivenExternalCatalog`（`metadataOps` 永为 `null`）。该类**仅 override `createTable`**，其余元数据写/内省操作的 FE 分发仍按 legacy `instanceof MaxComputeExternalCatalog` 路由 → 翻闸后落空。连接器侧方法（P4-T01/T02）**已存在**，本任务只补 **FE 接线**。

### 1.1 翻闸后回归矩阵（已 file:line 核实，当前行号）

| Smoke 项 | 现状 | 根因（当前行号） |
|---|---|---|
| SELECT / CREATE TABLE / INSERT 全家 | ✅ 已通 | 读路径 + `createTable` override + 写链路（T06a） |
| **DROP TABLE** | ❌ `Drop table is not supported` | `ExternalCatalog.java:1105`（`metadataOps==null`，未 override） |
| **CREATE DB** | ❌ `Create database is not supported` | `ExternalCatalog.java:1004` |
| **DROP DB** | ❌ `Drop database is not supported` | `ExternalCatalog.java:1029` |
| **SHOW PARTITIONS** | ❌ `Catalog of type 'max_compute' is not allowed` | `ShowPartitionsCommand.java:202-204` allow-list + `:255` 表类型校验 + `:415` dispatch |
| **partitions() TVF** | ❌ `not support catalog` | `MetadataGenerator.java:1310` instanceof 分发落空 |

### 1.2 ⚠️ 本设计新发现（超出 HANDOFF 原计划）：**FE 元数据缓存失效缺口**

legacy `MaxComputeMetadataOps` 在 DDL 成功后会失效 FE 本地缓存（`afterX` 钩子）；该钩子在 **master**（`metadataOps.createDb`→`afterCreateDb`）与 **follower**（`replayX`→`afterX`）两路均被触发。`PluginDrivenExternalCatalog` 的 `metadataOps==null` → **两路均 no-op** → DDL 后同一 FE 的 `SHOW DATABASES/TABLES` 缓存陈旧（直到 TTL/手动 REFRESH）。

legacy `afterX` 实际做的失效（已核实 `MaxComputeMetadataOps.java`）：

| Op | legacy `afterX` 失效动作 | 可达性（PluginDriven 可直接调） |
|---|---|---|
| createDb | `resetMetaCacheNames()` | `ExternalCatalog.java:1494` public |
| dropDb | `unregisterDatabase(dbName)` | `ExternalCatalog.java:1142` public |
| createTable | `db.resetMetaCacheNames()` | `ExternalDatabase.java:628` public（`getDbForReplay` @ `:842`） |
| dropTable | `db.unregisterTable(tblName)` | `ExternalDatabase.java:552` public |

**推论**：已落的 `createTable` override（`PluginDrivenExternalCatalog.java:257-277`）**缺** `db.resetMetaCacheNames()` → 翻闸已引入一处缓存陈旧回归（CREATE TABLE 后新表不立即出现在缓存表名列表）。本任务的新 override 若仅"镜像 createTable"会继承同一缺口。**故本设计将缓存失效纳入范围**，并顺带修复 `createTable`。

> 这是 Rule 7（surface conflicts）/ Rule 12（fail loud）触发点：HANDOFF 原计划写"镜像 createTable override"，但 createTable 自身缺缓存失效 → 单纯镜像 ≠ 与 legacy 行为对齐。

---

## 2. 目标 / 非目标

### 目标
- G1：`PluginDrivenExternalCatalog` override `createDb` / `dropDb` / `dropTable`，路由到 `connector.getMetadata(session).{createDatabase/dropDatabase/dropTable}`，写 editlog，并失效 FE 缓存（master 路）。
- G2：`SHOW PARTITIONS` 接受 `PluginDrivenExternalCatalog` + `PLUGIN_EXTERNAL_TABLE`，新增 handler 经 SPI `listPartitionNames` 取分区。
- G3：`partitions()` TVF 接受 `PluginDrivenExternalCatalog`，新增 helper 经 SPI `listPartitionNames` 构造结果。
- G4：补缓存失效一致性：新 3 个 DDL override + 修复已落 `createTable` + follower 侧 `replayX`（见 §6 决策）。
- G5：UT 覆盖（DDL 路由 / 缓存失效 / ShowPartitions+TVF PluginDriven 分支）。
- **成功判据**：fe-core gate 绿（compile + checkstyle 0 + import-gate）+ UT 绿 + **用户 live 验证 11 项全绿**（runbook 见 HANDOFF）。

### 非目标
- **RENAME TABLE**：SPI/任何连接器**无** `renameTable`（grep 零命中）→ 需先加 SPI 方法 + 连接器实现，**不在 T06c**。不在 live smoke 列表。`ExternalCatalog.renameTable:1082` 保持基类抛"not supported"。
- **partition_values() TVF**：OQ-5 **已解** —— `MetadataGenerator.java:2081` switch 仅 `HMS_EXTERNAL_TABLE` 一例；`MAX_COMPUTE_EXTERNAL_TABLE` 从不在内 → legacy MC **从未支持** → **非回归**，不补。
- 连接器侧改动：方法已存在（P4-T01/T02），本任务零连接器改动（守门只 `-pl :fe-core -am`）。
- IF NOT EXISTS / FORCE 的连接器级语义增强（见 §5 边界，FE 侧按现有契约桥接）。

---

## 3. 架构 / 数据流

所有改动集中 fe-core，通用 keyed on `PluginDrivenExternalCatalog` / `TableType.PLUGIN_EXTERNAL_TABLE`（非 MC 专有，自动惠及 jdbc/es/trino 同类缺口；并使 Batch D 退化为"删残留 legacy MC 引用"）。

```
DDL:   Nereids Command → ExternalCatalog.{createDb/dropDb/dropTable}
         → [T06c override on PluginDrivenExternalCatalog]
         → connector.getMetadata(buildConnectorSession()).{createDatabase/dropDatabase/dropTable}
         → editlog + 缓存失效
SHOW PARTITIONS: ShowPartitionsCommand.{validate→analyze→handleShowPartitions}
         → [T06c: allow-list + 表类型 + dispatch 分支] → handleShowPluginDrivenTablePartitions()
         → getConnector().getMetadata(session).getTableHandle(...).listPartitionNames(session, handle)
partitions() TVF: MetadataGenerator.partitionMetadataResult()
         → [T06c: instanceof PluginDrivenExternalCatalog 分支] → dealPluginDrivenCatalog()
         → 同上 SPI listPartitionNames → 单 string 列 TRow（镜像 dealMaxComputeCatalog 形状）
```

### SPI 目标方法（均已在 `MaxComputeConnectorMetadata` 实现）
| FE 调用 | SPI 方法 | 备注 |
|---|---|---|
| createDb | `createDatabase(session, dbName, properties)` `ConnectorSchemaOps:48` | **无 ifNotExists** 参数 |
| dropDb | `dropDatabase(session, dbName, ifExists)` `ConnectorSchemaOps:55` | **无 force** 参数 |
| dropTable | `dropTable(session, handle)` `ConnectorTableOps:92` | **takes handle，无 ifExists**；先 `getTableHandle` |
| 分区内省 | `listPartitionNames(session, handle)` `ConnectorTableOps:158` | **无 skip/limit**；FE 侧 applyLimit |
| 解析 handle | `getTableHandle(session, db, tbl)` `ConnectorTableOps:36` → `Optional` | |

---

## 4. 详细改动（5 站点 + 缓存）

### 4.1 `PluginDrivenExternalCatalog.java`（DDL override，镜像 `createTable:257`）

新增 3 个 override（签名严格对齐基类，见 `ExternalCatalog:1002/1027/1102`）：

**`createDb(String dbName, boolean ifNotExists, Map<String,String> properties)`**
```
makeSureInitialized();
if (ifNotExists && getDbNullable(dbName) != null) { return; }     // honor IF NOT EXISTS（FE 侧，SPI 无此参）
ConnectorSession session = buildConnectorSession();
try { connector.getMetadata(session).createDatabase(session, dbName, properties); }
catch (DorisConnectorException e) { throw new DdlException(e.getMessage(), e); }
Env.getCurrentEnv().getEditLog().logCreateDb(new CreateDbInfo(getName(), dbName, null));   // org.apache.doris.persist.CreateDbInfo
resetMetaCacheNames();                                            // 缓存失效（= legacy afterCreateDb）
```

**`dropDb(String dbName, boolean ifExists, boolean force)`**
```
makeSureInitialized();
if (getDbNullable(dbName) == null) { if (ifExists) return; else throw new DdlException("..."); }
ConnectorSession session = buildConnectorSession();
try { connector.getMetadata(session).dropDatabase(session, dbName, ifExists); }   // force 不传（SPI 无此参，见 §5）
catch (DorisConnectorException e) { throw new DdlException(e.getMessage(), e); }
Env.getCurrentEnv().getEditLog().logDropDb(new DropDbInfo(getName(), dbName));
unregisterDatabase(dbName);                                       // 缓存失效（= legacy afterDropDb）
```

**`dropTable(String dbName, String tableName, boolean isView, boolean isMtmv, boolean isStream, boolean ifExists, boolean mustTemporary, boolean force)`**
```
makeSureInitialized();
ConnectorSession session = buildConnectorSession();
Optional<ConnectorTableHandle> handle = connector.getMetadata(session).getTableHandle(session, dbName, tableName);
if (!handle.isPresent()) { if (ifExists) return; else throw new DdlException("Failed to get table: ..."); }
try { connector.getMetadata(session).dropTable(session, handle.get()); }
catch (DorisConnectorException e) { throw new DdlException(e.getMessage(), e); }
Env.getCurrentEnv().getEditLog().logDropTable(new DropInfo(getName(), dbName, tableName));
getDbForReplay(dbName).ifPresent(db -> db.unregisterTable(tableName));   // 缓存失效（= legacy afterDropTable）
```

**修复已落 `createTable`**（§1.2）：editlog 后补
```
getDbForReplay(createTableInfo.getDbName()).ifPresent(db -> db.resetMetaCacheNames());   // = legacy afterCreateTable
```

新 import：`org.apache.doris.persist.{CreateDbInfo, DropDbInfo, DropInfo}`、`org.apache.doris.connector.api.handle.ConnectorTableHandle`、`java.util.Optional`（`Map` 已有）。`getMetadata(session)` 每调一次（不缓存，连接器 stateless）。

### 4.2 follower 缓存失效（`ExternalCatalog.java` replayX，见 §6 决策 A）
`replayCreateDb:1020` / `replayDropDb:1042` / `replayCreateTable:1075` / `replayDropTable:1130` 现仅 `if (metadataOps != null) metadataOps.afterX()`。补 `else` 分支做等价失效（仅 `metadataOps==null` 即 PluginDriven 走到；HMS/Iceberg 等非 null，行为不变）：
```
} else {                          // PluginDriven path
    resetMetaCacheNames();        // createDb
    // dropDb:      unregisterDatabase(dbName);
    // createTable: getDbForReplay(dbName).ifPresent(d -> d.resetMetaCacheNames());
    // dropTable:   getDbForReplay(dbName).ifPresent(d -> d.unregisterTable(tblName));
}
```

### 4.3 `ShowPartitionsCommand.java`（3 gate + handler）
1. **allow-list**（`validate()` :202-204）：`|| catalog instanceof PluginDrivenExternalCatalog`
2. **表类型校验**（`analyze()` :255）：`getTableOrMetaException(..., TableType.PLUGIN_EXTERNAL_TABLE)` 追加
3. **dispatch**（`handleShowPartitions()` :415，**在 final else 前**插入）：
   `else if (catalog instanceof PluginDrivenExternalCatalog) return handleShowPluginDrivenTablePartitions();`
4. 新 handler（镜像 `handleShowMaxComputeTablePartitions:286` 形状，但走 SPI）：
```
PluginDrivenExternalCatalog pdc = (PluginDrivenExternalCatalog) catalog;
ConnectorSession session = pdc.buildConnectorSession();
ConnectorMetadata md = pdc.getConnector().getMetadata(session);
ConnectorTableHandle handle = md.getTableHandle(session, tableName.getDb(), tableName.getTbl())
        .orElseThrow(() -> new AnalysisException("table not found: " + tableName.getTbl()));
List<String> names = md.listPartitionNames(session, handle);      // SPI 无 skip/limit
// 构单列行 + sort + applyLimit(limit, offset, rows)（同 HMS/Paimon handler）；filterMap 忽略（同 MC handler）
```
   import 追加 `PluginDrivenExternalCatalog`、SPI 类型。注意 `isPartitionedTable()`（:257）须对 `PluginDrivenExternalTable` 正确返回（验证项）。

### 4.4 `MetadataGenerator.java`（partitions() TVF 分支 + helper）
`partitionMetadataResult()`（:1308 dispatch 链）在 MC 分支旁/前加：
```
} else if (catalog instanceof PluginDrivenExternalCatalog) {
    return dealPluginDrivenCatalog((PluginDrivenExternalCatalog) catalog, (ExternalTable) table);
}
```
新 helper `dealPluginDrivenCatalog`（镜像 `dealMaxComputeCatalog:1337` 的 TRow/TCell 单 string 列形状 + `TStatusCode.OK`）：
```
ConnectorSession session = catalog.buildConnectorSession();
ConnectorMetadata md = catalog.getConnector().getMetadata(session);
ConnectorTableHandle handle = md.getTableHandle(session, table.getDbName(), table.getName())....;  // 名称约定见 §5
List<String> names = md.listPartitionNames(session, handle);
// 每名一 TRow(单 TCell setStringVal) → dataBatch + TStatus OK
```

---

## 5. 边界 / 已知语义差（fail loud）

- **createDb 无 `ifNotExists`（SPI）**：FE override 先 `getDbNullable` 预检兑现 IF NOT EXISTS（存在则跳过、不写 editlog/不调 SPI）。
- **dropDb 无 `force`（SPI）**：`force` 参数被丢弃，仅传 `ifExists`。legacy `dropDbImpl` 的 force=级联删表逻辑（先 drop 库内全表）**不复刻**；MaxCompute 侧 dropDb 由连接器处理。若日后需级联 → 连接器侧增强（记 OQ）。
- **dropTable handle 解析**：SPI 用 `ConnectorTableHandle` 非 (db,tbl)；FE 先 `getTableHandle`，空 Optional 即"表不存在"→ ifExists 静默返回 / 否则抛。IF EXISTS 语义落在 FE，远端 drop 幂等。
- **分区名 db/tbl 名称约定**：`getTableHandle` 传本地名还是 remote 名 —— 对齐 `PluginDrivenExternalCatalog.tableExist:222`（传入 db/tbl，连接器内部解析 remote 映射）。ShowPartitions 用 `tableName.getDb()/getTbl()`；TVF 用 `table.getDbName()/getName()`。**实现时核连接器 `getTableHandle` 契约**（验证项）。
- **listPartitionNames 无 skip/limit**：offset/limit 在 FE handler 用既有 `applyLimit` 兜（不下推连接器）。SPI default 返回 `emptyList()` → 未 override 的连接器优雅显示 0 分区（非报错）。

---

## 6. 🔴 待批准决策

### 决策 A — 缓存失效深度（核心）
| 方案 | master（live 单 FE） | follower（HA 多 FE） | 改动面 | 一致性 |
|---|---|---|---|---|
| **A1（推荐）全对齐** | ✅ override 内失效 | ✅ replayX else 分支失效 | DDL override + `ExternalCatalog` 4 个 replayX + 修 createTable | 与 legacy 完全对齐 |
| A2 仅 master | ✅ override 内失效 | ❌ TTL 前陈旧 | 仅 DDL override（不动 replayX/createTable） | live 绿但 HA 有差 |
| A3 不补（纯镜像 createTable） | ❌ 可能陈旧 | ❌ | 最小 | **风险 live 不绿** |

**推荐 A1**：与 legacy 行为完全对齐，HA 正确，且顺带修复 createTable 已引入的缓存回归。`else` 分支只在 `metadataOps==null` 触发，对 HMS/Iceberg 零影响（surgical）。

### 决策 B — 是否在 T06c 内修复已落 `createTable` 的缓存缺口
- **推荐 是**：缓存失效是同一翻闸回归主题，不修则 createTable 与新 3 op 行为不一致。会触碰已 commit 代码（T05/T06a），commit message 明确标注。
- 否：createTable 留缺口（不一致），另开任务。

### 决策 C — 提交粒度（每 commit 独立，用户定时机）
- C1（推荐）3 commit：① DDL override + 缓存（含 createTable 修 + replayX）+ UT；② SHOW PARTITIONS + UT；③ partitions() TVF + UT。
- C2 1 commit 全量。

---

## 7. 测试（Rule 9：测意图）

模块/框架：fe-core = JUnit5 + Mockito。模板 = `PluginDrivenExternalCatalogConcurrencyTest` 的 `TestablePluginCatalog`（注 mock `Connector`，反射注入 private `connector` 字段，stub `buildConnectorSession`/`initLocalObjectsImpl` 绕 Env）。现有 0 个测试覆盖 createTable override 路由 / ShowPartitions 外表 dispatch / MetadataGenerator（无 MetadataGeneratorTest）。

- **T1 `PluginDrivenExternalCatalogDdlRoutingTest`（新，fe-core）**：
  - createDb/dropDb/dropTable 调到 mock `ConnectorMetadata` 对应方法（verify 调用 + 参数）。
  - `DorisConnectorException` → `DdlException` 包裹。
  - dropTable 先 `getTableHandle`；空 Optional + ifExists → 静默；空 + !ifExists → 抛。
  - **缓存失效断言**（编码 WHY）：DDL 成功后对应 `resetMetaCacheNames`/`unregisterDatabase`/`unregisterTable` 被触发（spy catalog/db）—— 即"翻闸后 catalog DDL 须与 legacy 一样使同 FE 缓存可见新状态"。
  - createTable 修复后亦 verify `db.resetMetaCacheNames()`。
  - editlog：stub/避开（真 Env 单例可用，或只验 SPI 调用 + 异常包裹 + 缓存）。
- **T2 ShowPartitions + MetadataGenerator PluginDriven 分支**：断言 `type=max_compute` 的 `PluginDrivenExternalCatalog` 现被 allow-list 接受、表类型校验过 `PLUGIN_EXTERNAL_TABLE`、dispatch 路由到 SPI `listPartitionNames`（编码 WHY：迁移后 MC catalog 须保持 SHOW PARTITIONS / partitions-TVF 可用）。重型可用 `TestWithFeService`，或聚焦单测 dispatch 分支。

守门（坑6/7/8）：
```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-core -am \
  -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -DskipTests test-compile   # 后台，读 BUILD/MVN_EXIT
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-core \
  -Dmaven.build.cache.enabled=false checkstyle:check
bash tools/check-connector-imports.sh
# UT：-pl :fe-core -Dtest=PluginDrivenExternalCatalogDdlRoutingTest,... test
```
live 验证：HANDOFF runbook（Layer1 连通 + Layer2 全链路 11 项），目标全绿 → 解锁 Batch D。

---

## 8. 风险 / 回滚
- flip（T06b）与本任务独立可 revert；**live 未绿前勿删 legacy**（Batch D 在后）。
- replayX `else` 分支误伤其他 catalog：已规避（仅 `metadataOps==null`）。需 fe-core UT + 编译守门确认 HMS/Iceberg 路径不变。
- 名称约定（local vs remote）若桥接错 → 分区/drop 找不到表；实现时核 `getTableHandle` 契约 + UT。

---

## 9. 有序 TODO
> 决策：A1（全对齐）+ C1（三 commit）已批准。实现状态见下（gate 均 file:line 验证：compile BUILD SUCCESS / checkstyle 0 / import-gate 0）。
1. [x] `PluginDrivenExternalCatalog`：override createDb/dropDb/dropTable + 缓存失效 + 修 createTable；imports。
2. [x] `ExternalCatalog` 4× replayX 加 `else`（决策 A1）。
3. [x] `PluginDrivenExternalCatalogDdlRoutingTest`（T1）—— **12/12 绿**。
4. [x] commit ① 改动 gate 绿（compile + checkstyle 0 + import-gate 0 + UT 12）。**待 commit（用户定时机）**。
5. [x] `ShowPartitionsCommand` 3 gate + handler；`ShowPartitionsCommandPluginDrivenTest`。gate 绿。**待 commit**。
6. [x] `MetadataGenerator` 分支 + `dealPluginDrivenCatalog`；`MetadataGeneratorPluginDrivenTest`。gate 绿。**待 commit**。
7. [ ] 用户跑 live 验证 11 项；全绿 → 更新 HANDOFF/decisions-log（[D-028] 落）→ 解锁 Batch D。
