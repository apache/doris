# 设计文档 — hudi 时间线检查（`hudi_meta` TVF）的处置

日期：2026-07-21 · 分支：`catalog-spi-review-17` · 关联：[HANDOFF.md](./HANDOFF.md) T5.3

## ⭐ 最终决定（2026-07-21，用户多次调整后定稿）：**彻底删除 `hudi_meta` 功能，不迁移、无替代**

> 决策演进（保留下方迁移分析作为"为何最终选删除"的历史记录）：
> 1. 先定"迁移到 `表$timeline` 系统表"；2. 再定"走 TVF/`meta_scanner` 路径、与 `partition_values` 统一"（因 hudi 时间线不是可读 SDK Table、享受不到 paimon 那种零 BE 复用）；3. **最终用户拍板：整个功能直接删掉**（BE 死分支也一并删，用户选"更彻底、需重编 BE"）。

**已执行的删除清单**（多 agent 对抗核验：无遗漏引用、无误删、hudi 数据读取路径零影响）：
- **fe-core 删**：`HudiTableValuedFunction`+`HudiMeta` 两整文件；4 处注册/分发（`BuiltinTableValuedFunctions`、`TableValuedFunctionIf` case、`TableValuedFunctionVisitor.visitHudiMeta`、`MetadataTableValuedFunction` case HUDI）；`MetadataGenerator` 的 `case HUDI`+`hudiMetadataResult`+2 imports；**只服务本功能的通用 SPI 缝** `PluginDrivenExternalTable.supportsMetadataTable/getMetadataTableRows`；fe-core 单测 `PluginDrivenExternalTableTest.supportsMetadataTableReflectsConnectorCapability`。
- **连接器删**：`HudiConnectorMetadata.getMetadataTableRows`（+2 unused imports）；`HudiConnector.getCapabilities`（+3 unused imports，回落到默认空能力集）；`HiveConnectorMetadata.getMetadataTableRows`（兄弟委派）；`ConnectorCapability.SUPPORTS_METADATA_TABLE` 枚举常量；`ConnectorMetadata.getMetadataTableRows` 默认 SPI；3 个连接器单测的相关片段。
- **thrift 就地弃用**（保留+标 `// deprecated`，对齐 iceberg/paimon 先例，零重生成/零 skew 风险）：`THudiMetadataParams`、`THudiQueryType`、`TMetaScanRange.hudi_params`(#12)。`TMetadataType.HUDI=11` 与 `FrontendService.hudi_metadata_params`(#14) 按先例保持不标（iceberg 同款未标）。
- **BE 删**：`meta_scanner.cpp` 的 `case HUDI` + `_build_hudi_metadata_request` + `meta_scanner.h` 声明（死代码；stale-FE 请求走 default 分支返回空、不崩）。
- **数据读取路径零改动**：`THudiFileDesc`、`TFileScanRangeParams.hudi_params`、`HUDI_TABLE`、`HudiScanPlanProvider`、`HadoopHudiJniScanner`、`hudi_jni_reader.cpp` 等——那是**另一个同名** `hudi_params`（数据），与本功能无关。
- **回归测试**：删 `test_hudi_meta.groovy`+`.out`；把 3 个数据用例（时间旅行/增量/分区裁剪）里用 `hudi_meta` 列提交时间戳的 `getCommitTimestamps` 助手改用 `SELECT DISTINCT _hoodie_commit_time`（p2 真实环境用例，本地跑不了，交 CI 验证）。

**验证**：FE `test-compile`（fe-core + 三个连接器模块，含测试编译）`BUILD SUCCESS`；BE 删除经 grep 证自包含（无悬挂引用、数据路径完好），须一次 BE 重编收尾。

---
（以下为迁移方案分析，最终未采用，保留作决策依据）

# （历史）设计 — 把 hudi 时间线检查从 `hudi_meta` TVF 迁到 `表$timeline` 系统表

> 用户曾拍板两项：(1) 现在就做迁移；(2) 面向用户写法改为 `表$timeline` 系统表语法。
> 约束（用户 2026-07-21 明确）：**不改 BE、复用 thrift**；hudi 专有内容搬进 `fe-connector-hudi`；参考本分支已完成的 connector 迁移做法。

---

## 1. 背景与研究小结（research note）

### 现状：`hudi_meta` 是最后一个用老式"每源一函数"写法的元数据 TVF
- 用户今天写：`SELECT ... FROM hudi_meta("table"="ctl.db.tbl","query_type"="timeline")`。
- 全链路是 FE→BE→FE 往返：`HudiMeta`(nereids) → `HudiTableValuedFunction`(fe-core, 建 `THudiMetadataParams`/`TMetadataType.HUDI` + 硬编码 4 列 schema) → `MetadataScanNode`(通用) → thrift → BE `meta_scanner.cpp` `case HUDI`(往返回 FE) → `MetadataGenerator.hudiMetadataResult`(解码 + 委派) → **`PluginDrivenExternalTable.getMetadataTableRows("timeline")`(已通用)** → 连接器 `HudiConnectorMetadata.getMetadataTableRows`(真读 hudi timeline)。
- **行产出已通用化**（走连接器 SPI，fe-core 不再 import `org.apache.hudi`）；残留在 fe-core 的只是"外壳 + 往返 plumbing"。
- 活 + 有回归测试：`regression-test/suites/external_table_p2/hudi/test_hudi_meta.groovy`（p2，`enableHudiTest`）。

### 另外两种格式（iceberg/paimon）怎么迁的
- 用户写法从 `xxx_meta(...)` 改成 `表$snapshots` 等**系统表后缀语法**（`SysTable` 框架：`SysTable`/`NativeSysTable`/`TvfSysTable`/`SysTableResolver`，见 `datasource/systable/`）。
- 它们走 **`NativeSysTable`**（`PluginDrivenSysTable`）→ `LogicalFileScan` → **新写的 BE JNI 扫描器**（`be-java-extensions/iceberg-metadata-scanner`）。→ **本次不采用**（用户要求不改 BE）。
- 关键佐证：本分支的 connector 迁移 PR（P5 paimon #64653、P6 iceberg #64688）把逻辑集中搬进 `XxxConnectorMetadata`，**没动 BE、没动 thrift**——BE 扫描器是更早的上游 PR 引入的。

### 复用 BE+thrift 的现成模板：`TvfSysTable` / `partition_values`
- hive 的 `表$partitions` 走 **`TvfSysTable`**（`PartitionsSysTable`）→ `createFunction` 返回 `partition_values` TVF → `LogicalTVFRelation` → `MetadataScanNode` → BE `meta_scanner.cpp` `case PARTITION_VALUES` → FE `MetadataGenerator.partitionValuesMetadataResult`。
- **这条链路与 hudi timeline 完全同构**（都是 `MetadataTableValuedFunction` 子类走 meta 往返）。→ 让 `表$timeline` 走 `TvfSysTable`，即可**复用现有 `TMetadataType.HUDI` + `THudiMetadataParams` + BE `meta_scanner` `case HUDI`，一行 BE / 一行 thrift 都不改**。

### 硬约束发现
1. **插件无法注册全局 TVF**（`fe-connector-api` 无 TVF 贡献 SPI）→ 全局函数只能在 fe-core 注册；改 `表$timeline` 系统表后就不再需要全局函数。
2. **`BindRelation.handleMetaTable` 的 native/TVF 分叉**（`BindRelation.java:612` `SysTableResolver.resolveForPlan`）：native→`LogicalFileScan`（需 BE 扫描器）；TVF→`LogicalTVFRelation`（复用 meta 往返）。**本设计走 TVF 分支。**
3. **诚实的取舍**：复用 BE meta 往返，就必须在 fe-core 保留一小段"往返 plumbing"（一个 `MetadataTableValuedFunction` 子类 + 对 `THudiMetadataParams`/`TMetadataType.HUDI` 的引用）。真正属于 hudi 领域知识的部分（**列 schema、timeline 读取、有哪些系统表**）搬进连接器。要把 fe-core 里最后这点 hudi-named plumbing 也清掉，只能走被排除的 native+BE 扫描器路线。

---

## 2. 目标 / 非目标

**目标**
- 用户写法：`SELECT ... FROM ctl.db.tbl$timeline`（列不变：`timestamp/action/state/state_transition_time`）。
- 删除全局 `hudi_meta` 函数面（`HudiMeta` nereids + 注册点）。
- hudi 领域知识（timeline schema、系统表清单）搬进 `fe-connector-hudi`；行产出已在连接器。
- **BE 零改动；thrift 零改动**（`TMetadataType.HUDI`/`THudiMetadataParams`/`meta_scanner case HUDI` 全部复用）。
- 改写回归测试为 `表$timeline`。

**非目标**
- 不写 BE 扫描器、不加 be-java-extension、不改 thrift schema。
- 不做 iceberg/paimon 式 native 执行路径。
- 不碰 hudi 的**数据**读取路径（`THudiFileDesc`、`hudi_jni_reader` 等——与元数据 TVF 无关，必须保留）。
- 不保留 `hudi_meta(...)` 向后兼容别名（对齐 iceberg/paimon 的破坏性做法；如需别名另立独立任务）。

---

## 3. 架构与数据流（目标态）

```
SQL: SELECT ... FROM ctl.db.tbl$timeline
  └─ BindRelation.handleMetaTable
       └─ SysTableResolver.resolveForPlan(table, ..., "tbl$timeline")
            └─ PluginDrivenExternalTable.getSupportedSysTables()   // 连接器声明 timeline
                 └─ 映射 "timeline" → TimelineSysTable (TvfSysTable)   // 新增：TVF 分支
            └─ TvfSysTable.createFunction(...) → 一个 MetadataTableValuedFunction   // 复用 plumbing
       └─ 返回 LogicalTVFRelation                                    // TVF 分支（非 native）
  └─ MetadataScanNode (通用, 不改)
       └─ tvf.getMetaScanRange() → TMetaScanRange{metadata_type=HUDI, THudiMetadataParams}  // 复用 thrift
  └─ thrift → BE meta_scanner.cpp case HUDI (不改) → 往返回 FE
  └─ MetadataGenerator.hudiMetadataResult (不改/微调) 
       └─ PluginDrivenExternalTable.getMetadataTableRows("timeline")   // 已通用
            └─ HudiConnectorMetadata.getMetadataTableRows(...)          // 连接器真读 timeline
```

**谁拥有什么（迁移后）**
- **连接器 `fe-connector-hudi`**：`listSupportedSysTables()→["timeline"]`；timeline 列 schema（4 列常量）；行产出（已有）。
- **fe-core（通用 plumbing，保留/新增）**：`SysTable` 框架；`MetadataScanNode`；meta 往返；一个 TVF-path 桥（`TimelineSysTable`）+ 一个复用 `THudiMetadataParams` 的 `MetadataTableValuedFunction` 子类（timeline schema 从连接器取）。
- **thrift / BE**：全部复用，零改动。

---

## 4. 文件级改动清单

### 连接器 `fe-connector-hudi`（+）
1. `HudiConnectorMetadata.listSupportedSysTables(session, baseHandle)` → 返回 `["timeline"]`（覆盖默认空）。
2. timeline 列 schema：以连接器常量给出 4 列（`timestamp/action/state/state_transition_time`, STRING），并暴露给 fe-core 系统表解析（经 sys handle / getTableSchema sys-aware，或一个小 SPI seam）。**待定实现细节见 §6 开放问题 Q1。**
3. `getMetadataTableRows(session, handle, "timeline")`：已存在，核对 kind 匹配即可（可能需容忍来自 sys-table 路径的调用上下文）。
4. `HudiConnectorOwnsHandleTest` / 新增 `HudiConnectorMetadataSysTableTest`：单测覆盖 `listSupportedSysTables` + schema。

### fe-core（−/±）
5. **删** `nereids/trees/expressions/functions/table/HudiMeta.java`。
6. **删** 注册点：`catalog/BuiltinTableValuedFunctions.java:63`（+import）、`tablefunction/TableValuedFunctionIf.java:60-61`（全局 dispatch case）、`nereids/.../visitor/TableValuedFunctionVisitor.java:112`（`visitHudiMeta`+import）。
7. **改** `tablefunction/HudiTableValuedFunction.java`：不再作全局函数；schema 改为从连接器取；仅由 `TimelineSysTable` 构造。（或重命名为通用名——见 §6 Q2）
8. **新增** `datasource/systable/TimelineSysTable.java`（`extends TvfSysTable`，仿 `PartitionsSysTable`，~40 行）：`createFunction`/`createFunctionRef` 返回 timeline TVF。
9. **改** `datasource/plugin/PluginDrivenExternalTable.getSupportedSysTables()`：把连接器声明的 `timeline` 路由到 `TimelineSysTable`（新增第三分支；现有分支：`isPartitionValuesSysTable`→`PartitionsSysTable`，否则→native `PluginDrivenSysTable`）。**待定：如何泛化这个分叉——见 §6 Q1。**
10. **改** `tablefunction/MetadataTableValuedFunction.java:45-46`（`getColumnIndexFromColumnName` 的 `case HUDI` 臂）：随 TVF 内部化保留/微调。
11. `MetadataGenerator.hudiMetadataResult`（302/433）：**保留**（已委派连接器）；仅在 schema/priv 上下文变化时微调。

### thrift / BE
- **零改动**。`TMetadataType.HUDI`、`THudiMetadataParams`、`meta_scanner.cpp case HUDI` 全部复用。

### 回归测试
12. 改写 `regression-test/suites/external_table_p2/hudi/test_hudi_meta.groovy`：`hudi_meta("table"=X,"query_type"="timeline")` → `SELECT ... FROM X$timeline`；更新 `.out` 基线（仿 iceberg 删 `test_iceberg_meta` 改查 `$snapshots` 的做法）。

---

## 5. 边界情况 / 风险
- **鉴权**：原 TVF 对目标表做 `checkTblPriv(SELECT)`；系统表路径须对**基表**鉴权。核对 `partition_values`/sys-table 路径的鉴权点，保证 `表$timeline` 同样校验基表 SELECT 权。
- **异构 HMS 网关**：hudi-on-HMS 时基表由 hive 网关委派给 hudi 兄弟连接器；`listSupportedSysTables`/`getSysTableHandle` 已有网关委派（`HiveConnectorMetadata` 对外来 handle 转发兄弟）——须确认 `timeline` 经网关也解析（对齐"hudi-on-HMS 新能力必配 e2e"纪律）。
- **FE/BE 滚动升级**：老 FE 仍可能发 `hudi_meta`；本次不删 thrift/BE，故老 FE→新 BE 无 skew 风险；新 FE 删了全局函数→老 SQL 报"函数不存在"（与 iceberg/paimon 一致的破坏性）。
- **`query_type` 退化**：`THudiQueryType` 只有 `TIMELINE`；`表$timeline` 隐含 timeline，`query_type` 参数消失，语义不变。
- **iron rule A（fe-core 只减不增）**：新增 `TimelineSysTable`（~40 行通用桥）+ 复用 plumbing 的 TVF；净效果是 hudi **专有性**下降（schema/清单外迁）、但 fe-core 行数近中性。属"通用框架件替换源专有件"，在本次用户明确授权的迁移语境下可接受；如 review 认为违 A，回退到"保留 `HudiTableValuedFunction` 原样、仅换触发入口"的更小变体。

---

## 6. 开放问题 —— 已定（2026-07-21，用户确认"和 partition_values 统一"后收敛）

> 决策依据：用户选 **TVF/`meta_scanner` 路径、与 `partition_values` 统一**。经完整读透 `partition_values` 链路（`PartitionsSysTable`+`PartitionValues`+`PartitionValuesTableValuedFunction`+`getSupportedSysTables` 分叉+`partitionValuesMetadataResultForPluginTable` 委派连接器），**范式=严格镜像 `partition_values`**。

- **Q1 已定** — 路由：`getSupportedSysTables()` 里在现有 `isPartitionValuesSysTable→PartitionsSysTable` / else→native 两分支间，**新增一条**：连接器声明的 `timeline` → fe-core 单例 `TimelineSysTable.INSTANCE`（按 `sysName` 精确名匹配，最小、最贴 `PartitionsSysTable` 单例先例）。连接器经 `listSupportedSysTables` 控制"是否出现 timeline"；fe-core 控制"timeline 走 TVF 路径"。
- **Q2 已定** — **严格镜像 `partition_values`：timeline TVF plumbing（`HudiTableValuedFunction`+`HudiMeta`）保留在 fe-core**（正如 `PartitionValuesTableValuedFunction`+`PartitionValues` 留在 fe-core），4 列 timeline schema 也留在 fe-core TVF（`partition_values` 的 schema 同样在 fe-core TVF 里）。**不**新增"连接器供 sys-table schema"的 SPI seam（那是 native 路径的做法，TVF 路径无此先例，加了违 iron rule A 且超简洁律）。→ 真正从 fe-core 移除的是**全局 `hudi_meta` 函数面**；hudi 行产出本就在连接器（`getMetadataTableRows`）。诚实记录：TVF 路径下 timeline TVF 必留 fe-core，这是"复用 BE meta 往返"的固有结果（见 §1 硬约束发现 3），与"和 partition_values 统一"的选择自洽。

## 6b. 定稿：严格镜像 `partition_values` 的最小改动
- **保留在 fe-core（TVF 路径 plumbing，镜像 `PartitionValues*`）**：`HudiTableValuedFunction`、`HudiMeta`、`TableValuedFunctionVisitor.visitHudiMeta`、`MetadataGenerator.hudiMetadataResult`(+`case HUDI`)、`MetadataTableValuedFunction` 的 `case HUDI`、`TableValuedFunctionIf` 的 `case hudi_meta`（describe/legacy 名解析需要）。
- **fe-core 删除**：仅 `BuiltinTableValuedFunctions` 的 `tableValued(HudiMeta.class,"hudi_meta")`(+import) —— 斩断 Nereids 全局 `hudi_meta(...)` 可调用性（surface 收敛到 `表$timeline`）。
- **fe-core 新增**：`datasource/systable/TimelineSysTable.java`（`extends TvfSysTable`，单例，镜像 `PartitionsSysTable`；`createFunction→HudiMeta.create([ctl,db,tbl])`，`createFunctionRef→TableValuedFunctionRefInfo("hudi_meta",...)`）；`getSupportedSysTables()` 加 timeline 分支。
- **fe-core 改**：`HudiMeta` 加 `create(List<String> qualifiedTableName)` 静态（镜像 `PartitionValues.create`）；`HudiTableValuedFunction` 构造改收 catalog/db/table 三分参（镜像 `PartitionValuesTableValuedFunction` 的 CATALOG/DB/TABLE，弃 "table"单串 split-on-"." 脆弱解析 + 弃 query_type，timeline 隐含）。
- **连接器 `HudiConnectorMetadata`**：`listSupportedSysTables→["timeline"]`（覆盖默认空；异构 HMS 网关已自动转发兄弟）。行产出 `getMetadataTableRows("timeline")` 已有。
- **thrift/BE**：零改动。

---

## 7. TODO（实现顺序）
1. [ ] 建绿色基线：`test-compile -pl fe-core -am` + 相关连接器模块。
2. [ ] 连接器：`HudiConnectorMetadata.listSupportedSysTables→["timeline"]` + timeline schema 常量 + sys-table 单测。
3. [ ] fe-core：`TimelineSysTable`（TvfSysTable）+ `getSupportedSysTables()` 路由分支。
4. [ ] fe-core：`HudiTableValuedFunction` 内部化（schema 取自连接器；去全局 NAME 用途）。
5. [ ] fe-core：删 `HudiMeta` + 三处注册点（Builtin/Dispatch/Visitor）。
6. [ ] 编译（含测试编译）+ 门禁绿；FE 单测（sys-table 解析 + 鉴权）。
7. [ ] 改写 `test_hudi_meta.groovy` → `表$timeline` + `.out` 基线；补 hudi-on-HMS `$timeline` e2e。
8. [ ] clean-room 对抗 review（5 维）+ 更新 HANDOFF/TASKLIST。
