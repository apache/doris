# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [tasks/P4](./tasks/P4-maxcompute-migration.md)（批次计划，Batch C 翻闸是下一步）→ [P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md) + [P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)（T03/T04 dormant→live 接线源）→ [写 RFC](./tasks/designs/connector-write-spi-rfc.md)。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-06（**P4-T04 写计划实现完成 = Batch A+B 全完成**）
- **本 session 主题**：**P4-T04 实现**。按 [P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md) §Ordered TODO 落地 OQ-2 = Approach A：新建 `MaxComputeWritePlanProvider.planWrite`（一处建 ODPS 写 session + 绑事务 + 盖 `txn_id`/`write_session_id`）+ 连接器 `getSettings()`/`getWritePlanProvider()`/`supportsInsert()` + fe-core W5 seam fill。**守门全绿**（compile BUILD SUCCESS + checkstyle 0 + import-gate 0，真实 EXIT 核验）。
- **分支**：`catalog-spi-05`。本场改 6 文件（4 连接器 + 2 fe-core seam）+ 新增 1 连接器类。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`（沿用，勿提交）。
- **Batch 状态**：**A ✅（T01+T02）+ B ✅（T03+T04）= 全完成，gate 关 dormant**；**下一 = Batch C 翻闸**（唯一 live 切点，前置 A+B 全绿 ✅ + R-004 防御测）。

---

## ✅ 本 session 完成项（P4-T04，gate 关 dormant）

- **`MaxComputeWritePlanProvider`（新）** `planWrite` = **OQ-2 Approach A 六步**：① 读 `handle.isOverwrite()` + `handle.getWriteContext()`（静态分区 col→val map）；② `connector.getSettings()`；③ 港 `MCTransaction.beginInsert` 建 ODPS 写 session（静态分区 `PartitionSpec`/动态 `DynamicPartitionOptions.createDefault`/overwrite；**ArrowOptions MILLI/MILLI**，≠scan MILLI/MICRO）→ `writeSession.getId()`；④ `session.getCurrentTransaction()`→`MaxComputeConnectorTransaction.setWriteSession(wsid, tableId, settings)` 绑事务（T03 槽）；⑤ 建 `TMaxComputeTableSink`（静态字段 + `static_partition_spec`(原样 map) + `partition_columns`(ODPS 表列, DV-012) + `write_session_id` + `txn_id`=`tx.getTransactionId()`；**不盖 block_id**=运行期 T03）；⑥ 回 `ConnectorSinkPlan(TDataSink(MAXCOMPUTE_TABLE_SINK))`。**无运行期注入 hook**（legacy `MCInsertExecutor.beforeExec` dance 消失）。
- **`MaxComputeDorisConnector`**：+`getSettings()`（**D-3**：把 scan provider :146-162 的 EnvironmentSettings 构造上移到 `buildSettings()`，scan/write 共用——决定性证据=legacy catalog 持单 `settings` 同供 scan+write，抽出=忠实港）+`getWritePlanProvider()`（镜像 `getScanPlanProvider`）。
- **`MaxComputeScanPlanProvider`**：local settings 构造 → `connector.getSettings()`（删 `RestOptions`/`Credentials` import）。
- **`MaxComputeConnectorMetadata`**：`supportsInsert()`=true（**D-4**；`beginInsert`/`finishInsert`/`getWriteConfig` 留 throwing-default——MC sink 经 planWrite、commit 经 `ConnectorTransaction.commit()`，实际 executor 调用面待 Batch C；显式 doc 不静默）。
- **fe-core seam（D-2a）**：`PluginDrivenTableSink.bindViaWritePlanProvider` 改收 `Optional<InsertCommandContext>`、读 `isOverwrite()`+`getStaticPartitionSpec()` 填 handle；`PluginDrivenInsertCommandContext` +`staticPartitionSpec` map+getter/setter（**非基类**——`MCInsertCommandContext` 已自带 `staticPartitionSpec` 且 shadow 基类 `overwrite`，加基类成 override/shadow 缠结；plugin-driven seam 只见 `PluginDrivenInsertCommandContext`，hive/iceberg 迁后复用同类）。
- **写前 javap 核（坑10 全过）**：`TableWriteSessionBuilder.withMaxFieldSize(long)`/`.partition(PartitionSpec)`/`.overwrite(boolean)`/`.withDynamicPartitionOptions`/`.buildBatchWriteSession()`(throws IOException)、`DynamicPartitionOptions.createDefault()`、`PartitionSpec(String)`、`getId()`(via `Session`)。
- **决策/偏差**：[D-025]（5 决策）、[DV-012]（partition_columns 源）入 log；P4 计划 T04 ✅、PROGRESS、connectors/maxcompute 已 sync。

---

## 🚧 下一 session = Batch C 翻闸（**唯一 live 切点**；前置 = A+B 全绿 ✅ + R-004 防御测）

> **这是 dormant→live 的翻闸**。A+B 已达功能 parity（DDL/分区/写事务/写计划），翻闸瞬间 catalog→`PluginDrivenExternalCatalog`、table→`PluginDrivenExternalTable`，读/写/DDL/分区/show 全切 SPI。**翻前必跑 R-004**（ODPS SDK 在插件 classloader 下连通性防御测）。

**P4-T05（翻闸接线，mechanical 镜像 trino/es/jdbc）**：
1. `GsonUtils` `registerCompatibleSubtype`（catalog→PluginDriven ~:405 / table→PluginDriven ~:478）——image 兼容。
2. `PluginDrivenExternalTable.getEngine`/`getEngineTableTypeName` 加 `case "max_compute"`；`legacyLogTypeToCatalogType`（`MAX_COMPUTE`→`max_compute`，核连字符/下划线）。
3. 保留 `TableIf.MAX_COMPUTE_EXTERNAL_TABLE`/`InitCatalogLog.MAX_COMPUTE` 作 image 兼容。

**P4-T06（翻闸 + R-004，live cutover）**：
4. `CatalogFactory.SPI_READY_TYPES += "max_compute"`（:52）+ 删 `CatalogFactory` case（:146）。
5. **T03/T04 dormant→live 接线（关键，编入检查单）**：
   - **executor**：`BaseExternalTableInsertExecutor.beginTransaction()` 现调无参 no-op `begin()`；改 `writeOps.beginTransaction(session)`→`PluginDrivenTransactionManager.begin(connectorTx)`，**并把 connectorTx 置入 `ConnectorSessionImpl`**（加 `setCurrentTransaction`，现 `getCurrentTransaction` default 返 empty）——否则 T04 `planWrite` 的 `session.getCurrentTransaction()` 抛（已 fail-loud）。
   - **`GlobalExternalTransactionInfoMgr` 注册** connectorTx（BE→FE block-alloc 回调经 `getTxnById(txn_id)` 找事务；W3/W6 已泛化）。
   - **binding 期填 `PluginDrivenInsertCommandContext`**：`BindSink`/`UnboundTableSinkCreator`/`InsertOverwriteTableCommand`/`InsertIntoTableCommand` 对 plugin-driven MC 表设 `overwrite` + `setStaticPartitionSpec(...)`（现仅设 legacy `MCInsertCommandContext`；`InsertIntoTableCommand:598` 现传**空** `PluginDrivenInsertCommandContext`）——否则翻闸后 INSERT OVERWRITE / 静态分区不生效。
6. **R-004 防御测**：插件 harness 验 ODPS SDK classloader 连通；过方算翻闸完成。
7. **守门**（改 fe-core ⇒ `-pl :fe-connector-maxcompute,:fe-core -am`）：compile + checkstyle + import-gate；读真实 BUILD/MVN_EXIT/CS_EXIT。
8. **doc-sync + 独立 commit `[P4-T05]`/`[P4-T06]`**（用户定时机）。

> **Batch C 后**：D（清 ~19 反向引用 + 删 `datasource/maxcompute/` + 验 `MCInsertExecutor` 死代码 OQ-1）→ E（连接器测试基线 P4-T10 含 planWrite/write-txn golden + PR）。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **commit 协议红线**（沿用）：`TMCCommitData` 反序列化必 `TBinaryProtocol`（T03 已落，`CommitDataSerializer` 单点）。T04 sink/BE 契约不变。
2. **block_id 不在 planWrite**（运行期，T03 `allocateWriteBlockRange`）；`session_id`(field 1) 不用；`partition_columns`(14) 取 ODPS 表列（DV-012）；`max_write_batch_rows`(17, deprecated) 不盖。
3. **T03/T04 dormant 边界**：`commit()`(T03) 依赖 T04 `setWriteSession` 填槽（planWrite 现填）；`planWrite`(T04) 依赖 Batch C 把 connectorTx 置入 session（`getCurrentTransaction`）。**dormant 期均不跑**（max_compute 未进 `SPI_READY_TYPES`，executor 未接线）。
4. **OQ-2 = Approach A（已实现）**：planWrite 一处建 session+绑 txn+盖 txn_id/write_session_id。**别**给 `PluginDrivenTableSink` 加运行期 `setWriteContext` / executor 存 sink 引用 / beforeExec 注入（=否决的 Approach B）。
5. **import-gate**（坑5）：连接器禁 `org.apache.doris.(common|catalog|datasource|qe|analysis|nereids|planner)`；异常 `DorisConnectorException`；允许 `thrift.*`（`TMaxComputeTableSink`/`TDataSink`/`TDataSinkType`）+ `connector.api.*`。**fe-core seam 文件（`PluginDrivenTableSink`/`PluginDrivenInsertCommandContext`）在 fe-core，不受 import-gate**。
6. **maven 必绝对 `-f` + `-pl :artifactId`**（坑6）：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute,:fe-core -am ...`（改 fe-core **必带 `:fe-core`**，`-am` 不自动带）。裸名被当相对路径 → reactor not found。
7. **读真实 exit code**（坑7）：命令尾 `echo "MVN_EXIT=$?" >> log`，grep `BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|Tests run:` 核；**勿信**后台 task-notification 的「exit code」（=尾 echo 的、非 maven）。
8. **checkstyle**（坑8）：`CustomImportOrder`（`org.apache.doris.*` 字母序 → 第三方 com.aliyun/org.apache.logging → `java.*`，组间空行、组内字母序大小写敏感）；`UnusedImports`/`RedundantImport`；`LineLength` 120。
9. （沿用）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只扫 `*/src/main/java`、单向。
10. **ODPS API jar**（坑10）：写 session 类（`TableWriteSessionBuilder`/`TableBatchWriteSession`/`ArrowOptions`/`DynamicPartitionOptions`/`EnvironmentSettings`(`com.aliyun.odps.table.enviroment`,注意拼写)）在 **odps-sdk-table-api**；`PartitionSpec`/`TableIdentifier` 在 odps-sdk-commons。`maxcompute.version=0.53.2-public`。

---

## 📂 关键文件锚点

```
P4 计划：  tasks/P4-maxcompute-migration.md（Batch A✅ + B✅，下一 = Batch C 翻闸）
T04 设计： tasks/designs/P4-T04-write-plan-design.md（OQ-2 解法 A 六步 / 5 决策 / legacy 映射）
T03 设计： tasks/designs/P4-T03-write-txn-design.md（setWriteSession 槽 / dormant→live 边界）
写 RFC：   tasks/designs/connector-write-spi-rfc.md（§5.5 写-plan-provider / §6 data flow / §12 W5）

本场产物（T04，勿重做）：
  连接器： MaxComputeWritePlanProvider(新) · MaxComputeDorisConnector(+getSettings/+getWritePlanProvider/+buildSettings) ·
           MaxComputeScanPlanProvider(用 connector.getSettings) · MaxComputeConnectorMetadata(+supportsInsert)
  fe-core seam： planner/PluginDrivenTableSink.bindViaWritePlanProvider(收 insertCtx) ·
                nereids/.../insert/PluginDrivenInsertCommandContext(+staticPartitionSpec)

Batch C 翻闸靶（recon §5 / 本设计 dormant→live 接线）：
  CatalogFactory:52 SPI_READY_TYPES + 删 :146 case · GsonUtils registerCompatibleSubtype ·
  PluginDrivenExternalTable.getEngine + legacyLogTypeToCatalogType ·
  executor beginTransaction→begin(connectorTx) + ConnectorSessionImpl.setCurrentTransaction(新) ·
  GlobalExternalTransactionInfoMgr 注册 · BindSink/UnboundTableSinkCreator 填 PluginDrivenInsertCommandContext

legacy（港的源，勿改/Batch D 删）：
  datasource/maxcompute/MCTransaction.beginInsert · planner/MaxComputeTableSink.bindDataSink · nereids/.../insert/MCInsertExecutor

守门命令（Batch C 改 fe-core，必带 :fe-core）：
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute,:fe-core -am \
    -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -DskipTests compile   # 后台
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute,:fe-core \
    -Dmaven.build.cache.enabled=false checkstyle:check
  bash tools/check-connector-imports.sh                           # 从 repo 根跑
```

---

## 🔴 开放问题（Batch 入口前确认）

- **OQ-1**：翻闸后 `InsertIntoTableCommand`/`InsertOverwriteTableCommand` 是否完全不再经 `MCInsertExecutor`（→ cast `MCTransaction` 成死代码）？**T04 加剧旁路**（写计划亦入 planWrite）。**Batch D / P4-T08 验**。
- **OQ-3**：反向引用穷举（category-C 注册站 gson/enum/metacache 未穷举）——**Batch D / P4-T07 入口先完整 re-grep**。
- ~~**OQ-2**~~ **✅ 已解并实现（P4-T04，Approach A）**；binding 期静态分区/overwrite 填充归 Batch C/D（坑3）。
- ~~**OQ-4**~~ **✅ 已定（P4-T02）**：不建连接器自有 cache。

---

## 🧠 给下一个 agent 的 meta 建议

- **Batch C = 翻闸，唯一 live 切点**：A+B 全绿 ✅ 已达前置；**翻前必跑 R-004 防御测**。别先翻闸 / 别回头改 W-phase 或 T03/T04 决策。
- **dormant→live 接线是 Batch C 核心**（坑3 / 本文 P4-T06 第 5 点）：executor 置 connectorTx 进 session + GlobalExternalTransactionInfoMgr 注册 + binding 期填 `PluginDrivenInsertCommandContext`——三者缺一翻闸即断（planWrite `getCurrentTransaction` 已 fail-loud）。
- **改 fe-core ⇒ 守门 `-pl` 必带 `:fe-core`**（坑6）；读真实 BUILD/MVN_EXIT/CS_EXIT，勿信后台「exit code」通知（坑7）。
- **每批独立 commit**（用户定时机）。
