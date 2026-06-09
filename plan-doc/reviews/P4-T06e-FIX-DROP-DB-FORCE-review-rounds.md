# P4-T06e · FIX-DROP-DB-FORCE — review 轮次记录

> issue=`P2-5 FIX-DROP-DB-FORCE`（DG-3 / F22 / F27, major, regression）
> design=`plan-doc/tasks/designs/P4-T06e-FIX-DROP-DB-FORCE-design.md`
> 流程：设计（含对抗验证）→ 改 → 编译+UT+checkstyle+import-gate+mutation → 对抗 review → 收敛 → commit。
> 用户定方向：**扩 SPI `dropDatabase` 带 force**（cascade 下推连接器），非 FE 侧级联。

## 设计对抗验证（design workflow `weepgfhwu`，2 phase：design→verify）

verdict = **approve-with-nits**（0 mustFix，parityCorrect=true，blastRadiusComplete=true，testPlanRule9Compliant=true）。
- nit①（out-of-scope）：`PluginDrivenExternalCatalog.dropDb` 仅 catch `DorisConnectorException`；`structureHelper.listTableNames` 可能抛裸 `RuntimeException`（OdpsException 被包成 unchecked）逃逸未包成 DdlException。**pre-existing**——非 force 路径早经 `listTableNamesFromRemote` 调同一 `listTableNames`，legacy `dropDbImpl:143` 同样暴露。Rule 3 不扩范围。
- nit②（out-of-scope）：`dropDb` 把 **local** dbName 直传 SPI（不像 dropTable/createTable 远端解析）。pre-existing，与已发布的非-force 3 参路径完全一致。归 DG-3/DG-4 DB-DDL triage 批次。

## 实现

**改 5 文件**（设计逐字落地）：
1. SPI `ConnectorSchemaOps.java`：加 additive 4 参 `default void dropDatabase(session, db, ifExists, force)` 委托 3 参（零破坏其余 6 连接器；唯 MaxCompute override）。
2. 连接器 `MaxComputeConnectorMetadata.java`：3 参 override 折成 4 参，`if(force)` 时 `structureHelper.listTableNames` 枚举 + 逐 `dropTable(...,true)`（catch OdpsException→DorisConnectorException 包，fail-loud）再 `dropDb`，镜像 legacy `dropDbImpl:142-155`。
3. fe-core `PluginDrivenExternalCatalog.dropDb:351`：改调 4 参传 `force` + 更正 Javadoc（"force 不转发"→"force 转发、连接器级联"）。
4. 测试 `PluginDrivenExternalCatalogDdlRoutingTest.java`：3 处 3 参 stub→4 参（:139/:151/:167）+ 新增 `testDropDbForceForwardsForceTrueToConnector` / `testDropDbNonForceForwardsForceFalseToConnector`。
5. 新测 `MaxComputeConnectorMetadataDropDbTest.java`（连接器，hand-written recording fake McStructureHelper，无 mockito）：force 级联序 / 非-force 不级联 / 空库 / 中途失败 fail-loud 4 测。

**守门**：编译 api+maxcompute+fe-core `BUILD SUCCESS`；UT `MaxComputeConnectorMetadataDropDbTest` 4/4 + `PluginDrivenExternalCatalogDdlRoutingTest` 19/19；checkstyle 3 模块 0；import-gate 净；mutation 三向红：
- fe-core `force`→`false` ⇒ `testDropDbForceForwardsForceTrueToConnector` 红（Argument(s) are different，force=true vs false）。
- 连接器删 `if(force){...}` 块 ⇒ `forceTrueCascadesAllTablesBeforeDroppingSchema`（log=[dropDb:db1]）+ `forceTrueSurfacesRemoteDropFailure`（无异常抛）双红；非-force/空库测仍绿。
- 连接器 `dropTable(...,true)`→`false` ⇒ `forceTrueCascades...` 红（":false" markers）——见 Round 1 改进。

## Round 1（impl 对抗 review，workflow `wpszxgfau`，4 lens find → verify）

7+ raw findings，2 非-nit 入 verify：
- **REFUTED**：`listTableNames` 裸 RuntimeException 逃逸未包 DdlException ——仍 fail-loud（非 swallow，不违 Rule 12）、**非新增**（legacy 与已发布非-force 路径同样暴露）、pre-existing 已 triage（nit①）。Rule 3 不扩范围。
- **REAL（shouldConsider，已修）**：cascade 硬编码 `dropTable(...,true)`（idempotency-under-race，镜像 legacy `dropTableImpl(tbl,true)`），但 fake 丢弃 `ifExists` 实参、无断言钉住 → `true→false` mutation 可漏（4 测全绿）。Rule 9 真空隙。**处置**：fake 改记 `"dropTable:<name>:<ifExists>"`，`forceTrueCascades...` 断言期望 `:true`；重测 4/4 绿 + mutation `true→false` 现红（":false"）。

**收敛**：0 mustFix。Round 1 唯一 real 已修（test-quality）。

## 累计结论

- **根因**（DG-3）：翻闸后 `PluginDrivenExternalCatalog.dropDb` 拿到 `force` 却不转发（SPI 无 force 参），连接器 `dropDatabase` 仅 `schemas().delete()` 无表清理 → 非空库 DROP DB FORCE 退化为非-force（ODPS 不自级联，legacy 的枚举循环本身为证）。
- **修**：additive 4 参 `dropDatabase` SPI overload（零破坏）+ MaxCompute override cascade（连接器层枚举+逐表 drop 再删库，fail-loud）+ fe-core 转发 force。FE 级 bookkeeping 不变（单 logDropDb+unregisterDatabase = legacy db 级完整效果，无逐表 editlog）。
- **真值闸**：UT 全绿 + mutation 三向红。live e2e（真实 ODPS：非空库 FORCE 删成功、非-FORCE 删失败）CI 跳，为标准真值闸。
- **Batch-D 红线**：删 legacy `MaxComputeMetadataOps.dropDbImpl`（cascade 逻辑副本）须待本 fix 落（已落）。
- **doc-sync（随后续批次）**：DG-3 在 `P4-cutover-review-findings.md`/T06c §5「记 OQ/可接受」措辞更正、deviations/decisions-log 登记 4 参 overload。
