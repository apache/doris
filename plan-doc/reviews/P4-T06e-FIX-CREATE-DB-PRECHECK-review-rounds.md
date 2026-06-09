# P4-T06e · FIX-CREATE-DB-PRECHECK — review 轮次记录

> issue=`P2-6 FIX-CREATE-DB-PRECHECK`（DG-4 / F26 / F23, major, regression）
> design=`plan-doc/tasks/designs/P4-T06e-FIX-CREATE-DB-PRECHECK-design.md`（含 §决策更新-实现）
> 用户定方向（OQ-1）：**能力门闸** = 加 additive `supportsCreateDatabase()`，远端预检 gate 在其上，使 jdbc/es/trino 字节不变（非原推荐"接受+登记 deviation"）。

## 设计对抗验证（design workflow `weepgfhwu`）

verdict = **approve-with-nits**（0 mustFix，parityCorrect=true，blastRadiusComplete=true）。关键产出：**OQ-1**（jdbc/es/trino 同走 `createDb` override、有真 `databaseExists` 但不支持 `createDatabase`，预检会令其 `CREATE DB IF NOT EXISTS <远端已存在库>` 从"not supported"变静默 no-op）——已升给用户拍板 → 选**能力门闸**。另：doc-citation nit（行号小偏）、micro-cleanup nit（double getMetadata）。

## 实现（能力门闸）

**改 5 文件**：
1. SPI `ConnectorSchemaOps.java`：加 additive `default boolean supportsCreateDatabase(){return false;}`（零破坏其余 6 连接器）。
2. 连接器 `MaxComputeConnectorMetadata.java`：override `supportsCreateDatabase()→true`。
3. fe-core `PluginDrivenExternalCatalog.createDb`：hoist `ConnectorMetadata metadata` 局部（消 double getMetadata，addr micro-cleanup nit）；gated 远端预检 `if (ifNotExists && metadata.supportsCreateDatabase() && metadata.databaseExists(session,dbName)) return;`（`&&` 短路：能力位 false 时连远端都不查）+ 保留 FE-cache 快路径 + Javadoc 更正。镜像 legacy `createDbImpl:110-124`（查 FE-cache+远端、IFNE 已存在 no-op）。
4. 测试 `PluginDrivenExternalCatalogDdlRoutingTest.java`：+3 测（remote-exists+supports→no-op / remote-absent→建库 / lacks-support→bypass 落 createDatabase 且不查 databaseExists）。
5. 新测 `MaxComputeConnectorMetadataCapabilityTest.java`：钉 MaxCompute `supportsCreateDatabase()==true`（fe-core 测用 mock 故不覆盖真 override，此为唯一钉点）。

**非-IFNE+远端已存在错误文案**：保持现状（连接器/ODPS 抛 DdlException），不补 FE 侧 `ERR_DB_CREATE_EXISTS`——两者皆 fail-loud，仅文案/errno 差，pre-existing 且 out-of-scope（Rule 2/3，登记 deviation）。

**守门**：编译 api+maxcompute+fe-core 绿；UT RoutingTest 22/22 + CapabilityTest 1/1 + DropDbTest 4/4；checkstyle 0×3；import-gate 净；mutation 三向红：(a) 删预检行→测 1&2 红、测 3 绿；(b) 去 `supportsCreateDatabase() &&` gate→测 3 红（`never().databaseExists` 违反）；(c) 连接器 capability true→false→CapabilityTest 红。

## Round 1（impl 对抗 review，workflow `wsrg9cwne`，4 lens）

5 finding 全 **nit**（0 mustFix/0 shouldConsider）：
- ✅（正面）"Cross-connector byte-identical claim VERIFIED — jdbc/es/trino 无行为变化"——关键风险经独立核码确认 clean。
- nit：非-IFNE+远端已存在 错误文案/SQLSTATE 异于 legacy（×2 lens 命中，pre-existing+out-of-scope，已记）。
- nit：无测显式钉 `&&` 求值序 BEFORE databaseExists（仅由 unsupported-connector case 推断）——测 3 `never().databaseExists` 实已推断性钉住，borderline，不改。
- nit（**已修**）：测 3 WHY 注释 + 设计 doc 误述 gate-removal mutation 的红因机制（实测 mutB 红在 `never().databaseExists` 断言、非 createDatabase）。**处置**：更正测 3 注释 + 设计 doc Test Plan MUTATION (b) 为准确机制（comment-only，无行为变更）。

**收敛**：0 mustFix。唯一可操作 nit（注释精度）已修。

## 累计结论

- **根因**（DG-4）：`createDb:314` 仅查 FE-cache，FE-cache miss+远端已存在时 `CREATE DB IF NOT EXISTS` 穿透到 ODPS `schemas().create()` 抛 "already exists"，违 IFNE 语义（legacy `createDbImpl` 同查 FE-cache+远端 `databaseExist`）。
- **修**：additive SPI `supportsCreateDatabase()`（default false）+ MaxCompute override true + fe-core gated 远端预检。**jdbc/es/trino 字节不变**（能力位 false → `&&` 短路，仍走 createDatabase 抛 "not supported"，连远端都不查）——R6 行为变化经能力门闸消除，无需 deviation。
- **真值闸**：UT 全绿 + mutation 三向红。live e2e（远端预建 schema、本 FE cache miss、CREATE DB IF NOT EXISTS 应静默成功）CI 跳。
- **doc-sync（随后续）**：DDL-C4 重开登记、task-list「6/6 完成」措辞更正、deviations-log 登记非-IFNE 文案偏差 + 能力门闸决策。
