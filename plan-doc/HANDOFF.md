# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [tasks/P4](./tasks/P4-maxcompute-migration.md) → **[Batch D 移除设计](./tasks/designs/P4-batchD-maxcompute-removal-design.md)（✅ 已 verify，turnkey 执行源——前置门=用户 live 验证）** → [P4-T05/T06 翻闸设计](./tasks/designs/P4-T05-T06-cutover-design.md)（已落）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-07（**T06b flip + doc-sync 已 commit（2 commit）+ gate 全绿**；Batch C 翻闸完成。下一 session = **用户跑 live 验证 → 再做 Batch D 删除**）
- **本 session 主题**：① **T06b 翻闸**（`CatalogFactory` `SPI_READY_TYPES += "max_compute"` + 删 legacy case + import + 注释）；② 用户追加「fe-core 不再依赖任何 maxcompute jar」→ **并行 re-grep + 对抗验证 recon**（OQ-3 入口门满足）产出 **Batch D 完整移除闭包**（21 删 / ~30 清·84 ref / keep 集 / pom drop）写成 turnkey 设计文档；③ doc-sync 5 步 + decisions-log [D-027] + RFC §20 E11（2 SPI 新增）。
- **分支**：`catalog-spi-05`。**本 session 2 commit 已落**：`9f2dba9ad24` `[docs]` Batch D 设计 + doc-sync + [D-027] / `2b135899411` `[P4-T06b]` flip（仅 `CatalogFactory.java`，isolated/易 revert）。前继 T05（`2534d76`+`67e0e5a`）、T06a（`afa46759`）已 commit。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`（勿提交）。
- **Batch 状态**：A ✅ + B ✅ + **C ✅（T05+T06a+T06b flip，gate 全绿）**；**D ⏳（闭包已 verify，执行待 live 验证）**；E ⏳。

---

## ✅ 本 session 完成项

**① T06b 翻闸 — 已 commit（`2b135899411`）、gate 全绿**：`CatalogFactory.java`：`SPI_READY_TYPES = ImmutableSet.of("jdbc","es","trino-connector","max_compute")`(:52)；删 `case "max_compute"`(原 :146-149)；删 unused `import ...maxcompute.MaxComputeExternalCatalog`(:30)；注释 `(hms, iceberg, paimon, hudi)` 去 max_compute。翻闸后 `max_compute` catalog→`PluginDrivenExternalCatalog`/table→`PluginDrivenExternalTable`（GSON T05 兼容），读/写/DDL/分区/show 全经 SPI；legacy `instanceof MaxCompute*` 全失配（dead，留 Batch D 删）。**gate 全绿（真实 EXIT 核，坑7）**：compile BUILD SUCCESS/MVN_EXIT=0（`-pl :fe-core -am`）+ checkstyle **0**/CS_EXIT=0 + import-gate 0。

**② Batch D 移除 recon + turnkey 设计**：`tasks/designs/P4-batchD-maxcompute-removal-design.md`。**去 fe-core odps 依赖 = 整个 Batch D**（fe-core `odps-sdk-core`/`odps-sdk-table-api` 仅经 legacy 子系统可达，7 文件 `import com.aliyun.odps` 全在删除集）。闭包（并行 re-grep + 对抗验证，**无 survivor risk**）：**删 21 文件**（`datasource/maxcompute/` 10 + `MaxComputeTableSink`/`Logical`/`PhysicalMaxComputeTableSink`/`UnboundMaxComputeTableSink`/`MCInsertExecutor`/`MCInsertCommandContext`/`Logical…ToPhysical…Rule`/`MCTransactionManager` + 2 测）；**清 ~30 文件 / 84 ref**（32 import + 43 dead branch）；**keep 集**（image/plan/thrift compat，见设计 §3）；**pom drop** 两块。对抗复核 1 必处理点：`ExternalMetaCacheMgr` **ctor 期 eager** 建 `MaxComputeExternalMetaCache`(:183/:310)——须删引用（非 dead-strip）。镜像 trino `524097e38d3`+`c4ac2c5911d`。

**③ doc-sync 5 步 + 决策 + RFC**：PROGRESS（§一/§二看板 SPI_READY ✅/§三）、tasks/P4（元信息/T06✅/T07-09 注/验收 SPI_READY ✅/阶段日志/阻塞项）、connectors/maxcompute（状态 75%/playbook 8-9 ✅/进度日志）、decisions-log **[D-027]**（翻闸 + 2 决策）、01-spi-rfc §20 E11（2 SPI 新增 `setCurrentTransaction`/`usesConnectorTransaction`，D-026 预授）。

---

## 🚧 下一 session = 用户 live 验证 → Batch D 删除

> **翻闸完成门（[D-027] D-1）= 用户跑** `OdpsLiveConnectivityTest`（设 4 个 `MC_*` 环境变量：`MC_ENDPOINT`/`MC_PROJECT`/`MC_ACCESS_KEY`/`MC_SECRET_KEY`）+ **手测 smoke**（SELECT / CREATE·DROP TABLE+DB / SHOW PARTITIONS / partitions·partition_values TVF / INSERT / INSERT OVERWRITE [PARTITION]）。**绿后**才执行 Batch D（删 legacy 即去掉易回退 fallback，故 flip 在 live 验证前保持独立可 revert）。

**Batch D 执行（turnkey，详见 [Batch D 移除设计](./tasks/designs/P4-batchD-maxcompute-removal-design.md) §5）**：
1. T07+T08+T09 **一个 compiling 单元**：应用 §2 全部 edits（import + dead branch）**并**删 §1 的 21 文件；keep 集（§3）勿动。
2. trim §2 测试（每个先对 keep 集复核——多处 "MaxCompute" 测试引用是 **plugin** 路径=保留）。
3. gate：`-pl :fe-core -am ... test-compile` + checkstyle + import-gate（真实 EXIT，坑7）。
4. **grep-empty 验收**：`com.aliyun.odps` 在 fe-core/src **空**；`MaxComputeExternal`/`MCTransaction`/`MCInsert` 仅剩 keep 集（enum/gson/thrift/plugin）。
5. commit `[P4-T07/T08/T09]`。
6. **pom drop**：删 `fe-core/pom.xml` 两 `odps-sdk-*` 块（~:362-381）→ test-compile BUILD SUCCESS + `mvn -pl :fe-core dependency:tree | grep odps`（仅剩 transitive odps-sdk-core via fe-common）。commit `[P4-T09]`。
7. doc-sync 5 步 + grep-empty 证据入阶段日志。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **去 fe-core odps 依赖 ≠ 小改**：= 整个 Batch D（21 删 + ~30 清）。两 `odps-sdk-*` 块仅经 legacy 子系统可达；不删 legacy 删不掉 jar（Java 不 dead-strip 源引用，每个 `instanceof MaxCompute*` 都引类符号）。
2. **fe-common 留 odps**（用户决定 Direct-only）：fe-core 删后仍 transitive 见 `odps-sdk-core`（fe-common 供 `MCUtils` → 连接器 + be-java-extensions）。`DatasourcePrintableMap` 只 import `MCProperties`（无 odps），不阻塞。
3. **keep 集勿删**（image/plan/thrift compat）：`TableType.MAX_COMPUTE_EXTERNAL_TABLE` · `InitCatalogLog/InitDatabaseLog.Type.MAX_COMPUTE` · `TransactionType.MAXCOMPUTE` · `TableFormatType.MAX_COMPUTE` · GSON 3 string 字面量 · nereids `PlanType`/`RuleType` MC 值 · `FrontendServiceImpl.getMaxComputeBlockIdRange`+`TMaxComputeBlockId*`（**plugin** 写路径 BE→FE block-alloc RPC，非 legacy）· `PluginDrivenTransactionManager` · `PluginDrivenExternalTable` max_compute case（T05）· `legacyLogTypeToCatalogType` 默认分支（无 MC case）。
4. **OQ-1 已 verify**：`MCInsertExecutor` 仅经 dead `instanceof UnboundMaxComputeTableSink`/`PhysicalMaxComputeTableSink` 门建 → 翻闸后死码；删前 grep-empty 步再确认。
5. **commit 协议红线**（沿用）：`TMCCommitData` 反序列化必 `TBinaryProtocol`（连接器 `MaxComputeConnectorTransaction.addCommitData` 已落）。
6. **maven 必绝对 `-f` + `-pl :artifactId`**（坑6）：改 fe-core 带 `:fe-core`（`-am` 不自动带上游 artifact，但本地 repo 须有上游 install；保险用 `-am`）。Batch D 只动 fe-core（不动 SPI）→ 无须 `:fe-connector-api`。
7. **读真实 exit code**（坑7）：命令尾 `echo "MVN_EXIT=$?" >> log`，grep `BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|CS_EXIT|Checkstyle.*violation`；**勿信**后台 task-notification 的「exit code」。
8. **checkstyle**（坑8）：`CustomImportOrder`（doris.* 字母序→第三方→java.*，组间空行、组内大小写敏感）；`UnusedImports`/`RedundantImport`；`LineLength` 120；`fe/pom.xml:162` 含 test 源。
9. （沿用）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只扫 `*/src/main/java`。
10. **R-004 两分**：① classloader 隔离（无 creds，CI，T06a 已落 `OdpsClassloaderIsolationTest`）；② live 连通（creds，**用户跑** `OdpsLiveConnectivityTest`）= 翻闸完成门。仓内无 ODPS creds/harness。

---

## 📂 关键文件锚点

```
设计（执行源）：tasks/designs/P4-batchD-maxcompute-removal-design.md（21 删 / 84 ref 闭包 / keep / pom / ordered TODO；前置门=live 验证）
            tasks/designs/P4-T05-T06-cutover-design.md（Batch C，已落）

T06b 已落靶： CatalogFactory.java:52(SPI_READY_TYPES) / 删原 :146-149 case / 删 :30 import
Batch D 靶（待 live 后）：见 Batch D 设计 §1（删 21）+ §2（清 ~30）+ §4（pom 两 odps-sdk-* 块 ~:362-381）
keep（勿删）： 见 §坑3 + Batch D 设计 §3

守门命令（Batch D 改 fe-core，必带 :fe-core；不动 SPI）：
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-core -am \
    -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -DskipTests test-compile   # 后台
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-core \
    -Dmaven.build.cache.enabled=false checkstyle:check
  bash tools/check-connector-imports.sh                           # 从 repo 根跑
  grep -rn "com.aliyun.odps" fe/fe-core/src                        # Batch D 后须空
```

---

## 🔴 开放问题（沿用）

- **OQ-1**：翻闸后 INSERT/INSERT OVERWRITE 不再经 `MCInsertExecutor` → **已 verify**（仅 dead instanceof 门建）；Batch D grep-empty 步终确认。
- **OQ-3**：反向引用穷举 → **已满足**（本 session 并行 re-grep + 对抗验证，84 ref 闭包入 Batch D 设计）。
- **R-004 part-2**：live 连通（creds，**用户跑** `OdpsLiveConnectivityTest`）= 翻闸完成门 + Batch D 前置门。
- ~~OQ-2~~ ✅（P4-T04 Approach A）；~~OQ-4~~ ✅（P4-T02 不建自有 cache）。

---

## 🧠 给下一个 agent 的 meta 建议

- **Batch D 勿重 recon**：闭包已 verify 入设计文档（OQ-3 满足）。直接照 §5 ordered TODO 执行——但**前置门=用户报 live 验证绿**，未绿勿删 legacy。
- **删前必跑 grep-empty**（坑4/OQ-1 终确认）；**keep 集逐项核**（多处 "MaxCompute" 是 plugin 路径=保留，尤其测试 + `getMaxComputeBlockIdRange` thrift）。
- **改 fe-core ⇒ `-pl :fe-core`**（坑6）；读真实 BUILD/MVN_EXIT/CS_EXIT（坑7）。
- **每 commit 独立**（用户定时机）。
