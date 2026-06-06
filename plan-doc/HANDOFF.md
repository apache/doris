# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [写-SPI RFC](./tasks/designs/connector-write-spi-rfc.md) / [recon](./research/connector-write-spi-recon.md) / [maxcompute recon](./research/p4-maxcompute-migration-recon.md)。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-06（**实现 session ③**）
- **本 session 主题**：**W-phase W4 + W5 + W7 落地 = W-phase（W1–W7）全完成**。W4=`PluginDrivenTransaction` 桥接委派 SPI `ConnectorTransaction`；W5=把 W1 写-plan SPI **layer 进既有 plugin-driven 写路径**（**非**新建、**非** route 3 个 concrete sink——见下 🔴 [DV-009]）；W7=补 D-021/D-022 + 01-spi-rfc §20 E11 + 同步文档。**behind gate、零行为变更、golden/TDD、三 commit 独立、gate 全绿**（真实 exit code 核验）。
- **分支**：`catalog-spi-05`。**W-phase commits**：W1+W2 `be945476ba7` · W3+W6 `9ad2bbe40ec` · **W4 `759cc0874c8`** · **W5 `9ebe5e27fa4`** · **W7 = 本次 docs commit**（含本 HANDOFF）。工作树：源码已全提交；未跟踪 `.audit-scratch/`（本场 `w4-*.log`/`w5-*.log`）/`conf.cmy/`/`regression-conf.groovy.bak`（沿用，非本场，勿提交）。

---

## ✅ 本 session 完成项（behind gate，零行为变更）

| 项 | commit | 结果 | 文件 |
|---|---|---|---|
| **W4** `PluginDrivenTransaction` 委派 4 写回调 | `759cc0874c8` | TDD RED 3F1E→GREEN 5/5；checkstyle 0；import-gate | `transaction/PluginDrivenTransactionManager.java` + **新** `transaction/PluginDrivenTransactionManagerTest.java` |
| **W5** 写-plan-provider layer 进 plugin-driven 写路径 | `9ebe5e27fa4` | TDD RED(缺 ctor 编译失败)→GREEN 1/1；checkstyle 0；import-gate | `planner/PluginDrivenTableSink.java` · `nereids/glue/translator/PhysicalPlanTranslator.java` + **新** `planner/PluginDrivenTableSinkTest.java` |
| **W7** 文档 | 本次 docs commit | — | `decisions-log.md`(D-021/D-022) · `deviations-log.md`(DV-009 + 修 stale 索引) · `01-spi-extensions-rfc.md`(§20 E11 + §3 矩阵行) · `PROGRESS.md` · `connectors/maxcompute.md` · 本 HANDOFF |

**W4 细节**：`PluginDrivenTransaction`（`PluginDrivenTransactionManager.java` 内类）override 4 个 W2 加在 fe-core `Transaction` 的 default，委派给 wrap 的 SPI `ConnectorTransaction`：`addCommitData`(null→no-op) / `supportsWriteBlockAllocation`(null→false) / `allocateWriteBlockRange`(null→throws UOE；**保 `throws UserException` 对齐接口**，SPI 调用 unchecked) / `getUpdateCnt`(null→0)。legacy null marker（jdbc/es auto-commit）保持 inert。**dormant 至连接器从 `ConnectorWriteOps.beginTransaction` 返真 `ConnectorTransaction`（P4 adopter）**；那时才 live，否则真连接器 txn 会被 no-op default 静默吞掉 commit 数据。

---

## 🔴 关键修正（**与上 handoff / RFC §5.5·§12 W5 字面冲突，务必读**）= [DV-009]

上 handoff / 写 RFC 的 W5 措辞「**新建** fe-core `PluginDrivenTableSink` + route 3 个 `visitPhysicalXxxTableSink`(hive/iceberg/mc) → `planWrite`，保 PhysicalXxxSink fallback」**与代码不符**（写于不知既有路径之时）。实测（2 路 Explore recon + 主线核读）：

1. **`PluginDrivenTableSink` 已存在**（`planner/PluginDrivenTableSink.java`，P0/P1 JDBC 期建），**非新建**。
2. **plugin-driven 写 INSERT 走专路**：`UnboundConnectorTableSink → LogicalConnectorTableSink → PhysicalConnectorTableSink → PhysicalPlanTranslator.visitPhysicalConnectorTableSink`（`:644`），已据 `ConnectorWriteConfig` 建 `PluginDrivenTableSink`。那 3 个 `visitPhysicalXxxTableSink` 服务 **legacy（非 plugin-driven）** hive/iceberg/mc 表；mc/iceberg/hive 迁 plugin-driven 后走专路 → **在 3 个 concrete 方法加 planWrite 路由是死代码**。
3. **两写-sink 模型并存**（W5 调和）：
   - **config-bag**（既有、jdbc live）：连接器返 `ConnectorWriteConfig` 属性包 → fe-core `PluginDrivenTableSink.bindDataSink` 建 `THiveTableSink`/`TJdbcTableSink`。**表达不了 mc/iceberg**（无 schema_json/sort/write_session 等）。
   - **opaque-sink**（W1 新、W5 接线）：`ConnectorWritePlanProvider.planWrite() → ConnectorSinkPlan(TDataSink)`，连接器**自建** `T*TableSink`，可泛化。
4. **W5 实际做法**（用户 AskUserQuestion 签字「Corrected W5 (layer planWrite)」）：`visitPhysicalConnectorTableSink` 在 `connector.getWritePlanProvider()!=null` 时构 plan-provider-mode `PluginDrivenTableSink`；`bindDataSink` 走 `planWrite()` 取 opaque `TDataSink`；否则既有 config-bag 路径**逐字不变**（jdbc 不受影响）。**零行为变更**：无连接器 override `getWritePlanProvider`（已 grep 证），新分支 dormant。
> `ConnectorWriteHandle`(={tableHandle,columns,overwrite,writeContext}) / `ConnectorSinkPlan`(包 TDataSink) 形状经使用确认充分，W5 未改。overwrite/静态分区/writePath 等 write context 的 handle 填充**留 P4 adopter**（base `InsertCommandContext` 是空 marker，无通用 overwrite；强行 instanceof 子类会再耦合 fe-core）——W5 仅建 seam（空 context）。

---

## 🚧 下一 session = P4 maxcompute adopter（**首个 full 迁移 + 翻闸**）

> W-phase 已把**共享写接线 seam** 备好（W4 事务桥 + W5 写-plan-provider）；adopter 只需 impl + 搬类 + 翻闸。路线见 [写 RFC §12「P4 maxcompute」节](./tasks/designs/connector-write-spi-rfc.md) + [maxcompute recon](./research/p4-maxcompute-migration-recon.md)。

第一步（建议）：读 `maxcompute recon §5` + `fe-connector-maxcompute` 现状，再定批次。核心动作：
1. **搬类** `datasource/maxcompute/`（`MCTransaction`/`MaxComputeMetadataOps`/MetaCache/SchemaCacheValue/ScanNode 等）→ `fe-connector-maxcompute`。
2. **impl 写 SPI**：`ConnectorWriteOps`(insert) + `ConnectorTransaction`(over `addCommitData` 反序列化 `TMCCommitData` + `supportsWriteBlockAllocation`/`allocateWriteBlockRange` 真分配 block-id) + `ConnectorWritePlanProvider`(产 `TMaxComputeTableSink`，含 W5 留的 write context 填充：runtime `txn_id`/`write_session_id` 经 `MCInsertExecutor.beforeExec` 注入路径需在 plugin-driven 侧重建)。
3. **翻闸** `CatalogFactory.SPI_READY_TYPES += "max_compute"` + 删 `CatalogFactory` case + GSON `gsonPostProcess` 加 `max_compute→plugin` + `getEngine` 分支。
4. **清 ~12 反向 instanceof**（`PhysicalPlanTranslator`/`ShowPartitionsCommand`/`PartitionsTableValuedFunction` 等）。
5. **删** `datasource/maxcompute/`；`McStructureHelper` 去重（删 fe-core 副本，P1-T02）。
6. **测试基线**（仿 hudi 5 文件，JUnit5 手写替身）。

**别越界**：W-phase 已完，不要回头改 W1–W7；P4 是 adopter（搬类 + 翻闸 + 删 legacy），这才是动 `SPI_READY_TYPES` 的阶段。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **两写-sink 模型**（见上 🔴 [DV-009]）：config-bag（jdbc）⊥ opaque-sink（W1/W5，mc/iceberg 用）。adopter 走 opaque-sink（impl `getWritePlanProvider`）。
2. **maven 必用绝对 `-f`**：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl <m> ...`。勿在命令里 `cd` 子目录（破相对 `-f`，假失败）。
3. **读真实 exit code，非后台通知**：后台 task-notification 的 "exit code 0" 是末尾 `echo` 的，**非 maven 的**。命令尾 `echo "MVN_EXIT=$?" >> log`，再 `grep -E "BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|CS_EXIT|Checkstyle violations|Tests run:"` 日志核。
4. **`getTxnById` 抛异常非返 null**（`GlobalExternalTransactionInfoMgr:30`）——任何复用 `getTxnById` 的改动都要 guard 在 "任一 commit 字段 set" 内（W3 已修；adopter 注意）。
5. **序列化协议契约（golden 红线）**：commit 载荷 `TBinaryProtocol`，单点 `CommitDataSerializer`。adopter 的 `ConnectorTransaction.addCommitData` 反序列化必须同协议，否则 `CommitDataSerializerTest` 红。
6. **checkstyle 规则**：`CustomImportOrder` = `org.apache.doris.*`（按包名字母序）→ 第三方（`com.*`/`org.apache.thrift.*`/`org.junit.*`）→ `java.*`，组间空行、组内字母序（大小写敏感）；`UnusedImports`/`RedundantImport` 开；`LineLength` max **120**；无 IllegalThrows/IllegalCatch。
7.（沿用）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只禁 connector→fe-core 单向、只扫 main。fe-core→fe-connector-api（SPI）是**允许**方向。
8. **跑 W 式定向测**：`mvn -f …/fe/pom.xml -pl fe-core -am -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -Dtest=类A,类B -DfailIfNoTests=false test`（慢，后台）；checkstyle 单独 `-pl fe-core ... checkstyle:check`。

---

## 📂 关键文件锚点

```
RFC：     tasks/designs/connector-write-spi-rfc.md（§5 API / §8 fe-core 改动表 / §12 W1→W7 + P4）
recon：   research/connector-write-spi-recon.md · research/p4-maxcompute-migration-recon.md
W1(be94547)：fe-connector-api/.../connector/api/ handle/{ConnectorTransaction,ConnectorWriteHandle} ·
            write/{ConnectorWritePlanProvider,ConnectorSinkPlan} · Connector.getWritePlanProvider
W2(be94547)：fe-core/.../transaction/Transaction.java · datasource/{maxcompute/MCTransaction,
            hive/HMSTransaction,iceberg/IcebergTransaction}（各 addCommitData = TBinaryProtocol 反序列化）
W3+W6(9ad2bbe)：qe/Coordinator · qe/runtime/LoadProcessor · service/FrontendServiceImpl
            + transaction/CommitDataSerializer + 2 测
W4(759cc08)：transaction/PluginDrivenTransactionManager.java（PluginDrivenTransaction 内类 override 4 写回调）
            + transaction/PluginDrivenTransactionManagerTest.java
W5(9ebe5e2)：planner/PluginDrivenTableSink.java（plan-provider mode + nested ConnectorWriteHandle impl）
            · nereids/glue/translator/PhysicalPlanTranslator.java:644（visitPhysicalConnectorTableSink 加 planWrite 分支）
            + planner/PluginDrivenTableSinkTest.java
既有写路径：nereids/analyzer/UnboundConnectorTableSink → trees/plans/logical/LogicalConnectorTableSink
            → trees/plans/physical/PhysicalConnectorTableSink → visitPhysicalConnectorTableSink（plugin-driven 写专路）
P4(待做)：datasource/maxcompute/ → fe-connector-maxcompute；CatalogFactory.SPI_READY_TYPES 翻闸
不动：     W1–W7 已完；datasource/{hive,iceberg} legacy；RewriteDataFileExecutor(procedure,P6)
守门命令：
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
    -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true \
    -Dtest=<类> -DfailIfNoTests=false test
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core \
    -Dmaven.build.cache.enabled=false checkstyle:check
  bash tools/check-connector-imports.sh                           # 从 repo 根跑
```

---

## 🧠 给下一个 agent 的 meta 建议

- **W-phase 已全完成**（W1–W7，5 commit）。下一步是 **P4 maxcompute adopter**——这才是翻闸（动 `SPI_READY_TYPES`）+ 搬类 + 删 legacy 的阶段。
- **写接线已就位**：adopter impl `getWritePlanProvider()`（产 `TMaxComputeTableSink`）即走 W5 的 opaque-sink seam；impl `ConnectorTransaction`（over W4 委派的 4 写回调）即走 W4 的事务桥。W5 留的 write context（overwrite/分区/runtime txn_id+write_session_id）填充是 adopter 的活。
- **守门循环**：compile（慢，后台）+ checkstyle（绝对 -f）+ import-gate；**读真实 BUILD/MVN_EXIT/CS_EXIT 行**，勿信后台 "exit code" 通知（坑 3）。
- **提交**：W4/W5/W7 已独立 commit（用户选「commit each W independently」）。P4 adopter 按批次独立 commit，由用户决定时机。
- Maven：cwd 无关（绝对 `-f`）；`-pl <module> -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`；checkstyle 单独跑。
