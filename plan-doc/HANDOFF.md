# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🔥 2026-06-09 — P5 paimon recon + 设计完成；下一步 = 分批实现（B0 起）

> **本 session**：用户要求对 paimon 5 功能区（普通表读取 / 系统表读取 / procedure / DDL / mtmv）在旧框架的实现做全面 code-grounded 分析，映射到新 catalog SPI 框架设计，并对齐 maxcompute 连接器接口一致性。**本 session 仅调研 + 设计，0 产线代码。**

## ✅ 本 session 已完成

- **14-agent workflow recon + cross-cut 对抗复审**（5 区 fe-core 旧实现 + 新 SPI 面 + MC 样板 + 连接器现状 → 5 区 old→new 设计 → 跨切面 critic），主线 firsthand 核 4 个 load-bearing 锚点（SPI_READY_TYPES / GSON 7 注册 / PluginDrivenExternalTable 无 MTMV / ConnectorPartitionInfo.lastModifiedMillis 已存在）。
- **产物 1**：[`research/p5-paimon-migration-recon.md`](./research/p5-paimon-migration-recon.md) —— 5 功能区旧实现 + E1–E10 SPI 状态 + 跨切面风险 + **MC 一致性 11 约定** + 测试基线 + 沿用坑。
- **产物 2**：[`tasks/P5-paimon-migration.md`](./tasks/P5-paimon-migration.md) —— old→new 映射表 + **30 TODO 分 B0–B9 批** + 批次依赖图 + 验收标准 + 开放决策（已签字）。
- **doc 同步**：`connectors/paimon.md`（修 3 stale 表述）、`decisions-log.md`（+D-037/D-038，计数 36→38）、`PROGRESS.md`（header/§一/§二/§三/§四/§六/§七）、本 HANDOFF（覆盖）、auto-memory `catalog-spi-p5-paimon-design`。

## 👤 用户签字决策（2026-06-09）

- **D-037（P5-D1，flavor 模型）= A 单 Catalog + flavor switch**：6 flavor(hms/filesystem/dlf/rest/jdbc) 在 `PaimonConnector.createCatalog` 内 switch on `paimon.catalog.type`，拷 warehouse/conf/S3-normalize + 重建 Hadoop·HiveConf + **每-flavor ExecutionAuthenticator** 入模块（MC 一致）。**不**建 backend 模块（5 个 `fe-connector-paimon-backend-*` 是空壳）。
- **D-038（P5-D2，MTMV/MVCC scope）= A P5 内实现桥**：fe-core 新建 `PaimonPluginDrivenExternalTable extends PluginDrivenExternalTable` 实现 MTMVRelatedTableIf/MTMVBaseTableIf/MvccTable + 首个真 E5 消费者 override `beginQuerySnapshot` 三方法。**翻闸(B7) gated on MTMV 桥(B5)**；**禁**静默读 latest 回归。

## 🧠 核心发现（5 区 + 证伪 3 先验，连接器档原 stale）

1. **普通表读取**：最接近 MC 样板，scan 骨架近完工。补缺=transient-Table reload BLOCKER（`PaimonTableHandle:41/73` + `PaimonScanPlanProvider:95` 无 fallback）、session-TZ 谓词 bug（`PaimonPredicateConverter:284` 固定 UTC）、`listPartitions*`、6-arg planScan。
2. **系统表读取**：须**新建 E7 SPI hook**（greenfield，paimon 首个消费者）+ 通用 `PluginDrivenSysExternalTable`（**必须报 PLUGIN_EXTERNAL_TABLE**，否则路由到将删的 legacy 节点）；binlog/audit_log 须按 sysName 强制 JNI（是 DataTable 走 native = 行错且静默）。
3. **procedure**：**零可迁，doc-only no-op**（fe-core 无 paimon procedure；`expire_snapshots`=iceberg、`CALL paimon.sys.migrate_table`=Spark 两假阳性）。
4. **DDL**：迁 `PaimonMetadataOps`→`PaimonConnectorMetadata`（连接器远端 + `PluginDrivenExternalCatalog` override edit-log）+ flavor 装配（D-037）；**每-flavor authenticator 必须保**（否则 Kerberos DDL 炸）；`DorisToPaimonTypeVisitor`→`PaimonTypeMapping` 反向（保留 legacy gap）。
5. **mtmv**：SPI **MTMV 完全无面（E10 缺）** + paimon 首个真 E5 消费者；MTMV 类型留 fe-core 子类、SPI-neutral 数据经 E5 snapshotId + `ConnectorPartitionInfo.getLastModifiedMillis()`（已存在）。最高 correctness 风险=**单-pin 不变式 + GAP-LISTPART-AT-SNAPSHOT**（at-snapshot 列分区）。
- **证伪 3 先验**：① backend 模块=空壳（非已建工厂，连接器走单 Catalog stub）；② FE 分发部分已预接（DROP/CREATE·DROP DB/SHOW PARTITIONS/TVF，残留=连接器 listPartitions）；③ Base64 非 blocker（BE `PaimonUtils:42-47` 有 STD fallback；真风险=pin paimon-core 三方版本对齐）。

## 🎯 下一 session = P5 分批实现（B0 起）

- **B0**（无前置）：建 `fe-connector-paimon` 测试模块 + no-mockito 注入式 `PaimonCatalogOps` seam + parity baseline（vs 旧 `PaimonScanNode`）+ FE→BE round-trip smoke + **pin paimon-core 版本三方对齐**。
- **B6**（独立）：procedure doc no-op。
- 续 **B1**（单 Catalog flavor 装配 + 每-flavor authenticator）→ **B2**（普通读补完）+ **B3**（DDL）→ **B4**（E7 sys-table + E5 MVCC）→ **B5**（MTMV 桥）→ **B7 翻闸**（gated on B2+B3+B4+B5 + live e2e）→ **B8 删 legacy** → **B9 回归**。批次依赖图见 [tasks/P5](./tasks/P5-paimon-migration.md)。

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false`；改连接器 `:fe-connector-paimon`、改 SPI `:fe-connector-api`（**须 -am 连带 rebuild**）、改 fe-core `:fe-core`。读真实 `Tests run:`/`BUILD`，勿信后台 echo exit（[[doris-build-verify-gotchas]]）。
- 连接器禁 import fe-core（import-gate `bash tools/check-connector-imports.sh`）；session 值经 session-property 透传（[[catalog-spi-connector-session-tz-gotcha]]）。连接器测试无 mockito（纯 seam / child-first loader，[[catalog-spi-fe-core-test-infra]]）；checkstyle 含 test 源、test 阶段不跑（单独 `checkstyle:check`）。
- 翻闸 GSON **7 注册原子齐迁**（5 catalog + db + table，比 MC 多，[[catalog-spi-gson-migrate-all-three]] / [[catalog-spi-cutover-fe-dispatch-gap]]）；删 legacy 后验 paimon-core FE classpath 恰一份（[[catalog-spi-be-java-ext-shared-classpath]]）。
- 分支 `branch-catalog-spi`（HEAD `e96037cf6aa` #64300）；建议 off 最新 upstream 起新分支。未跟踪 scratch（`.audit-scratch/`/`conf.cmy/`/`*.bak`/`.claude/scheduled_tasks.lock`）勿提交。

## 🧠 给下一个 agent 的 meta

- **D-037/D-038 已签字** —— 直接按设计 doc 的 B0–B9 落地，无须重开 scope 讨论。
- **live e2e（真实 paimon 各 flavor 环境）仍是翻闸真正完成门**（CI 跳），翻闸前须用户验。
- **MTMV 单-pin 不变式**是最高 correctness 风险，B5 必须一次物化分区集 + at-snapshot listPartitions；`lastFileCreationTime()` 跨 flavor 可靠性须 live 验。
- auto-memory：[[catalog-spi-p5-paimon-design]]（本 session 决策 + 3 证伪先验索引）。
