# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取是**独立子线**，单独跟踪在
> [`metastore-storage-refactor/`](./metastore-storage-refactor/)（其 HANDOFF/PROGRESS/tasks/decisions 自洽，本文件不复述细节）。

---

# 🎯 下一个 session 的任务 — **docker kerberos e2e 真闸（HDFS kerberized + HMS）→ 然后 P6 iceberg**

> **✅ P3b 已完成（2026-06-21）**：kerberos authenticator 机制已收口到 `fe-kerberos` 单一真相源。P3b-T01 三步全完成（local-commit `4a740e1387f` trino→JDK + `8898e15134c` relocate 13 类 + `5e3e8963023` 统一 HadoopAuthenticator 接口+删 fe-filesystem-hdfs 副本，**均未 push**）。三处 kerberos 实现合一（fe-common 包移除、fe-filesystem-hdfs 副本删除、双 `HadoopAuthenticator` 接口统一为 fe-kerberos 单接口、trino→JDK）。`fe-kerberos` 仍是顶层中立叶子（no-cycle）。详见元存储子线 [`metastore-storage-refactor/HANDOFF.md`](./metastore-storage-refactor/HANDOFF.md)「下一步」+ tasks `P3b-T01` + `DV-010`。
>
> **🟢 下一步 = docker kerberos e2e（唯一未跑的真闸）**：P3b 改的是 `doAs`/UGI 登录路径（安全敏感，UT 抓不到），须部署 docker 验 **HDFS kerberized**（DFSFileSystem 经 fe-kerberos authenticator 读写）+ **HMS kerberos**（`enablePaimonTest=true` 覆盖 paimon-HMS；hive/iceberg-on-kerberized-HDFS 另需 KDC env）不回归。**接受的行为变更须确认**：simple/无 `hadoop.username` 的 HDFS catalog 现以 remote user "hadoop" 跑（旧=FE 进程用户直跑）。如回归 → 回退 DV-010 pure-consolidation 选择。**然后 = P6 iceberg**（复用收口后干净的 `fe-kerberos` authenticator）。
>
> 下文是 P3b 的**历史 scope 记录**（已完成），仅作参考。

<details><summary>（历史）P3b scope — 已完成</summary>

### P3b：kerberos authenticator 机制收口到 `fe-kerberos`（元存储子线，先于 P6 iceberg）

> **🔱 完整 scope + 启动流程见元存储子线**：[`metastore-storage-refactor/HANDOFF.md`](./metastore-storage-refactor/HANDOFF.md)「下一步」+ [`metastore-storage-refactor/tasks.md`](./metastore-storage-refactor/tasks.md) **P3b-T01 块**（现码 scope）+ [`decisions-log.md` D-017](./metastore-storage-refactor/decisions-log.md)。启动按子线顶部强制流程：读 PROGRESS→HANDOFF→WORKFLOW→tasks P3b 块→decisions，**对照真实代码 review**，实施前 **AskUserQuestion 定 scope 粒度**（一次全搬 vs 分步 re-export 桥）。
>
> **为什么现在做 P3b（用户 2026-06-21 定，D-017）**：① **P5-T29（B8）✅** paimon legacy 删除完成，fe-core 完全 paimon-SDK-free（local-commit `32de7d16702`+`895e214b2bd`，未 push）。② **P2-T05 ✅** 用户手动 docker 验证（paimon 5-flavor 读 + vended REST/DLF + Kerberos HMS，`enablePaimonTest=true`）——**B9/P5-T30 的 live-e2e 内容即此，视为已覆盖**（如需正式 deviations 签字另议）。③ **P3b 提前到 P6 iceberg 之前单独做**：P3b 是纯 kerberos 机制收口，不依赖 iceberg 即可完成，且**先做反而服务 P6**——iceberg metastore-props 迁移届时可直接复用收口后的干净 `fe-kerberos` authenticator。
>
> **P3b 一句话**：把 fe-common `security.authentication.*`（**12 类机制**）收口到 `fe-kerberos` 作唯一真相源 + 删 fe-filesystem-hdfs 自有 `KerberosHadoopAuthenticator`/`SimpleHadoopAuthenticator` 副本 + 统一两个打架的 `HadoopAuthenticator` 接口（fe-common `PrivilegedExceptionAction` vs fe-filesystem-spi `IOCallable`）+ trino `KerberosTicketUtils`→JDK。**blast radius = 40 非测试消费方（24 fe-core + 12 fe-common + 3 be-java-extensions scanner）**——跨 FE/BE-java、安全敏感、**须 docker kerberos e2e（HDFS/HMS）把关**（UGI 登录 UT 抓不到）。**这是 P3b 之后才能把 paimon/iceberg/hive metastore-props 搬出 fe-core 的前置**（见本 session 关于 metastore-props 迁移可行性的讨论）。
>
> 下文 §「P5-T29 …」是**已完成**条目（local-commit，详见 design doc §5/§6），仅作历史参考。样板 = **P4 #64300**（`73832991962`）。

</details>

**✅ Batch 1（C1）已完成 + local-commit `7632a074e4b`（未 push）**：删 **33 dead 文件**（`datasource/paimon/*` 除 LIVE `PaimonVendedCredentialsProvider`、`metacache/paimon/*`、`systable/PaimonSysTable`）+ 清 **6 处 live reverse-ref**（`ExternalCatalog`/`ExternalMetaCacheMgr`/`ExternalMetaCacheRouteResolver`/`Env`[保 LIVE D-046 PLUGIN 分支]/`UserAuthentication`/`ShowPartitionsCommand`[保 `hasPartitionStatsCapability`+live `PAIMON_EXTERNAL_TABLE` 枚举]）+ **3 javadoc scrub** + **5 dead test 删** + 2 generic fixture test 修（`StatementContextTest` mock→`PluginDrivenMvccExternalTable`、`ExternalMetaCacheRouteResolverTest`）+ metastore-props `getPaimonCatalogType` 内联字面量（脱钩已删的 `PaimonExternalCatalog`，免「常量搬家」前置）。**fe-core test-compile BUILD SUCCESS + checkstyle 0 + 49 改动测试绿**；`datasource/paimon/` 现仅剩 `PaimonVendedCredentialsProvider`。`reverse-ref + 删文件须同一 commit`（P4 precedent：`PaimonUtils:57`→已删的 `ExternalMetaCacheMgr.paimon()`）。

**🔱 关键 scope 修正（本 session firsthand，推翻旧 §D 框架）**：方案 A/B 都**只碰 7 个 metastore-props**，都**不能单独删** 5 个 paimon maven 依赖——31 个 fe-core 文件 import `org.apache.paimon.*`，其中 ~23 是 Batch 1 已删的 dead 子树；剩 **6 metastore-props**（SDK 100% 在 dead catalog-building 方法→可 strip）+ **`PaimonVendedCredentialsProvider`**（genuinely LIVE，runtime 用 paimon REST SDK，挂在 generic `VendedCredentialsFactory.getProviderType` 的 `case PAIMON`，经 `CatalogProperty:182`）。**用户签 = Plan B（fe-core fully paimon-free）+ D-PB1 strip-in-place（不物理搬 7 类，与 iceberg/hive parity）+ D-PB2 phased**。strip 因「reshape 6 live 类 + trim 7 test 文件 + 单独不删 dep」从 Batch 1 **移到 Batch 2**（用户 2026-06-20 签）。

**Batch 2（✅ 已完成；下为「原计划」历史记录，第 3 项 vended 实际改用 GAMMA — 见上 banner + design doc §5）**：
1. **B1-strip 6 metastore-props**（`AbstractPaimonProperties`+5 flavor）：删 `initializeCatalog`/`buildCatalogOptions`/`appendCatalogOptions`/`appendCustomCatalogOptions`/`getCatalogOptionsMap`/`getCatalogOptions`(catalogOptions 字段)/`getMetastoreType`/`appendUserHadoopConfig`/`normalizeS3Config`/Jdbc `getBackendPaimonOptions`+`registerJdbcDriver`+`DriverShim` + 全 `org.apache.paimon.*` import。**保 LIVE**：`warehouse` @ConnectorProperty、`executionAuthenticator`+`getExecutionAuthenticator`、`initExecutionAuthenticator`/`initHdfsExecutionAuthenticator`（`PluginDrivenExternalCatalog:137-138` 读，Kerberos doAs）、`initNormalizeAndCheckProps`/validation、`getPaimonCatalogType`（已内联）。**这些 strip 方法 0 live main caller**（只 test）。
2. **trim 7 test**：`PaimonCatalogTest`（@Disabled 手测→直接删）、`AbstractPaimonPropertiesTest`（test-local subclass override 被删的抽象方法→修）、`Paimon{HMS,FileSystem,Jdbc,Rest,AliyunDLF}MetaStorePropertiesTest`（去 catalog-building 断言，保 validation/binding/type/auth）。
3. **迁 `PaimonVendedCredentialsProvider` 出 fe-core** + 改 generic `VendedCredentialsFactory`（switch on `MetastoreProperties.Type.PAIMON`，与 iceberg 共享；需新 fe-core seam 让 plugin-loader 侧 provider 喂回，cross-loader）。**这是真正的 cross-cutting 件**，碰 generic/shared fe-core（iceberg 也在同 factory）。
4. **删 5 paimon maven dep**（`fe-core/pom.xml` paimon-core/common/format/s3/jindo）+ 改 `:577` s3-transfer-manager 注释（iceberg-aws 仍需故 s3-transfer-manager 留）。**验**：`grep org.apache.paimon fe-core/src/main`=∅ + `dependency:tree|grep paimon`=∅ + checkstyle0 + import-gate净 + live-e2e `enablePaimonTest=true`（5-flavor 读+vended REST/DLF+Kerberos HMS+sys-table+MTMV+DDL 不回归）。

## P5-T29 scope ledger（已在 `branch-catalog-spi` firsthand 核实 2026-06-20）

**这不是一次 `rm -rf datasource/paimon/`**：存在「STILL-CONSUMED 子树」与「常量耦合」前置，naive 删除会断编译。

### A. DEAD —— 可删（连同消费方的死分支/import）
- `fe/fe-core/.../datasource/paimon/`（**30 文件**，含 `source/`、`profile/`：catalog/factory/db/table、`PaimonExternalCatalog`、`PaimonExternalMetaCache`、`PaimonSysExternalTable`、legacy `source/PaimonScanNode`/`PaimonSplit`/`PaimonSource`、legacy 重复 `source/PaimonPredicateConverter`/`PaimonValueConverter`(P1-T02 推迟项，现可收) 等）。
- `fe/fe-core/.../datasource/metacache/paimon/`（**3 文件**：`PaimonTableLoader`/`PaimonPartitionInfoLoader`/`PaimonLatestSnapshotProjectionLoader`）。
- `fe/fe-core/.../datasource/systable/PaimonSysTable.java`（**1 文件**）。
- **消费方死分支/import 清理**（文件保留，只删 paimon 分支）：`ExternalMetaCacheMgr`（`paimon()`/`ENGINE_PAIMON` 路由 + `PaimonExternalMetaCache` 返回）、`metacache/ExternalMetaCacheRouteResolver`（`ENGINE_PAIMON` 注册）、`catalog/Env`（getDdlStmt 等 legacy 分支若有残留）、`nereids/rules/analysis/UserAuthentication`、`nereids/.../ShowPartitionsCommand`、`credentials/VendedCredentialsFactory`、`ExternalCatalog`（`buildDbForInit` 死分支）。逐个 grep 确认是死分支再删。
- **死测试**：`ExternalMetaCacheRouteResolverTest`、`planner/PaimonPredicateConverterTest`（测 legacy 重复转换器）、`StatementContextTest`（paimon 用法）等——按编译失败/语义死亡逐个判。

### B. 硬前置（删 datasource/paimon/ **之前**必做，否则断编译/checkstyle）
1. **迁出 `PaimonExternalCatalog` 常量**：`PAIMON_FILESYSTEM`/`PAIMON_HMS`（及其它被引常量）被 **5 个 STILL-CONSUMED** `property/metastore/Paimon*MetaStoreProperties` 类 `import` 引用（已核实）。须先把这些常量搬到一个存活的家（metastore-props 模块内的常量持有者 / `fe-kerberos` / 新常量类），再删 `datasource/paimon/PaimonExternalCatalog`。
2. **scrub 悬空 javadoc** `{@link PaimonSysTable}`（如 `PluginDrivenSysTable`、`NativeSysTable` 里的 `@link`）否则 strict checkstyle/javadoc 挂。
3. **保 load-bearing dispatch ordering**（PluginDriven 分支须先于任何 legacy 分支）。
4. **`ENGINE_PAIMON` 区分**：`metacache` 两处是 DEAD（删）；但 `nereids/.../info/CreateTableInfo.ENGINE_PAIMON`（`:123`，被 `:790/:937/:967/:1150` 用作**翻闸后的 engine 名 + distribution 校验**）是 **LIVE，保留**。

### C. STILL-CONSUMED —— **不在 P5-T29 删除范围**（删了会断 cutover 的 Kerberos 装配）
- `fe/fe-core/.../datasource/property/metastore/Paimon*MetaStoreProperties`（HMS/DLF/Rest/Jdbc/FileSystem，5）+ `AbstractPaimonProperties` + `PaimonPropertiesFactory`（共 7）。这些是 cutover `initPreExecutionAuthenticator`→Kerberos 装配 **LIVE** 路径（P6 review R1）。它们的测试 `Paimon*MetaStorePropertiesTest` 同样保留。
- 这些类属 **metastore-storage-refactor 子线**（D-016 那条线也不碰），不在主线 B8 scope。

### D. Maven 依赖（用户明确点名「相关 maven 依赖」）—— ⚠️ **核心冲突，须先定决策**
`fe/fe-core/pom.xml` 现含 5 个 paimon 依赖：`paimon-core`、`paimon-common`、`paimon-format`、`paimon-s3`、`paimon-jindo`（`:543-563`）+ s3 aws-bundle 注释（`:576`，与 iceberg-aws 共享）。
- **关键事实**：C 项的 STILL-CONSUMED `property/metastore/Paimon*` 类 **直接 import `org.apache.paimon.*` SDK**（已核实 6 文件）。⇒ **只要这些类留在 fe-core，fe-core 就不可能像 P4(odps-free) 那样做到完全 paimon-free。**
- **可能可删**：`paimon-format`/`-s3`/`-jindo`（legacy reader/格式/对象存储 IO 路径专用，随 `datasource/paimon/source` 删除而无消费方）。
- **可能保留**：`paimon-core`/`-common`（被 STILL-CONSUMED metastore-props 的 `Options`/`CatalogContext` 等用）。
- 真实可删集合须由下一 session 经 `dependency:tree | grep paimon` + fe-core 编译验证敲定。
- **🔱 开放决策（建议下一 session 先 AskUserQuestion）**：P5-T29 是否把 `property/metastore/Paimon*` 一并迁出 fe-core（→ metastore SPI 模块/连接器），从而让 fe-core 完全 paimon-free？
  - **方案 A（推荐，对齐 master plan B8 / D-016 scope）**：保留 STILL-CONSUMED metastore-props 在 fe-core，**只删 DEAD 子树 + 部分 maven 依赖**（fe-core 保留 paimon-core/common）。surface 小、与已签 B8 scope 一致。
  - **方案 B（更大，越界子线）**：连带迁出 metastore-props，fe-core 完全 paimon-free（对齐 P4 终态）。但这碰 metastore-storage-refactor 子线领域，scope/风险更高，宜单独立项或与子线 P2-T05 合并。

### E. 守门 / 验证（mirror P4 #64300）
- fe-core 编译 BUILD SUCCESS + checkstyle 0 + import-gate 净（`tools/check-connector-imports.sh`）。
- 连接器测试仍绿（删 legacy 不应触连接器）。
- `dependency:tree` 验证 paimon-core 在 FE classpath **恰一份**（R-004/R-007 `NoClassDefFound`/SDK 单例守）。
- regression-gated live-e2e（`enablePaimonTest=true`，用户跑）= 删除后 5-flavor 读 + sys-table + MTMV + DDL 不回归。
- 逐子树删 + 每批跑编译，参 master plan [§3.9/§4 playbook 第 13 步](./00-connector-migration-master-plan.md)。

---

# 🔭 主线 backlog（按优先级）

1. **✅ P5-T29（B8）删 legacy + maven 依赖 — 已完成**（local-commit `32de7d16702`+`895e214b2bd`，未 push）。fe-core 完全 paimon-SDK-free。
2. **✅ P5-T30（B9）post-cutover 回归 — 已由用户手动 docker 覆盖**（即元存储子线 P2-T05：paimon 5-flavor 读 + vended REST/DLF + Kerberos HMS，`enablePaimonTest=true`）。原列项（SHOW PARTITIONS / partitions TVF / DROP·CREATE / no-ENGINE CREATE / edit-log replay / MTMV / sys-table / session-TZ）如需逐项正式签字可另起，但主体已覆盖。
3. **✅ P3b（kerberos authenticator 机制收口到 `fe-kerberos`）= 已完成**（commit 1/2/3：`4a740e1387f`+`8898e15134c`+`5e3e8963023`，均未 push）。三处 kerberos 实现合一到 `fe-kerberos` 单一真相源。**🟢 NEXT = docker kerberos e2e 真闸（HDFS kerberized + HMS）验 `doAs` 不回归 → 然后 P6 iceberg**（见上 §头条 + 子线 [`metastore-storage-refactor/HANDOFF.md`](./metastore-storage-refactor/HANDOFF.md)「下一步」+ tasks P3b-T01 + D-017/DV-010）。元存储子线 **P2 全 5/5 ✅** + **P3b ✅** → 核心 15/15 完成。
4. **accepted-deviation 用户签字残项**：P6 review 未转 fix 的剩余 MINOR/NIT 刻意偏离 + `PluginDrivenExternalCatalog:140` 吞 authenticator-wiring 异常 + uncheckedFallbacks（REFRESH cache invalidation / partitions-TVF auth / split-plan RPC 在 `executeAuthenticated` 外）。逐条记入 `deviations-log.md` accept-as-deviation（含用户签字）。
5. **D-057 re-scope**：deferred `TablePartitionValues:162` prune-path sentinel residue **不影响 paimon**（MVCC override 绕过）→ re-scope 到非-MVCC 插件连接器（maxcompute/es/jdbc）。
6. **后续阶段**：**P6 iceberg（在 P3b 之后）** / P7 hive(+HMS) / P8 收尾（删 SPI_READY_TYPES、删 instanceof）——见 master plan §3.7–3.9。P6 起可复用 P3b 收口后的 `fe-kerberos` authenticator + 评估把 iceberg(/hive/paimon) metastore-props 搬出 fe-core（本 session 已分析：P3b 是其前置）。

---

# 📦 仓库 / 进度状态
- **当前分支 = `branch-catalog-spi`**（开发主分支）。HEAD 近端：`38e7140ce56`（#64446 P5 迁移+翻闸）← `e9c5b3e70ce`（修编译）。
  P0–P5(迁移+翻闸) + P3 hybrid + P4 全部已合入本分支。
- **P5 状态**：B0–B7 合入 #64446；**B8（P5-T29 删 legacy）= Batch 1 + Batch 2 全完成**（local-commit，未 push）；**仅剩 B9（P5-T30 live-e2e 回归，用户跑）**。
- **fe-core 现已完全 paimon-SDK-free**：`datasource/paimon/` 整目录已删；6 个 `property/metastore/Paimon*` 已 strip 成 SDK-free 描述符（保 LIVE auth/validation/@ConnectorProperty/type）；5 个 paimon maven 依赖已删。`grep org.apache.paimon fe/fe-core/src/{main,test}` = ∅。**fe/pom.xml `paimon.version` 保留**（R-007：fe-connector-paimon + BE 仍用）。
- ⚠️ `regression-test/conf/regression-conf.groovy` 若仍 modified 且含**明文 Aliyun key** → commit 前继续 path-whitelist，**严禁 `git add -A`**；`regression-conf.groovy.bak` 同理排除。
- 未跟踪 scratch：`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak` 等——commit 前清，勿 add。
- `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md`（B8 readiness ledger 来源）若仍未跟踪，下次方便时 vet + commit 或保留本地。

## 🗺️ 代码脚手架
- **Plugin connector（终态归宿）**：`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/`
  （`PaimonConnector` / `PaimonConnectorProvider` / `PaimonCatalogFactory`[`MetaStoreProviders.bind` + 薄 `assembleHiveConf`] / `PaimonScanPlanProvider` / `PaimonConnectorMetadata` / `PaimonCatalogOps` seam / `PaimonTableResolver` / `PaimonSchemaBuilder` / `PaimonTypeMapping` / `PaimonIncrementalScanParams`）。
- **fe-core 通用桥（保留）**：`connector/DefaultConnectorContext`、`datasource/PluginDriven*`（含 `PluginDrivenMvccExternalTable`/`PluginDrivenSysExternalTable`/`PluginDrivenSysTable`/`NativeSysTable`）、`fs/FileSystem{Factory,PluginManager}`、nereids scan-node 分发。
- **B8 删除目标（legacy 对照基准）**：fe-core `datasource/paimon/`、`metacache/paimon/`、`systable/PaimonSysTable`。
- **STILL-CONSUMED（B8 不碰）**：fe-core `datasource/property/metastore/Paimon*` + `AbstractPaimonProperties` + `PaimonPropertiesFactory`；`datasource/property/storage/{S3,OSS,COS,OBS,Minio}Properties`（跨连接器共享）。
- **BE 消费端（不在 FE 范围）**：`be/src/format/table/`（`paimon_cpp_reader.cpp`、`paimon_reader.cpp`、`partition_column_filler.h`）。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix/删除批独立 commit；message = `[P5-T29] <subj>` + 根因 + 解法 + 测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。删除 PR 带其 design doc（repo 惯例，参 P4 #64300 的 `P4-batchD-maxcompute-removal-design.md`）。
- **PR 流程**：P5-T29 在 `branch-catalog-spi` 上做（或开 feature 分支 off 它），走**新 PR**（mirror P4 #64300，base = `branch-catalog-spi`）。
  ⚠️ 历史的 `catalog-spi-07-paimon` 分支 + PR #64445 force-push 流程**已作废**（那条线已并入 #64446）。memory `catalog-spi-07-paimon-branch-pr-workflow` 据此过时，勿再 force-push 那两个分支。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`（memory `doris-build-verify-gotchas`）。**漏 `-am` → `could not resolve … ${revision}` 假错**。paimon 连接器模块需 `-am package -Dassembly.skipAssembly=true`（shade jar 携带 HiveConf）。**checkstyle 在 `validate` phase（编译前）跑**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试 harness：`PaimonCatalogFactoryTest` / `PaimonScanPlanProviderTest`(real-table `FileSystemCatalog`) / `PaimonIncrementalScanParamsTest` / `RecordingConnectorContext` / `RecordingPaimonCatalogOps` / `FakePaimonTable`（`.copy` no-op recorder）/ metastore-spi 的 `*MetaStorePropertiesTest`。live-e2e CI-gated（`enablePaimonTest` 默认 false）→ 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **P5-T29 的 D 项（maven）须先与用户对齐 scope（方案 A vs B）再动手**——这是 fe-core 能否完全 paimon-free 的分叉，影响 metastore 子线。
- **删除前必 grep 全调用方 + 确认实际实例类（base vs MVCC 子类 / DEAD vs STILL-CONSUMED）**；逐子树删 + 每批跑 fe-core 编译，别一次性大删。
- **改 storage/auth 装配注意** raw `hadoop.*`/`fs.*` passthrough 跑最后会 clobber 之前 authoritative 设置（FIX-4 4d/4e 亲证）——B8 不应碰这些，但若动 metastore-props（方案 B）须警惕。
- **元存储子线**细节不在本文件——读 `metastore-storage-refactor/HANDOFF.md`。
