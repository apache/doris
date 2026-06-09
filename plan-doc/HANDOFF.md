# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🔥 2026-06-09 — P5 paimon B1 完成（flavor 装配，全 5 flavor，单 Catalog）；下一步 = B2（normal-read）

> **本 session**：按 [tasks/P5-paimon-migration.md](./tasks/P5-paimon-migration.md) 落地 **B1**（T03 flavor 装配 + T04 属性键 + T05 validateProperties）。**用户签字 all-5-flavors now**（非分阶段）。subagent-driven（内部 2-dispatch，每 dispatch implement→spec-review→quality-review→fix-loop→re-review + final holistic review + 主线 firsthand 复跑）。

## ✅ 本 session 已完成（B1 = T03 + T04 + T05）

- **新 `PaimonCatalogFactory`**（连接器侧，镜像 MC `MCConnectorClientFactory` 角色）：纯 `validate(props)`（flavor 合法性 + 每-flavor 必需键 fail-fast）+ 纯 `buildCatalogOptions(props)`（`paimon.catalog.type`→paimon `metastore` opt[filesystem/hive/rest/jdbc] + warehouse + 每-flavor opts + `paimon.*` 透传排除 4 storage 前缀）+ 纯 `buildHadoopConfiguration`/`buildHmsHiveConf`/`buildDlfHiveConf` + `requireOssStorageForDlf`。**纯=可离线 UT**（31 个 PaimonCatalogFactoryTest，WHY+MUTATION）。
- **`PaimonConnector`**：线程 `ConnectorContext`（之前 provider.create 丢弃了 context）；`createCatalog` 全 5 flavor 活线——filesystem/jdbc=`CatalogContext.create(options, conf)`、rest=Options-only、hms/dlf=HiveConf；**全部 `context.executeAuthenticated(...)` 包裹**（authenticator seam，FE 注入 Kerberos UGI，默认 no-op）；JDBC DriverShim 移植，driver_url 经 `context.getEnvironment()` 解析（替禁用的 `JdbcResource`）。
- **`PaimonConnectorProperties`**：全 flavor key 常量（HMS/REST/JDBC/DLF，多别名 `String[]`）。**`PaimonConnectorProvider`**：`create` 传 context + override `validateProperties`→`PaimonCatalogFactory.validate`。
- **pom**：加 `paimon-hive-connector-3.1`+`hadoop-common`+`hive-common`（compile，managed 版）；**弃 hive-catalog-shade** 避 fastutil 冲突。
- **验证（主线 firsthand）**：`Tests run: 43, Failures: 0, Errors: 0, Skipped: 1`（1 skip=live）+ BUILD SUCCESS + checkstyle 0 + import-gate 0。spec+quality 双审/dispatch + final holistic review=READY。

## 🧠 核心发现 / 纠偏（影响后续批次 + 翻闸）

1. **2 个新 blocker（非 plan 预见）已解**：① JDBC 用 `org.apache.doris.catalog.JdbcResource`（禁 import）→ 改 `ConnectorContext.getEnvironment()`(`jdbc_drivers_dir`/`doris_home`)；② storage `Configuration` 由 fe-core `StorageProperties`（禁）构建 → 连接器 minimal 重建（`fs.*`/`dfs.*`/`hadoop.*` + `paimon.s3.*`→`fs.s3a.` normalize）。
2. **reachability 真相**（firsthand）：paimon-core 1.3.1 只含 filesystem/jdbc/rest catalog；hms+dlf 都 → paimon `metastore=hive`（dlf=HMS+Aliyun ProxyMetaStoreClient+DataLakeConfig）须 `paimon-hive-connector-3.1`。DLF key 全 inline 字面量（`dlf.catalog.*`，javap 证）避 Aliyun 编译依赖。
3. **纠偏：rest 同样必需 warehouse**（recon「rest Options-only 无 warehouse」证伪）——legacy base warehouse `@ConnectorProperty` required 默认 true 且 rest 未 override。已改齐 parity。
4. **authenticator 简化**：legacy 每-flavor 条件 `HadoopExecutionAuthenticator`；连接器统一 `executeAuthenticated` 包裹全 flavor（FE 无 Kerberos 时注入 no-op，等价且更简）。

## ⚠️ 翻闸(B7)硬门新增（B1 落地，live-e2e 必验，pre-cutover 离线不可测）

> 详见 plan-doc「风险/开放问题」R-高/R-中 翻闸门条 + 阶段日志 B1 条 + 代码内 NOTE。**用户真实 paimon 各 flavor 环境必验。**

1. **hms/dlf Thrift metastore client 跨 classloader**：连接器**不打包** `IMetaStoreClient`/`HiveMetaStoreClient`（paimon-hive-connector 的 hive-exec/metastore=test scope）；翻闸时由 FE host `hive-catalog-shade`(3.1.x) 提供。plugin child-first 下 host(3.1.x) 与 plugin bundled(hadoop 3.4.2/hive 2.3.9) 的 `Configuration`/`HiveConf` 身份隐患。**编译 ABI 已证良性**（paimon-3.1 引用的 HiveConf 子集在 2.3.9 全存在），但 live 须验真实 HMS 建 catalog 不抛 `NoClassDefFoundError`/`LinkageError`/`ClassCastException`。
2. **jdbc driver_url FE 安全 allow-list 未接**（white-list/secure-path/jar 名校验，须经 ConnectorContext hook；paimon 未入 SPI_READY_TYPES 故未触达）。
3. **HMS 外部 hive-site.xml 文件加载延后**（kerberos sasl.enabled/service-principal/auth_to_local 已移植；UGI doAs 经 executeAuthenticated FE 注入）。

## 🎯 下一 session = B2（normal-read；gated on B1 已完成）

- **T06（BLOCKER）**：修 `PaimonScanPlanProvider:95` transient-Table reload fallback（transient null 时 `catalog.getTable(Identifier)` 重建；序列化后 NPE）。可参照 metadata 侧 `getColumnHandles` 已有 fallback。
- **T07**：`PaimonPredicateConverter` session-TZ 化（读 `getTimeZone()` 惰性解析+降级，替 `:284` 固定 UTC）；[[catalog-spi-connector-session-tz-gotcha]]。
- **T08**：`listPartitionNames/listPartitions/listPartitionValues`（填 `ConnectorPartitionInfo` 含 `lastModifiedMillis=Partition.lastFileCreationTime()`）+ `getProperties`（现 stub `:154`）。
- **T09**：override 6-arg `planScan(...requiredPartitions)` 让引擎分区裁剪生效（`PluginDrivenScanNode:474`，现只 override 4-arg），OR 文档化纯谓词裁剪 + 测。
- **T10**：连接器内 cache 已解析 Table+schema（替 `PaimonExternalMetaCache`）；核 REFRESH CATALOG/TABLE seam。
- 批次依赖图 / 翻闸前置硬门见 [tasks/P5-paimon-migration.md](./tasks/P5-paimon-migration.md) §批次依赖。**B6**（procedure doc no-op，独立）可随时穿插。

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false`（**须 -am**，裸 -pl 会因 `${revision}` 兄弟解析虚假失败）；改连接器 `:fe-connector-paimon`、改 SPI `:fe-connector-api`、改 fe-core `:fe-core`。读真实 `Tests run:`/`BUILD`，勿信后台 echo exit（[[doris-build-verify-gotchas]]）。
- 连接器禁 import fe-core/fe-common（`org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`；import-gate `bash tools/check-connector-imports.sh`）。连接器测试无 mockito（纯 seam / child-first loader，[[catalog-spi-fe-core-test-infra]]）；checkstyle 含 test 源、绑 validate（`mvn test` 即跑）。
- 翻闸（B7）GSON **7 注册原子齐迁**（5 catalog + db + table，[[catalog-spi-gson-migrate-all-three]] / [[catalog-spi-cutover-fe-dispatch-gap]]）；删 legacy（B8）后验 paimon-core FE classpath 恰一份（[[catalog-spi-be-java-ext-shared-classpath]]）。
- 分支 `catalog-spi-07-paimon`。**B1 改动未提交**（用户决定何时 commit）；连接器新文件 `PaimonCatalogFactory.java`/`PaimonCatalogFactoryTest.java` 未跟踪。**未跟踪/本地 scratch 勿提交**：`regression-test/conf/regression-conf.groovy`(+`.bak`)、`.audit-scratch/`、`conf.cmy/`、`.claude/scheduled_tasks.lock`（用户本地集群配置）。

## 🧠 给下一个 agent 的 meta

- **D-037/D-038 已签字 + all-5-flavors 已签**，B0+B1 已落 —— 按设计 doc B2→B9 续。
- **live e2e（真实 paimon 各 flavor 环境）= 翻闸真正完成门**（CI 跳），翻闸前用户验；B1 新增 3 个 live-e2e 硬门（见上 ⚠️）；parity doc §4 有 run plan。
- **MTMV 单-pin 不变式**（B5）是最高 correctness 风险；`lastFileCreationTime()` 跨 flavor 可靠性须 live 验。
- auto-memory：[[catalog-spi-p5-paimon-design]]（设计决策）、[[catalog-spi-p5-b1-design]]（B1 flavor 装配定夺 + 2 blocker + 翻闸门）。
