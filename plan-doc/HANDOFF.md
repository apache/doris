# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🔥 2026-06-09 — P5 paimon B0 完成（测试基建 + parity baseline）；下一步 = B1（flavor 装配）

> **本 session**：按 [tasks/P5-paimon-migration.md](./tasks/P5-paimon-migration.md) 落地 **B0**（T01 测试基建 + T02 parity baseline）。subagent-driven（每任务 implement→spec-review→quality-review + 主线 firsthand 复跑构建）。**B0 是 P5 首个产线代码批次**；B1–B9 待续。

## ✅ 本 session 已完成（B0 = T01 + T02）

- **T01（测试基建 + seam）**：抽 `PaimonCatalogOps` 注入式 seam（**5 读方法**：listDatabases/getDatabase/listTables/getTable/close，B0 只读）over 远端 paimon `Catalog`；`PaimonConnectorMetadata` **6 调用点齐迁**（读路径字节级不变，`Catalog` import 仅留两 NotExist catch）；`PaimonConnector` 装配 `CatalogBackedPaimonCatalogOps`。建 `fe-connector-paimon` **首个测试模块**（MC `McStructureHelper` 范式，no-mockito）：`RecordingPaimonCatalogOps` + `PaimonConnectorMetadataTest`（9 UT，钉 `databaseExists` try/catch→bool + `getColumnHandles` reload-fallback，各带 WHY+MUTATION 注释）+ `FakePaimonTable`（28 非读方法 fail-loud）+ env-gated `PaimonLiveConnectivityTest`。
- **T02（parity baseline）**：① **R-007 版本三方已对齐**（`${paimon.version}=1.3.1` 单源 `fe/pom.xml:399`；FE 连接器 + BE paimon-scanner + preload-extensions 同源）→ 落不变式注释（**非改版本/非加 enforcer**）。② offline FE→BE serde round-trip smoke `PaimonTableSerdeRoundTripTest`：真 `FileSystemCatalog`/`LocalFileIO`@TempDir → 真 `FileStoreTable` → 连接器 encode（InstantiationUtil+STD Base64，镜像 `PaimonScanPlanProvider.encodeObjectToString`）→ BE-side decode（镜像 `PaimonUtils.deserialize` **URL-first/STD-fallback**）→ 断 rowType/partition/primary keys；CI 跑、**非** env-gated。③ parity-baseline doc [`research/p5-paimon-parity-baseline.md`](./research/p5-paimon-parity-baseline.md)。
- **验证（主线 firsthand）**：连接器 `Tests run: 12, Failures: 0, Errors: 0, Skipped: 1`（1 skip=live）+ **BUILD SUCCESS** + checkstyle 0 + import-gate 净。每任务 spec+quality 双审 PASS；主线追加 3 处准确性修正后复绿（见下「准确性修正」）。
- **doc 同步**：`tasks/P5-paimon-migration.md`（T01→✅、T02→✅、元信息→进行中、阶段日志 +B0 条、当前阻塞项更新）、`fe/pom.xml`（R-007 注释）、本 HANDOFF（覆盖）。

## 🧠 核心发现 / 纠偏（影响后续批次）

1. **R-007 三方版本已对齐**（非待修）：单 `${paimon.version}=1.3.1` 属性即真源；删 legacy 后（B8）仍须验 paimon-core FE classpath 恰一份。
2. **parity baseline 早已存在**（证伪 recon「无 baseline」）：**41 套**回归（33 p0 + 6 p2 flavor + 3 MTMV + fe-core `PaimonScanNodeTest`）今跑 legacy `PaimonScanNode`，**翻闸（B7）后同套自动变 connector-SPI after 门**——无须新写「after」套。真 gap = 连接器侧 UT（① `PaimonPredicateConverter` 无连接器测，legacy 侧已有 fe-core `PaimonPredicateConverterTest`；② native/deletion-vector 连接器分类断言；③ sys-table forced-JNI 断言）+ **live-e2e 硬门**（用户跑，CI 跳，flavor 套全 env-gated）。详见 parity doc §3。
3. **seam B0 只读**：`PaimonCatalogOps` 故意不含 DDL；**B1–B3 须扩** createDb/dropDb/createTable/dropTable 并**同步** `CatalogBackedPaimonCatalogOps` + 测试 `RecordingPaimonCatalogOps`。
4. **transient-Table reload BLOCKER 仍在**（T06/B2）：`PaimonScanPlanProvider:95` 取 `getPaimonTable()` 无 null fallback、无 catalog 访问；序列化后 NPE。B2 须修（metadata 侧 `getColumnHandles` 已有 fallback 可参照）。

## 🛠 准确性修正（主线在 quality-review PASS 后追加，已复绿）

- `PaimonTableSerdeRoundTripTest.beDecode`：改为**真镜像 BE** `PaimonUtils.deserialize`（先 `getUrlDecoder` 再 STD fallback + scanner classloader），并修 javadoc 过度声明。
- 第二测试 `base64VariantMustMatchBetweenEncodeAndDecode` → 重命名 `standardBase64LegRoundTripsSerializedBytesVerbatim`；修「corrupts」措辞为「throws→触发 BE STD fallback」。
- parity doc §3.1：注明 legacy 转换器**已有**直接 fe-core UT（`PaimonPredicateConverterTest`），gap 精确化为「**连接器** converter 无测」。

## 🎯 下一 session = B1（flavor 装配，单 Catalog 模型；gated on D1=A 已签）

- **B1**：T03 `PaimonConnector.createCatalog` flavor switch on `paimon.catalog.type`（warehouse/options/重建 Hadoop·HiveConf/**每-flavor `ExecutionAuthenticator`**；filesystem→hms→rest/jdbc/dlf 渐进）+ T04 拷 HMS/REST/DLF/JDBC + credential/storage 属性键入 `PaimonConnectorProperties` + T05 扩 `validateProperties`（flavor 合法性 fail-fast）。**每-flavor authenticator 丢=Kerberos DDL 炸**（无离线测覆盖）。
- **B6**（procedure doc no-op，独立）可随时穿插落。
- 批次依赖图 / 翻闸前置硬门见 [tasks/P5-paimon-migration.md](./tasks/P5-paimon-migration.md) §批次依赖。

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false`（**须 -am**，裸 -pl 会因 `${revision}` 兄弟解析虚假失败）；改连接器 `:fe-connector-paimon`、改 SPI `:fe-connector-api`（-am 连带）、改 fe-core `:fe-core`。读真实 `Tests run:`/`BUILD`，勿信后台 echo exit（[[doris-build-verify-gotchas]]）。
- 连接器禁 import fe-core（import-gate `bash tools/check-connector-imports.sh`）；session 值经 session-property 透传（[[catalog-spi-connector-session-tz-gotcha]]）。连接器测试无 mockito（纯 seam / child-first loader，[[catalog-spi-fe-core-test-infra]]）；checkstyle 含 test 源、绑 validate 阶段（`mvn test` 即跑；或单 `checkstyle:check`）。
- 翻闸（B7）GSON **7 注册原子齐迁**（5 catalog + db + table，[[catalog-spi-gson-migrate-all-three]] / [[catalog-spi-cutover-fe-dispatch-gap]]）；删 legacy（B8）后验 paimon-core FE classpath 恰一份（[[catalog-spi-be-java-ext-shared-classpath]]）。
- 分支 `catalog-spi-07-paimon`。**未跟踪/本地 scratch 勿提交**：`regression-test/conf/regression-conf.groovy`(+`.bak`)、`.audit-scratch/`、`conf.cmy/`、`.claude/scheduled_tasks.lock`（用户本地集群配置）。

## 🧠 给下一个 agent 的 meta

- **D-037/D-038 已签字**，B0 已落 —— 直接按设计 doc B1→B9 续，无须重开 scope。
- **live e2e（真实 paimon 各 flavor 环境）仍是翻闸真正完成门**（CI 跳），翻闸前须用户验；parity doc §4 有 run plan。
- **MTMV 单-pin 不变式**（B5）是最高 correctness 风险；`lastFileCreationTime()` 跨 flavor 可靠性须 live 验。
- auto-memory：[[catalog-spi-p5-paimon-design]]（设计决策 + 3 证伪先验索引）。
