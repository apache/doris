# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取是**独立子线**，单独跟踪在
> [`metastore-storage-refactor/`](./metastore-storage-refactor/)（其 HANDOFF/PROGRESS/tasks/decisions 自洽，本文件不复述细节）。

---

# 🎯 下一个 session 的任务 — **P6.1 继续：T04（pom 依赖闭包）/ T05（5 CatalogUtil flavor）**

> **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**翻闸全有或全无，P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（翻闸只在 P6.6）。

## ✅ 上一 session 产出（2026-06-21，T01-T03）

1. **P6.1 code-grounded recon 完成** → `research/p6.1-iceberg-metadata-recon.md`。**核心认知**：连接器 6-file 骨架是「编译通过但误导性的近-no-op」——真正的 per-flavor catalog 装配在 metastore-props（`AbstractIcebergProperties.initializeCatalog:133`，**非** `datasource/iceberg/*`；`IcebergExternalCatalog.initCatalog:73-76` 仅转调），骨架坍缩成裸类名 switch 且含 **4 个 silent 读路径 bug**。
2. **P6.1 逐 task 拆解（10 task P6-T01..T10）** 落盘 `tasks/P6-iceberg-migration.md` §P6.1（含新建类清单 + 验收门 + old→new）。
3. **2 个 scope 决策用户签字 [D-059]**：**Q1 = DLF port-now read-only**；**Q2 = 扩 `fe-connector-metastore-spi` 加 iceberg-flavored REST/glue/s3tables provider**（⚠️**非推荐项，用户主动选**，跨入 metastore 子线）。已采默认：s3tables/glue→`fe/pom.xml` dM；结构 seam（`executeAuthenticated`+CL-pin）P6.1 即纳入。
4. **P6-T01/T02/T03 已实现 + 验证 + commit `ae54a2174ff`**：`IcebergCatalogFactory`（纯静态）+ `IcebergCatalogOps`（seam interface + `CatalogBackedIcebergCatalogOps`）；rewire `IcebergConnector` + `IcebergConnectorMetadata`（吃 seam，behavior frozen）；测试基建从无到有（27 UT）。

## ✅ 本 session 产出（2026-06-21，T08）

5. **P6-T08（type-mapping parity，决策无关）已实现 + 验证 + TDD RED→GREEN，未 commit**。HANDOFF 列 2 修，实现中坚持「断长度而非只断类名」（验收门 parity-by-omission 风险）→ **额外发现并修第 3 个 type-mapping 偏差**：
   - **修 1（TIMESTAMPTZ 名）**：`IcebergTypeMapping:116` `TIMESTAMPTZV2`→`TIMESTAMPTZ`（保 scale 6）。converter `:215` 只认 `TIMESTAMPTZ`（→`createTimeStampTzType(precision)`）；旧名静默 UNSUPPORTED。
   - **修 2（点分 mapping-flag key）**：`IcebergConnectorProperties:46-47` 下划线 → `enable.mapping.varbinary` / `enable.mapping.timestamp_tz`（= `CatalogProperty:50/52`）。真实 catalog map 携点分→旧下划线常量恒读 default-false。
   - **修 3（BINARY 无界长度，本 task 新发现）**：`IcebergTypeMapping` BINARY+flag `of("VARBINARY",65535,0)`→`of("VARBINARY")`（precision=-1→converter else 分支 `createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH=0x7fffffff)`=legacy `IcebergUtils:592` 同常量）。**关键耦合**：修 2 令此 BINARY 路径生产**首次真激活**（旧下划线 key 下 enableMappingVarbinary 恒 false）→ 不一并修＝翻闸后 flip-on 回归（DESCRIBE/SHOW CREATE 渲染 `VARBINARY(65535)` ≠ legacy `VARBINARY(MAX)`）。
   - **测试**：新 `IcebergTypeMappingReadTest`（9，全 primitive + 双 flag + nested 递归，断 typeName **及** precision/scale vs legacy）；改 `IcebergConnectorMetadataTest` 2 mapping 测（喂**字面点分 key** + 断 `TIMESTAMPTZ`）；format-version 测留 T09。
   - **验证**：`mvn -pl :fe-connector-iceberg -am test`（cache off）BUILD SUCCESS，**iceberg 模块 36 run/0F/0E/0skip**（Factory13+Metadata14+TypeMappingRead9）+ checkstyle 0 + import-gate exit 0 + `SPI_READY_TYPES` iceberg 缺席（零行为变更）。docker e2e 未跑（翻闸在 P6.6）。

## 🔴 关键认知（写下来免下次重踩）

- **silent 读路径 bug（骨架，翻闸后才在 regression 暴露）—— T08 已修 #1#2 + 新发现 #1b；#3#4 待 T09**：
  1. ✅ **mapping-flag key 拼写（T08 已修）**：`ENABLE_MAPPING_*` 下划线 → 点分 `enable.mapping.varbinary` / `enable.mapping.timestamp_tz`（= `CatalogProperty:50/52`）。
  1b. ✅ **BINARY 无界长度（T08 新发现并修）**：`IcebergTypeMapping` BINARY+flag emit `VARBINARY(65535)` ≠ legacy `createVarbinaryType(MAX=0x7fffffff)` → 改 emit 无长度 `of("VARBINARY")`（converter 默认 MAX）。**被修 #1 激活**（旧下划线 key 下该支从不触发）。
  2. ✅ **`TIMESTAMPTZV2` 名（T08 已修）**：emit `TIMESTAMPTZ(6)`（converter 只认 `TIMESTAMPTZ`）。
  3. ⬜ **format-version 算错（T09）**：`spec().specId()>=0?2:1` 恒 stamp "2"（unpartitioned specId==0 也 >=0）⇒ 永不出 "1"。应读 table `format-version` 元数据。`getTableSchemaStampsFormatVersionTwoForAnyValidSpec` 测当前 pin frozen "2"，T09 修时同步翻。
  4. ⬜ **column 构造 parity（T09）**：nullable 应恒 true（现 `field.isOptional()`）；isKey 应恒 true（现 5-arg ctor 默认 false）；缺小写化 + `WITH_TIMEZONE` Extra marker（`ConnectorColumn.withTimeZone()` 字段存在未用）；listing 缺 nested-namespace 递归 + view 过滤。
- **metastore-spi 复用仅限 HMS/DLF**：`HmsMetaStorePropertiesImpl.toHiveConfOverrides:158` + `DlfMetaStorePropertiesImpl.toDlfCatalogConf:119` SDK-free 已是 paimon dep；但其 REST/DLF impl 是 paimon-specific（`paimon.rest.*`）、无 glue/s3tables。**Q2=B ⇒ T05/T06/T07 实现前须对 `MetaStoreProviders.bind` 注册机制单独 mini-recon**（扩 metastore-spi 加 iceberg provider，跨入 metastore 子线，须读 `metastore-storage-refactor/`）。
- **跨切面风险（带入 T04–T07 + P6.6 翻闸门）**：R-004 AWS-SDK `ExecutionAttribute` static 撞（iceberg-aws+s3tables+glue/sts 都拉 awssdk，child-first 须自包含）；DLF `ProxyMetaStoreClient` 按类名反射加载须入 plugin-zip 闭包；hive-metastore+shaded-thrift 可能与 host thrift 撞（paimon FIX-C/RC-1，或需与 paimon 共享 shade）；JDBC DriverShim 进程级 `DriverManager` static；**field-id 丢失**（`ConnectorColumn` 无载体，P6.2+ scan 前须重引，否则同 paimon BE SIGSEGV/DCHECK 类 bug）。**这些 UT 不可见，仅 P6.6 docker plugin-zip e2e 可验**。

## 🟢 下一步（精确）

- **首选 = P6-T04（pom 依赖闭包）**：`fe-connector-iceberg/pom.xml` 加 iceberg-hive-metastore + fe-connector-metastore-spi + s3tables 两 jar + glue/sts + aliyun DLF SDK；`fe/pom.xml` 加 s3tables dM 条目（`fe/pom.xml:427` 现仅 version property，`<dependency>` 仅在 `fe-core/pom.xml:738-744`）。
- **T04 后 = T05（5 CatalogUtil flavor）→ T06（s3tables bespoke）→ T07（DLF port）**：均需先做 **Q2=B 的 metastore-spi mini-recon**（`MetaStoreProviders.bind` + 现有 provider 形态，读 `metastore-storage-refactor/`）。**T09（column 构造 parity + format-version + nested-namespace + view 过滤）待 T03+T08（现可起）**；T10 待 T05/T06/T07/T09。
- **T08（本 session）未 commit**：4 改 1 新（`IcebergTypeMapping.java` + `IcebergConnectorProperties.java` + `IcebergConnectorMetadataTest.java` 改、`IcebergTypeMappingReadTest.java` 新）。独立对抗验证（2-agent workflow）确认**全 17 primitive byte-parity、0 漏 divergence**。下一 session 起步前先 commit T08（path-whitelist），或与 T04 同批 commit。

---

# 📦 仓库 / 进度状态

- **工作分支 = `catalog-spi-10-iceberg`**（P6.1 起步前 HEAD = `e5959e1b53d` #64655 P3b）。branch off `branch-catalog-spi`，PR 时 squash 合并。
  - **已 commit（上一 session，T01-T03）**：**code `ae54a2174ff`**（`{IcebergCatalogFactory,IcebergCatalogOps}.java` 新 + `{IcebergConnector,IcebergConnectorMetadata}.java` 改 + 5 test 新）+ **docs commit**（`research/p6.1-iceberg-metadata-recon.md` + `tasks/P6` §P6.1 + `decisions-log.md` D-059 + `PROGRESS.md` + HANDOFF + `connectors/iceberg.md`）。
  - **未 commit（本 session，T08）**：code = `IcebergTypeMapping.java`(改) + `IcebergConnectorProperties.java`(改)；test = `IcebergConnectorMetadataTest.java`(改) + `IcebergTypeMappingReadTest.java`(新)；docs = `tasks/P6`(T08 行+记录) + `PROGRESS.md` + 本 HANDOFF。**待用户决定 commit**（建议 message `[refactor](catalog) P6.1 iceberg: T08 type-mapping read parity (TIMESTAMPTZ + dotted mapping-flag key + unbounded BINARY)`）。
- **P0–P5 + P3 hybrid + P4 + P3b 全部已合入**（#63582/#63641/#64096/#64143/#64253/#64300/#64446/#64653/#64655）。iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case。
- ⚠️ `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ `*.bak` + scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）**严禁 `git add -A`**，commit 前 path-whitelist。

## 🗺️ 代码脚手架（iceberg）

- **连接器（终态归宿）**：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/`（现：`IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping` + 本 session 新增 `CatalogFactory`/`CatalogOps`）。
- **paimon 模板**（P6.1 镜像）：`fe/fe-connector/fe-connector-paimon/`（`PaimonCatalogFactory`/`PaimonCatalogOps` seam/`PaimonConnector.createCatalogFromContext:316-333` CL-pin+executeAuthenticated/测试 `RecordingPaimonCatalogOps`/`FakePaimonTable`）。
- **legacy 对照（P6.1 读路径）**：fe-core `datasource/iceberg/`（`IcebergMetadataOps`1362 读半 / `IcebergExternalTable`535 读 / `IcebergUtils`1826 schema-type / `DorisTypeToIcebergType`134 / 7 flavor catalog + `HiveCompatibleCatalog`181 + `dlf/`4）+ `datasource/property/metastore/`（`AbstractIcebergProperties`285 + 7 flavor + factory，**STILL-CONSUMED 留 fe-core 至翻闸后**）。
- **metastore-spi（Q2=B 将扩）**：`fe/fe-connector/fe-connector-metastore-spi/`（`MetaStoreProviders.bind` + `HmsMetaStorePropertiesImpl`/`DlfMetaStorePropertiesImpl`，现 paimon-specific REST/DLF、无 glue/s3tables）。

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`。**漏 `-am`→`${revision}` 假错**。**checkstyle 在 `validate` phase（编译前）跑**。连接器模块 art = `fe-connector-iceberg`；paimon 连接器需 `-am package -Dassembly.skipAssembly=true`（shade jar 携 HiveConf）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- 测试无 Mockito（fail-loud fake）；live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。

## ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-conf.groovy` 明文 key + `*.bak` + scratch）。
- message `[refactor](catalog) P6.1 iceberg: <subj>`（mirror #64653/#64655）+ 根因/解法/测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- PR base = `branch-catalog-spi`，squash 合并。历史 `catalog-spi-07-paimon` force-push 流程**已作废**。

## 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（参 P5-T29 教训；metastore-props 是 STILL-CONSUMED）。
- **Q2=B 是用户主动选的非默认项** —— 扩 metastore-spi 前务必读 `metastore-storage-refactor/HANDOFF.md` + 对 `MetaStoreProviders.bind` 单独 mini-recon，勿盲目加 provider。
- **大文件（`IcebergUtils`1826 等）用 subagent 总结**（playbook §3.1），勿主线整读。
