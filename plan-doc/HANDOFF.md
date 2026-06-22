# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取子线**已彻底 CLOSED**（2026-06-22 收官，全部合入主线 #64446/#64653/#64655）——
> [`metastore-storage-refactor/`](./metastore-storage-refactor/) 文档仅作历史留存、**后续勿读**；需了解 metastore-spi 现状请直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 — **P6.1 继续：T06（s3tables bespoke）→ T07（DLF port）→ T09（column parity，可并行）**

> **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**翻闸全有或全无，P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（翻闸只在 P6.6）。

## ✅ T05 = DONE（2026-06-22，part3 `b0dbe91e1a5` 已 commit，未 push）

> 设计文档 = `plan-doc/tasks/designs/P6-T05-iceberg-catalog-assembly-design.md`。前序：part1 `562201deb9f`（metastore-spi `bindForType`），part2 `6f7b292b5c6`（factory common base + S3FileIO）。
> **T05 全部完成**：5-flavor 装配 + connector LIVE 接线 + DriverShim + 52 parity 测试。**验收全绿**：fe-connector-iceberg UT 75/0/0、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`。

1. **5-agent code-grounded 复核**（legacy `AbstractIcebergProperties`+5 flavor 实体逐行）→ 精确 key 派生契约。**纠正 2 处过时 HANDOFF**：
   - **GLUE 是自包含的**（非旧 HANDOFF 说的「走 base chooseS3」）：legacy `appendS3Props` **无条件 plain put** 5 个 `s3.*`（空串也发，从 `S3Properties.of(origProps)` 专属 store，**忽略** storagePropertiesList），不走 base `toS3FileIOProperties`（那条才 blank-guard + assume-role）。连接器版从 `chosenS3`（fe-filesystem 单一真源 D-061）无条件发，`glue.*` 凭据从 raw props 读。
   - **HADOOP warehouse FE 不强制**（旧 HANDOFF 说「必填」错）：仅 JDBC 强制 warehouse；HADOOP 靠 HadoopCatalog 下游自己抛——**不要加 warehouse 检查**。
   - REST 凭据类型优先级（核 `IcebergAwsClientCredentialsProperties.getCredentialType`）：**EXPLICIT(AK&SK) > ASSUME_ROLE(roleArn) > PROVIDER_CHAIN**（连接器用 `hasStaticCredentials()`/`hasAssumeRole()` 对齐）。
2. **实现（纯静态，镜像 paimon）**：`IcebergCatalogFactory.{appendRestProperties,appendGlueProperties,appendJdbcProperties,buildCatalogProperties(编排),resolveCatalogName,buildHadoopConfiguration,assembleHiveConf}`；`IcebergConnector.createCatalog` flavor switch（HMS `bindForType`→HiveConf；GLUE conf=null；JDBC driver+catalog_name 位置参数；REST/HADOOP storage conf）+ TCCL-pin + `executeAuthenticated` + DriverShim + `preCreateValidation`。pom 加 `hadoop-mapreduce-client-core`+`commons-lang` **test-scope**（`new HiveConf()` 静态初始化需,运行时闭包已含,不入 plugin-zip,镜像 paimon）。
3. **对抗 parity review**（3-flavor 独立比对 legacy）= **0 confirmed bug**（2 个 minor 经独立 verify 均非真 bug：REST oauth2 token else 分支 `""` vs `null` 仅在校验拒绝的不可达态;JDBC driver dedup 是有意的 paimon-parity 重构,可观察结果一致）。

## 🟢 T05 已交付的可复用件（T06/T07 直接接）

- **`buildCatalogProperties(props,flavor,chosenS3)`**：编排器,handles base+impl+per-flavor+type 删除+jdbc catalog_name 删除。**s3tables/dlf 当前走 default 分支 = 仅 base+impl（占位）**——T06/T07 在此扩。
- **`IcebergConnector.createCatalog` switch 的 `default` 分支** = rest/hadoop/s3tables/dlf 共用 `buildHadoopConfiguration`；s3tables/dlf 的 bespoke 实例化（`new S3TablesCatalog().initialize` / `new DLFCatalog().setConf().initialize`）尚未接,现仍走 `CatalogUtil.buildIcebergCatalog`(impl-name)——**dlf 会 ClassNotFound（`...dlf.DLFCatalog` 未建），s3tables 可能能起但缺 region/s3 派生**。因 iceberg 不在 SPI_READY 故未被触发。
- **`MetaStoreProviders.bindForType("dlf",props,storageConf)`** 已就绪（part1）：T07 DLF 直接调 → `DlfMetaStoreProperties.toDlfCatalogConf()`,勿再做 metastore-spi recon。
- **`assembleHiveConf` / `buildHadoopConfiguration` / `firstNonBlank`**（public static）= T06/T07 复用。

## 🟡 T05 已记录的 deviation（UT 不可见,仅 P6.6 docker plugin-zip e2e 真验）

- **REST PROVIDER_CHAIN 非-DEFAULT** provider-class 跳过（连接器不能 import fe-core `AwsCredentialsProviderFactory.getV2ClassName`；DEFAULT=no-op=常见情形无缺口）。
- **GLUE/REST 的 `s3.*`** 取自 fe-filesystem 类型化 `S3CompatibleFileSystemProperties`（D-061）而非 legacy `S3Properties.of(origProps)`——若 glue 仅用 `glue.*` 别名供凭据（fe-filesystem 不读这些进 S3 store），s3.* FileIO 凭据可能缺（glue-client 凭据仍从 `glue.*` 读,不受影响）。
- **base 的 no-S3-store fallback region**（`getRegionFromProperties`）未移植（niche；连接器读类型化 storage）。

## 🔴 关键认知（写下来免下次重踩）

- **T04 残留风险（UT/打包不可见，仅 P6.6 docker plugin-zip e2e 真闸可验）**：
  1. `hive-catalog-shade` **内含** iceberg 1.10.1 与连接器直接 `iceberg-core` 在 child-first loader 共存——版本相同→预期 byte-identical benign（fe-connector-hive 同款已上线），但**首次** direct-iceberg + shade 组合，未 live 验。
  2. **glue 显式-AK 凭据 provider 类 `com.amazonaws.glue.catalog.credentials.*` 来源未定**（不在 hive-catalog-shade / iceberg-aws；fe-core `aws-java-sdk-glue` v1 疑源未证）→ **T05 glue flavor wiring 时核**（不挡 T04 闭包）。
  3. `apache-client` 经 awssdk 传递 runtime 入闭包（paimon 同款故意 ship，无害）。
- **silent 读路径 bug（骨架，翻闸后才在 regression 暴露）—— T08 已修 #1#1b#2；#3#4 待 T09**：
  3. ⬜ **format-version 算错（T09）**：`spec().specId()>=0?2:1` 恒 stamp "2"（unpartitioned specId==0 也 >=0）。应读 table `format-version` 元数据。`getTableSchemaStampsFormatVersionTwoForAnyValidSpec` 测当前 pin frozen "2"，T09 修时同步翻。
  4. ⬜ **column 构造 parity（T09）**：nullable 应恒 true（现 `field.isOptional()`）；isKey 应恒 true（现 5-arg ctor 默认 false）；缺小写化 + `WITH_TIMEZONE` Extra marker（`ConnectorColumn.withTimeZone()` 字段存在未用）；listing 缺 nested-namespace 递归 + view 过滤。
- **Q2=B metastore-spi mini-recon + 改造 = ✅ DONE（part1 `562201deb9f`）**：新增 `MetaStoreProvider.supportsType(String)` + `MetaStoreProviders.bindForType(flavor,props,storageConf)`（5 provider 全转，paimon `supports(Map)` 字节不变）。HMS/DLF conf 复用 `HmsMetaStorePropertiesImpl.toHiveConfOverrides` + `DlfMetaStorePropertiesImpl.toDlfCatalogConf`（SDK-free）。**iceberg 只用 `toHiveConfOverrides()`/`toDlfCatalogConf()`，不调 paimon `validate()`**（paimon-ism: requireWarehouse for all flavors；iceberg HMS 不要求 warehouse）。无 glue/s3tables provider（glue/s3tables 在连接器 `IcebergCatalogFactory` 内直接装配，不走 metastore-spi）。
- **跨切面风险（带入 T05–T07 + P6.6 翻闸门）**：R-004 AWS-SDK `ExecutionAttribute` static 撞（已用 child-first 自包含 awssdk 闭包缓解，待 docker 验）；DLF `ProxyMetaStoreClient` 按类名反射加载须入 plugin-zip 闭包（hive-catalog-shade 已带，T07 决定 vendored-source vs shade-bundled）；hive-catalog-shade relocated thrift vs host（已 exclude fe-thrift/libthrift）；**field-id 丢失**（`ConnectorColumn` 无载体，P6.2+ scan 前须重引，否则同 paimon BE SIGSEGV/DCHECK 类 bug）。**这些 UT 不可见，仅 P6.6 docker plugin-zip e2e 可验**。

## 🟢 下一步（精确）

- **立即 = T06（s3tables bespoke）**：legacy `IcebergS3TablesMetaStoreProperties`（73 行附近,未读全）走 `new S3TablesCatalog().initialize(name, props)`（**不**经 `CatalogUtil.buildIcebergCatalog`）。需：①读 legacy `IcebergS3TablesMetaStoreProperties` 精确 key 派生（s3tables 的 `warehouse`=table-bucket ARN + region + S3FileIO）；②在 `IcebergConnector.createCatalog` 给 s3tables 单独分支：`buildCatalogProperties`(base+impl+S3FileIO) 后 `new software.amazon.s3tables.iceberg.S3TablesCatalog()` + `setConf` + `initialize(catalogName, opts)`（不走 `CatalogUtil`）；③测试装配 map + 分支。s3tables SDK 已在 pom（`s3-tables-catalog-for-iceberg`）。
- **然后 = T07（DLF 子树 port）**：`DLFCatalog`+`HiveCompatibleCatalog`(181)+`DLFTableOperations`+2 client pool + vendored `ProxyMetaStoreClient`，断 3 fe-core import；**复用 `MetaStoreProviders.bindForType("dlf",props,storageConf)`→`DlfMetaStoreProperties.toDlfCatalogConf()`**（已就绪,勿重做 metastore-spi recon）；`IcebergConnector` dlf 分支 `new DLFCatalog().setConf(hiveConf).initialize(...)`。DLF 子树在 fe-core `datasource/iceberg/dlf/`，连接器终态归宿 `org.apache.doris.connector.iceberg.dlf`（`resolveCatalogImpl` 已指向该 FQCN）。
- **T09（column 构造 parity + format-version + nested-namespace + view 过滤）**：依赖 T03+T08，**与 T06/T07 独立、现可并行起**（见下「🔴 关键认知」#3#4）。T10 待 T06/T07/T09。
- **起步先 re-read 已 commit 的 `IcebergCatalogFactory`（T05 全套 appender/编排/conf 方法）+ `IcebergConnector`（T05 LIVE switch，s3tables/dlf 现走 default 占位）+ `PaimonConnector`（接线模板）。**
- 验收门（每 task）：连接器 UT 绿（无 Mockito，fail-loud fake）+ checkstyle 0 + `tools/check-connector-imports.sh` 净 + **断言 assembled prop map / Hadoop conf / column flag vs legacy 期望值**（不能只断类名——parity-by-omission 风险）+ `grep` 确认 iceberg **不在** `SPI_READY_TYPES`。

---

# 📦 仓库 / 进度状态

- **工作分支 = `catalog-spi-10-iceberg`**（P6.1 起步前 HEAD = `e5959e1b53d` #64655 P3b）。branch off `branch-catalog-spi`，PR 时 squash 合并。**所有 commit 均未 push。**
  - **已 commit（旧 session）**：T01-T03 `ae54a2174ff`；T08 `d41fa4faf3e`（type-mapping read parity）；T04 `1fd4d42c297`（pom 7-flavor 闭包 + plugin-zip + fe/pom dM）。
  - **已 commit（T05 全套）**：`562201deb9f`（part1：metastore-spi `bindForType`/`supportsType` + 设计文档）；`6f7b292b5c6`（part2：factory `buildBaseCatalogProperties`/`appendS3FileIOProperties`/`chooseS3Compatible`）；`b0dbe91e1a5`（**part3：5-flavor appender + connector LIVE wiring + DriverShim + 52 parity 测试；UT 75/0/0、checkstyle 0、import-gate 净**）。**HEAD = 本 HANDOFF commit。**
  - **metastore 子线 = 已彻底 CLOSED**（8 文档加 CLOSED banner；后续勿读，见顶部范围注）。
- **stale cruft = 本 session 已清理**：删除 `fe-connector-{iceberg,paimon}-{api,backend-*}` 共 12 个目录（仅含 gitignored 生成物 `.flattened-pom.xml`，0 tracked、不在 reactor = 本地 `phase3-module-split` 旧实验遗留；untracked 故 git 无变更）。当前线用单 `fe-connector-iceberg` + flavor switch。
- **P0–P5 + P3 hybrid + P4 + P3b 全部已合入**（#63582/#63641/#64096/#64143/#64253/#64300/#64446/#64653/#64655）。iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- ⚠️ `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ `*.bak` + scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）**严禁 `git add -A`**，commit 前 path-whitelist。

## 🗺️ 代码脚手架（iceberg）

- **连接器（终态归宿）**：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/`（现：`IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps`）。**pom 闭包（T04）已就绪**：7-flavor SDK + hive-catalog-shade + metastore-spi。
- **paimon 模板**（P6.1 镜像）：`fe/fe-connector/fe-connector-paimon/`（`PaimonCatalogFactory`/`PaimonCatalogOps` seam/`createCatalogFromContext` CL-pin+executeAuthenticated/测试 `RecordingPaimonCatalogOps`/`FakePaimonTable`）+ `fe-connector-paimon-hive-shade`（paimon 专建 thrift shade，**iceberg 不复用，改用 hive-catalog-shade**）。
- **legacy 对照（P6.1 读路径）**：fe-core `datasource/iceberg/`（`IcebergMetadataOps`1362 读半 / `IcebergExternalTable`535 读 / `IcebergUtils`1826 schema-type / `DorisTypeToIcebergType`134 / 7 flavor catalog + `HiveCompatibleCatalog`181 + `dlf/`4）+ `datasource/property/metastore/`（`AbstractIcebergProperties`285 + 7 flavor + factory，**STILL-CONSUMED 留 fe-core 至翻闸后**；T05/T07 移植 per-flavor 装配的源）。
- **metastore-spi（Q2=B 已扩 part1）**：`fe/fe-connector/fe-connector-metastore-spi/`（`MetaStoreProviders.bind`(paimon)+**新 `bindForType(flavor,…)`(iceberg)** + `MetaStoreProvider.supportsType` + `HmsMetaStorePropertiesImpl.toHiveConfOverrides`/`DlfMetaStorePropertiesImpl.toDlfCatalogConf`）。无 glue/s3tables provider（这两者在连接器内直接装配）。**T04 已加为 iceberg 连接器依赖。**

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`。**漏 `-am`→`${revision}` 假错 / dependency:tree 找不到 reactor sibling**。**checkstyle 在 `validate` phase（编译前）跑**。连接器模块 art = `fe-connector-iceberg`。
- **plugin-zip 实查**（T04 起验闭包真相）：`mvn -pl :fe-connector-iceberg -am package -DskipTests` → `unzip -l target/doris-fe-connector-iceberg.zip | grep lib/`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- 测试无 Mockito（fail-loud fake）；live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。

## ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`）。
- message `[refactor](catalog) P6.1 iceberg: <subj>`（mirror #64653/#64655）+ 根因/解法/测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` +（本工作分支约定）`Claude-Session: …`（最终 squash 入上游时会被剥离）。
- PR base = `branch-catalog-spi`，squash 合并。历史 `catalog-spi-07-paimon` force-push 流程**已作废**。

## 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（参 P5-T29 教训；metastore-props 是 STILL-CONSUMED）。
- **HANDOFF 的依赖名可能过时**——T04 实证「iceberg-hive-metastore」「aliyun DLF SDK」均不存在（在 hive-catalog-shade 内）。下次动 pom/依赖前**先 recon（grep repo + unzip 实证）再信 HANDOFF**。
- **Q2=B（用户主动选）已落地 part1** —— `bindForType`/`supportsType` 已加（`562201deb9f`）；后续 flavor（T07 DLF）直接调 `bindForType("dlf",…)`，勿再重做 metastore-spi recon。
- **REST signing-cred 的 PROVIDER_CHAIN 非-DEFAULT gap**（见「🟡 T05 剩余」REST 条）：实现 REST 时若要补该 niche 路，需新增一个 `ConnectorContext` seam 让 fe-core 解析 `AwsCredentialsProviderFactory.getV2ClassName`（连接器不能 import）；DEFAULT mode 无缺口，可先 NOTE 跳过。
- **大文件（`IcebergUtils`1826 等）用 subagent 总结**（playbook §3.1），勿主线整读。
