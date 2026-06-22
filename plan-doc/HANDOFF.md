# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取子线**已彻底 CLOSED**（2026-06-22 收官，全部合入主线 #64446/#64653/#64655）——
> [`metastore-storage-refactor/`](./metastore-storage-refactor/) 文档仅作历史留存、**后续勿读**；需了解 metastore-spi 现状请直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 — **P6.1 继续：T05 收尾（factory 5-flavor appender + connector wiring + DriverShim + parity tests）→ T06（s3tables）→ T07（DLF port）→ T09（column parity）**

> **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**翻闸全有或全无，P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（翻闸只在 P6.6）。

## ✅ 本 session 产出（2026-06-22，T05 part1+part2 已 commit）

> 设计文档 = `plan-doc/tasks/designs/P6-T05-iceberg-catalog-assembly-design.md`（parity 契约 + 验证过的 iceberg-SDK key 字面量）。
> **已 commit**：part1 `562201deb9f`（metastore-spi `bindForType` + 设计文档），part2 `6f7b292b5c6`（factory common base + S3FileIO，纯静态，25 factory UT 绿）。

1. **6-agent code-grounded recon**（base+factory / rest / hms+hadoop / glue+jdbc / metastore-spi / paimon-mirror）→ 全 flavor parity 属性表 + paimon 镜像结构 + metastore-spi bind 形态。
2. **2 个跨模块决策（用户确认，CORRECT 过时 HANDOFF）**：
   - **D-061（S3FileIO `s3.*`/`aws.*` 方言）**：旧 HANDOFF「移植 `toS3FileIOProperties` 进连接器」**不可行**（它读 fe-core `S3Properties` getter,连接器不能 import）。改：连接器直读 `S3CompatibleFileSystemProperties`（**fe-filesystem-api 已存在**,纯 JDK getter,S3/OSS/COS/OBS 全实现,白名单允许）→ 本地拼 iceberg key（key 拼写留连接器,类型化数据来自 fe-filesystem 单一真源 D-003）。**无需新 ConnectorContext / fe-filesystem 方法**。影响 REST/JDBC/HADOOP。
   - **D-062（metastore-spi 复用,HMS+后续 DLF）= 已实现 GREEN**：providers 写死 `paimon.catalog.type`,iceberg 传 `iceberg.catalog.type` 不命中。改：`MetaStoreProvider.supportsType(String)`（抽象）+ `supports(Map)` 变 default 委托它（**paimon 字节不变**）+ `MetaStoreProviders.bindForType(flavor,props,storageConf)`；5 provider 全转。iceberg 调 `bindForType("hms",…)` 且**只用 `toHiveConfOverrides()` 不调 paimon `validate()`**（paimon-ism:requireWarehouse for all flavors;iceberg HMS 不要求 warehouse）。**验证:metastore-spi 全模块测试绿,dispatch 10/10（+3 新），paimon 既有测试全绿=parity 守住**。
3. **验证过的 iceberg-SDK key 字面量**（recon agent 猜错 2 处）：manifest cache 是**点分** `io.manifest.cache-enabled`；OAuth2 是**裸** `credential`/`token`/`scope`/`oauth2-server-uri`/`token-refresh-enabled`（非 `client.credentials.*`）；`AwsClientProperties.CLIENT_REGION` = `client.region`（非 `aws.region`）；S3FileIO=`s3.endpoint`/`s3.access-key-id`/`s3.secret-access-key`/`s3.session-token`/`s3.path-style-access`；assume-role=`client.assume-role.{arn,external-id,region}`+`client.factory`。

## 🟡 T05 剩余（next round = 纯实现，legacy 已全部读完精确映射如下）

> ✅ DONE（part2）：`buildBaseCatalogProperties`（copy-all+warehouse+manifest-cache）/ `appendS3FileIOProperties` / `chooseS3Compatible`。
> ⬜ 待做：3 个 per-flavor appender（纯静态）+ connector LIVE wiring + DriverShim + 测试。

**per-flavor appender 精确映射（已核 legacy 实体，无需再读）：**

- **REST**（`IcebergRestProperties`）：`uri`=firstNonBlank(`iceberg.rest.uri`,`uri`)（恒发,默认""）；`prefix`=`iceberg.rest.prefix`(非空才发)；`warehouse`(非空)；vended：`iceberg.rest.vended-credentials-enabled`(parseBool true)→`header.X-Iceberg-Access-Delegation`=`vended-credentials`；timeouts **恒发默认值**：`rest.client.connection-timeout-ms`=`iceberg.rest.connection-timeout-ms`??`10000`、`rest.client.socket-timeout-ms`=??`60000`；oauth2(`iceberg.rest.security.type`==oauth2)：有 credential→`credential`+(`oauth2-server-uri`?)+(`scope`?)+`token-refresh-enabled`(默认 `OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT`)；否则→`token`=`iceberg.rest.oauth2.token`。signing(`iceberg.rest.signing-name` 非空)：`rest.signing-name`/`rest.sigv4-enabled`(=`iceberg.rest.sigv4-enabled`)/`rest.signing-region`+creds：name∈{glue,s3tables}用 chosenS3（EXPLICIT→`rest.access-key-id`/`rest.secret-access-key`/(`rest.session-token`?)；ASSUME_ROLE→`appendAssumeRoleProperties`），否则用 `iceberg.rest.access-key-id`/`-secret-access-key`/`-session-token`。**GAP（NOTE）**：PROVIDER_CHAIN 非-DEFAULT 的 provider-class 需 fe-core `AwsCredentialsProviderFactory.getV2ClassName`（连接器不能 import；DEFAULT=no-op=常见情形,无行为缺口）。catalog-impl 由 connector 设。
- **GLUE**（`IcebergGlueMetaStoreProperties`+`AWSGlueMetaStoreBaseProperties`）：appendS3Props **无条件**发 5 个 `s3.*`(从 chosenS3 getter;chosenS3 空则跳过=legacy 发 5 空串的等价)；`glue.endpoint`=`AwsProperties.GLUE_CATALOG_ENDPOINT`(从 `glue.endpoint`/`aws.endpoint`/`aws.glue.endpoint`)；若 glueAccessKey(`glue.access_key`/`aws.glue.access-key`/`client.credentials-provider.glue.access_key`)&glueSecretKey(同构) 非空→`client.credentials-provider`=`com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x`+`client.credentials-provider.glue.access_key`/`...secret_key`+`aws.catalog.credentials.provider.factory.class`=`...ConfigurationAWSCredentialsProviderFactory`+(`...session_token`=`aws.glue.session-token`?)；否则若 glueIAMRole(`glue.role_arn`)非空→`client.factory`=AssumeRole+`aws.region`+`client.assume-role.arn`/`.region`+(`.external-id`=`glue.external_id`?)；最后 `client.region`=glueRegion(从 `glue.region`/`aws.region`/`aws.glue.region`,空则从 endpoint 正则提取,再空则 `us-east-1`)；`putIfAbsent(WAREHOUSE_LOCATION,"s3://doris")`。catalog-impl 由 connector 设；conf=null。
- **JDBC**（`IcebergJdbcMetaStoreProperties`）：`uri`=firstNonBlank(`uri`,`iceberg.jdbc.uri`)；addIfNotBlank `jdbc.user`=`iceberg.jdbc.user`、`jdbc.password`=`iceberg.jdbc.password`、`jdbc.init-catalog-tables`=`iceberg.jdbc.init-catalog-tables`、`jdbc.schema-version`=`iceberg.jdbc.schema-version`、`jdbc.strict-mode`=`iceberg.jdbc.strict-mode`；raw `jdbc.*` passthrough（已随 copy-all 进 opts,幂等）。**LIVE（connector）**：`iceberg.jdbc.catalog_name`(required)= iceberg catalogName **位置参数**（替代 ctx.getCatalogName()）且**从 opts 删**；DriverShim：`iceberg.jdbc.driver_url` 非空→注册（`JdbcResource.getFullDriverUrl`→fe-core,连接器需经 `ConnectorContext` env `jdbc_drivers_dir` 自解析或新 seam；driver_class required）。
- **HMS**：无额外 catalog-prop 派生；`bindForType("hms",props,storageConf)`→`toHiveConfOverrides(env hive_metastore_client_timeout_second??"10")`→HiveConf 当 conf；catalog-impl=HiveCatalog。**HADOOP**：仅 common+S3FileIO+Hadoop conf(storageHadoopConfig + 原 fs./dfs./hadoop. passthrough)+impl=HadoopCatalog；warehouse 必填。

- **`IcebergConnector.createCatalog`（LIVE）**：resolveFlavor → `buildBaseCatalogProperties` + impl(`resolveCatalogImpl`) → flavor switch（REST/GLUE/JDBC append + HADOOP/JDBC/REST 走 `chooseS3Compatible`+`appendS3FileIOProperties` from `ctx.getStorageProperties()`；HMS `bindForType`）→ remove `type` → TCCL-pin + `ctx.executeAuthenticated` → `CatalogUtil.buildIcebergCatalog`。镜像 `PaimonConnector.createCatalogFromContext`。
- **测试**：扩 `IcebergCatalogFactoryTest`（无 Mockito,复用 `FakeS3CompatibleStorageProperties`）；断言**装配后的 prop MAP vs 上面字面量 key**（防 parity-by-omission）；每断言带 WHY+mutation（Rule 9）。connector 级测 `catalog_name` 位置参数 + flavor 分支（用 `RecordingConnectorContext`）。

## 🔴 关键认知（写下来免下次重踩）

- **T04 残留风险（UT/打包不可见，仅 P6.6 docker plugin-zip e2e 真闸可验）**：
  1. `hive-catalog-shade` **内含** iceberg 1.10.1 与连接器直接 `iceberg-core` 在 child-first loader 共存——版本相同→预期 byte-identical benign（fe-connector-hive 同款已上线），但**首次** direct-iceberg + shade 组合，未 live 验。
  2. **glue 显式-AK 凭据 provider 类 `com.amazonaws.glue.catalog.credentials.*` 来源未定**（不在 hive-catalog-shade / iceberg-aws；fe-core `aws-java-sdk-glue` v1 疑源未证）→ **T05 glue flavor wiring 时核**（不挡 T04 闭包）。
  3. `apache-client` 经 awssdk 传递 runtime 入闭包（paimon 同款故意 ship，无害）。
- **silent 读路径 bug（骨架，翻闸后才在 regression 暴露）—— T08 已修 #1#1b#2；#3#4 待 T09**：
  3. ⬜ **format-version 算错（T09）**：`spec().specId()>=0?2:1` 恒 stamp "2"（unpartitioned specId==0 也 >=0）。应读 table `format-version` 元数据。`getTableSchemaStampsFormatVersionTwoForAnyValidSpec` 测当前 pin frozen "2"，T09 修时同步翻。
  4. ⬜ **column 构造 parity（T09）**：nullable 应恒 true（现 `field.isOptional()`）；isKey 应恒 true（现 5-arg ctor 默认 false）；缺小写化 + `WITH_TIMEZONE` Extra marker（`ConnectorColumn.withTimeZone()` 字段存在未用）；listing 缺 nested-namespace 递归 + view 过滤。
- **T05/T06/T07 实现前置 = Q2=B `MetaStoreProviders.bind` mini-recon（直接读代码，metastore 子线规划文档已 CLOSED 勿读）**：metastore-spi 现有 hms/dlf/filesystem/jdbc/rest provider 是 paimon-specific（`paimon.rest.*` key），无 glue/s3tables。HMS/DLF conf 复用点已确认是 `HmsMetaStorePropertiesImpl.toHiveConfOverrides` + `DlfMetaStorePropertiesImpl.toDlfCatalogConf`（SDK-free，已是 paimon dep）。mini-recon 读 **`fe/fe-connector/fe-connector-metastore-spi/`**（`MetaStoreProviders` + 现有 provider impl 形态），扩 iceberg provider。
- **跨切面风险（带入 T05–T07 + P6.6 翻闸门）**：R-004 AWS-SDK `ExecutionAttribute` static 撞（已用 child-first 自包含 awssdk 闭包缓解，待 docker 验）；DLF `ProxyMetaStoreClient` 按类名反射加载须入 plugin-zip 闭包（hive-catalog-shade 已带，T07 决定 vendored-source vs shade-bundled）；hive-catalog-shade relocated thrift vs host（已 exclude fe-thrift/libthrift）；**field-id 丢失**（`ConnectorColumn` 无载体，P6.2+ scan 前须重引，否则同 paimon BE SIGSEGV/DCHECK 类 bug）。**这些 UT 不可见，仅 P6.6 docker plugin-zip e2e 可验**。

## 🟢 下一步（精确）

- **首选 = P6-T05（5 CatalogUtil flavor）**：REST/HMS/GLUE/HADOOP/JDBC 完整 per-flavor 属性装配（含 manifest-cache + warehouse + JDBC DriverShim + `iceberg.jdbc.catalog_name`），从 fe-core `AbstractIcebergProperties` + 各 `Iceberg*MetaStoreProperties#initCatalog` 移植到连接器 `IcebergCatalogFactory`（现仅 flavor switch + class-name 解析）。**前置 = Q2=B `MetaStoreProviders.bind` mini-recon**（直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`；metastore 子线规划文档已 CLOSED 勿读）。
- **T05 后 = T06（s3tables bespoke `new S3TablesCatalog().initialize`）→ T07（DLF 子树 port：`DLFCatalog`+`HiveCompatibleCatalog`+`DLFTableOperations`+2 client pool + vendored `ProxyMetaStoreClient`，断 3 fe-core import，复用 `DlfMetaStorePropertiesImpl.toDlfCatalogConf`）**。
- **T09（column 构造 parity + format-version + nested-namespace + view 过滤）**：依赖 T03+T08，**现可起、与 T05 独立**——可作为 T05 的 metastore-spi mini-recon 阻塞时的并行项。T10 待 T05/T06/T07/T09。
- 验收门（每 task）：连接器 UT 绿（无 Mockito，fail-loud fake）+ checkstyle 0 + `tools/check-connector-imports.sh` 净 + **断言 assembled prop map / Hadoop conf / column flag vs legacy 期望值**（不能只断类名——parity-by-omission 风险）+ `grep` 确认 iceberg **不在** `SPI_READY_TYPES`。

---

# 📦 仓库 / 进度状态

- **工作分支 = `catalog-spi-10-iceberg`**（P6.1 起步前 HEAD = `e5959e1b53d` #64655 P3b）。branch off `branch-catalog-spi`，PR 时 squash 合并。
  - **已 commit**：T01-T03 = **`ae54a2174ff`**（`{IcebergCatalogFactory,IcebergCatalogOps}` + rewire + 测试基建 + docs）；T08 = **`d41fa4faf3e`**（type-mapping read parity 4 改 1 新 + docs）；**T04 = 本 commit**（`fe-connector-iceberg/pom.xml` + `plugin-zip.xml` + `fe/pom.xml` dM + 5 doc）。
  - **metastore 子线 = 已彻底 CLOSED + 本 session committed**（8 文档加 CLOSED banner；后续勿读，见顶部范围注）。
- **stale cruft = 本 session 已清理**：删除 `fe-connector-{iceberg,paimon}-{api,backend-*}` 共 12 个目录（仅含 gitignored 生成物 `.flattened-pom.xml`，0 tracked、不在 reactor = 本地 `phase3-module-split` 旧实验遗留；untracked 故 git 无变更）。当前线用单 `fe-connector-iceberg` + flavor switch。
- **P0–P5 + P3 hybrid + P4 + P3b 全部已合入**（#63582/#63641/#64096/#64143/#64253/#64300/#64446/#64653/#64655）。iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- ⚠️ `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ `*.bak` + scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）**严禁 `git add -A`**，commit 前 path-whitelist。

## 🗺️ 代码脚手架（iceberg）

- **连接器（终态归宿）**：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/`（现：`IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps`）。**pom 闭包（T04）已就绪**：7-flavor SDK + hive-catalog-shade + metastore-spi。
- **paimon 模板**（P6.1 镜像）：`fe/fe-connector/fe-connector-paimon/`（`PaimonCatalogFactory`/`PaimonCatalogOps` seam/`createCatalogFromContext` CL-pin+executeAuthenticated/测试 `RecordingPaimonCatalogOps`/`FakePaimonTable`）+ `fe-connector-paimon-hive-shade`（paimon 专建 thrift shade，**iceberg 不复用，改用 hive-catalog-shade**）。
- **legacy 对照（P6.1 读路径）**：fe-core `datasource/iceberg/`（`IcebergMetadataOps`1362 读半 / `IcebergExternalTable`535 读 / `IcebergUtils`1826 schema-type / `DorisTypeToIcebergType`134 / 7 flavor catalog + `HiveCompatibleCatalog`181 + `dlf/`4）+ `datasource/property/metastore/`（`AbstractIcebergProperties`285 + 7 flavor + factory，**STILL-CONSUMED 留 fe-core 至翻闸后**；T05/T07 移植 per-flavor 装配的源）。
- **metastore-spi（Q2=B 将扩）**：`fe/fe-connector/fe-connector-metastore-spi/`（`MetaStoreProviders.bind` + `HmsMetaStorePropertiesImpl`/`DlfMetaStorePropertiesImpl`，现 paimon-specific REST/DLF、无 glue/s3tables）。**T04 已加为 iceberg 连接器依赖。**

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
- **Q2=B 是用户主动选的非默认项** —— 扩 metastore-spi 前对 `MetaStoreProviders.bind` + 现有 provider impl 单独 mini-recon（**直接读代码** `fe/fe-connector/fe-connector-metastore-spi/`；metastore 子线规划文档已 CLOSED 勿读）。
- **大文件（`IcebergUtils`1826 等）用 subagent 总结**（playbook §3.1），勿主线整读。
