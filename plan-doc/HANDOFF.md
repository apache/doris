# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取是**独立子线**，单独跟踪在
> [`metastore-storage-refactor/`](./metastore-storage-refactor/)（其 HANDOFF/PROGRESS/tasks/decisions 自洽，本文件不复述细节）。

---

# 🎯 下一个 session 的任务 — **P6.1 继续：T05（5 CatalogUtil flavor）→ T06（s3tables）→ T07（DLF port）→ T09（column parity）**

> **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**翻闸全有或全无，P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（翻闸只在 P6.6）。

## ✅ 本 session 产出（2026-06-22，T08 commit + T04）

1. **T08 已 commit `d41fa4faf3e`**（commit 前先 re-verify `mvn -pl :fe-connector-iceberg -am test` = **36 run/0F/0E/0skip**，不凭 handoff 信任）。type-mapping read parity 3 修（TIMESTAMPTZ 名 + 点分 mapping-flag key + BINARY 无界长度）。
2. **T04（pom 7-flavor 依赖闭包）实现 + 验证 + commit（= 本 doc 同 commit）[D-060]**。
   - **关键更正（修 D-059 / 旧 HANDOFF 误述）**：2-agent code-grounded recon（unzip 实证）证 ① repo **无** `iceberg-hive-metastore` artifact——`org.apache.iceberg.hive.HiveCatalog`（hms）+ 反射加载的 `com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient`（dlf）均**捆在** `org.apache.doris:hive-catalog-shade`（127MB fat jar，thrift relocate `shade.doris.hive.org.apache.thrift`，内含 iceberg 1.10.1 + aliyun DLF SDK 2266 类）；② 无独立 aliyun DLF SDK 可加。**旧 HANDOFF「加 iceberg-hive-metastore + aliyun DLF SDK」不成立。**
   - **用户签字 [D-060]（AskUserQuestion）**：HMS/DLF 闭包在「复用 `hive-catalog-shade`（合 convention）」vs「专建精简 iceberg-hive-shade（仿 paimon-hive-shade）」vs「推迟」中 → **复用 hive-catalog-shade**。
   - **实改（无 Java 改；flavor 由 `CatalogUtil` 按类名反射加载 → T04 纯运行时闭包）**：`fe-connector-iceberg/pom.xml` + `fe-connector-metastore-spi`（Q2=B）+ `hive-catalog-shade`（hms/dlf）+ AWS SDK v2 child-first 集（s3/glue/sts/s3tables/s3-transfer-manager/sdk-core/url-connection-client/aws-json-protocol/protocol-core；glue/sts 排 apache-client）+ `s3-tables-catalog-for-iceberg`（s3tables）；`fe/pom.xml` + s3tables-catalog dM；`plugin-zip.xml` + `fe-thrift`/`libthrift` 排除（仿 fe-connector-hive）。
   - **验证**：36 UT + checkstyle 0 + import-gate 0 + `dependency:tree` iceberg-core 恰 1（1.10.1）+ **assembled plugin-zip 实查（143 jar）**：全 AWS SDK 在、全 `iceberg-*.jar` 皆 1.10.1 无 skew、`libthrift` 缺席、hadoop 仅 3.4.2；`SPI_READY_TYPES` iceberg 缺席（零行为变更）。
3. 顺手核清 worktree 的 **stale phase3-module-split cruft**（详见仓库状态）。

## 🔴 关键认知（写下来免下次重踩）

- **T04 残留风险（UT/打包不可见，仅 P6.6 docker plugin-zip e2e 真闸可验）**：
  1. `hive-catalog-shade` **内含** iceberg 1.10.1 与连接器直接 `iceberg-core` 在 child-first loader 共存——版本相同→预期 byte-identical benign（fe-connector-hive 同款已上线），但**首次** direct-iceberg + shade 组合，未 live 验。
  2. **glue 显式-AK 凭据 provider 类 `com.amazonaws.glue.catalog.credentials.*` 来源未定**（不在 hive-catalog-shade / iceberg-aws；fe-core `aws-java-sdk-glue` v1 疑源未证）→ **T05 glue flavor wiring 时核**（不挡 T04 闭包）。
  3. `apache-client` 经 awssdk 传递 runtime 入闭包（paimon 同款故意 ship，无害）。
- **silent 读路径 bug（骨架，翻闸后才在 regression 暴露）—— T08 已修 #1#1b#2；#3#4 待 T09**：
  3. ⬜ **format-version 算错（T09）**：`spec().specId()>=0?2:1` 恒 stamp "2"（unpartitioned specId==0 也 >=0）。应读 table `format-version` 元数据。`getTableSchemaStampsFormatVersionTwoForAnyValidSpec` 测当前 pin frozen "2"，T09 修时同步翻。
  4. ⬜ **column 构造 parity（T09）**：nullable 应恒 true（现 `field.isOptional()`）；isKey 应恒 true（现 5-arg ctor 默认 false）；缺小写化 + `WITH_TIMEZONE` Extra marker（`ConnectorColumn.withTimeZone()` 字段存在未用）；listing 缺 nested-namespace 递归 + view 过滤。
- **T05/T06/T07 实现前置 = Q2=B `MetaStoreProviders.bind` mini-recon**：metastore-spi 现有 hms/dlf/filesystem/jdbc/rest provider 是 paimon-specific（`paimon.rest.*` key），无 glue/s3tables。HMS/DLF conf 复用点已确认是 `HmsMetaStorePropertiesImpl.toHiveConfOverrides` + `DlfMetaStorePropertiesImpl.toDlfCatalogConf`（SDK-free，已是 paimon dep）。扩 metastore-spi 加 iceberg provider **跨入 metastore 子线，须读 `metastore-storage-refactor/`**。
- **跨切面风险（带入 T05–T07 + P6.6 翻闸门）**：R-004 AWS-SDK `ExecutionAttribute` static 撞（已用 child-first 自包含 awssdk 闭包缓解，待 docker 验）；DLF `ProxyMetaStoreClient` 按类名反射加载须入 plugin-zip 闭包（hive-catalog-shade 已带，T07 决定 vendored-source vs shade-bundled）；hive-catalog-shade relocated thrift vs host（已 exclude fe-thrift/libthrift）；**field-id 丢失**（`ConnectorColumn` 无载体，P6.2+ scan 前须重引，否则同 paimon BE SIGSEGV/DCHECK 类 bug）。**这些 UT 不可见，仅 P6.6 docker plugin-zip e2e 可验**。

## 🟢 下一步（精确）

- **首选 = P6-T05（5 CatalogUtil flavor）**：REST/HMS/GLUE/HADOOP/JDBC 完整 per-flavor 属性装配（含 manifest-cache + warehouse + JDBC DriverShim + `iceberg.jdbc.catalog_name`），从 fe-core `AbstractIcebergProperties` + 各 `Iceberg*MetaStoreProperties#initCatalog` 移植到连接器 `IcebergCatalogFactory`（现仅 flavor switch + class-name 解析）。**前置 = Q2=B `MetaStoreProviders.bind` mini-recon**（读 `metastore-storage-refactor/HANDOFF.md` + 现有 provider 形态）。
- **T05 后 = T06（s3tables bespoke `new S3TablesCatalog().initialize`）→ T07（DLF 子树 port：`DLFCatalog`+`HiveCompatibleCatalog`+`DLFTableOperations`+2 client pool + vendored `ProxyMetaStoreClient`，断 3 fe-core import，复用 `DlfMetaStorePropertiesImpl.toDlfCatalogConf`）**。
- **T09（column 构造 parity + format-version + nested-namespace + view 过滤）**：依赖 T03+T08，**现可起、与 T05 独立**——可作为 T05 的 metastore-spi mini-recon 阻塞时的并行项。T10 待 T05/T06/T07/T09。
- 验收门（每 task）：连接器 UT 绿（无 Mockito，fail-loud fake）+ checkstyle 0 + `tools/check-connector-imports.sh` 净 + **断言 assembled prop map / Hadoop conf / column flag vs legacy 期望值**（不能只断类名——parity-by-omission 风险）+ `grep` 确认 iceberg **不在** `SPI_READY_TYPES`。

---

# 📦 仓库 / 进度状态

- **工作分支 = `catalog-spi-10-iceberg`**（P6.1 起步前 HEAD = `e5959e1b53d` #64655 P3b）。branch off `branch-catalog-spi`，PR 时 squash 合并。
  - **已 commit**：T01-T03 = **`ae54a2174ff`**（`{IcebergCatalogFactory,IcebergCatalogOps}` + rewire + 测试基建 + docs）；T08 = **`d41fa4faf3e`**（type-mapping read parity 4 改 1 新 + docs）；**T04 = 本 commit**（`fe-connector-iceberg/pom.xml` + `plugin-zip.xml` + `fe/pom.xml` dM + 5 doc）。
  - **未 commit 遗留（非本 session 产物）**：`plan-doc/metastore-storage-refactor/HANDOFF.md`（metastore 子线收官 doc，2026-06-21 kickoff session 遗留，内容 accurate，与 T08/T04 无关——故未并入；可单独 commit 或随后续 P6 doc 批）。
- **stale cruft（无害，未动）**：worktree 有 `fe/fe-connector/fe-connector-iceberg-backend-{rest,hms,glue,hadoop,dlf,s3tables}` + `-api`（及 paimon 同款）目录，**仅含 gitignored 生成物 `.flattened-pom.xml`（2026-04，0 tracked 文件，不在 reactor）= 本地 `phase3-module-split` 分支遗留**（一个 per-backend-module 架构旧实验）。当前 June 线用单 `fe-connector-iceberg` + flavor switch，与之无关。
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

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-conf.groovy` 明文 key + `*.bak` + scratch + stale `*-backend-*/.flattened-pom.xml`）。
- message `[refactor](catalog) P6.1 iceberg: <subj>`（mirror #64653/#64655）+ 根因/解法/测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` +（本工作分支约定）`Claude-Session: …`（最终 squash 入上游时会被剥离）。
- PR base = `branch-catalog-spi`，squash 合并。历史 `catalog-spi-07-paimon` force-push 流程**已作废**。

## 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（参 P5-T29 教训；metastore-props 是 STILL-CONSUMED）。
- **HANDOFF 的依赖名可能过时**——T04 实证「iceberg-hive-metastore」「aliyun DLF SDK」均不存在（在 hive-catalog-shade 内）。下次动 pom/依赖前**先 recon（grep repo + unzip 实证）再信 HANDOFF**。
- **Q2=B 是用户主动选的非默认项** —— 扩 metastore-spi 前务必读 `metastore-storage-refactor/HANDOFF.md` + 对 `MetaStoreProviders.bind` 单独 mini-recon。
- **大文件（`IcebergUtils`1826 等）用 subagent 总结**（playbook §3.1），勿主线整读。
