# P4 实现设计 —— 缓存隔离（授权敏感缓存的越权修复，独立安全 track）

> 本文记录"缓存隔离"这一步的定稿设计。设计先行、经用户确认路线后实现。全部 `file:method:line` 来自本轮读码核实 + grounding workflow（`wf_ffad1480-f41`，6 reader recon + 3 安全断言对抗验证）。**本文不引任务代号，用户已豁免铁律 A（fe-core 可增源）。**

## 0. 问题 —— list ≠ load 的真实越权

某些 iceberg 目录（REST + `iceberg.rest.session=user`）按登录用户逐个鉴权：每个用户拿自己的委派令牌去远端 REST 服务器"加载表"，服务器决定该用户能否看这张表。**"能列出表名"与"能加载表"是两套独立授权**（Polaris/Unity 支持 per-table 授权）。

鉴权发生在**加载表的远端往返里**（`IcebergCatalogOps.loadTable:340-341` 走 per-user 委派 SDK 目录），FE 侧无独立鉴权层。**任何"命中缓存直接返回、不调 loadTable"的路径都跳过了鉴权** → 无权用户读到别人才该见的元数据（越权，非纵深防御）。用户已确认 list ≠ load。

## 1. Grounding 对原计划的三处纠正（对抗验证产出）

1. **真正活跃的泄漏只有两个，不是四个。**
   - `latestSnapshotCache`（**真漏**）：`beginQuerySnapshot`（`IcebergConnectorMetadata.java:1610-1622`）读它时 `loadTable` 在**未命中加载器里面**——命中就返回、不加载表。按表名建键、无用户维度、无条件构建、session=user 下仍注入 per-user metadata（`IcebergConnector.java:202-203/255-257`）。默认 24h TTL、每条查询走（`SUPPORTS_MVCC_SNAPSHOT` 无条件），活跃。泄漏值 = `(snapshotId, schemaId)` 两个 long。
   - `partitionCache` / `formatCache`（**非活跃漏**）：每个读取点都**先调 per-user `resolveTableForRead`/`resolveTable`（现载、因 session=user 下 tableCache=null）**，无权用户在那步被拦。今天安全，但依赖"上游恰好先加载表"，**脆弱**。
   - 先例：`commentCache`（`IcebergConnector.java:223-232`）早已对 session=user 排除，注释明说"共享 comment 缓存绕过 per-user loadTable 鉴权 = 元数据披露"——同一类威胁，当年用排除解决。
2. **fe-core 表结构缓存是第二个真漏。** `SchemaCacheKey` 只按 `nameMapping`（`SchemaCacheKey.java`），**无 schema 缓存 bypass**。开发者给库/表名缓存加了 bypass 挡越权（`shouldBypassDbNameCache/TableNameCache`），却漏了 schema 缓存。无权用户命中 → 拿到别人的列结构。
3. **异构 HMS 网关是"潜在"洞，不是"最尖的活跃洞"。** 网关内嵌 iceberg 兄弟被**无条件强制 `iceberg.catalog.type=hms`**（`IcebergSiblingProperties.synthesize:62-63`），而 session=user 要求 REST flavor → **兄弟永不可能 session=user**，泄漏今天不可达（被 hms-forcing 不变量挡住，非被 fe-core 门挡住）。原计划"传属主标签到 fe-core 分片"被证伪：属主标签只是类型判别串（`"iceberg"`/`"hudi"`），非身份/能力，且活在每语句作用域、不桥接 fe-core 跨语句缓存。存在的是**结构性隐患**（fe-core bypass 只看前门 hive 能力、看不到被委派兄弟能力），作前瞻加固处理。

## 2. 用户决策（2026-07-20 签字）

- **路线 = 禁用**（非身份分片）：授权敏感的跨查询缓存在 `isUserSessionEnabled()` 时置空。→ 无需新 SPI `getIdentityShardKey()`；**无撤权陈旧窗口**（每次现载即时鉴权）；无后台线程身份漂移风险；与现有 `tableCache`/`commentCache` 先例完全一致。session=user 本就是"每次现载原始表"档位，投影现算成本可忽略。
- **范围 = 三个投影缓存统一处理**：`latestSnapshotCache`(真漏) + `partitionCache` + `formatCache` 一并禁用 → 铁律"session=user ⇒ iceberg 无活跃跨查询元数据缓存"成立，移除脆弱依赖。
- **异构网关 = 加 fail-loud 守卫**：网关建 iceberg 兄弟时断言兄弟非 session=user；今天恒不触发（兄弟强制 hms），守未来。
- **威胁模型签字**：影响面仅 REST + `iceberg.rest.session=user` + fe-core schema 缓存；泄漏为元数据（snapshotId/schemaId + schema 列）非凭证/数据；禁用路线下无撤权陈旧窗口。

## 3. Seam-by-seam 实现（四小步，各独立 commit）

### 4a · iceberg 三投影缓存禁用（`fe-connector-iceberg`）
- **构造门控**（`IcebergConnector.java:202-222`）：三个缓存均改 `isUserSessionEnabled() ? null : new Iceberg*Cache(...)`（镜像 tableCache 的 null 门；tableCache 门为 `isUserSessionEnabled() || restVended`，投影缓存与凭证过期轴无关故只需前者）。
- **失效路径补 null 守卫**（`IcebergConnector.java` `invalidate/invalidateDb/invalidateAll`，行 567/571/572、591/595/596、611/615/616）：三缓存的失效调用当前**无条件**，null 化后 ALTER/REFRESH/DROP 会 NPE → 加 `!= null` 守卫（镜像 tableCache/commentCache 已有守卫）。
- **`beginQuerySnapshot` 补 null 回退**（`IcebergConnectorMetadata.java:1614`）：`latestSnapshotCache != null ? getOrLoad(...) : <loader 本体>`（镜像 `resolveTableForRead:614` 的 tableCache 三元）。
- **读取点已 null-safe，无需改**：`partitionCache`（`IcebergPartitionUtils.loadRawPartitions:744-745` 有 `cache == null` 回退）、`formatCache`（`IcebergWriterHelper.resolveFileFormatName:319` 有 `cache == null` 回退）。
- 测试：session=user 目录下三 `*CacheForTest()` 返回 null；`beginQuerySnapshot` 在 null 缓存下仍正确 pin（每次现载）；非 session=user 目录字节不变（缓存仍在）。

### 4b · fe-core 表结构缓存 bypass（`fe-core`，镜像名字缓存 bypass）
- **基类默认**（`ExternalCatalog.java`，`shouldBypassDbNameCache:348` 附近）：`protected boolean shouldBypassSchemaCache(SessionContext ctx) { return false; }`。
- **插件 override**（`PluginDrivenExternalCatalog.java:1202` 附近）：`return supportsUserSession() && ctx != null && ctx.hasDelegatedCredential();`（与 `shouldBypassTableNameCache` 同判据同措辞）。
- **读取点拦截**（`ExternalTable.getSchemaCacheValue():435`，唯一 key 构造点）：bypass 时 `initSchemaAndUpdateTime(key)` 现读、不碰共享缓存。覆盖 MVCC 路径（`PluginDrivenMvccExternalTable` 的 latest 分支经 `cachedSchemaCacheValue()→super.getSchemaCacheValue()` 汇入本方法；time-travel pin 已 per-user 解析）。
- 测试：session=user + 有委派凭证 → bypass（现读，不入缓存）；无凭证 → 保留缓存（fail-closed 在连接器侧）；非插件目录默认 false 不变。

### 4c · 异构网关 fail-loud 守卫（`fe-connector-hive`）
- `HiveConnector.getOrCreateIcebergSibling():510` 建成兄弟后断言 `!sibling.getCapabilities().contains(SUPPORTS_USER_SESSION)`，违则抛（消息说明：前门 hive 非 session=user，fe-core per-user 缓存 bypass 不触发，session=user 兄弟会静默泄漏；兄弟应恒 hms flavor）。今天恒不触发。
- 测试：正常兄弟（hms flavor）不抛；桩一个 session=user 兄弟 → 抛。

### 4d · 防漂移门禁（`tools/check-authz-cache-sharding.sh` + `.test.sh` + maven validate）
- 仿 `check-fecore-metadata-funnel.sh`：standalone bash grep gate，RED/GREEN mktemp 自测，exec-maven-plugin validate 挂 `fe-connector` pom（`inherited=false`）。
- 目标：`IcebergConnector.java` 构造里每个 `this.\w*[Cc]ache =` 赋值语句（到 `;` 的整条）须**含 `isUserSessionEnabled(`** 或带豁免标记 `authz-cache-exempt: <理由>`（供 manifestCache 这类 default-off + 读在 per-user load 之后的缓存）。缺则构建失败——把"加了新缓存忘了对 session=user 处理"从静默泄漏变构建失败。
- manifestCache（`IcebergConnector.java:181`，default-off `meta.cache.iceberg.manifest.enable` + 读在 loadTable 之后）用 `authz-cache-exempt` 标记 + 一句理由。

## 4. 后续
- 越权 e2e（can-list-cannot-load 命中不泄漏 + 异构网关）落 `regression-test/suites/external_table_p2/refactor_catalog_param`，随读写共用步骤欠的异构网关 e2e 一并"择机统一补"。

## 5. 不做
- 不新增 `getIdentityShardKey()` SPI（禁用路线不需要）。
- 不动 `partitionCache`/`formatCache` 的读取点（已 null-safe）。
- hive/paimon/hudi 无 SUPPORTS_USER_SESSION、无按用户授权轴，不动。
