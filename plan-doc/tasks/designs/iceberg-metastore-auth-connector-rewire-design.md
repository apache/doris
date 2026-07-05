# Design — iceberg 属性/鉴权迁移到连接器 + fe-core iceberg 属性簇删除

> 权威执行设计（对应 removal-execution-plan **§P5**）。基于 2026-07-05 的 recon 工作流 `wf_73999dcf-412`（6 readers + synth + 独立交叉核对）逐文件核验产出。**用户 2026-07-05 裁定：完整删除、一次到位**（接受鉴权刀 flip-gated 本地无法验证）。
>
> **⚠️ 起步先读本节：recon 纠正了原计划的核心前提**（Rule 7 冲突已暴露并裁定）。

## 0. 现状（recon 确认，信控制流不信旧行号）

翻闸后 plugin iceberg catalog 经 fe-core 属性簇产出**三样活的东西**，其余全是死码（连接器已重实现）：

1. **鉴权器（authenticator）** — `Iceberg*MetaStoreProperties` 构造 `HadoopExecutionAuthenticator`：
   - HMS flavor：`IcebergHMSMetaStoreProperties.initNormalizeAndCheckProps` eager 建（从 `hmsBaseProperties.getHmsAuthenticator()`）。
   - FileSystem flavor：`IcebergFileSystemMetaStoreProperties.initExecutionAuthenticator(storageList)` lazy 建（仅当单一 `HdfsProperties && isKerberos`）。
   - cloud flavor（rest/glue/s3tables/dlf/jdbc）：继承 `AbstractIcebergProperties` 默认 no-op。
   - 交付：`PluginDrivenExternalCatalog.initPreExecutionAuthenticator:142-154` → `catalogProperty.getMetastoreProperties().getExecutionAuthenticator()` → `DefaultConnectorContext(name,id,this::getExecutionAuthenticator,…)` lazy supplier → `IcebergConnector` 包进 `TcclPinningConnectorContext`。
   - **关键实证**：真 Kerberos 存储（`hadoop.security.authentication=kerberos`）时，连接器 `pluginAuthenticator()` 自建 plugin-UGI 鉴权器并**绕过** fe-core 鉴权器（`TcclPinningConnectorContext:108 auth.doAs`）。fe-core 鉴权器**唯一做实事的场景 = HMS-metastore-kerberos + SIMPLE 存储**（如 Kerberos HMS + S3 数据）：此时 `pluginAuthenticator()` 返回 null → `TcclPinningConnectorContext:104` delegate 到 fe-core 建的 HMS 鉴权器做 metastore thrift 登录。**这就是 CUT 1 必须补上的缺口。**

2. **vended 凭据判定（gate）** — `CatalogProperty.initStorageProperties` 经 `VendedCredentialsFactory.getProviderType(msp)`，iceberg 是**最后一个** `case ICEBERG`（→ `IcebergVendedCredentialsProvider`，cast `IcebergRestProperties` 读 `iceberg.rest.vended-credentials-enabled`）。vended REST → 静态 storage map 建成**空**（每张表凭据由连接器 `IcebergScanPlanProvider.extractVendedToken` 供）。fe-core 的 per-table 提取路径在 plugin path **已死**（仅 HMS-iceberg `IcebergScanNode` 走）。paimon 已迁到中立 SPI `msp.isVendedCredentialsEnabled()`（`CatalogProperty:186-192`）——**iceberg 是唯一残留 SDK 分支**。

3. **derived-storage（warehouse→fs.defaultFS 桥）** — 仅 `IcebergFileSystemMetaStoreProperties.getDerivedStorageProperties():90-100` override：`warehouse=hdfs://<ns>/path` → `singletonMap(fs.defaultFS, hdfs://<ns>)`，供 HA-nameservice hadoop catalog 只配 warehouse 时 BE 仍能绑定 HDFS。**活行为码，非死码**，paimon 无此 override（无样板）。在 `CatalogProperty.initStorageProperties` 里 **连接器构造之前** 消费、且要喂 fe-core→BE map → **不能进连接器 metastore-spi，只能重新安置到 fe-core 非簇代码**。

## 1. Rule 7 更正（recon 推翻原计划前提）

- **原计划**："给连接器 metastore provider 补 authenticator 构建（镜像 paimon 已有做法）"。
- **实证**：**paimon 的 HMS 鉴权器也在 fe-core 建**（`PaimonHMSMetaStoreProperties:60`，与 iceberg 同型），paimon 连接器**不**自建 HMS 鉴权器（只在存储 kerberos 时自建，与 iceberg 连接器同）。fe-core paimon 属性簇**完好、仍在用**。→ **"镜像 paimon 的连接器侧鉴权"无样板**；真删 fe-core iceberg 簇必须做 paimon 都没做的新东西（连接器自建 HMS 鉴权器）= CUT 1。
- **paimon 真正的样板仅在 vended gate**（中立 `isVendedCredentialsEnabled()`）= CUT 3 依据。
- **方向仍正确（pro-Trino）**：Trino 连接器自持元数据鉴权 + 存储凭据解析，引擎核心只给中立 session/identity + doAs 执行面。本仓 metastore-spi 边界正是照此设计。P5 删掉 fe-core 凭据层最后一处引擎专属耦合。
- **by-design 保留（P5 后仍在 fe-core，非泄漏）**：`ConnectorContext.executeAuthenticated` doAs 面、存储凭据 NORMALIZATION 到 BE 规范 AWS_*/hadoop 键（`getBackendStorageProperties`/`vendStorageCredentials`，paimon 同赖）、fe-filesystem provider registry。**P5 不得把 normalization 挪进连接器**——边界是"连接器供 raw token/typed StorageProperties，fe-core 归一化"。

## 2. 已裁定的子决策（recon 推荐 default，本设计采纳）

- **[D1] 鉴权器建在哪** = 连接器内 `IcebergConnector.pluginAuthenticator()` bespoke 建（从 `hms.kerberos()` facts），**不新增 SPI 方法**（铁律：无投机 SPI 面；Rule 2/3）。仅 iceberg，paimon 待后续 follow-up 复制同 pattern。
- **[D2] 删除深度** = 完整删（用户裁定）。两个 storage hook 重新安置到 fe-core 非簇代码：vended gate → 从 raw prop 算（CUT 3）；derived-storage → 独立 helper / 通用化 HDFS warehouse 检测（CUT 4）。
- **[D3] paimon 残留** = iceberg-only，登记 paimon follow-up（`[FU-paimon-hms-connector-auth]`）；CUT 1 写成可复制 pattern。
- **[D4] fail-loud vs 静默 no-op** = iceberg 鉴权改**确定性 + kerberos 误配 fail-loud**（Rule 12）；`PluginDrivenExternalCatalog.initPreExecutionAuthenticator:157` 的通用 catch-swallow 仅保留给非 metastore 类型（jdbc/es）。

## 3. 刀序（7 刀，每刀独立 commit + build + test + checkstyle 0 + import-gate 净）

> PREP（1-4，行为保持的增项/重新安置，各自独立可落）→ REWIRE（5，翻掉 plugin 路径对簇的依赖）→ DELETE（6,7）。CUT 5 需 1-4 全落；6/7 需 5。

### ✅ CUT 1（DONE `cf8dda9f058`，PREP，novel/最高风险，flip-gated）= 连接器自建 HMS-metastore Kerberos 鉴权器
- **改**：`IcebergConnector.pluginAuthenticator()`（~738）。现仅 storage-kerberos gate（`properties.get("hadoop.security.authentication")=="kerberos"` → 从 storage conf 建）。**新增**：storage gate 不命中时，若 flavor==hms 且 `hms.kerberos()`（`MetaStoreProviders.bindForType("hms",…)` → `HmsMetaStoreProperties.kerberos()`）present-with-creds → `HadoopAuthenticator.getHadoopAuthenticator(new KerberosAuthenticationConfig(spec.getPrincipal(), spec.getKeytab(), …))`。
- **等价证明**：`AbstractHmsMetaStoreProperties.kerberos()` javadoc 明写 mirrors `HMSBaseProperties.initHadoopAuthenticator`；fe-core `HMSBaseProperties:176-180` 正是 `new KerberosAuthenticationConfig(clientPrincipal, clientKeytab,…)` → `getHadoopAuthenticator`。连接器 `HadoopAuthenticator`+`KerberosAuthenticationConfig` 均 fe-kerberos（child-first bundled，非 fe-core 依赖）。
- **precedence**：storage-kerberos（共用 principal 的常见场景）仍走现有 storage-conf 路（不改）；仅补 HMS-kerberos-simple-storage 窄场景。
- **✅ 落地**：抽包内 static `IcebergConnector.buildPluginAuthenticator(properties, storageHadoopConfig)`；storage-kerberos 分支逐字不变；新增 HMS 分支用 `hms.kerberos()` 的 client principal/keytab 建 `KerberosAuthenticationConfig`（镜像 `HMSBaseProperties.initHadoopAuthenticator:176-180`）。新增 `IcebergConnectorPluginAuthenticatorTest` 5 例。**验收**：fe-connector-iceberg BUILD SUCCESS + checkstyle 0 + 5/5 绿 + mutation 击杀（翻 HMS 分支守卫 → 焦点用例转红）。
- **⚠️ flip-gated（未跑，登记 ENG-3）**：真正证明须 Kerberized-HMS-on-simple-storage e2e（本地无集群）。注意 CUT 1 使 HMS-kerberos-simple-storage 场景从 fe-core-delegate doAs 立即改走 plugin-UGI doAs（同 principal/keytab，且 plugin-UGI 才是 plugin FileSystem 的正确副本 → 更正确），但该行为变更须 e2e 证明。

### CUT 2（PREP，纯机械）= SHOW CREATE 脱敏改绑，脱离 IcebergRestProperties
- **改**：`DatasourcePrintableMap:63` `getSensitiveKeys(IcebergRestProperties.class)` → 等价来源（连接器 `IcebergRestMetaStoreProvider.sensitivePropertyKeys()` 或显式 key list）。
- **UT**：断言脱敏 key 集合 delete 前后**逐字不变**（防 SHOW CREATE CATALOG 密钥泄漏静默回归）。

### CUT 3（PREP，align paimon）= iceberg vended gate 走 raw prop（SDK-free）
- **改**：使 iceberg vended 判定独立于 SDK provider——从 raw `iceberg.rest.vended-credentials-enabled` 算（paimon-style `isVendedCredentialsEnabled` gate，`CatalogProperty:186-192`），使其 `msp==null`（CUT 5 后）仍成立。**暂留** `VendedCredentialsFactory case ICEBERG`（仍返回同 boolean，本刀行为保持）；实删在 CUT 7。
- **⚠️ 起步先解的设计难点（recon 后新发现，非 CUT 1 阻塞）**：paimon 靠 `msp.isVendedCredentialsEnabled()` 成立**是因为 paimon 保留 msp**；iceberg CUT 5 后 msp==null → 该机制失效 → vended 判定必须能在 msp==null 下工作。但通用 `CatalogProperty.initStorageProperties` 是引擎无关的，直接读 iceberg 专属串 `iceberg.rest.vended-credentials-enabled` 逼近铁律（引擎名判别新 seam）。**候选**：① 该 gate 在 CUT 5 前（msp 尚在）仍走 `msp.isVendedCredentialsEnabled()`，CUT 5 把"iceberg 无 msp 时 vended?"的判定连同鉴权/derived-storage 一起在 `PluginDrivenExternalCatalog`/连接器侧解决（连接器构造前的 init-order 阻塞使连接器供不了 → 需另设中立途径）；② 用中立、非引擎前缀的 vended 标志键。**须在动 CUT 3/CUT 5 前定夺**（届时按 `ask-user-explain-in-chinese-first` 找用户拍板或给出推荐）。
- **UT**：raw prop true/false/缺省 三态断言 vended gate 结果与今日一致。

### CUT 4（PREP）= warehouse→fs.defaultFS 重新安置，脱离待删类
- **改**：**首选（connector-agnostic）** 通用化 HDFS storage 检测使 `HdfsProperties` 对任意 engine 从 `hdfs://` warehouse 推 `fs.defaultFS`；**或** 把 `IcebergFileSystemMetaStoreProperties.getDerivedStorageProperties():90-100` 逐字抽到 fe-core 独立 helper（keyed on raw props）。
- **UT**：只配 warehouse 的 HA-nameservice hadoop catalog 仍绑同一 `fs.defaultFS`。

### CUT 5（REWIRE，flip-gated）= plugin 路径停止构造 fe-core iceberg 簇
- **改**：删 `MetastoreProperties` 对 `Type.ICEBERG` 的 register + `IcebergPropertiesFactory` dispatch → type=iceberg 的 `getMetastoreProperties()` 返回 null（对齐 jdbc/es 既有 null-msp 路径）。CUT 1-4 落地后三活 hook 均有非簇归宿：鉴权器 ← 连接器（CUT 1）、vended gate ← raw prop（CUT 3）、derived-storage ← CUT 4。`initPreExecutionAuthenticator` 对 msp==null 跳过 fe-core 鉴权器（连接器独占）；实现 [D4] fail-loud。
- **⚠️ 全 flavor e2e**（hms/hadoop/rest/glue/s3tables/dlf/jdbc）——这是翻掉簇的一刀，独立 commit。

### CUT 6（DELETE）= 死连通性探测
- 删 4 个 `Iceberg*ConnectivityTester` + `AbstractIcebergConnectivityTester` + `CatalogConnectivityTestCoordinator:284-303` iceberg 臂 + imports。**前置核验**：iceberg override `checkWhenCreating`→`connector.testConnection`，从不走 base ExternalCatalog 连通性 → 臂死。

### CUT 7（DELETE）= fe-core iceberg 属性簇
- 删 `IcebergPropertiesFactory`、`AbstractIcebergProperties`、`Iceberg{HMS,Glue,FileSystem,Jdbc,S3Tables,AliyunDLF,Rest}MetaStoreProperties`、`property/common/IcebergAws{ClientCredentials,AssumeRole}Properties`、`datasource/iceberg/IcebergVendedCredentialsProvider`、`VendedCredentialsFactory case ICEBERG:62-63`、`MetastoreProperties` enum `ICEBERG` 常量 + register。CUT 1-6 后零活引用。**保留**：`InitCatalogLog/InitDatabaseLog.Type.ICEBERG`（GSON 回放）、`TableType.ICEBERG_EXTERNAL_TABLE`、GsonUtils 字符串别名。

## 4. 风险（静默回归，多数 flip-gated；每刀 mutation 击杀防之）
- HMS-kerberos+simple-storage 今仅靠 fe-core HMS 鉴权器 → 缺 CUT 1 直接删会**静默降级 SIMPLE**（无单测红，仅 Kerberized-HMS e2e 暴露）。
- CUT 1 把该路径从 FE-app-UGI doAs 改 plugin-UGI doAs → 须 e2e 验证 principal/keytab 解析一致（两处独立解析须一致）。
- warehouse→fs.defaultFS：只配 warehouse 的 HA catalog 缺 CUT 4 会丢 HDFS 绑定（单 NN 测试目录带显式 fs.defaultFS，抓不到）。
- CUT 5 后 msp==null，vended REST 须仍建空 map（缺 CUT 3 先落 → 建非空/抛错，喂 BE 陈旧凭据）。
- SHOW CREATE 密钥泄漏（缺 CUT 2）。
- iceberg/paimon 鉴权 divergence（iceberg 连接器自持、paimon 仍 fe-core）→ 维护缝，靠 [FU-paimon-hms-connector-auth] 收敛。

## 5. 验收口径（Rule 12）
- 每刀：fe-core（或连接器）BUILD SUCCESS + 相关 UT 全绿 + checkstyle 0 + import-gate 净 + mutation 击杀。
- **flip-gated 诚实**：CUT 1/CUT 5 的真 Kerberos/全 flavor e2e 本地无集群 → 标注未跑、登记 ENG-3，**不谎称已验**。
