# 决策日志（ADR，append-only，时间倒序）

> 编号 `D-NNN` **仅在本子项目内有效**，与上层 `../decisions-log.md` 独立。
> 新决策写在顶部。修改设计文档某节时，在该节加 `（D-NNN）` 脚注。

---

## D-015 — P2-T03 JDBC driver 注册副作用留连接器，仅纯 `resolveDriverUrl` 共享（不下移注册）
- **日期**：2026-06-18 ｜ **决策者**：用户（AskUserQuestion 选「方案 A：注册留连接器（推荐）」）
- **背景**：P2-T02 只上移了纯 `JdbcDriverSupport.resolveDriverUrl`（其 javadoc 明记 live 注册「无调用方、P2-T03 前不搬，Rule 2」）。HANDOFF 把「driver 注册下移与否」列为 P2-T03 决策点。driver 逻辑两消费方：①`PaimonConnector`（FE 侧）真执行注册（`DriverManager.registerDriver`+`DriverShim`+静态 `DRIVER_CLASS_LOADER_CACHE`/`REGISTERED_DRIVER_KEYS`）；②`PaimonScanPlanProvider.getBackendPaimonOptions`（BE 选项）只解析 URL 不注册。唯一共享=`resolveDriverUrl`。
- **内容**：**注册副作用留 `PaimonConnector` 不动**；P2-T03 仅把两处 `PaimonCatalogFactory.resolveDriverUrl`（删）改调 `JdbcDriverSupport.resolveDriverUrl`（字节相同）。
- **理由**：单消费方（FE 注册）→ 下移是为 hive/iceberg 将来服务的投机（Rule 2）；metastore-spi 刻意 SDK/JVM-副作用-free（DV-007 保持 hadoop/fs-free），注入 `DriverManager` 全局变更+活 `URLClassLoader`+`Class.forName` 模糊 api/spi 纯净边界；`DriverShim` 的 loader 身份对 DriverManager 接受是 load-bearing 的，子优先插件 loader 下迁移它有 classloader 风险而零功能收益（CI RCA 记忆 RC-1/3/5 反复证 classloader 危害）。**被否**：方案 B（下移注册）。
- **影响**：`JdbcDriverSupport` javadoc「注册留待 P2-T03」状态=**已决定不下移**（待 hive/iceberg 真第二消费方时再议）；无新模块改动。

## D-014 — P2-T03 采用 spi 的 legacy-faithful validate（CREATE CATALOG 比当前 paimon 更严，故意收敛）
- **日期**：2026-06-18 ｜ **决策者**：用户（AskUserQuestion 选「采用 spi validate（legacy-faithful）」）
- **背景**：P2-T02 的 spi `validate()` 故意比 paimon 手抄 `PaimonCatalogFactory.validate` 严，恢复了真 legacy 规则（手抄版漏掉的）：HMS **case-sensitive** `forbidIf(simple)`/`requireIf(kerberos)`（client principal+keytab）、REST **case-sensitive** `"dlf".equals(token.provider)`（手抄是 equalsIgnoreCase）、DLF 在 **CREATE** 要求 OSS 存储（手抄在 catalog-build 时 `requireOssStorageForDlf`）。P2-T02 只建 spi、无消费方；P2-T03 cutover 是这些规则首次作用于真实 CREATE CATALOG。
- **内容**：cutover **直接采用** `MetaStoreProviders.bind(props,{}).validate()`，接受 CREATE CATALOG 比当前 paimon 更严（今天通过 paimon 宽松检查的某些 catalog 现在会在 CREATE 被拒）。这是项目 T2「等价=对齐 legacy 非对齐手抄」目标的兑现。
- **理由**：spi 的更严规则才是权威 legacy（`HMSBaseProperties.buildRules`/`AliyunDLFBaseProperties`/`ParamRules`）；保留手抄宽松行为需 compat shim（更多代码、偏离目标、spi validate 部分闲置）。**被否**：方案 B（compat shim 保 CREATE 字节兼容）。
- **影响**：行为变更 = 三处更严校验在 CREATE CATALOG 生效；unknown-flavor 错误文案从「Unknown paimon.catalog.type value: X」→ bind 的「No MetaStoreProvider supports...」（两者皆 IAE→DdlException，CREATE 仍失败）；DLF S3-only 拒绝时机 build→CREATE（无 reload 回归：无效 catalog 在新模型下根本无法 CREATE 故不会持久化/reload）。测试：`PaimonConnectorValidatePropertiesTest` 钉新行为（3 tightening RED→GREEN）。

## D-013 — Kerberos 中立 facts 类型（AuthType + KerberosAuthSpec）落 fe-kerberos，先于 P2-T01 建；metastore-api 依赖 fe-kerberos
- **日期**：2026-06-18 ｜ **决策者**：用户（AskUserQuestion 选「In fe-kerberos (build it first)」）
- **背景**：P2-T01 的 `HmsMetaStoreProperties` 需要 `AuthType`(SIMPLE/KERBEROS) + `KerberosAuthSpec`(principal/keytab facts)。`AuthType` 现仅在 `fe-common`（连接器 import gate 禁）；设计把 `KerberosAuthSpec` 归 `fe-kerberos`（P3a-T01，原排在 P2-T02 之后）→ P2-T01 引用它有构建顺序冲突。
- **内容**：**遵循 D-007 字面归属** = 这两个中立 facts 类型落 **`fe-kerberos`**；**先建 fe-kerberos 的 facts-carrier 切片**（`AuthType` + `KerberosAuthSpec`，纯 Java、零 hadoop 依赖）于 P2-T01 **之前**；`fe-connector-metastore-api` **依赖 fe-kerberos**（设计 §3.1 header 依赖集 +fe-kerberos）。fe-kerberos 仍是顶层中立叶子（authenticator 机制子集 = hadoop 依赖部分留待 P2-T02 消费时增量补，仍属 P3a-T01 scope）。
- **被否**：「`KerberosAuthSpec`/`AuthType` 落 metastore-api 自包含」（更简、不改顺序，但偏离 D-007 归属；用户选保持 D-007 单一真相源）。
- **核实**：现有 `fe-authentication` 模块是**用户登录/角色映射**鉴权（password 插件/Principal/role-mapping），与 hadoop service kerberos **无关**，**不复用**；fe-kerberos 确为新建。
- **影响**：实施顺序 = **P3a-T01（facts-carrier 切片）→ P2-T01 → P2-T02（补 authenticator 机制 + 消费 facts）**；WORKFLOW §4.1 白名单 +`fe/fe-kerberos/**` + `fe/pom.xml`（新增 `<module>fe-kerberos</module>`，已属「仅新增模块声明」允许）；设计 §3.1 header 注「+fe-kerberos」；tasks P3a-T01 标 🚧（facts-carrier 落地，机制待续）。**fe-kerberos facts-carrier 零 hadoop**：`AuthType` enum(SIMPLE/KERBEROS) + `KerberosAuthSpec`(client `principal`+`keytab` 中立标量——doAs 登录事实；HMS service principal 是 HiveConf override 不在此，镜像 fe-common `KerberosAuthenticationConfig` 字段)。

## D-012 — 跳过/推迟 P1-T06 docker 验证，直接进入 P2（metastore SPI）；docker 验证集中到 P2-T05
- **日期**：2026-06-18 ｜ **决策者**：用户（「跳过 p1-t06，先开始做下一阶段」）
- **内容**：**不在此刻跑 P1-T06**（P1 storage 收口的 docker 5-flavor 真等价闸）；直接开始 **P2（metastore SPI，从 P2-T01 起）**。P1-T06 **非取消而是推迟**——其 docker 验证（T1 storage 等价：S3/OSS/COS/OBS/HDFS + 无凭据 OSS/COS/OBS + 调优默认）与 P2-T05 的 docker 验证（T2 metastore 等价 + 5 flavor + vended + kerberos）**合并为一次 docker 跑**（P2-T05 本就需要同一套 `enablePaimonTest=true` 5-flavor 环境），避免重复部署。
- **理由**：R-006/R-007/R-008 已在 UT/mutation 层闭环，P1 storage 路径无已知漂移；P1-T06 与 P2-T05 共用同一 docker 套件，分两次跑无收益。用户优先推进架构进度（P2 大阶段），把所有 e2e 验证留到 P2 收口一次性做。
- **影响**：实施顺序 P1-T06 → 推迟到 P2-T05 之后/合并；PROGRESS/HANDOFF「下一步」改为 P2-T01；**P1-T06 task 状态保持「未完成（推迟）」**（不标 ✅，docker 未跑）；CLAUDE.md Rule 12：在 P2-T05 docker 真跑前，所有「完成」均须标「未跑 e2e」。WORKFLOW §4.1 白名单按 P2 需要纳入 `fe-connector-metastore-api/spi`（原已列为新建允许路径）。

## D-011 — P1-T06 之前先处理 R-008 + R-006（授权触碰 fe-filesystem-{s3,oss,cos,obs}）
- **日期**：2026-06-18 ｜ **决策者**：用户（「在做 p1-t06 之前，把 r-008 和 r-006 先处理掉」）
- **内容**：**调整实施顺序** = 先 **FU-T02（R-008）** + **FU-T03（R-006）**，再 **P1-T06**。授权本次**局部解禁**对象存储 typed 模块（原 D-005 / WORKFLOW §4.1 禁碰 fe-filesystem，D-010 仅放行 fe-filesystem-hdfs）：
  - **FU-T02（R-008）**：给 `fe-filesystem-{oss,cos,obs}` 的 `Oss/Cos/ObsFileSystemProperties` 补 `AWS_CREDENTIALS_PROVIDER_TYPE`（镜像 `S3FileSystemProperties`），**精确 parity** = ak/sk **皆空** → `ANONYMOUS`，否则**省略**（legacy OSS/COS/OBS 仅 blank-creds 才发 ANONYMOUS，**非**无条件 `DEFAULT`；S3 override 恒非空故 S3 typed 已有）。修无凭据 OSS/COS/OBS catalog 在带 IAM-role 云主机的凭据选择漂移。
  - **FU-T03（R-006）**：给 `S3/Oss/Cos/ObsFileSystemPropertiesTest` 加 **test-only** 调优默认断言（S3=50/3000/1000、OSS/COS/OBS=100/10000/10000），守护 P1-T03 删 paimon canonical 测试暴露的 fe-filesystem 测试缺口（**功能今日正确**，仅测试健壮性）。
  - 两者均纯新增/外科：**不动** fe-core 旧 storage 包、不动其它连接器、不动 fe-filesystem-{api,spi,hdfs,azure,broker,local}（除非 recon 证 R-008 须给 api/spi 加 credentials-provider-type 共享类型——届时再 AskUserQuestion 扩）。
- **理由**：R-008/R-006 同源「fe-filesystem typed 对象存储模型对 legacy 不完整」，docker P1-T06 才会暴露 R-008（无凭据 OSS/COS/OBS）；用户决定**在 P1-T06 docker 之前先补齐**，使 P1-T06 成为干净的全绿验收（而非带已知漂移）。FU-T01 已证 fe-filesystem 自有模块可写真 parity UT（非 paimon Option C），故 R-008/R-006 都能 UT 落地 + 与 S3 typed 对照。
- **影响**：WORKFLOW §4.1 白名单 +`fe/fe-filesystem/fe-filesystem-{s3,oss,cos,obs}/**`（main + test；仅 FU-T02/FU-T03）；tasks FU-T02 ⬜→ active-next、新增 **FU-T03**（R-006）；risks R-008/R-006 状态「监控/已触发」→「修复中（P1-T06 前）」；实施顺序 P1-T06 后移到 FU-T02/FU-T03 之后。**实施前仍按 WORKFLOW §2：先 recon 真实代码 + 一句话复述 + TDD**（R-008 TDD：合成 OSS/COS/OBS 无凭据 map → `toBackendKv()` 应含 `AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS` 当 ak/sk 皆空，RED→GREEN；对照 legacy `AbstractS3CompatibleProperties` :117-129）。

## D-010 — 授权触碰 fe-filesystem-hdfs（FU-T01 HDFS typed BE model）+ kerberos 选 K1（不建 fe-kerberos）
- **日期**：2026-06-17 ｜ **决策者**：用户（确认设计 + AskUserQuestion 选 K1）
- **内容**：授权本次**局部解禁** `fe-filesystem-hdfs`（原 D-005 / WORKFLOW §4.1 禁碰 fe-filesystem），把 **FU-T01 从 follow-up 提升为 active 任务**，修复 P1-T04 引入的 HDFS BE 配置回归（DV-004 / R-007）。范围：
  1. 新建 `fe-filesystem-hdfs` 的 **`HdfsFileSystemProperties`**（`implements FileSystemProperties, BackendStorageProperties`，**不**实现 `HadoopStorageProperties`——BE-only，catalog/Hadoop 路保持现状即 P1-T03 后的 raw passthrough，零新行为）+ 小工具 **`HdfsConfigFileLoader`**（XML `hadoop.config.resources` 加载，移植 fe-property `PropertyConfigLoader`，使叶子不依赖 fe-common/fe-core）。
  2. 改 `HdfsFileSystemProvider`：re-type 为 `FileSystemProvider<HdfsFileSystemProperties>` + 新增 `bind()`/`create(P)`；**`create(Map)` 与 `supports()` 保持字节级不变**（hive/iceberg/broker 的 FE filesystem 路零回归）。
  3. `fe-filesystem-hdfs/pom.xml` 增 `fe-foundation` + `commons-lang3`（镜像 sibling `fe-filesystem-s3`）。
  - **toMap()（BE map）= 忠实移植 legacy `HdfsProperties.initBackendConfigProperties()`** → 与 fe-core `getBackendConfigProperties()` parity（含 XML 资源、`hadoop./dfs./fs./juicefs.` 透传、恒发 `ipc.client.fallback...`+`hdfs.security.authentication`、kerberos 块、`hadoop.username`、HA 校验、kerberos required-check）。源 = fe-property `HdfsProperties`（依赖轻、BE-key-only、无 authenticator 的孪生）。
  - **kerberos = K1**（用户 AskUserQuestion 选）：BE-key 字符串内联发射（`hadoop.security.authentication=kerberos`/principal/keytab），**不新建 fe-kerberos**、**不碰** fe-filesystem-hdfs 现有 create()-side `KerberosHadoopAuthenticator`。recon 证 BE model 仅需字符串发射、不需 fe-kerberos（真正 `UGI.doAs` 仍留 fe-core/ctx + 现有 DFSFileSystem，§5 不变量 4）。fe-kerberos/P3a/P3b 仍为独立未来工作。
- **理由**：R-007/DV-004 闭环物理上须给 typed 路一个 HDFS BE model（否则 `getStorageProperties()` 对 HDFS catalog 返回空）。recon（`wf_de5f54be-668` 4-agent）证：①fe-property `HdfsProperties` 是现成的依赖轻 BE-key-only 移植源（parity by construction）；②`BackendStorageKind.HDFS`/`FileSystemType.HDFS`/`StorageKind.HDFS_COMPATIBLE` 已存在；③`bindAll` 只 catch UOE → 校验错误正常上抛非吞掉；④BE model 的 kerberos 仅字符串、不需 fe-kerberos（K1）。BE-only + create(Map) 不动 = 最外科、对 catalog 路零新行为。**真 parity 可在 UT 落地**（不同于 paimon Option C）：fe-filesystem-hdfs 自有模块，可写 golden parity UT 钉 `toMap()` == legacy BE 键集。
- **影响**：WORKFLOW §4.1 白名单 +`fe/fe-filesystem/fe-filesystem-hdfs/**`（仅本任务；其它 fe-filesystem 模块仍禁碰）；R-007 状态「已触发(接受)」→「修复中」→**「已闭环」**；tasks FU-T01 ⬜(follow-up)→🚧→✅(active)；R-005/P3a/P3b 不受影响（K1 不碰 kerberos 收口）；R-008（OSS/COS/OBS ANONYMOUS）仍 FU-T02 独立。**被否**：K2（建 fe-kerberos 仅放常量=低值）、K3（连 create()-side doAs 一并收口=P3b scope，触碰工作中的 create() 路、回归面大，宜独立任务）。
- **对抗 review 增补（`wf_5db99e32-2ad`，27 agent，4 lens + verify；2026-06-17）**：~13 confirm（多 NIT/MINOR + 清场），3 实质修：①malformed-`uri` 由 swallow 改 **fail-loud**（对齐 legacy `extractDefaultFsFromUri` 不 try/catch）；②两处被本改动作废的 stale 注释（`FileSystemPluginManager.bindAll` javadoc 去 HDFS、paimon `KNOWN GAP 1` 标 CLOSED）；③+11 覆盖测试（empty-input fallback、kerberos-creds-but-simple 判别、viewfs/jfs derive vs ofs/oss no-derive、allowFallback-blank、multi-uri、HA 三分支、malformed-uri、sysprop 解析）。**F1（用户选「现在接好」）= XML config-dir 接线**：legacy 走 `Config.hadoop_config_dir`（可被 operator 覆盖），新 leaf 不能 import fe-core `Config`→在 **`FileSystemFactory.bindAllStorageProperties`（已白名单）** `System.setProperty("doris.hadoop.config.dir", Config.hadoop_config_dir)`，`HdfsConfigFileLoader.resolveHadoopConfigDir()` 读该 sysprop（默认 dir 与 Config 默认相同，仅非默认安装受影响）。**额外触碰**（均 project-owned 方法体微改 + 注释，已在 commit/HANDOFF 标注）：fe-core `FileSystemFactory.java`（+1 行 setProperty，本项目 P1-T02 加的方法）、`FileSystemPluginManager.java`（bindAll javadoc，本项目 P0-T02 加的方法）、fe-connector-paimon `PaimonScanPlanProvider.java`（注释）。**清场（非缺陷）**：packaging 无跨 loader 风险（无 fe-foundation 类穿越边界、hadoop parent-first）、`new Configuration()` 默认 bloat 是 legacy-faithful、BE-only 不引入新 catalog 路回归、独立 agent 逐键复核 byte-level parity。**残留已知（非本任务）**：oss-hdfs 在 typed 路仍缺 JindoFS oss 凭据（P1-T04 已起、需 fe-filesystem OssHdfs typed model，独立 FU）；scan-time 重 validate（valid catalog 内禀 dormant，bindAll 无 memoization 是 typed-路通性非 FU-T01）。

## D-009 — bind-all 机制 + 白名单 +1（FileSystemPluginManager）（应对 DV-001）
- **日期**：2026-06-17 ｜ **决策者**：用户（应对 P0-T01 证伪 P0-1 预期）
- **内容**：实现 `ConnectorContext.getStorageProperties()`（返回 fe-filesystem typed `StorageProperties`）所需的 raw map → `List<StorageProperties>` 绑定，落在 fe-core `FileSystemPluginManager` 新增 additive `public List<StorageProperties> bindAll(Map)`（镜像现有 `createFileSystem` 的 provider 循环，但用 `provider.bind(props)` 全量收集所有 `supports()` 命中者，而非首个命中 `create`）。`DefaultConnectorContext.getStorageProperties()` 调它；raw map 经现有 `storagePropertiesSupplier` 值的 `getOrigProps()` 取（fe-core `ConnectionProperties` 公有 getter），**不改构造点**（`PluginDrivenExternalCatalog` 零改动）。
- **理由**：守 D-003「连接器只见 fe-filesystem-api」架构（否决 C「ctx 返回 Map」=放弃该目标边）；bindAll 放 fe-core（非 fe-filesystem-spi 静态）契合设计 §2.1「fe-core 用 providers 全量绑定」且能见 directory 插件（否决 B）。
- **🔧 二次确认（2026-06-17，P0-T02 实证后）**：fe-core 改动从原估 **2 文件修正为 3 文件**（均纯新增）。实证：生产中对象存储 providers 是 `Env.loadPlugins(pluginRoot)` **目录插件**，只存于 **live** `FileSystemPluginManager`（藏于 `FileSystemFactory.pluginManager` private、无 getter）；`DefaultConnectorContext` 无法直达（构造点 `PluginDrivenExternalCatalog` 被 R-004 禁）。→ 须在 `FileSystemFactory` 加 additive static `bindAllStorageProperties(Map)`（委托 live `pluginManager.bindAll`，else ServiceLoader fallback，镜像现有 `getFileSystem` 双路径）。**三文件** = `DefaultConnectorContext`（+getStorageProperties）+ `FileSystemPluginManager`（+bindAll）+ `FileSystemFactory`（+bindAllStorageProperties）。raw map 经 `storagePropertiesSupplier.get().values()` 任一 `getOrigProps()`（已核实 = 完整 catalog map）。用户 2026-06-17 二次确认接受。
- **影响**：WORKFLOW §4.1 白名单 +`FileSystemPluginManager.java` +`FileSystemFactory.java`（均仅新增方法）；risks R-004 改「唯一」为「**三处** fe-core 新增」；设计 §4 P0-1/P0-2 回写（+DV-001 脚注）；tasks P0-T02/P1-T02。**fe-core 旧 storage 包仍零改动**（bindAll 用 fe-filesystem providers，不碰 `datasource.property.storage`）。

## D-008 — Vended creds 边界：连接器只「抽取」，fe-core 单点「归一」
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：vended creds 处理边界 = **连接器只负责 SDK 特定的原始 token 抽取**（paimon 已落地于 `PaimonScanPlanProvider.extractVendedToken(table)`，从活的 paimon SDK 表对象抠出任意 shape 的 `s3.*`/`oss.*` token）；**raw-token → 统一 BE map** 的归一（`CredentialUtils.filterCloudStorageProperties` + `StorageProperties.createAll`（provider 重绑、派生 region/endpoint/调优默认）+ `getBackendPropertiesFromStorageMap` → `AWS_ACCESS_KEY/SECRET_KEY/TOKEN/ENDPOINT/REGION`）**仍由 fe-core `ConnectorContext.vendStorageCredentials(rawToken)` 单点实现**。fe-core 旧 `Paimon/IcebergVendedCredentialsProvider` 的抽取逻辑后续随各连接器迁移正式下沉到连接器（paimon 已下沉），本次不删 fe-core 旧类（D-005）。
- **理由**：raw-token→BE-map 必须套**后端特定**默认值（region/endpoint 推导、S3=50/3000/1000 vs OSS/COS/OBS=100/10000/10000 调优分叉），需 storage providers（ServiceLoader 发现）；而连接器按 D-003 **只见 fe-filesystem-api 接口、不持有 providers**，物理上无法独立完成全程归一。延续 D-003「动态/provider 发现留 fe-core」的精确边界：连接器最轻、归一单点、无漂移。**备选 B（放宽红线让连接器依赖 fe-filesystem-spi/providers 自做端到端）被否**：加重连接器 classpath + 破坏「fe-connector 仅依赖 fe-filesystem-api」红线。
- **影响**：设计 §2.3（细化边界）；tasks `P1-T04` 已符合（vended 路径不动，仍走 `ctx.vendStorageCredentials`），**无新增 task**；为后续 hive/iceberg 迁移确立统一 vended 模式。

## D-007 — Kerberos 抽到新叶子模块 fe-kerberos（单一真相源）
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：新建叶子模块 **`fe-kerberos`**（依赖**仅** hadoop-auth/hadoop-common；把唯一外部依赖 trino `KerberosTicketUtils` 用 JDK `javax.security.auth.kerberos` 替掉），把 fe-common `org.apache.doris.common.security.authentication.*` 整套搬入作**唯一真相源**；`fe-filesystem-hdfs` **删掉自有的** `KerberosHadoopAuthenticator`、改依赖 fe-kerberos；统一两个打架的 `HadoopAuthenticator` 接口（fe-common 用 `PrivilegedExceptionAction` vs fe-filesystem-spi 用 `IOCallable`）为单接口 + 消费侧 adapter。`fe-common`/`fe-core`/`fe-filesystem-*`/`fe-connector-*` 共用。`fe-kerberos` **置于顶层**（与 `fe-foundation`/`fe-common` 平级的中立叶子），无环。
- **归属/命名（用户 2026-06-17 二次确认）**：**否决** `fe-connector-auth`（置于 fe-connector 组）——会破坏两条规则：(1) `fe-filesystem-* ──╳──► fe-connector` gate（fe-filesystem-hdfs 无法依赖它删除自有副本）；(2) `fe-common`（低层）反向依赖 fe-connector 子模块=层级倒挂。故必须是**顶层中立叶子**才能被 fe-filesystem-hdfs + fe-common 共用（=满足原始需求「HMS 与 HDFS 都能用」）。**模块名定 `fe-kerberos`**（用户 2026-06-17 确认）。
- **理由**：现状 kerberos **三处实现**——(1) fe-common `security.authentication.*`（fe-core/HMS/HDFS 用）；(2) fe-filesystem-hdfs **自抄一份** `KerberosHadoopAuthenticator`（为避免依赖 fe-common，一年前拷贝、TGT 刷新可能已漂移）；(3) paimon 手抄 HMS 的 kerberos HiveConf 键 + 回调 `ctx.executeAuthenticated`。改一处要改三处。auth 类 import 干净（仅 JDK/hadoop/log4j/commons/guava + 1 个 trino），fe-common 不依赖 fe-core → 抽取无阻力；`fe-foundation` 现为纯净（零 hadoop）的 `@ConnectorProperty` 叶子，**不应**被 hadoop 污染（故否「折进 fe-foundation」）；也否「fe-filesystem/fe-connector 直接依赖较重的 fe-common」。
- **范围（分两步，用户 2026-06-17 确认）**：(a) **P3a 纳入本次** = 先建 `fe-kerberos` + 让 **paimon** 的 HMS kerberos facts 走它（paimon-local、纯新增，**不碰** fe-common/fe-filesystem-hdfs 既有路径，符合 D-005）；(b) **P3b = follow-up（本次不做）** = 全量去重（删 fe-filesystem-hdfs 副本、fe-common 重指向 fe-kerberos、统一两个 `HadoopAuthenticator` 接口），与 hive/iceberg 迁移同批——此步改 fe-common + fe-filesystem-hdfs，超出 D-005，故独立。
- **影响**：设计 §3.5（新增 Kerberos 节）+ 依赖图（加 fe-kerberos 叶子）；tasks 新增 P3；risks 补「kerberos 三处漂移」项。

## D-006 — MetaStore 后端「类型」用 Provider 自识别，api 层不放 per-backend 枚举
- **日期**：2026-06-17 ｜ **决策者**：用户（修正 D-001/设计 §3.1 初版的 `MetaStoreType` 枚举）
- **内容**：`fe-connector-metastore-api` 的 `MetaStoreProperties` 接口**不放 per-backend `MetaStoreType` 枚举**；后端标识用 **`String providerName()`**，横切行为用**能力方法**（如 `boolean needsStorage()` / `needsVendedCredentials()`）表达。新增 `fe-connector-metastore-spi` 的 **`MetaStoreProvider<P extends MetaStoreProperties>` SPI**（`boolean supports(Map)` 自识别 + `bind(raw, storageList)`），经 `META-INF/services` + ServiceLoader 发现，**镜像 `FileSystemProvider`**。新增后端（Glue/S3Tables/自定义）= 新模块 + 新 provider + 一行 services 文件，**api/spi 零改动、无中心 switch**。唯一旧消费者 `VendedCredentialsFactory:61` 的 `getType()` switch 用能力方法替代。
- **理由**：把 per-backend 枚举放进 api 会**原样继承**旧 `MetastoreProperties.Type` 的脆性（每加后端改 api 枚举 + 找全散落 switch、漏一个无编译期报错）；fe-filesystem 已用 provider 模式干净解决同一问题，连 `FileSystemType` per-backend 枚举顶上都挂着官方「加类型要改多处、易错」的反模式 TODO。**高层 category 枚举（如 `StorageKind`）可留**（封闭小集 + 承载横切行为），但 metastore 当前无此需要，能力方法足矣。
- **影响**：设计 §3.1（重写 type() 部分为 provider 模型）/ §3.2（加 `MetaStoreProvider` SPI + ServiceLoader）；tasks `P2-T01` 改写（去枚举、加 provider）；**修正 D-001** 措辞中的「MetaStoreType 枚举」。

## D-005 — 本次任务范围：纯新增/迁移，只动 paimon
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：本次任务 **不删除** fe-core `datasource.property.{storage,metastore}` 任何类；**不修改** hive/hudi/iceberg/es/jdbc/mc/trino 连接器；`fe-property` 仅断开 paimon 依赖边（变孤儿），**不物理删**；import gate 不收紧。Phase 3+ 及 fe-core 两包的最终删除属范围外（后续全连接器迁完再做）。
- **理由**：保持改动外科化、可回滚；隔离 paimon 的迁移风险，不波及在用的 hive/hudi/iceberg。
- **影响**：设计文档 §0.1 / §4（Phase 1/2 改为 additive、删除 Phase 3+ 与 fe-core 删除步骤）/ §6。

## D-004 — typed MetaStore 属性复用 fe-foundation @ConnectorProperty 绑定
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：新 MetaStore 属性模型用 `@ConnectorProperty` + `ConnectorPropertiesUtils` 注解绑定（别名优先级/required/sensitive/matchedProperties 全免费），镜像 fe-filesystem StorageProperties 做法。
- **理由**：消除 paimon 手抄的 `String[]` 别名数组 + `firstNonBlank`；单一真相源。
- **影响**：设计 §3.2（typed holder）。

## D-003 — fe-core 绑定 Storage，经 ConnectorContext 下发已解析的 StorageProperties
- **日期**：2026-06-17 ｜ **决策者**：用户（修正 agent 初版"混合 Option C"）
- **内容**：CREATE CATALOG 入口在 fe-core；fe-core 用 fe-filesystem（全量+providers）绑定 `StorageProperties`，经 `ConnectorContext.getStorageProperties(): List<StorageProperties>`（fe-filesystem-api 类型）传给连接器；连接器只见 api 接口，调 `toHadoopProperties()/toBackendProperties()`。动态/RPC（vended creds、URI 归一、thrift `TS3StorageParam`）留 fe-core 经 ConnectorContext 委托。
- **理由**：provider 发现（ServiceLoader）集中 fe-core；连接器 classpath 无需 providers、只依赖 fe-filesystem-api 接口——精确满足"fe-connector 仅依赖 fe-filesystem-api"；比"连接器自调静态门面"（agent 初版 Option C）更干净。
- **影响**：设计 §2 / §3.2 / §4 P1-1,P1-2。**取代** agent 初版的静态门面方案（无需给 fe-filesystem-api 加 `buildObjectStorageHadoopConfig` 等价静态门面）。

## D-002 — 去重策略：混合（共享后端 fact 解析器 + 薄 per-connector adapter）
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：HMS/DLF/Glue/REST/JDBC 的"连接事实解析器"在 `fe-connector-metastore-spi` 实现**一次**（含 JDBC DriverShim）；每个连接器只写薄 catalog adapter 消费 facts 建各自 SDK catalog。
- **理由**：最大化去重（HMS 后端现被复制约 4 次、DLF 8-key 块 3 次、DriverShim 2 次），又不把引擎 SDK 塞进共享层。
- **影响**：设计 §3.2 / §3.3。

## D-001 — 新建 fe-connector-metastore-api + fe-connector-metastore-spi 模块对
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：MetaStore Property SPI/API 放在新建的模块对，依赖 fe-foundation + fe-filesystem-api，镜像 fe-filesystem 的 api/spi 拆分；不折叠进现有 fe-connector-api（其极简，仅 fe-thrift provided）。
- **理由**：metastore 模型需要 @ConnectorProperty 绑定（fe-foundation）+ StorageProperties 入参（fe-filesystem-api），不应污染 fe-connector-api 的消费契约层。
- **影响**：设计 §0 / §3.1 / §3.2。
