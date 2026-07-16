# FIX-CLR — hive 连接器 HMS 路径 classloader split-brain（TeamCity #991951 / PR #65474）

> 插队任务（2026-07-11）。TeamCity `Doris_External_Regression` build **991951**（PR #65474，hive connector 迁移）50 个外表 case 失败。经取回 fe.log + be.log + fe.conf/be.conf + 4 路对抗验证（`wf_bf3a50e5-046`，全部 high-confidence），定位为**一个** FE 侧 classloader split-brain 根因。

## Problem

- 50 失败中 **49 直接** + **1（`test_hms_partitions_tvf`，表象为 `Communications link failure`）** = 同一根因；另 1（`test_hdfs_parquet_group0` BE `MEM_LIMIT_EXCEEDED`）与本 PR 无关（HDFS TVF，ASAN 内存压力 flake，重测/忽略）。
- 集群健康：无 BE 宕机/OOM/配置错误；366 通过；"No backend available" 仅启动轮询期（05:52，测试前）。
- 失败文案：FE 侧 `java.sql.SQLException`：首次 `ExceptionInInitializerError`，其后一律 `Could not initialize class org.apache.hadoop.security.SecurityUtil`（fe.log 185 次）——`SecurityUtil` 静态初始化失败被 JVM **永久毒化**。

## Root Cause

首次失败栈（fe.log:10851/10922）：
```
ThriftHmsClient.doAs(:636) → DefaultConnectorContext.executeAuthenticated(:178)
 → RetryingMetaStoreClient.getUGI → UserGroupInformation.getCurrentUser → SecurityUtil.<clinit>(:95)
 → setConfigurationInternal(:123) → DomainNameResolverFactory.newInstance(:70) → Configuration.getClass(:2764)
   ✗ RuntimeException: class org.apache.hadoop.net.DNSDomainNameResolver not org.apache.hadoop.net.DomainNameResolver
```
`DNSDomainNameResolver extends DomainNameResolver`，`isAssignableFrom` 却为 false ⇒ 两类来自**不同 classloader**。运行时确有两份 Hadoop：
- fe-core `hadoop-client`（`fe-core/pom.xml:447`）→ **app/system** loader；
- hive 插件 `hadoop-common`（`fe-connector-hms/pom.xml:71` + `plugin-zip.xml`）→ **plugin child-first** loader（`ChildFirstClassLoader` allowlist 不含 `org.apache.hadoop.*` / `org.apache.doris.connector.hms.*`）。

**罪魁**：`ThriftHmsClient.doAs`（`fe-connector-hms/.../ThriftHmsClient.java:632-641`）在建 metastore client 前把 TCCL 钉成 `ClassLoader.getSystemClassLoader()`。`SecurityUtil.<clinit>` 内 `new Configuration()` 捕获该 TCCL ⇒ `DNSDomainNameResolver` 从 system 副本加载，而其父接口 `DomainNameResolver`（由 plugin 加载的 `SecurityUtil` 引用）来自 plugin 副本 ⇒ 身份不一致 ⇒ 毒化。

**为何只有 hive 中招**：iceberg/paimon 在 `IcebergConnector:182`/`PaimonConnector:137` 用 `TcclPinningConnectorContext` 把 context 钉到 `getClass().getClassLoader()`（plugin loader）；`HiveConnector:113-117` 存裸 context，且 `doAs` 反钉 system loader（钉反）。`doAs` 是 hms+hive 两模块里**唯一** `getSystemClassLoader()` 钉点。同模块既有约定（`HiveConnectorMetadata:808/842`、`HudiConnector.metaClientExecutor`）均钉 plugin loader。

### 派生的 latent edge（未被 49 用例触发，用户要求本批一并加固）
`HiveConnector.buildPluginAuthenticator`（:598）在 `hadoop.security.authentication=kerberos` **但缺 principal/keytab** 的错配下 → `AuthenticationConfig.getKerberosConfig` 回落 `SimpleAuthenticationConfig`（`AuthenticationConfig.java:98-102`）→ `new HadoopSimpleAuthenticator`（`HadoopSimpleAuthenticator.java:37` **eager** `UGI.createRemoteUser`）→ 在**未钉的 createClient 线程**上初始化 `SecurityUtil` ⇒ 独立毒化。`buildHadoopConf` 只 `conf.setClassLoader(plugin)`（外层 conf），管不住 `SecurityUtil.<clinit>` 内**另建**的 `new Configuration()` 捕获 TCCL，故须钉 TCCL。`HadoopKerberosAuthenticator` 的 login 是 lazy（在 `getUGI` 内，随 doAs 已被 FIX-CLR1 钉），不受影响。

## Design

两个独立、最小、连接器局部的 TCCL 钉点（对齐 iceberg/paimon/hudi 与同模块 stats 方法先例；**不**在 fe-core 加任何 source-specific 代码）：

- **FIX-CLR1（根因，解 50/50 里的 49+outlier1）**：`ThriftHmsClient.doAs` 把 `ClassLoader.getSystemClassLoader()` 改为 `getClass().getClassLoader()`（= 加载 ThriftHmsClient 的 plugin child-first loader，是 system loader 的严格超集）。`doAs` 是 client 创建（`createFreshClient:722`）与每次 RPC（`execute:618`）的**唯一咽喉**，且 Kerberos/非 Kerberos 两条 authAction 都在其内 ⇒ 一处修复覆盖两路（`TcclPinningConnectorContext` 式"包 context"改法修不了 Kerberos，因 Kerberos authAction 绕过 `context.executeAuthenticated`）。附带更新 `HiveConnector.java:520-521` 现已过时的注释（"system classloader" → "plugin classloader"）。

- **FIX-CLR2（latent 加固）**：在 `HiveConnector.buildPluginAuthenticator` 方法体外包 TCCL 钉到 `HiveConnector.class.getClassLoader()`，try/finally 还原。使 `HadoopSimpleAuthenticator` eager UGI 在 plugin loader 下初始化。

## Implementation Plan

1. FIX-CLR1：改 `ThriftHmsClient.doAs` 一行 + 更新 `HiveConnector:520-521` 注释。
2. FIX-CLR2：`buildPluginAuthenticator` 方法体包 `ClassLoader prev=TCCL; try{ set(HiveConnector.class.getClassLoader()); <原体> } finally { set(prev); }`。

## Risk Analysis

- plugin child-first loader 委派父加载器解析未自带类 ⇒ 对 system loader 严格超集，无可见性回归；hadoop/hive/thrift 解析到 plugin 自带副本（正是所需）。
- 铁律核对：无 fe-core 改动、无 source-specific 分支、不解析属性；仅连接器局部钉 TCCL（memory `catalog-spi-plugin-tccl-classloader-gotcha` 的 HMS-client-创建 locus，为其**第 4 个**登记 locus）。
- 残余（本 PR 不 own）：TVF analyze 阶段抛 `java.lang.Error` 未转 SQL 错误、直接拆连接（outlier1 表象）——毒化去除后触发点即消失，另开 hardening ticket。

## Test Plan

### Unit Tests（连接器模块无 Mockito；用 recording fake + child-first marker loader，须 RED-able）
- **FIX-CLR1** `ThriftHmsClientDoAsClassLoaderTest`（fe-connector-hms）：注入 recording `MetaStoreClientProvider` 捕获 `doAs` 内建 client 时的 TCCL；调 `listDatabases()`。断言：TCCL == `ThriftHmsClient.class.getClassLoader()`、!= 调用方 marker、事后还原 marker；当 `system != connectorLoader`（env 允许时）额外断言 != `getSystemClassLoader()`（精确根因回归门）。
- **FIX-CLR2** `HiveConnectorPluginAuthenticatorTccl` 断言（并入/毗邻 `HiveConnectorPluginAuthenticatorTest`）：传一个在 `get()` 时记录 TCCL 的 Map，marker 下调 `buildPluginAuthenticator`；断言方法体内 TCCL == `HiveConnector.class.getClassLoader()`、!= marker（**未钉时停留在 marker ⇒ RED**）、事后还原 marker。

### E2E Tests（用户自跑，勿丢）
真集群重跑 build 991951 的 49 个 SecurityUtil 用例（hive/iceberg-on-HMS/hudi/mtmv/kerberos/tvf/export/cache），断言全绿；系统 vs 插件双 loader 拓扑只在真插件 child-first 环境复现，单测只钉 intent + 还原（对齐 `HiveConnectorMetadataFileListStatsTest` 先例）。`test_hdfs_parquet_group0` 与本 PR 无关，另议 BE mem_limit/测试数据。
