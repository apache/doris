# FIX-CLR Summary — hive HMS classloader split-brain（TeamCity #991951 / PR #65474）

## Problem
build 991951（hive connector SPI 迁移 PR）50 个外表 case 失败。集群健康（366 通过、无 BE 宕机/OOM、`priority_networks` 正确、"No backend available" 仅启动轮询期）。

## Root Cause
FE 侧 classloader split-brain：`ThriftHmsClient.doAs` 在建 metastore client 前把 TCCL 钉成 `ClassLoader.getSystemClassLoader()`。`SecurityUtil.<clinit>` 内 `new Configuration()` 捕获该 TCCL 反射加载 `DNSDomainNameResolver`——system loader 是 fe-core 的 hadoop 副本，而 `SecurityUtil`/`DomainNameResolver` 来自 hive 插件 child-first 副本 → `isAssignableFrom` false（"class DNSDomainNameResolver not DomainNameResolver"）→ `ExceptionInInitializerError` → `SecurityUtil` 全 JVM 永久毒化 → 所有 hive/iceberg-on-HMS/hudi/mtmv/kerberos 操作报 "Could not initialize class SecurityUtil"。iceberg/paimon 用 `TcclPinningConnectorContext` 钉 plugin loader 故免疫；hive 独中招。4 路对抗验证（`wf_bf3a50e5-046`）high-confidence 确认。

## Fix
- **CLR1（根因）** `92004ef1d0d`：`ThriftHmsClient.doAs` → `getClass().getClassLoader()`（plugin child-first loader，system loader 严格超集）。单一咽喉覆盖 client 创建 + 每次 RPC + Kerberos/非 Kerberos 两 authAction。附带更新 `HiveConnector:517-521` stale 注释。
- **CLR2（latent 加固）** `15d3df1dfd6`：`HiveConnector.buildPluginAuthenticator` 方法体钉 `HiveConnector.class.getClassLoader()`（try/finally），挡 kerberos-无凭证错配下 `HadoopSimpleAuthenticator` eager `UGI.createRemoteUser` 在未钉线程毒化。

## Tests
- `ThriftHmsClientDoAsClassLoaderTest`（+ `DoAsTcclProbe`）：经隔离 child-first loader 跑探针（镜像 `OdpsClassloaderIsolationTest`），使 `getClass().getClassLoader()` 与 system loader 不同 → 精确区分根因。**RED**（fix 还原）=`PIN_WRONG_SYSTEM_LOADER`、**GREEN**（fix）双向实测。
- `HiveConnectorPluginAuthenticatorTcclTest`：TCCL-recording Map 观测方法体内 TCCL == plugin loader + 还原。**RED**（钉去除）=marker、**GREEN** 双向实测。
- 回归：fe-connector-hms 40/40、fe-connector-hive 186/186（含 5 个既有 authenticator 用例）、0 checkstyle。

## Result
两连接器局部 TCCL 钉点，无 fe-core 改动、无 source-specific 分支、不解析属性（守铁律）。**系统 vs 插件双 loader 拓扑只在真 child-first 插件环境复现**（单测已用隔离 loader 复刻 CLR1 精确根因）：**e2e 欠账 = 真集群重跑 991951 的 49 个 SecurityUtil 用例断言全绿**（用户自跑）。
`test_hdfs_parquet_group0`（BE `MEM_LIMIT_EXCEEDED`，HDFS TVF/ASAN 内存 flake）与本 PR 无关，重测/忽略（另议 BE mem_limit/测试数据）。

## 残余（非本批 own，另开 ticket）
- TVF analyze（Nereids parse 阶段）抛 `java.lang.Error` 未转 SQL 错误、直接拆 client 连接（outlier1 `test_hms_partitions_tvf` 的 comms-failure 表象来源）——毒化去除后触发点消失，可另开 FE 硬化 ticket。
