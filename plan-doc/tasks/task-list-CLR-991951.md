# Task List — TeamCity #991951 / PR #65474 classloader split-brain (插队任务, 2026-07-11)

设计 + RCA：`plan-doc/tasks/designs/FIX-CLR-classloader-splitbrain-design.md`
证据：fe.log/be.log/fe.conf/be.conf（build 991951 archive）+ 对抗验证 `wf_bf3a50e5-046`（4 agent，high-confidence）。

- [x] **FIX-CLR1**（根因，解 50 里的 49 + outlier `test_hms_partitions_tvf`）：`ThriftHmsClient.doAs` 钉 plugin loader（`getSystemClassLoader()`→`getClass().getClassLoader()`）+ 更新 `HiveConnector` stale 注释 + `ThriftHmsClientDoAsClassLoaderTest`（隔离 child-first loader 探针，RED=`PIN_WRONG_SYSTEM_LOADER`/GREEN 双验）。commit `92004ef1d0d`。fe-connector-hms 40/40 绿、0 checkstyle。
- [x] **FIX-CLR2**（latent 加固，用户要求本批一并）：`HiveConnector.buildPluginAuthenticator` 方法体钉 plugin loader（挡 `HadoopSimpleAuthenticator` eager UGI 毒化）+ `HiveConnectorPluginAuthenticatorTcclTest`（RED=marker/GREEN 双验）。commit `15d3df1dfd6`。fe-connector-hive 186/186 绿（含 5 个既有 authenticator 用例）、0 checkstyle。

## 归类（50 失败；muted 忽略）
- 49 直接 `SecurityUtil` 毒化 + 1 outlier1（`test_hms_partitions_tvf`，comms 中断表象）= FIX-CLR1 解。
- 1 outlier2（`test_hdfs_parquet_group0`，BE `MEM_LIMIT_EXCEEDED`，HDFS TVF/ASAN 内存 flake）= **与本 PR 无关**，重测/忽略（另议 BE mem_limit/测试数据）。

## e2e（用户自跑，勿丢）
真集群重跑 49 个 SecurityUtil 用例断言全绿；系统 vs 插件双 loader 只在真 child-first 环境复现（单测钉 intent+还原）。
