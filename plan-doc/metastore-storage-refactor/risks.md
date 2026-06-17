# 风险登记册（滚动状态）

> 编号 `R-NNN` **仅在本子项目内有效**。状态：监控中 / 缓解中 / 已闭环 / 已触发。
> 风险=可能发生（在此）；问题=已发生（记在 `tasks.md` 对应 task 的 blocker）。

---

## R-001 — 新旧 Storage 配置/BE map 等价性漂移 ｜ 状态：监控中
- **描述**：新 `toHadoopConfigurationMap()`/`toBackendProperties().toMap()` 与 fe-core 旧 `getHadoopStorageConfig()`/`getBackendConfigProperties()` 可能在某些键/默认值上不一致。已知默认调优值分叉：S3=50/3000/1000 vs OSS/COS/OBS=100/10000/10000。
- **影响**：paimon 读私有桶 403、Hadoop FS 行为变化、静默错误。
- **缓解（DV-003 修订）**：T1 自动逐键 UT **不可在单测落地**（fe-filesystem 对象存储 impl 是运行时插件，不在任何单测 classpath）→ 改为 **paimon connector-local 契约 UT**（storage map 叠加/last-write-wins/kerberos-ordering）+ **docker P1-T06 5-flavor 作真等价闸**；P0-T01 4-agent recon + DV-002 code-read 等价为依据。
- **触发判据**：docker P1-T06 任一 flavor 读私有桶 403 / 配置缺键。

## R-006 — 调优默认值（tuning defaults）无显式 UT 守护（P1-T03 删 canonical 测试暴露的 fe-filesystem 测试缺口）｜ 状态：监控中
- **描述**：P1-T03 删 paimon `buildHadoopConfigurationEmitsS3TuningDefaults` 等 canonical 测试（翻译职责移交 fe-filesystem）。对抗 review（`wf_76df09a4-c2f`）确认 + 直接核实：fe-filesystem `S3FileSystemPropertiesTest.toHadoopProperties_*` **不显式断言**调优默认值（`fs.s3a.connection.maximum=50`/`request.timeout=3000`/`timeout=1000`；line72 只设输入 `s3.connection.maximum=64` 非断默认），`Oss/Cos/ObsFileSystemPropertiesTest` 同样**零调优断言**（OSS/COS/OBS 默认 100/10000/10000）。**canonical 键翻译 + endpoint-from-region 派生 IS 已覆盖**（已核：`OssFileSystemPropertiesTest:108-110` region→`-internal` endpoint、Cos/Obs endpoint+creds），唯**调优默认值**裸奔。
- **影响**：**功能今日正确**（`S3FileSystemProperties.toHadoopConfigurationMap()` 经字段默认 `DEFAULT_MAX_CONNECTIONS="50"` 等真发，paimon `buildStorageHadoopConfig` 正确调用）；但若未来改 fe-filesystem 误删某调优默认，**无 UT 报红**（仅 docker 运行期暴露）→ 测试健壮性回归。
- **缓解**：**docker P1-T06** 为运行期兜底；**建议 follow-up**（**超出当前 P1 白名单——fe-filesystem 禁碰**）：在 `S3FileSystemPropertiesTest` + `Oss/Cos/ObsFileSystemPropertiesTest` 加调优默认断言（test-only additive）。在 fe-filesystem 收口/迁移批次或经用户批准的小补丁中做。**不在 paimon 重复断言**（Option C：paimon 无 fe-filesystem impl 于测试 classpath，合成 map 断言为同义反复，不守 fe-filesystem 默认）。
- **触发判据**：fe-filesystem 调优默认被改且 docker P1-T06 未跑 → 静默 mis-tune。

## R-002 — 双 Storage 路径并存窗口 ｜ 状态：监控中
- **描述**：迁移期 fe-core 旧 storage（hive/hudi/iceberg 用）与 fe-filesystem 新 storage（paimon 用）并存；同一 catalog 若两路推出不同配置会冲突。
- **影响**：配置/凭据不一致。
- **缓解**：paimon **完全**切到新路（P1 全 task 完成）即隔离；本项目不动其它连接器（D-005），天然不交叉。
- **触发判据**：paimon catalog 出现 connector 侧与 engine 侧配置分歧。

## R-003 — 打包 / 类加载（relocated thrift + child-first）｜ 状态：监控中
- **描述**：HMS/DLF 活连接需 relocated thrift（`fe-connector-paimon-hive-shade`）build-order 在前 + child-first hadoop/aws bundling。新建/改动模块时若破坏，会重现 S3A/thrift 跨 classloader cast 崩溃（历史 bug）。
- **影响**：docker paimon HMS/DLF flavor 运行期崩。
- **缓解**：模块改动保持 shade build-order 与 child-first/parent-first 白名单不变；**T4 docker 5 flavor** 覆盖 HMS/DLF。
- **触发判据**：docker HMS/DLF 启动报 ClassCastException / NoClassDefFound（thrift/S3A）。

---

## R-004 — fe-core 改动越界 ｜ 状态：监控中（白名单 2026-06-17 +2，DV-001/D-009 二次确认）
- **描述**：本项目允许的 fe-core 改动**仅三处、均纯新增**：`DefaultConnectorContext`（+getStorageProperties）、`FileSystemPluginManager`（+bindAll）、`FileSystemFactory`（+bindAllStorageProperties，取 live manager；D-009 二次确认）。若实现时顺手碰了 `datasource.property.*` 包、这三文件的既有方法、或构造点 `PluginDrivenExternalCatalog` 即越红线。
- **缓解**：每次提交前 `git diff --name-only` 对照 WORKFLOW §4.1 白名单；`git diff` 这三文件须只见**新增**方法，无既有方法改动；验收 §6「零改动核对」。
- **触发判据**：`git diff` 出现 fe-core property 包、其它连接器路径、或这三文件的非新增改动。

## R-005 — Kerberos 三处实现漂移（D-007）｜ 状态：监控中
- **描述**：kerberos 现有**三处实现**：fe-common `security.authentication.*`、fe-filesystem-hdfs 自抄 `KerberosHadoopAuthenticator`（约一年前拷贝、TGT 刷新逻辑可能已偏离）、paimon `PaimonCatalogFactory` 手抄 HMS kerberos HiveConf 键。改一处需同步三处，否则行为分叉。
- **影响**：kerberized HMS/HDFS 鉴权行为不一致；UGI 刷新/JVM-全局锁语义分叉；安全相关静默失败。
- **缓解**：D-007 抽 `fe-kerberos` 单一真相源；**P3a（本次）paimon 先收口**到 fe-kerberos；**P3b（follow-up）** fe-common + fe-filesystem-hdfs 全量收口并统一两个 `HadoopAuthenticator` 接口（`PrivilegedExceptionAction` vs `IOCallable`），与 hive/iceberg 同批。**过渡期（P3a 后、P3b 前）三处副本仍在**，须知晓改一处需同步。
- **触发判据**：三处之一改动未同步导致 kerberos e2e（HMS/HDFS）行为不一致。
- **范围注**：全量去重（P3b）改 fe-common + fe-filesystem-hdfs，超出 D-005「只动 paimon」，属 follow-up。
