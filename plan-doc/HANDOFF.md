# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **B8 legacy 删除 + round-3 follow-ups（先 AskUserQuestion 定 scope）**

**第三轮 clean-room 对抗 review 转出的 4 个 user-approved fix 已全部完成并各自 commit**
（[`task-list-P5-rereview3-fixes.md`](./task-list-P5-rereview3-fixes.md)）。无剩余已批准 fix。
下一步是清尾工作，**开工前先 `AskUserQuestion` 确认要做哪一项**（B8 删除是大动作）：

1. **B8 legacy 删除**（task-list Follow-ups + 第三轮报告 R-1…R-8）：legacy `fe/fe-core/.../datasource/paimon/*`
   + `datasource/property/storage/{OSS,COS,OBS,S3,Minio}Properties`、`property/metastore/HMSBaseProperties` 等是
   dead residue。**删除前提**：FIX-4 不再需要它们作 literal 复刻对照（现已 commit，对照完成 → 可删）。
   **删除须保 load-bearing dispatch ordering**（`ShowPartitionsCommand:478-480`，R-4）。逐子树删 + 每删一批跑
   fe-core 编译 + 连接器测 + 全量 regression-gated。
2. **D-057 re-scope**（报告 §D.3）：deferred 的 `TablePartitionValues:162` prune-path sentinel residue **不影响
   paimon**（MVCC override 绕过）。把 deferral re-scope 到 **非-MVCC** 插件连接器（maxcompute/es/jdbc）；
   base-class DATE-epoch + HIVE_DEFAULT 路（P11-1/P11-2）是那边的隐患，非 paimon。
3. **accepted-deviation 用户签字**（task-list「NOT in this fix scope」§）：~10 MINOR + ~12 NIT + C-1
   observability + uncheckedFallbacks（REFRESH cache invalidation / partitions-TVF auth / split-plan RPC 在
   `executeAuthenticated` 外 / `PluginDrivenExternalCatalog:140` 吞 authenticator-wiring 异常）。逐条让用户
   accept-as-deviation 或转 fix。

## ✅ 已完成（本 session）— **FIX-4 `FIX-FECONF-STORAGE-PARITY`（cluster P8-1..4·P9-2/3）— commit `f0210b51871`**
- **连接器 only**（`PaimonCatalogFactory`，无 fe-core/SPI/BE）。FE-side Hadoop `Configuration`/`HiveConf` 重建
  补齐到 legacy full parity：4a OSS endpoint-from-region（移入共享 OSS 块，删 DLF-local 死块）+ S3A base；
  4b S3 path-style + 4 个 tuning 键（**per-backend 默认**：S3 `50/3000/1000` + `AWS_*` twins，OSS/COS/OBS
  `100/10000/10000`）；4c 新 COS/OBS 块（**endpoint-PATTERN 检测** `myqcloud.com`/`myhuaweicloud.com` OR
  scheme 键；S3A base + **无条件** `fs.cosn.*`/`fs.obs.*`；OBS native-vs-s3a by classpath）；**user-approved**
  S3 endpoint-from-region；4d HMS username alias（`hadoop.username`，移到 storage overlay 之后避 passthrough
  clobber）；**4e folded-in pre-existing MAJOR**：kerberos 块移到 overlay 之后（kerberized-HMS + simple-HDFS
  否则被 clobber 成 `auth=simple`+`sasl=true` 坏 GSSAPI）。
- **meta（本 session 亲证有效，下次照用）**：① **design red-team 在写码前**（`wf_a6385c61-669`，5 skeptic +
  completeness critic）抓出三处会 ship-wrong 的 framing：tuning 默认非均一、COS/OBS 按 endpoint pattern 检测
  非 scheme-key、`fs.cosn.*`/`fs.obs.*` 无条件发；② **impl verification 在写码后**（`wf_f90260cb-5e6`）逐键 diff
  legacy（fidelity CLEAN）+ 揪出 4e pre-existing MAJOR；③ **测试钉真不变式**：username-priority + kerberos 两个
  新测在旧 ordering 是 RED（抓出 raw `hadoop.*` passthrough clobber authoritative 设置）。
- 验证：连接器 56/0/0 + 全模块绿；checkstyle 0；import-gate 干净。live-e2e CI-gated（注明 gated，未谎称跑过）。
  设计/总结：`FIX-FECONF-STORAGE-PARITY-design.md` + `-summary.md`。

## 🗺️ 代码脚手架
- **Plugin connector**：`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/`
  （存储装配 `PaimonCatalogFactory.java`：`applyStorageConfig`→`applyCanonicalS3/Oss/Cos/ObsConfig` +
  共享 `applyS3aBaseConfig`；`buildHmsHiveConf`/`buildDlfHiveConf`；scan `PaimonScanPlanProvider.java`；
  @incr `PaimonIncrementalScanParams.java`）。`-api`/`-backend-*` 模块在 git 内为空壳。
- **fe-core 桥/SPI**：`fe/fe-core/.../connector/DefaultConnectorContext.java`、`.../datasource/PluginDriven*.java`；
  SPI `fe/fe-connector/fe-connector-{api,spi}/`。
- **Legacy 对照基准（B8 删除目标）**：`fe/fe-core/.../datasource/paimon/`、`.../datasource/property/storage/`
  下 `{OSS,COS,OBS,S3,Minio}Properties`、`.../property/metastore/HMSBaseProperties`。**FIX-4 已 commit → 对照
  完成，B8 可删。**
- **BE 消费端**：`be/src/format/table/`（`paimon_cpp_reader.cpp`、`paimon_reader.cpp`、`partition_column_filler.h`）。

---

# 📦 仓库状态
- **HEAD = `f0210b51871`（FIX-4）**。迁移链：…→`c376aba1264`(FIX-1)→`2e845e88bf9`(FIX-2)→`f08bc22b9bd`(FIX-3)
  →`d4aeaaccc45`(docs)→**`f0210b51871`(FIX-4, HEAD)**。本 session 后另有一个 `docs:` 提
  （滚 HANDOFF + task-list 勾 FIX-4 + 加 `FIX-FECONF-STORAGE-PARITY-summary.md`）。
- ⚠️ `regression-test/conf/regression-conf.groovy` 仍 modified 未 commit 且含**明文 Aliyun key** —— commit 前继续
  path-whitelist，**严禁 `git add -A`**；`regression-conf.groovy.bak` 同理排除。
- 未 commit/未跟踪：scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）；
  `plan-doc/reviews/P5-paimon-rereview3-2026-06-12.md`（第三轮 review 报告，未跟踪——大文件，下次方便时
  vet+commit 或保留本地）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）。**P0~P4 无阻塞；P9-1/P7-1/P2-1 + P8/P9-config 全清。**
  round-3 的 4 个 user-approved fix 全部完成。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 key）+ 清 scratch（`.audit-scratch/` `conf.cmy/`
  `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。fix commit 带其 design doc（repo 惯例）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false
  -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`/`MVN_EXIT`（[[doris-build-verify-gotchas]]）。
  **漏 `-am` → `could not resolve fe-connector-spi ${revision}` 假错**。改 fe-core `-pl :fe-core -am`；SPI
  `-pl :fe-connector-api`/`:fe-connector-spi -am`。**checkstyle**：连接器
  `mvn -f …/fe/pom.xml -pl :fe-connector-paimon -am checkstyle:check`。**checkstyle 在 `validate` phase 跑（编译前）**——
  多行数组初始化 `{` 须留在 `=` 同行（见 FIX-4），否则 'array initialization lcurly' indentation 报错。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试 harness：`PaimonCatalogFactoryTest`（纯 Map→Configuration/HiveConf，56 测）/`PaimonScanPlanProviderTest`
  (real-table `FileSystemCatalog`)/`PaimonIncrementalScanParamsTest`/`RecordingConnectorContext`/
  `RecordingPaimonCatalogOps`/`FakePaimonTable`（`.copy` 是 no-op recorder，reset/merge fail-before 须 real
  table）/`DefaultConnectorContextNormalizeUriTest`(fe-core)。live-e2e CI-gated（`enablePaimonTest` 默认 false）→
  注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **B8 是大动作**：开工前 `AskUserQuestion` 定 scope（删哪个子树 / follow-ups 哪几条先做）。逐子树删 + 每批跑
  fe-core 编译 + 连接器测 + regression-gated；**保 load-bearing dispatch ordering**（grep 全调用方，base vs MVCC
  子类）。
- **改 handle/分区/scan 流必 grep 全调用方 + 确认实际实例类（base vs MVCC 子类）**；改 storage/auth 装配流必
  grep `applyStorageConfig` 全 caller（filesystem/jdbc/hms/dlf）+ 注意 raw `hadoop.*`/`fs.*` passthrough 跑在最后
  （last-write-wins）会 clobber 之前的 authoritative 设置（FIX-4 4d/4e 亲证）。
- **design red-team（写码前）+ impl verification（写码后）两道**本 session 各抓真 defect，**勿跳**。
- **fail-before 闸要真验**：钉「旧 ordering 会 RED」的测试（username-priority / kerberos-survives-simple-HDFS）。
- **历史不压制新发现**；完整背景：报告 `reviews/P5-paimon-rereview3-2026-06-12.md`；memory `catalog-spi-p5-*`。
