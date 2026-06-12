# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **FIX-4 `FIX-FECONF-STORAGE-PARITY`（FE-config FULL legacy parity）**

第三轮 clean-room 对抗 review 转出 4 个 user-approved fix（[`task-list-P5-rereview3-fixes.md`](./task-list-P5-rereview3-fixes.md)）。
**FIX-1 / FIX-2 / FIX-3 已完成并各自独立 commit**；剩 **FIX-4**。每步走 `step-by-step-fix` skill
（design doc → impl → tests → **独立 commit**），design 前建议跑对抗 red-team（FIX-1/FIX-3 亲证有效）。

## ✅ 已完成（本 session）
- **FIX-3 `FIX-INCR-SCAN-RESET`（P2-1 MAJOR）— commit `f08bc22b9bd`**。@incr 不再因基表持久化的
  stale `scan.snapshot-id`/`scan.mode` 而崩/丢行。**Option 2**：`validate()` 保持 null-free（共享
  `ConnectorMvccSnapshot` SPI 不进 null）；两个 null reset 在唯一 `Table.copy` chokepoint
  `PaimonScanPlanProvider.resolveScanTable` 经新 `PaimonIncrementalScanParams.applyResetsIfIncremental`
  重新施加（覆盖 native + JNI 两 caller）。paimon `copyInternal` 把 null 当 `options.remove(k)`。
  gate=`incremental-between`/`-timestamp` 存在性（真 snapshot/tag pin 原样放行）；严格 legacy parity
  只 reset 两键。**实测失败态是 `copy()` 硬抛 `IllegalArgumentException`（非仅静默丢行）**；real-table 测
  proven fail-before/pass-after。design red-team `wf_ffd11631-ed2`（DESIGN-SOUND）。
  验证：连接器 20/44/37 绿；checkstyle 0；import-gate 干净。设计/总结见
  `FIX-INCR-SCAN-RESET-design.md` + `-summary.md`。

## 📋 待修清单（详见 task-list）
4. **FIX-4 `FIX-FECONF-STORAGE-PARITY`**（cluster P8-1/2/3/4·P9-2/3，用户定 **FULL legacy parity**）—
   `PaimonCatalogFactory.buildHadoopConfiguration` 从 raw props 重建 Configuration 不全（filesystem/jdbc/HMS
   flavor → catalog/metadata 访问在缺失 backend 上失败）。**连接器 only（禁 import fe-core，literal 复刻
   legacy 键逻辑，同既有 `applyCanonical*` 模式）**。拆 **4 独立 commit**（也可单 commit）：
   - **4a `FIX-FECONF-OSS`**（P8-1/P8-3）：endpoint 缺省时由 region 推 `fs.oss.endpoint`
     （`oss-<region>[-internal].aliyuncs.com`，ref legacy `OSSProperties.getOssEndpoint:277-279,314-326`）+ 补 OSS 的 S3A 键（`fs.s3.impl`/`fs.s3a.*`）。
   - **4b `FIX-FECONF-S3`**（P8-2/P9-3）：由 `use_path_style`/`s3.path-style-access` 出 `fs.s3a.path.style.access` + conn/timeout 键（MinIO/path-style）。
   - **4c `FIX-FECONF-COS-OBS`**（P9-2）：加 `cos.*`/`obs.*` alias 数组 + 出 COS 键（`fs.cosn.impl`/`fs.cosn.userinfo.secretId|secretKey`/`fs.cosn.bucket.region`，ref `COSProperties:174-182`）+ OBS 键（`fs.obs.impl`/`fs.AbstractFileSystem.obs.impl`/`fs.obs.access.key|secret.key`，ref `OBSProperties:194-204`）。
   - **4d `FIX-FECONF-HMS-USER`**（P8-4）：`buildHmsHiveConf` 出 `hive.metastore.username` alias（映 `hadoop.username`）。
   测试 `PaimonCatalogFactoryTest`：每 backend 一例（region-only OSS→`fs.oss.endpoint`；COS→`fs.cosn.*`；
   OBS→`fs.obs.*`；S3 path-style；HMS username alias）。**Build：连接器 only**。

## ⚠️ 关键结论（修复时参照，**勿当先验压制新发现**）
- **P11-1（DATE-epoch prune）是假 BLOCKER**：paimon 走 `PluginDrivenMvccExternalTable.getNameToPartitionItems`
  override（解析 rendered name），不走 base raw-epoch 路 → D-057 的「prune-路 paimon 残留」框定有误，
  **B8 时 re-scope 到非-paimon 连接器**（task-list Follow-ups）。
- 翻闸结构性 OK（R-1…R-8）。legacy `datasource/paimon/*` = dead residue，**B8 删除放最后**；FIX-4 期间仍需
  legacy `*Properties`（`OSSProperties`/`COSProperties`/`OBSProperties`/`HMSBaseProperties`）作 literal 复刻对照。

## 🗺️ 代码脚手架
- **Plugin connector**：`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/`
  （**flavor/存储装配 `PaimonCatalogFactory.java`[FIX-4]**：`buildHadoopConfiguration:390-394`、
  `applyStorageConfig:412-426`、`applyCanonicalS3Config:437-465`、`applyCanonicalOssConfig:475-499`、
  alias 数组 `:87-106`、`buildHmsHiveConf`；scan `PaimonScanPlanProvider.java`+`PaimonScanRange.java`；
  @incr `PaimonIncrementalScanParams.java`）。`-api`/`-backend-*` 模块在 git 内为空壳。
- **fe-core 桥/SPI**：`fe/fe-core/.../connector/DefaultConnectorContext.java`、`.../datasource/PluginDriven*.java`；
  SPI `fe/fe-connector/fe-connector-{api,spi}/`。
- **Legacy 对照基准（仍在树内，勿删；FIX-4 literal 复刻源）**：`fe/fe-core/.../datasource/property/storage/`
  下 `OSSProperties`/`COSProperties`/`OBSProperties`/`HMSBaseProperties`（grep 确认实际路径）；
  `fe/fe-core/.../datasource/paimon/`（`source/PaimonScanNode.java`、`PaimonUtil.java`、
  `PaimonRestMetaStoreProperties` 等）。
- **BE 消费端**：`be/src/format/table/`（`paimon_cpp_reader.cpp`、`paimon_reader.cpp`、`partition_column_filler.h`）。

---

# 📦 仓库状态
- **HEAD = `f08bc22b9bd`（FIX-3）**。迁移链：…→`c376aba1264`(FIX-1)→`2e845e88bf9`(FIX-2)→
  `1b2b4236db3`(docs)→**`f08bc22b9bd`(FIX-3, HEAD)**。本 session 后另有一个 `docs:` 提
  （滚 HANDOFF + task-list 勾 FIX-3 + 加 `FIX-INCR-SCAN-RESET-summary.md`）。
- ⚠️ `regression-test/conf/regression-conf.groovy` 仍 modified 未 commit 且含**明文 Aliyun key** —— commit 前继续
  path-whitelist，**严禁 `git add -A`**；`regression-conf.groovy.bak` 同理排除。
- 未 commit/未跟踪：scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）；
  `plan-doc/reviews/P5-paimon-rereview3-2026-06-12.md`（第三轮 review 报告，未跟踪——大文件，下次方便时
  vet+commit 或保留本地）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）。**无 P0~P4 阻塞遗留**；P9-1 BLOCKER 已清；P2-1 已清。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 key）+ 清 scratch（`.audit-scratch/` `conf.cmy/`
  `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。fix commit 带其 design doc（repo 惯例）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false
  -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`/`MVN_EXIT`（[[doris-build-verify-gotchas]]）。
  **漏 `-am` → `could not resolve fe-connector-spi ${revision}` 假错**（FIX-3 fail-before 验证亲证）。改 SPI
  `-pl :fe-connector-api`/`:fe-connector-spi -am`。**checkstyle**：连接器
  `mvn -f …/fe/pom.xml -pl :fe-connector-paimon -am checkstyle:check`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE 单测。harness：`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/`FakePaimonTable`
  （注意 `FakePaimonTable.copy` 是 no-op recorder，不能当 reset/merge 的 fail-before 闸——须 real
  `FileSystemCatalog`，见 FIX-3 `resolveScanTableResetsStalePinForIncrementalRead`）/`PaimonScanPlanProviderTest`
  (real-table `FileSystemCatalog`)/`PaimonIncrementalScanParamsTest`/`PaimonCatalogFactoryTest`[FIX-4]/
  `DefaultConnectorContextNormalizeUriTest`(fe-core)。live-e2e CI-gated（`enablePaimonTest` 默认 false）→ 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **逐项修**：一次一个 fix，design→impl→test→commit，别批量糊。各 fix 根因/site/legacy 对照/test 都在
  [task-list-P5-rereview3-fixes.md](./task-list-P5-rereview3-fixes.md) + 各 `FIX-*-design.md`。
- **design 前对抗 red-team 见效（FIX-1/FIX-3 亲证）**：5-skeptic 各驳一 claim + completeness critic 在写码前
  抓出 signature-fanout（FIX-3：`resolveScanTable` 两 caller 共 chokepoint）、test-double 矛盾
  （`FakePaimonTable.copy` 是 no-op→fail-before 须 real table）、framing 纠偏（FIX-3 失败态实为硬抛非静默丢行）。
  **改 handle/分区/scan 流必 grep 全调用方 + 确认实际实例类（base vs MVCC 子类）。**
- **fail-before 闸要真验**：FIX-3 neuter 掉 fix 后跑 real-table 测确认 RED（`IllegalArgumentException`）再恢复
  （verification-before-completion；勿凭「应该会红」自满）。
- **历史不压制新发现**：P9-1（FIX-1）正是被 DV-025「合理化 defer」却没真修的；P2-1（FIX-3）的 strip 也是被
  「fresh table 无 inherited scan.*」错误合理化。
- 完整背景：报告 `reviews/P5-paimon-rereview3-2026-06-12.md`；memory `catalog-spi-p5-*`。
