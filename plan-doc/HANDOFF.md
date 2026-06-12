# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **继续逐项实现第三轮 review 修复（FIX-3 起）**

第三轮 clean-room 对抗 review 转出 4 个 user-approved fix（[`task-list-P5-rereview3-fixes.md`](./task-list-P5-rereview3-fixes.md)）。
**FIX-1 与 FIX-2 已完成并独立 commit**；下一步 = 按 task-list **继续 FIX-3 → FIX-4**，每个走 `step-by-step-fix` skill
（design doc → impl → tests → **独立 commit**）。

## ✅ 已完成（本 session）
- **FIX-1 `FIX-REST-VENDED-URI-NORMALIZE`（P9-1 BLOCKER）— commit `c376aba1264`**。REST+native+对象存储读不再抛
  `No storage properties found for schema: oss`。SPI 加 `normalizeStorageUri(uri, token)` overload；fe-core
  `DefaultConnectorContext` 抽 `buildVendedStorageMap`（与 `vendStorageCredentials` 单一来源），2-arg override
  用 vended-overlay map normalize（legacy「vended 替换 static」），1-arg delegate（行为不变）；连接器
  `PaimonScanPlanProvider` 把 once-per-scan `extractVendedToken(table)` thread 到两个 native normalize 站点。
  设计前跑了 5-skeptic + completeness-critic 对抗 workflow（DESIGN-SOUND）。
- **FIX-2 `FIX-JNI-FILE-FORMAT`（P7-1 MAJOR）— commit `2e845e88bf9`**。JNI/count split 不再发 `file_format="jni"`。
  `buildJniScanRange`/`buildCountRange` 改发真 `defaultFileFormat`（`buildCountRange` 加形参+call-site thread）；
  `PaimonScanRange.Builder` 默认 `"jni"`→`""`。**关键：JNI formatType 由 `paimon.split` 属性存在性 gate，非
  fileFormat 字符串**，故安全。

## 📋 待修清单（详见 task-list；建议按序）
3. **FIX-3 `FIX-INCR-SCAN-RESET`**（P2-1, MAJOR）— @incr 漏了 legacy 的 `scan.snapshot-id=null`/`scan.mode=null`
   防御性 reset；对持久化 `scan.*` 选项的表会错。**design 须先定**：`ConnectorMvccSnapshot.Builder.property()`
   **拒 null** → reset 须直接喂进 `table.copy(scanOptions)` 的 map（可持 key→null），或仅 incremental 路放行 null。
   site：`PaimonIncrementalScanParams.java` + `resolveScanTable`/`applySnapshot`（`table.copy(scanOptions)` 处）。连接器 only。
4. **FIX-4 `FIX-FECONF-STORAGE-PARITY`**（cluster P8-1/2/3/4·P9-2/3，用户定 **FULL legacy parity**）—
   `PaimonCatalogFactory.buildHadoopConfiguration` 从 raw props 重建 Configuration 不全。拆 4 独立 commit：
   **4a OSS**(endpoint-from-region+S3A 键)、**4b S3**(path-style+conn/timeout)、**4c COS/OBS**(fs.cosn.*/fs.obs.*+alias)、
   **4d HMS**(hive.metastore.username alias)。连接器 only（禁 import fe-core，literal 复刻 legacy 键逻辑）。

## ⚠️ 关键结论（修复时参照，**勿当先验压制新发现**）
- 本轮唯一 live BLOCKER = P9-1（**已修**）。**P11-1（DATE-epoch prune）是假 BLOCKER**：paimon 走
  `PluginDrivenMvccExternalTable.getNameToPartitionItems` override（解析 rendered name），不走 base raw-epoch 路 →
  D-057 的「prune-路 paimon 残留」框定有误，**B8 时 re-scope 到非-paimon 连接器**（task-list Follow-ups）。
- 翻闸结构性 OK（R-1…R-8）。legacy `datasource/paimon/*` = dead residue，**B8 删除放最后**（FE-config parity
  期间仍需 legacy `*Properties` 作对照）。

## 🗺️ 代码脚手架
- **Plugin connector**：`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/`
  （flavor/存储装配 `PaimonCatalogFactory.java`[FIX-4]；scan `PaimonScanPlanProvider.java`+`PaimonScanRange.java`；
  @incr `PaimonIncrementalScanParams.java`[FIX-3]）。`-api`/`-backend-*` 模块在 git 内为空壳。
- **fe-core 桥/SPI**：`fe/fe-core/.../connector/DefaultConnectorContext.java`、`.../datasource/PluginDriven*.java`；
  SPI `fe/fe-connector/fe-connector-{api,spi}/`。
- **Legacy 对照基准（仍在树内，勿删）**：`fe/fe-core/.../datasource/paimon/`（`source/PaimonScanNode.java`、
  `PaimonUtil.java`、property `OSSProperties/COSProperties/OBSProperties`、`PaimonRestMetaStoreProperties` 等）。
- **BE 消费端**：`be/src/format/table/`（`paimon_cpp_reader.cpp`[FILE_FORMAT/MANIFEST_FORMAT backfill :397-411]、
  `paimon_reader.cpp`、`partition_column_filler.h`）。

---

# 📦 仓库状态
- **HEAD = `2e845e88bf9`（FIX-2）**。迁移链：…→`199485bbde9`(round-3 review 任务)→`c376aba1264`(FIX-1)→**`2e845e88bf9`(FIX-2, HEAD)**。
  本 session 后另有一个 `docs:` 提（滚 HANDOFF + task-list FIX-1/2 勾掉）。
- ⚠️ `regression-test/conf/regression-conf.groovy` 仍 modified 未 commit 且含**明文 Aliyun key** —— commit 前继续
  path-whitelist，**严禁 `git add -A`**；`regression-conf.groovy.bak` 同理排除。
- 未 commit/未跟踪：scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）；`plan-doc/reviews/P5-paimon-rereview3-2026-06-12.md`
  （第三轮 review 报告，未跟踪——大文件，下次方便时 vet+commit 或保留本地）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）。**无 P0~P4 阻塞遗留**；P9-1 BLOCKER 已清。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 key）+ 清 scratch（`.audit-scratch/` `conf.cmy/`
  `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。fix commit 带其 design doc（repo 惯例）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false
  -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。改 fe-core `-pl :fe-core -am`；
  改 SPI `-pl :fe-connector-api`/`:fe-connector-spi -am`。**checkstyle**：连接器 `mvn -pl :fe-connector-paimon
  checkstyle:check`；fe-core `mvn -pl :fe-core checkstyle:check`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE 单测。harness：`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/`FakePaimonTable`/
  `PaimonScanPlanProviderTest`(real-table `FileSystemCatalog` 取真 DataSplit)/`PaimonIncrementalScanParamsTest`[FIX-3]/
  `PaimonCatalogFactoryTest`[FIX-4]/`DefaultConnectorContextNormalizeUriTest`(fe-core)。live-e2e CI-gated
  （`enablePaimonTest` 默认 false）→ 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **逐项修**：一次一个 fix，design→impl→test→commit，别批量糊。每个 fix 的根因/site/legacy 对照/test 都在
  [task-list-P5-rereview3-fixes.md](./task-list-P5-rereview3-fixes.md) + 各 `FIX-*-design.md`。
- **design 前对抗 verify 见效**（FIX-1 亲证）：5-skeptic 各驳一 claim + completeness critic 在写码前抓出
  signature-fanout（`buildNativeRanges` 连带破 2 额外测试点）+ test-double 矛盾（`RecordingConnectorContext` 必
  override 2-arg）。**改 handle/分区/scan 流必 grep 全调用方 + 确认实际实例类（base vs MVCC 子类）。**
- **历史不压制新发现**：P9-1 正是被 DV-025「合理化 defer」却没真修的。
- 完整背景：报告 `reviews/P5-paimon-rereview3-2026-06-12.md`；memory `catalog-spi-p5-*`（含
  `catalog-spi-p5-fix1-rest-vended-uri`）。
