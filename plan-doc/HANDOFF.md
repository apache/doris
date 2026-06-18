# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取是**独立子线**，单独跟踪在
> [`metastore-storage-refactor/`](./metastore-storage-refactor/)（其 HANDOFF/PROGRESS/tasks/decisions 自洽，本文件不复述细节）。

---

# 🎯 下一个 session 的任务 — **paimon connector 全功能路径 clean-room 对抗 review**（先于 B8 legacy 删除）

整个 paimon connector cutover（P0–P5 + round-3 fixes + 元存储子线）已落地。**删除任何 legacy 之前**，先对**整条连接器**做一次完整的、不带历史先验的回归式复审：从**设计**与**实现交付**两层判断对错，并**逐一对照 legacy 找差异**（区分有意偏离 vs 漏移植/回归）。这是整体复审，不是某个增量 task 的局部 review。

**6 个 review 维度**（用户指定；每维度独立成一条对抗 review 线）：
1. **读取**（scan / split planning → BE 下发的整条读路）
2. **写入**（INSERT / sink，若存在则审，无则明确记录"无写路径"）
3. **DDL**（CREATE/DROP CATALOG·DB·TABLE、CTAS、属性校验）
4. **元数据回放（metadata replay）**（catalog/db/table 持久化 + FE 重启 / edit-log replay 重建 + GSON 序列化/反序列化注册）
5. **元数据 cache**（schema / partition / sys-table / MVCC snapshot 等的填充·命中·失效·刷新）
6. **残留旧逻辑 / fallback**：还有哪些路径**仍走旧逻辑**或在某条件下 **fallback 回旧逻辑**（仍引用 fe-core legacy 类 / legacy/compat/instanceof/兜底分支）

**方法**：clean-room 多 agent 对抗 review（参 memory `clean-room-adversarial-review-pref`）。每维度：finder（独立读**当前** paimon 实现 + **legacy 参照**实现，自行下判断）→ adversarial verifier（逐条试图**证伪**）→ synth。建议 workflow 编排（find→verify pipeline，每维度一条线）。

**⚠️⚠️ 关键约束（用户 2026-06-18 明确，最高优先级）：本轮不得注入开发过程已有的先验知识。**
不把 `decisions-log` / `deviations-log` / `risks` / 既往 review 报告（含 `reviews/P5-*`）/ 过往 CI-RCA / `~/.claude/.../memory/*`（`catalog-spi-*`）/ tasks-doc rationale 当作 review 的前提或预设答案喂给 review agents——这些会用「早已查过 / 已判定 OK / 已知非 bug」制造盲区、限制 review 的公正性与开放性。review agents 的输入**只有代码**（当前 paimon connector 实现 + legacy 参照实现 + 6 维度问题）。clean-room 靠 **fresh subagent + 编排者精选 prompt** 实现（reviewer 不继承主 session 上下文；尤其要挡住主 session **自动注入**的 `catalog-spi-*` auto-memory 正文——对 clean-room 也是「待验证历史声明」非事实）。orchestrator 读本 HANDOFF **仅为流程定向**，不把任何历史结论作为 review 输入。**当作第一次看这套代码，从零独立判断对错与 parity。与既往「Phase C 交叉核对历史结论」相反，本轮刻意不做。**

**对照基线（legacy / 原先逻辑）＝ 同时也是 B8 的删除目标**：fe-core `.../datasource/paimon/*` +
`.../datasource/property/storage/{OSS,COS,OBS,S3,Minio}Properties` + `.../property/metastore/HMSBaseProperties` 等
（+ 必要时 git 历史里 paimon 迁移前实现）。**故 review 必须先于 B8**（删了就没了对照基线）。仅读其**代码**、不读其历史结论 / commit message 里的判断。

**产物**：review 报告落 `plan-doc/reviews/`（每维度 finding 分级 BLOCKER/MAJOR/MINOR/NIT + 与 legacy 差异清单[有意偏离 vs 回归] + 处置建议）。**先 review、不改代码**；发现的修复各自另起 task（AGENT-PLAYBOOK 单任务循环）。

---

# 🔭 review 之后的主线 backlog（review 出报告后再排）

1. **B8 legacy 删除**（[`task-list-P5-rereview3-fixes.md`](./task-list-P5-rereview3-fixes.md) Follow-ups + 第三轮报告 R-1…R-8）：删 fe-core
   `datasource/paimon/*` + legacy `{OSS,COS,OBS,S3,Minio}Properties` / `HMSBaseProperties` 等 dead residue。
   **删除前提**：①上面的 review 完成（对照基线用完）②FIX-4 已 commit（literal 复刻对照完成）。**须保 load-bearing
   dispatch ordering**（`ShowPartitionsCommand:478-480`，R-4）。逐子树删 + 每批跑 fe-core 编译 + 连接器测 + regression-gated。
   **⚠️ 跨线 tension**：元存储子线 D-016 记「fe-core `datasource.property.{storage,metastore}` 两包仍服务
   hive/hudi/iceberg、不碰」；B8 想删其中 paimon-only 部分——**B8 scope 须先经 review dim-6（残留旧逻辑/fallback）确认
   哪些真 dead、哪些仍被 hive/hudi/iceberg 消费**，别误删在用类。
2. **元存储子线收尾**（[`metastore-storage-refactor/`](./metastore-storage-refactor/)）：P2-T04（paimon pom + gate，
   ⚠️ `MetaStoreProviders` ServiceLoader 改 2-arg 显式 loader 防子优先 loader 下发现不到 provider）→ P2-T05（docker
   5-flavor 真闸 + vended(REST/DLF) + Kerberos HMS + storage 等价，合并原 P1-T06；`enablePaimonTest=true`）。
3. **D-057 re-scope**（第三轮报告 §D.3）：deferred `TablePartitionValues:162` prune-path sentinel residue **不影响
   paimon**（MVCC override 绕过）→ re-scope 到非-MVCC 插件连接器（maxcompute/es/jdbc）。
4. **accepted-deviation 用户签字**（task-list「NOT in this fix scope」）：~10 MINOR + ~12 NIT + C-1 observability +
   uncheckedFallbacks（REFRESH cache invalidation / partitions-TVF auth / split-plan RPC 在 `executeAuthenticated` 外 /
   `PluginDrivenExternalCatalog:140` 吞 authenticator-wiring 异常）。逐条 accept-as-deviation 或转 fix。

---

# 📦 仓库 / 进度状态
- **HEAD = `13d3876d25d`**（元存储子线 P1-T07：删 fe-property 孤儿模块）。当前分支 **`catalog-spi-07-paimon`**（非 master）；
  已同步 push 到 `master-catalog-spi-07-paimon`（= PR [#64445](https://github.com/apache/doris/pull/64445) head，
  force-with-lease）。
- **主线（P0–P5）**：paimon connector SPI cutover + round-3 clean-room review 的 4 个 user-approved fix 全完成
  （FIX-1 `c376aba1264` rest-vended-uri / FIX-2 `2e845e88bf9` jni-file-format / FIX-3 `f08bc22b9bd` incr-scan-reset /
  FIX-4 `f0210b51871` feconf-storage-parity）。详见 `task-list-P5-rereview3-fixes.md` + `reviews/P5-paimon-rereview3-2026-06-12.md`。
- **元存储/storage 子线**（独立目录，本 session 推进）：storage 收口到 `fe-filesystem-api` typed（P1）+ 新建
  `fe-connector-metastore-{api,spi}` + `fe-kerberos`（P2-T01..T03，paimon 已 cutover 到共享 metastore SPI）+
  **fe-property 模块已物理删除**（P1-T07，0 消费者孤儿）。剩 P2-T04/T05（见 backlog）。**注**：fe-core
  `datasource.property.{storage,metastore}` 两包仍在（子线 D-016 不碰；B8 才考虑删其 paimon-only 部分）。
- ⚠️ `regression-test/conf/regression-conf.groovy` 仍 modified 未 commit 且含**明文 Aliyun key** → commit 前继续
  path-whitelist，**严禁 `git add -A`**；`regression-conf.groovy.bak` 同理排除。
- 未 commit/未跟踪：scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）；`reviews/P5-paimon-rereview3-2026-06-12.md`
  （第三轮 review 报告，未跟踪——大文件，下次方便时 vet+commit 或保留本地）。

## 🗺️ 代码脚手架
- **Plugin connector**：`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/`
  （`PaimonConnector` / `PaimonConnectorProvider` / 存储+HiveConf 装配 `PaimonCatalogFactory`[现 cutover 到
  `MetaStoreProviders.bind` + 薄 `assembleHiveConf`] / scan `PaimonScanPlanProvider` / @incr `PaimonIncrementalScanParams`）。
- **共享 SPI / 叶子**：`fe/fe-connector/fe-connector-{api,spi}/` + `fe-connector-metastore-{api,spi}/`（metastore 解析器 +
  `MetaStoreProvider` SPI/ServiceLoader）+ 顶层叶子 `fe/fe-kerberos/`（kerberos facts）+ `fe/fe-filesystem/`（typed
  storage，含 `-hdfs` BE model）。
- **fe-core 桥**：`fe/fe-core/.../connector/DefaultConnectorContext.java`、`.../datasource/PluginDriven*.java`、
  `.../fs/FileSystem{Factory,PluginManager}.java`；nereids scan-node 分发。
- **Legacy 对照基准（＝ review 对照 + B8 删除目标）**：fe-core `.../datasource/paimon/`、
  `.../datasource/property/storage/` 下 `{OSS,COS,OBS,S3,Minio}Properties`、`.../property/metastore/HMSBaseProperties`。
- **BE 消费端**：`be/src/format/table/`（`paimon_cpp_reader.cpp`、`paimon_reader.cpp`、`partition_column_filler.h`）。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 key）+ 清 scratch（`.audit-scratch/` `conf.cmy/`
  `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` / `[Pn-Tnn] <subj>` + 根因 + 解法 + 测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。fix commit 带其 design doc（repo 惯例）。
- **收尾推送惯例**（见 memory `catalog-spi-07-paimon-branch-pr-workflow`）：push `catalog-spi-07-paimon`(ff) +
  **force-with-lease** `master-catalog-spi-07-paimon`（PR #64445 head）+ 在 PR #64445 评论 `run buildall`。⚠️ 两分支
  历史曾发散；force 前先 fetch 对比、用 `--force-with-lease`。⚠️ remote URL 明文嵌 GitHub PAT（`git remote -v` 会打印）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false
  -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`（memory `doris-build-verify-gotchas`）。**漏 `-am` →
  `could not resolve … ${revision}` 假错**。paimon 模块需 `-am package -Dassembly.skipAssembly=true`（shade jar 携带
  HiveConf）。**checkstyle 在 `validate` phase（编译前）跑**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试 harness：`PaimonCatalogFactoryTest`（纯 Map→Configuration/HiveConf）/`PaimonScanPlanProviderTest`(real-table
  `FileSystemCatalog`)/`PaimonIncrementalScanParamsTest`/`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/
  `FakePaimonTable`（`.copy` 是 no-op recorder，reset/merge fail-before 须 real table）/ metastore-spi 的
  `*MetaStorePropertiesTest` / `DefaultConnectorContextNormalizeUriTest`(fe-core)。live-e2e CI-gated
  （`enablePaimonTest` 默认 false）→ 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **本轮是 review、不是改码**：先出 review 报告，发现项各自另起 fix task；**review 须 clean-room、零历史先验**（见上「关键约束」）。
- **review 必须先于 B8**（legacy ＝ 对照基线）；B8 scope 须经 review dim-6 确认真 dead（别误删仍被 hive/hudi/iceberg 消费的类）。
- **改 handle/分区/scan/storage/auth 流必 grep 全调用方 + 确认实际实例类（base vs MVCC 子类）**；storage/auth 装配注意 raw
  `hadoop.*`/`fs.*` passthrough 跑最后会 clobber 之前 authoritative 设置（FIX-4 4d/4e 亲证）。
- **design red-team（写码前）+ impl verification（写码后）两道**历史证有效（修复阶段照用，但 review 阶段保持 clean-room）。
- **元存储子线**细节不在本文件——读 `metastore-storage-refactor/HANDOFF.md`。
