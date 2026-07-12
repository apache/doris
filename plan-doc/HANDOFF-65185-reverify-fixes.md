# 🔖 HANDOFF —— #65185 复核修复系列(独立任务线快照)

> **用途**：这是**「复核修复系列」这一条任务线**的**独立**交接快照，与主线滚动 `plan-doc/HANDOFF.md`（HMS 翻闸主线）**分开**。你切去做别的实现后，回到这条线时**先读本文件**即可续做，不必炒对话历史。
> **生成**：2026-07-11（更新 2026-07-12）。**分支** = `catalog-spi-11-hive`（off `branch-catalog-spi`，PR base = `branch-catalog-spi`，squash 合并）。**HEAD 快照** = `88aa55b831b`（本线 L1 提交；主线另有 HMS 翻闸提交穿插,与本线文件不重叠,fast-forward）。
> **公开 tracking issue** = apache/doris#65185。

---

## 0. 这条任务线是什么

HMS 翻闸（catalog 类型 `hms` 从旧代码切到插件 SPI）后，第三方复核报告
`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` 判定为**真实/活跃且需改代码**的条目，按批次逐条修。
**不含**已登记/验收偏差（reverify §5）与不适用/已修/parity（reverify §6）——那些**明确不做**（见跟踪表文末「明确不做」）。

**处理纪律（每条一遍）**：起步读本文件 + 跟踪表 → 选一条 → **对 HEAD 复核现码**（reverify 行号可能已漂）→ 设计
（`plan-doc/tasks/designs/FIX-<id>-design.md`）→ **设计红队**（多 agent 对抗，见 memory `clean-room-adversarial-review-pref`）
→ 实现 → build + 靶向 UT → **独立 commit** → 勾跟踪表 → 更新本 HANDOFF + commit。**每条一个独立 commit**，code 与 doc 分开。

---

## 1. 起步必读（回来先读这几个，行号信 HEAD 不信文档）

1. **跟踪表（权威进度 + 每条现码/fix/测试意图 + 每轮滚动更新）** = `plan-doc/task-list-65185-reverify-fixes.md`。
2. **复核报告（证据/失败场景/对抗结论）** = `plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md`
   （§2 高危 / §3 中危 / §4 设计债 / §5 已登记偏差 / §6 不适用）。
3. **各条设计** = `plan-doc/tasks/designs/FIX-<id>-design.md`（含 recon + 设计红队结论 + 最终改动清单）。
4. **完成明细** = `git log`（commit message 详尽，勿在文档里重述）。

---

## 2. 当前进度（截至 HEAD `af1cb7e853a`）

| 批次 | 内容 | 状态 |
|------|------|------|
| 批次 0 | H1–H4 高危（hudi/hive 分区剪枝：转义/DATETIME/非-hive-style/混大小写列名） | ✅ 全 DONE + 3-skeptic 复审 CLEAN |
| 批次 1 | M5/M7/M6/M4/M2 中危（连接器局部：iceberg×3 + mc + hive） | ✅ 全 DONE + 最终复核 CLEAN |
| 批次 2 | M3→M1 中危(fe-core 通用节点) | ✅ 全 DONE |
| **批次 3** | **L1(import 门禁) + M8(发布工具/文档)** | **✅ L1 DONE;M8 ⏸ 用户跳过(07-12)** |
| 批次 4 | 低危连接器 L3–L20（trino/kerberos/mc/paimon/iceberg） | 🔄 trino L3-L6 ✅ · kerberos L7/L8 ✅ · mc L9 ✅ · paimon L11/L13/L14 ✅;**← 续 iceberg/杂项 L15–L19** |
| 决策类 | L2 / L10 / L12 / L20 | ⏸ 先用中文讲清背景问用户再动 |
| 设计债 | D-系列 | ⏸ 择机 / 随 P8 |

**批次 2 明细（本轮完成，含用户签字，勿再擅自翻）**：
- **M3** `6963de4124f`(code) + `97363fc9c33`(doc) —— batch 闸门 `shouldUseBatchMode` 的 `!isPruned` → `== NOT_PRUNED`：
  无谓词大分区表（MaxCompute + 翻闸 hive）恢复异步 batch split，**顺带解 M2 的 BATCH-UNPRUNED-SYNC**。
  证据 = legacy `MaxComputeScanNode:227` 的 `!= NOT_PRUNED`（git `1da88365e85^`）+ sibling `displayPartitionCounts`
  + 全 producer 枚举证闭合。设计红队 `wf_811e6242-d8b` 命中 1 blocker + 1 major 均已解。
  **⭐ 用户签字（2026-07-11）**：docker-hive golden `test_hive_partitions:200` `(approximate)inputSplitNum` **60→6**
  ——采用 **SPI 统一分区数口径**（不给 hive 补 legacy split-count 估算；对齐 MaxCompute + Trino「引擎层统一报分片」）。
  **反转**被前次评审特意锁定的 pinning 测试；**登记 supersession**：`decisions-log` D-035 / `deviations-log` DV-019
  的 LP-1「`!isPruned` 等价」判定已批注 SUPERSEDED。
- **M1** `17b432dc1e1`(code) + `af1cb7e853a`(doc) —— 翻闸 hive 上 `TABLESAMPLE` 被静默丢弃（全表扫）。
  设计红队 `wf_32decfa0-349` **推翻原「对所有连接器通用采样」方案（UNSOUND）**：`Split.getLength()` 语义因连接器而异
  （hive/iceberg=字节；MaxCompute 默认 byte_size / Paimon JNI range = **-1**；MaxCompute row_offset = **行数**）→
  盲目按字节采样出乱结果。**scope 更正 = hive-only 回归**（只有 hive 曾采样）。
  **⭐ 用户签字（2026-07-11）**：**只修 hive** —— 加 `ConnectorScanPlanProvider.supportsTableSample()` 默认 false 能力
  opt-in、仅 `HiveScanPlanProvider` override true；通用节点仅在连接器声明支持时 `sampleSplits`（legacy `selectFiles`
  端口），不支持连接器 no-op + WARN（非静默）。对齐 Trino `applySample` + `supportsBatchScan` 先例。
  （新记 memory `catalog-spi-split-length-not-universal-byte-size`。）

---

## 3. 批次 3 收尾 + 下一步 = 批次 4（低危连接器）

**批次 3 结果**：
- **L1 DONE** `88aa55b831b`（code+test）—— import 门禁补 3 洞 + **设计红队 `wf_643c11b4-3fe` 发现的第 4 洞**：
  4 条白名单 `grep -v` 按**整行**匹配（正则 `.`≡`/`）→ 连接器命名空间文件（608 个,全根在 `org.apache.doris.connector.**`）
  里的违规 import 被**按路径抑制**,门禁对其本该守护的模块**结构性失明**（实测:连接器目录下放 `import ...catalog.Type;`
  旧脚本 exit 0 放行）。修法=候选 grep 加宽（static / test glob / +6 包）+ **白名单锚定到 import 目标**（`:import ...`
  非整行）+ fqn sed 剥 `static`（修 static-vendored 误报,红队证 E3=正确性非装饰）+ 新增自测 `check-connector-imports.test.sh`
  （8 违规上报/2 vendored skip/3 allow;GREEN 于新、RED 于旧、真树 exit 0）。设计 `designs/FIX-L1-design.md`。**非 live**。
- **M8 ⏸ 用户 2026-07-12 明确跳过**（转做 L 系列）。侦察留档:`build.sh:1069-1083` 已部署连接器插件到
  `output/fe/plugins/connector/`（**非构建缺口**）;缺口在**升级流程**——只替 `fe/lib/` 漏拷新 `fe/plugins/connector/` 目录
  → replay 时全部 `type=hms` degraded（`CatalogFactory.java:119-127`）→首访抛（`PluginDrivenExternalCatalog.java:148-150`）。
  修法=升级文档/release-note + (**可选**)replay 收尾聚合 degraded ERROR（触 fe-core,需编译）。**不 silently drop**,表中留待办;
  回来做时先中文讲清「仅文档 vs 文档+可选防御码」让用户拍板。

**批次 4 已完成子群**：trino `L3–L6` ✅ · kerberos `L7/L8` ✅ · mc `L9` ✅ · **paimon `L11/L13/L14` ✅（本轮）**。
- **paimon 子群（L11/L13/L14）**：统一设计红队 `wf_05574ccb-bd2`（3 设计 × 3 lens = 9 agent,无 UNSOUND）。
  - `L11` `4a8650bd062` — JNI/COUNT range file_format 按首数据文件后缀取(legacy `getFileFormat(getPathString())` parity)+补 `.avro` 臂;
    call-site RED 测(`Table.copy` 令表默认≠磁盘后缀,红队 MAJOR:原 helper 孤立测不守护接线)。
  - `L13` `ced4775b844` — `toPaimonType` 嵌套 nullability `.copy(isChildNullable)`(ARRAY/MAP-value/STRUCT-field);
    **scope 仅 nullability**(comment=DV-035 M10.1 已接受、field-id 顺序 parity);`.copy(true)` 默认恒等,既有 parity 测保持绿。
  - `L14` `478718aca6f` — honor `ignore_split_type`(null-tolerant `resolveIgnoreSplitType` + 三 legacy continue 位;
    `IGNORE_PAIMON_CPP` no-op=legacy parity,全树 grep 证);nonDataSplit IGNORE_JNI 位 E2E-only。
  - **⚠ 构建坑复现**：paimon 模块 `mvn test` 因 hive-shade 模块 shade 绑 `package` 阶段→`org.apache.hadoop.hive.conf` NoClassDefFound
    **假失败**;改 `package` 阶段即绿。模块靶向 UT 全绿(scan 69/69 + type-mapping/schema-builder 26/26)、0 checkstyle、gate 净。

**下一步 = 批次 4 剩余（低危连接器/杂项）**：`L15–L19`（`L15` paimon `PAIMON_SCAN_METRICS` 悬空常量、`L16/L17` iceberg
快照/schema 缓存偏斜 + version-blind schema 绑定、`L18` iceberg 未知/v3 类型静默 UNSUPPORTED、`L19` `partition_columns` 魔法键撞名）。
逐条走单任务循环（复核现码→设计→红队→实现→build+UT→独立 commit→勾表→更 HANDOFF）。
⏸ **决策类 `L2/L10/L12/L20` 先中文讲清背景+选项问用户再动**（memory `ask-user-explain-in-chinese-first`）。设计债 D-系列择机。
跟踪表「建议批次」节有全清单。

---

## 4. 铁律 / 约束（每条修复都受约束）

- **fe-core 不得**新增 `if(hive/iceberg/hudi)` / `instanceof HMSExternal*` / `switch(dlaType)` / 源名判别；**不解析属性**
  （storage→fe-filesystem、meta→fe-connector）；通用 SPI 节点 connector-agnostic
  （memory `catalog-spi-plugindriven-no-source-specific-code`、`catalog-spi-no-property-parsing-in-fecore`）。
  **按连接器区分能力用 `supports*()` opt-in**（非源名分支），如 `supportsBatchScan`/`supportsTableSample`。
- **`Split.getLength()` 语义因连接器而异**（-1 / 行数 / 字节）——通用节点凡按 split 大小处理须能力 opt-in、禁假设字节大小
  （memory `catalog-spi-split-length-not-universal-byte-size`）。
- 跨插件/跨边界**须 pin TCCL**（memory `catalog-spi-plugin-tccl-classloader-gotcha`，含 4 locus + HiveConf 构造点）。
- `history_schema_info` 嵌套字段名逐层 lowercase（memory `catalog-spi-history-schema-info-lowercase-nested-names`）。

---

## 5. 构建 / 验证备忘（复用）

- fe-core：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false`
  （**漏 `-am`→假 `${revision}` 错**）。连接器：`-pl :fe-connector-<mod> -am`。SPI 改动（fe-connector-api）会被
  `-am` 连带 rebuild。
- 靶向 UT 加 `-Dtest=<Class> -DfailIfNoTests=false`（`-am` + `-Dtest` 上游无匹配测试会报假「No tests were executed!」）。
- **⚠ paimon 模块用 `install`/`package`**（shade jar 绑 package 阶段）；hms/hive/hudi/iceberg/mc 无此坑。
- **信 LOG 不信 exit**：后台 task 通知的 exit 是 wrapper 的；重定向到文件 grep `BUILD SUCCESS|BUILD FAILURE|[ERROR].*\.java:|Tests run:|You have N Checkstyle`。全量编译 ~6min → 后台跑。cwd 会重置 → 绝对路径。**勿用 worktree 隔离编译 agent**（`/mnt/disk1` 盘紧）。
- 连接器测试**无 Mockito**（真 recording fake）；**fe-core 有 Mockito**。checkstyle 禁 static import、扫 test 源、`UnusedImports` fail build。
- `bash tools/check-connector-imports.sh` 须 exit 0（连接器不得 import fe-core）。
- **memory** `doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`。

---

## 6. 提交 / 工作树纪律

- **path-whitelist `git add`，严禁 `git add -A`**：工作树大量遗留 scratch（`regression-test/conf/regression-conf.groovy`
  明文 key【本轮它被 build 改动过、勿提交】、`*.bak`、`.audit-scratch/`、`conf.cmy/`、`META-INF/`、`docker/...`、
  `plan-doc/reviews/P5-*`、`.claude/`、`failed-cases.out`——**非本线程产物，勿混入任何 commit**）。
- commit message：`[fix|doc](catalog) …` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
  + `Claude-Session: …`。**每子步/每条 fix = 独立 commit**；设计文档 + HANDOFF 单独 commit（与 code 分开）。
- 上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）。

---

## 7. ⚠ 并行 session 风险（本轮真实踩过）

本工作树是 linked worktree、**多 session 共享同一分支 + 同一构建 target**。本轮：
- 另一 session（你自己，`morningman`）在批次 2 期间往同分支提交了 2 个 hive 改动（`4dbb8e02056` 回归文档、
  `99e0b4a6ade` HiveConf classloader 修复，改 `fe-connector-hms/HmsConfHelper.java`）——与本线程文件**不重叠**、无覆盖。
- 另一 session 的 `mvn ... be-java-extensions package -am -T 1C` **污染共享 target**，一度令本地 UT 报
  「cannot access 生成类」**假失败**（非本码问题；待其结束干净重跑即绿）。
- **起步/动码前先探**：`git log`/`git status`、运行中 maven（`ps aux | grep plexus.classworlds.launcher`）、近 90s mtime；
  发现活跃即优先只写新文件 + 小步快提交；build 假失败先排查并发污染再怀疑本码（memory
  `concurrent-sessions-shared-worktree-hazard`）。

---

## 8. e2e 欠账（用户自跑，勿丢，非静默）

**批次 0/1/2 所有 e2e 均 live-gated（真集群）**，回归清单：
- 批次 0：H1–H4（转义值 / DATETIME / 非-hive-style 带 filter / MOR-JNI 混大小写读）。
- 批次 1：equality-delete 统计 UNKNOWN / vended-region / s3tables 默认凭证链 / mc 分区缓存往返 / hive 大分区异步 split。
- **批次 2（本轮新增）**：M3 = docker-hive `test_hive_partitions` `(approximate)inputSplitNum=6` + MaxCompute 无谓词
  ≥阈值分区表进 batch（结果与同步逐行一致）；M1 = docker-hive `test_hive_tablesample_p0` 结果不变式（**强基数缩减
  sample<full 须多文件 fixture**，单文件 student 采样最小粒度=1 文件=全表）+ 非 hive 连接器 `TABLESAMPLE` 返全表 + FE log WARN。
- 连同翻闸原有欠账（异构 `type=hms` 读/写/DDL/procedure/MTMV/time-travel/@incr、从库事件同步陈旧、Kerberos-HMS 冒烟、
  GSON replay、hive 视图）：完整矩阵见 `plan-doc/tasks/hms-cutover-execution-plan-2026-07-10.md` §4/§5 +
  `hms-spi-cutover-flip-2026-07-10.md` §5。memory `hms-iceberg-delegation-needs-e2e`。

---

## 9. memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` ·
`session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` ·
`catalog-spi-plugindriven-no-source-specific-code` · `catalog-spi-no-property-parsing-in-fecore` ·
`catalog-spi-split-length-not-universal-byte-size` · `plugindriven-mvcc-table-is-live-not-dormant` ·
`concurrent-sessions-shared-worktree-hazard` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` ·
`memory-keep-only-general-or-requested`。
