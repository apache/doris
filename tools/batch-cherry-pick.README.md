<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 批量准入（Batch Cherry-Pick Admission）

> 类 Mergify 的**批量流水线准入**机制：把多个待合入 release 分支的 PR 叠在一条集成分支上，**只跑一次**回归流水线；通过后这一批 PR 全部准入并合入。用于大量 `master → branch-4.0/4.1` cherry-pick 场景，显著降低 TeamCity 测试资源占用。

涉及文件：

| 文件 | 角色 |
|---|---|
| `.github/workflows/batch-cherry-pick.yml` | 入口：`issue_comment`（命令）+ `schedule`（每 ~10min 轮询） |
| `tools/batch-cherry-pick.py` | 编排核心：选 PR、组装、触发 CI、状态机、轮询、对账合入 |
| `regression-test/pipeline/common/batch-utils.sh` | 薄封装，`source teamcity-utils.sh` 复用 `trigger_or_skip_build` |

---

# 第一部分 · 使用者指南

## 1. 它解决什么问题

发布分支（如 `branch-4.0`）经常需要从 master 批量 cherry-pick 大量 PR。若每个 PR 都各跑一遍完整 TeamCity 回归（p0/p1/external/cloud/…），测试资源消耗巨大。

本功能让你**把一批 PR 合在一条集成分支上只跑一次流水线**：绿了，则这一批 PR 的准入状态全部置绿并逐个合入；红了，由你决定重跑 / 修复 / 放弃。

> ⚠️ 它替代的是**重的 TeamCity 回归流水线**。每个 member PR 上那些**轻量的 per-PR check**（license、clang-format、checkstyle、title-checker 等）仍各自照常跑——它们本来就便宜，且已 approve 的 PR 上通常早已是绿的。

## 2. 一次性准备（管理员做一次）

1. 在仓库创建 label：`batch-queue/branch-4.0`、`batch-queue/branch-4.1`（每个要启用的发布分支一个）。
2. 每个发布分支开一个**常驻 tracking Issue**，打上对应 `batch-queue/<branch>` label。例如标题「branch-4.0 backport queue」。
3. 确认 workflow 的接线项（见第二部分「上线 Checklist」）：合入 token 权限、`BRANCHES` 列表等。

之后所有操作都在对应分支的 tracking Issue 里通过**评论**完成。

## 3. 快速开始

在 `branch-4.0` 的 tracking Issue 里评论：

```
run batch
```

系统会自动挑选一批符合条件的 PR、组装、跑一次流水线，并在 Issue 里回复进展。等它回复「✅ Batch green」即代表这批已自动合入。

## 4. 命令详解

所有命令都在**对应发布分支的 tracking Issue** 里评论；只有 **committer**（GitHub 关系为 OWNER / MEMBER / COLLABORATOR）评论才生效。

| 命令 | 作用 | 在飞时能用？ |
|---|---|---|
| `run batch` | 开新批：自动 sweep 该分支下所有合格的 open PR（上限 `MAX_BATCH`，默认 10） | ❌ 会被拒 |
| `run batch #101 #102 #103` | 开新批：只用你显式列出的这些 PR | ❌ 会被拒 |
| `rerun batch` | 重跑当前批：按**同一组 PR 的当前 head** 重新组装并重跑 CI | ✅ |
| `cancel batch` | 放弃当前批：关闭载具 PR、删集成分支、释放锁 | ✅ |

> **同一时刻每个分支只允许一个批次在飞**（串行，类似 merge queue）。在飞期间 `run batch` 会被拒绝并提示当前批次。

## 5. 一次完整流程你会看到什么

以 `run batch` 为例，Issue 里会依次出现：

1. **启动回复**：
   > 🚂 Batch `20260703-...` started on `branch-4.0` — vehicle #67890
   > Members (8): #101 #103 #105 ...
   > Dropped: #107 (cherry-pick conflict)

   同时会新建一个标题为 `[test](batch) DO NOT MERGE — batch ...` 的**载具 PR**（#67890）。它只是测试载体，**永远不会被合入**，最后会自动关闭。

2. **CI 运行**：载具 PR 上会像普通 PR 一样跑完整流水线（一次）。

3. **结果回复**（由每 ~10min 的轮询触发）：
   - 全绿 → `✅ Batch green — admission complete.` 列出已合入 / 跳过的 PR，随后自动清理。
   - 有红 → `❌ Batch CI failed.` 列出失败与仍在跑的检查项，并给出下一步选项。

member PR 是被**逐个正常 merge**（因此每个 PR 都正常显示为 **Merged**），不需要你手动关闭。

## 6. 哪些 PR 会被选入

`run batch`（无参 sweep）时，一个 open PR 被选中需**同时满足**：

- base 分支 == 该 tracking Issue 对应的发布分支；
- **不是** draft；
- **不带**冲突 label（`branch-4.0` → `dev/4.0.x-conflict`）；
- **带 `approved` label**（可用 `REQUIRE_APPROVED=false` 关闭该要求）；
- 不是 GitHub 已明确判定的冲突态（`mergeable == false`）。

按 PR 号升序选取，取满 `MAX_BATCH` 个为止。

`run batch #a #b` 显式列表：仍会套用上述过滤（你列了但不合格的会被跳过并在日志说明）。

## 7. 异常情况处理大全

| 场景 | 系统行为 | 你要做什么 |
|---|---|---|
| **组装时某 PR 与本批其他 PR / 当前 base 冲突** | 该 PR 被自动**踢出**本批（`Dropped`），其余照常继续 | 无需处理；该 PR 留待单独处理或下一批 |
| **CI 因 flaky 失败** | 批次进入 `failed`，挂起等你 | 评论 `rerun batch` 换一次运气 |
| **你修好了批内某个 PR（push 了新提交）** | —— | 评论 `rerun batch`：会按**当前 head** 重新组装，自动带上你的修复 |
| **某个检查确实误报，你想手动放行** | 轮询只看载具 SHA 是否全绿，**不关心怎么变绿的** | 直接去 TeamCity 把那条失败子构建**重跑 / 改绿**；下一拍轮询检测到全绿会**自动**续跑并回复「全绿」 |
| **想放弃这一批** | —— | 评论 `cancel batch`，立即释放锁 |
| **在飞时又评论 `run batch`** | 被拒绝，提示当前在飞批次链接 | 等它结束，或先 `cancel batch` |
| **在飞时你手动合入了批内某个 PR** | 合入阶段检测到它已 merged/closed → **跳过**（不重复合），其余照常 | 无需处理；结果回复里会标注「already merged/closed」 |
| **批内某 PR 在组装后又被 push 了新提交，但你没 rerun** | 合入阶段发现其 head 与被测时不一致 → **跳过**（避免合入未测内容） | 想合它就把它放进下一批，或 `rerun batch` |
| **合入时 base 已变化导致该 PR 冲突** | 该 PR merge 返回冲突 → **跳过并上报**，其余照常合 | 把被跳过的 PR 放进下一批 |
| **member 来自 fork，无法给其 head 打状态** | 该 PR 被跳过并上报，不影响整批 | 通常 release backport 都在同仓分支，极少触发 |
| **批次长时间无人处理** | 超过 `BATCH_MAX_AGE_H`（默认 12h）自动 `cancel` 并回复 | 重新 `run batch` |
| **合入权限不足**（分支保护禁止 bot 合入） | 该 PR merge 报错、被跳过并上报 | 让管理员配置 `MERGE_TOKEN`（见第二部分） |
| **合入中途 workflow 崩溃**（罕见） | 已合的保持已合，其余未合；状态可能卡在 `merging` | 评论 `cancel batch` 立即释放锁，再对剩余 PR `run batch` |

**关键原则**：本系统**永远不会把未通过的代码合进发布分支**——分支保护（approved + required status）与「组装冲突剔除 / 合入前实况对账」是双重兜底。最坏情况只是某些 PR 没被合，留给下一批。

## 8. FAQ

**Q：批过了是不是就等于每个 PR 单独都过了？**
不完全。它证明的是「这一批一起测是绿的」。所以系统**只把整批按被测的样子合入**，且合入期间要求发布分支不被旁路推进。这正是它安全的前提，也是「在飞时尽量别手动往发布分支合东西」的原因。

**Q：为什么要建一个 `DO NOT MERGE` 的载具 PR？**
为了**零改动复用**现有 TeamCity 触发链（它构建 `pull/<N>` ref），并让流水线把结果状态自然回写到该 PR。它只是测试载体，最后会自动关闭。

**Q：`run batch` 一次最多多少个？**
默认 10（`MAX_BATCH`）。批越大，撞上 flaky / 一次失败浪费的资源也越大，建议中等批量。

**Q：能同时对 4.0 和 4.1 各跑一批吗？**
可以。串行化是**按分支**的——不同分支的 tracking Issue 各自独立，互不阻塞。

---

# 第二部分 · 维护者指南（原理与机制）

## 1. 准入的本质

Doris 里一个 PR 能否合入发布分支，取决于两件事：

1. **人工**：`approved` label（committer approve 后由 `approve-label.yml` 自动打）。
2. **CI**：分支保护要求的一组 **GitHub commit status（context）** 在 **PR head SHA** 上为 `success`。

这组 context 对 `branch-4.0` 共 12 个（见 `teamcity-utils.sh` 的 `conment_to_context`）：

```
COMPILE (DORIS_COMPILE)            FE UT (Doris FE UT)
BE UT (Doris BE UT)                Cloud UT (Doris Cloud UT)
P0 Regression (Doris Regression)   P1 Regression (Doris Regression)
External Regression (...)          cloud_p0 (Doris Cloud Regression)
cloud_p1 (Doris Cloud Regression)  vault_p0 (Doris Cloud Regression)
NonConcurrent Regression (...)     check_coverage (Coverage)
```

**准入 = 给某个 SHA 打上这组 status。** 这就是可以被 batch 利用的支点。

## 2. 为什么可以 batch —— `skip_build` 原语

`teamcity-utils.sh` 里已有的 `skip_build(sha, type)` 做的事，就是：

```
POST /repos/apache/doris/statuses/{sha}  {state:"success", context: <该 type 对应的 context>}
```

即「**不跑流水线、直接给某个 SHA 打准入状态**」。它现网用于 `skip buildall`。

batch 模式的全部把戏就是：**让一批 PR 一起在一条集成分支上真跑一次流水线来"背书"，然后把 success 状态广播到这一批每个 PR 的 head SHA**。广播用的正是同一套 commit-status 机制。

## 3. 架构总览

```
tracking Issue (label: batch-queue/branch-4.0)
        │  评论 run / rerun / cancel batch
        ▼
.github/workflows/batch-cherry-pick.yml
   ├── on issue_comment  → python  run|rerun|cancel <issue>
   └── on schedule(10m)  → python  poll
        │
        ▼
tools/batch-cherry-pick.py  (编排：选PR/组装/状态机/对账合入)
        │  触发CI / 读required contexts
        ▼
regression-test/pipeline/common/batch-utils.sh
        │  source
        ▼
regression-test/pipeline/common/teamcity-utils.sh
   (trigger_or_skip_build / skip_build / 所有 TeamCity URL·凭据·映射)
```

**复用原则**：所有 TeamCity URL、凭据、流水线名映射、触发逻辑都留在 `teamcity-utils.sh` 一处；batch 只在其上薄薄封装两个函数（`list_required_contexts`、`trigger_batch_ci`），其余编排在 Python 里。

## 4. 关键设计决策

- **载具 PR 而非裸分支**：集成分支以一个 `[test](batch) DO NOT MERGE` PR 暴露出来，从而复用现有「构建 `pull/<N>`」的 TeamCity 触发与回写，**无需改 TeamCity VCS 配置**。载具 PR 永不合入，终态关闭。
- **Model A（广播 + 逐个合 member）而非合并载具 PR**：让每个 member PR 正常显示 **Merged**，保留 provenance；载具分支只当测试载体。
- **单批在飞（串行化）**：类似 merge queue，保证「被测的 commit 集合/顺序 == 实际合入的」这一不变式。
- **状态存在 tracking Issue 的一条 marker 评论里**：无需外部存储；人可读、可追溯；轮询与命令都从这里读写。

## 5. 状态机

状态存于 marker 评论内嵌的 JSON（`state` 字段）：

| 状态 | 含义 | 持锁？ |
|---|---|---|
| `assembling` | 正在组装集成分支 | ✅ |
| `testing` | CI 运行中 | ✅ |
| `failed` | CI 失败，挂起等操作员 | ✅ |
| `merging` | 全绿，正在对账合入 | ✅ |
| `done` | 完成 | ❌ |
| `canceled` | 放弃 / 超时 | ❌ |

`ACTIVE_STATES = {assembling, testing, failed, merging}` 即「持锁」集合——`run batch` 见到活跃批次即拒绝。`done/canceled` 为终态。

同一条 marker 评论**跨批次复用**（每开新批在原评论上 edit 覆盖），因此一个 Issue 上永远只有一条状态评论；里程碑（选中/结果）另发独立评论构成时间线。

## 6. 完成检测（轮询）

`poll`（cron 每 ~10min）对每个活跃 Issue：

1. 超时检查：`updated_at` 距今 > `BATCH_MAX_AGE_H` → 自动 `cancel`。
2. `classify(target, 载具SHA)`：拉载具 SHA 的 combined status，逐一比对 `list_required_contexts`：
   - 任一 required context 为 `failure/error` → **failed**；
   - 有 required context 尚未出现或 `pending` → **pending**（继续等）；
   - 全部 `success` → **green**。
3. green → 进入对账合入；failed（且原为 testing）→ 置 `failed` 并回复失败详情。

> **场景 2.3（手动放行）之所以零成本**：轮询只判断「载具 SHA 是否全绿」。你在 TeamCity 手动重跑/改绿那条失败子构建后，TeamCity 会把该 context 回写为 success，下一拍轮询自然判定 green 并续跑。

## 7. 组装算法

```
git fetch origin <target>
git checkout -B <integration_branch> origin/<target>
for pr in 选中的 PR（按号升序）:
    git fetch origin pull/<pr>/head
    head = FETCH_HEAD
    mb   = merge-base(origin/<target>, head)
    if rev-list --count mb..head == 0: 记 dropped「no new commits」; continue
    git cherry-pick mb..head
        成功 → members += {num, head}
        冲突 → cherry-pick --abort; 记 dropped「conflict」
git push -f origin <integration_branch>
```

即把每个 PR 相对 base 的**自有提交**（`mb..head`）依次 restack 到全新的集成分支上。组装冲突即最准的「不可干净合入」判据。

## 8. 对账合入的正确性

全绿后 `reconcile_and_merge` 对每个 member **以 GitHub 实况为准**（不盲信 manifest）：

```
for m in manifest.prs:
    pr = get_pull(m.num)
    (a) 已 merged/closed          → skip「already merged/closed」
    (b) pr.head.sha != m.head     → skip「head changed；未测内容」
    (c) 广播 success 到 pr.head.sha（失败 → skip「could not set status」）
    (d) merge(MERGE_METHOD)
          成功 → merged
          冲突/被挡 → skip「merge blocked」
```

- **(a)** 处理「在飞时被手动合入」——不重复合。
- **(b)** 处理「组装后成员又被改动但没 rerun」——不合入未被测的内容。
- **(d)** 处理「base 移动导致冲突」——跳过而非强合。

**不变式**：真正落入发布分支的 commit 集合/顺序，必须与被测的集成分支一致，且合入期间发布分支未被旁路推进。串行化 + (a)(b)(d) 三道对账共同维持它。

## 9. required contexts 的推导

`list_required_contexts(branch)`（`batch-utils.sh`）直接遍历 `teamcity-utils.sh` 的
`targetBranch_to_pipelines[branch]`，对每个流水线取 `conment_to_context[pipeline]`（无映射的如 `check_coverage_fe` 跳过）。

**好处**：required 集合永远从 `teamcity-utils.sh` 的权威映射派生，不会与真实流水线漂移。检测「全绿」和「广播」用的是同一份集合，天然一致。

> 注意：这份集合是「一个正常绿 PR 会带的 TeamCity context」。若分支保护另外要求了**非 TeamCity** 的 required check（license 等），那些仍需在各 member PR 上自行为绿——它们本就随 `pull_request` 各自跑，approve 的 PR 上通常已绿。

## 10. 触发链路复用

`trigger_batch_ci(vehicle_pr, branch, sha)` 对 `feut/beut/cloudut/compile` 各调一次
`trigger_or_skip_build "true" ...`：

- `"true"`（FILE_CHANGED）→ 一律全跑（batch 默认做完整校验）；
- 复用了其中的「**先取消 `pull/<N>` 上在跑/排队的 build，再触发**」——这正是 `rerun batch` 便宜的原因（载具 PR 号不变，取消按 `pull/<N>` 生效）；
- `compile` 在 TeamCity 有 snapshot 依赖，会级联触发 p0/p1/external/cloud_p0/cloud_p1/vault_p0/nonConcurrent/check_coverage——与 `run buildall` 路径一致。

## 11. 权限模型

| 能力 | 用的 token | 说明 |
|---|---|---|
| 推分支 / 删分支 / 建载具 PR / 评论 / 打 status | `GITHUB_TOKEN` | workflow `permissions` 已声明 contents/pull-requests/issues/statuses: write |
| **合入 member PR** | `MERGE_TOKEN`（默认 = `GITHUB_TOKEN`） | 若分支保护禁止 bot 合入发布分支，需换成有权限的 PAT/App token |
| 命令鉴权 | —— | 仅 `author_association ∈ {OWNER, MEMBER, COLLABORATOR}` 的评论触发；可按需收紧成 user-id 白名单（参考 `comment-to-trigger-teamcity.yml`） |

## 12. 并发控制

workflow 用 `concurrency: group=doris-batch, cancel-in-progress=false` 把**所有** batch workflow run（命令处理 + 轮询）串起来，避免两个处理器同时改同一批状态。run 都很短，串行代价可忽略。

## 13. 配置项（workflow env）

| 变量 | 默认 | 说明 |
|---|---|---|
| `BRANCHES` | `branch-4.0,branch-4.1` | 轮询扫描的发布分支 |
| `MAX_BATCH` | `10` | sweep 上限 |
| `REQUIRE_APPROVED` | `true` | 是否只选带 `approved` 的 PR |
| `MERGE_METHOD` | `squash` | member 合入方式（merge/squash/rebase）|
| `BATCH_MAX_AGE_H` | `12` | 批次最长存活小时数，超时自动 cancel |
| `MERGE_TOKEN` | `GITHUB_TOKEN` | 合入用 token（权限不足时替换）|

## 14. 代码地图（`tools/batch-cherry-pick.py`）

| 函数 | 职责 |
|---|---|
| `target_branch_of_issue` | 从 Issue 的 `batch-queue/*` label 解析目标分支 |
| `find_state_comment` / `write_state` / `render_state` | marker 评论里的状态读写与渲染 |
| `select_prs` / `is_eligible` | PR 选择与合格性过滤 |
| `assemble` | 组装集成分支（cherry-pick + 冲突剔除）|
| `ensure_vehicle_pr` | 建/取载具 PR |
| `classify` / `status_map` / `required_contexts` | 完成检测 |
| `broadcast_success` | 广播准入 status 到 member head |
| `reconcile_and_merge` / `finish_green` / `cleanup` | 对账合入与收尾 |
| `cmd_run` / `cmd_rerun` / `cmd_cancel` / `cmd_poll` | 四个入口命令 |

## 15. 已知限制与未来改进

- **无自动二分**：失败按约定挂起等操作员（`rerun` / 手动放行 / `cancel`）。后续可加「二分拆批定位坏 PR」。
- **无 file-change 优化**：batch 一律全跑（对 release 批次是更安全默认）。后续可复用 `github-utils.sh` 的 `file_changed_*` 做裁剪。
- **`merging` 中途崩溃**：状态可能卡住，靠 `BATCH_MAX_AGE_H` 超时兜底或手动 `cancel batch` 释放；后续可让轮询把 `merging` 作为幂等可恢复态。
- **fork PR 广播**：给 fork 的 head SHA 打 status 可能失败 → 该成员被跳过。release backport 基本同仓分支，少见。

## 16. 上线 Checklist

- [ ] 创建 label `batch-queue/branch-4.0`（及需要的其它分支）。
- [ ] 每分支开一个 tracking Issue 并打对应 label。
- [ ] 确认发布分支保护的 required checks 与本机制广播的 12 个 context 口径一致；其余非 TeamCity check 在 member PR 上能各自变绿。
- [ ] 若禁止 bot 合入发布分支，配置 `MERGE_TOKEN` secret 并在 workflow 引用。
- [ ] 确认 `MERGE_METHOD` 与该分支实际合入策略一致。
- [ ] **先用一个测试 Issue + 1~2 个 PR 小范围试跑**，验证触发→组装→CI→对账合入全链路，再放开使用。
