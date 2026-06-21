# 开发流程（仅适用于本子项目）

> 派生自 `../README.md` 的开发设计原则（文件职责矩阵 / 决策vs偏差 / ID 规则 / 维护规则 / 防腐 / 不在范围），并融合本仓库使用的工作流技能：`research-design-workflow`、`step-by-step-fix`、`test-driven-development`、`verification-before-completion`。
> 一句话：**研究/设计已完成 → 进入「逐任务、测试先行、独立提交、严守红线、文档同步」的实现循环。**

---

## 1. 阶段模型（research-design-workflow）

| 阶段 | 状态 | 产物 |
|---|---|---|
| Research（取证） | ✅ 完成 | 8-agent 调研 + grep 复核（见设计文档附录 A） |
| Design（设计） | ✅ 完成 | `../designs/metastore-storage-property-refactor-design-2026-06-17.md`（4 决策 + 目标架构 + SPI + 有序 TODO） |
| **Implement（实现）** | ⏳ 待批准后开始 | 按 `tasks.md` 逐 task 落地，每 task 独立提交 |
| Verify（验证） | 每 task 内联 | UT / checkstyle / docker；T1/T2 等价性测试 |
| Refine（精修） | 每阶段末 | review + 简化，必要时回写设计/记偏差 |

**禁止**：未经用户批准 `tasks.md` 的 TODO 列表，不开始 Implement（research-design-workflow 要求 "Get approval before implementation"）。

---

## 2. 单任务开发循环（step-by-step-fix + TDD）

**起步（每个 session / 阶段开始，用户 2026-06-17 立规，强制）**：先读 `PROGRESS.md` → `HANDOFF.md` → 本文件 → 下一 task 的 `tasks.md` 块 + 相关 `decisions-log`/`deviations-log` 条；**再对照真实代码 review 下一步方案**（不照搬 HANDOFF 旧计划——先 grep/读真实调用流确认方案仍成立）；一句话复述确认（必要时 AskUserQuestion 定边界）后才动手。

每个 `Pn-Tnn` 严格走以下 8 步，**一个 task = 一个独立 commit**：

1. **认领**：在 `tasks.md` 把该 task 状态 `⬜→🚧`，在 `HANDOFF.md` 记"正在做 Pn-Tnn"。
2. **微设计**（如该 task 有不确定点）：在 task 块"备注"写 1–3 行实现要点；若偏离设计文档 → 先记 `deviations-log.md`（见 §6）。
3. **测试先行（RED）**：先写/改测试表达*意图*（不是行为镜像）——尤其 T1/T2 等价性（新产物 == 旧产物的 key/value）。确认测试 RED。
4. **实现（GREEN）**：最小改动让测试通过。匹配既有代码风格。
5. **守门核对**（§4 红线）：`git diff --name-only` 必须落在**白名单路径**内；依赖方向不破。
6. **验证**（§5）：FE 编译 + checkstyle + 相关 UT 全绿；必要时 docker paimon。**"完成"前必须有命令输出佐证**（verification-before-completion）。
7. **提交**：`[Pn-Tnn] <subject>`，结尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。先在非默认分支。
8. **同步文档**：`tasks.md` 状态 `🚧→✅`（加日期 + commit）；更新 `PROGRESS.md`；如产生决策/偏差/风险，记对应 log；更新 `HANDOFF.md`。

> 卡住（blocker）时：在 task 块记 blocker 备注，停下来向用户澄清，**不要猜**（项目 CLAUDE.md Rule 1）。

---

## 3. 任务编号与依赖

- Task ID：`Pn-Tnn`（如 `P1-T03`）。**永不复用/重排**，删除也留占位标 `[deleted]`。
- 阶段：`P0` 准备 / `P1` storage 收口 / `P2` metastore SPI。范围外阶段不在本项目。
- 依赖：task 块标注前置（如 `P1-T03` 依赖 `P1-T01,P1-T02`）。可并行的标 `∥`。

---

## 4. 红线守门（本项目特有，每次提交前核对）

### 4.1 路径白名单（`git diff --name-only` 只允许落在）
```
fe/fe-connector/fe-connector-metastore-api/**          (新建)
fe/fe-connector/fe-connector-metastore-spi/**          (新建)
fe/fe-connector/fe-connector-paimon/**                 (改造)
fe/fe-connector/fe-connector-spi/**                    (仅 ConnectorContext 新增方法)
fe/fe-core/src/main/java/.../connector/DefaultConnectorContext.java  (仅新增 getStorageProperties)
fe/fe-core/src/main/java/.../fs/FileSystemPluginManager.java          (仅新增 bindAll；D-009/DV-001)
fe/fe-core/src/main/java/.../fs/FileSystemFactory.java                (仅新增 bindAllStorageProperties；D-009 二次确认)
fe/fe-filesystem/fe-filesystem-hdfs/**                 (FU-T01：HDFS typed BE model；D-010 授权局部解禁)
fe/fe-filesystem/fe-filesystem-{s3,oss,cos,obs}/**     (FU-T02 R-008 / FU-T03 R-006；D-011 授权；main+test；其它 fe-filesystem 模块[api,spi,azure,broker,local]仍禁碰)
fe/fe-kerberos/**                                       (新建；P3a-T01 fe-kerberos 叶子；D-007/D-013)
fe/fe-property/**                                       (P1-T07：彻底删除该孤儿模块；D-016 授权，覆盖 D-005 不删条款)
fe/fe-common/src/{main,test}/java/org/apache/doris/common/security/authentication/**  (P3b-T01：trino→JDK + 整包 relocate 出 fe-common；D-017)
fe/fe-filesystem/fe-filesystem-spi/**                  (P3b-T01：统一 HadoopAuthenticator 接口/IOCallable；D-017)
fe/be-java-extensions/**                               (P3b-T01：3 scanner auth import 重指向 + java-common/pom.xml 加 fe-kerberos 依赖；D-017)
fe/fe-core/src/{main,test}/java/**                      (P3b-T01：24 main+14 test 消费方 **仅 import 行重指向**；含 datasource.property.{storage,metastore} 下的文件——D-017 显式授权对这些「禁碰」包做 import-only 修改，不改逻辑)
fe/fe-connector/pom.xml                                (仅新增模块声明)
fe/pom.xml                                             (新增模块声明；P1-T07 额外允许删除 fe-property 的 <module>+dependencyManagement 条目，D-016)
plan-doc/metastore-storage-refactor/**                 (本跟踪目录)
```
**禁止**出现的路径（出现即停、回滚或记偏差）：
- `fe/fe-core/src/main/java/.../datasource/property/storage/**`（fe-core 旧 storage 包，保持不动；**P3b-T01 例外**：仅允许 auth import 行重指向，D-017）
- `fe/fe-core/src/main/java/.../datasource/property/metastore/**`（fe-core 旧 metastore 包，保持不动；**P3b-T01 例外**：仅允许 auth import 行重指向，D-017）
- `fe/fe-connector/fe-connector-{hive,hudi,iceberg,es,jdbc,maxcompute,trino}/**`（其它连接器，不动）
- ~~`fe/fe-property/**` 的删除~~ → **P1-T07 已授权删除（D-016）**，移入上方允许区（fe-core 两包仍禁碰）

### 4.2 依赖方向（CI gate + 人工核对）
- `fe-connector-*` 不得 import `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`（`tools/check-connector-imports.sh`）。
- paimon 模块 `grep -r 'org.apache.doris.property'` 应在 P1 后归零；`grep -r 'org.apache.doris.datasource'` 恒为 0。
- `fe-filesystem-*` 不得 import fe-connector / fe-core。
- 新模块 `fe-connector-metastore-api/spi` 只可依赖 `fe-foundation` + `fe-filesystem-api`(+ `fe-connector-api/spi`)。

### 4.3 不变量（设计文档 §5，改动不得破坏）
- metastore 不持有 storage 字段；storage 作入参传入（`parse(raw, storageList)`）。
- storage 叠加保序：canonical 在前、`paimon.*/fs./dfs./hadoop.` 覆盖在后（last-write-wins）。
- HMS Kerberos 条件键在 storage 叠加**之后**施加。
- 特权/RPC（Kerberos doAs、hive.conf.resources、vended 绑定、thrift `TS3StorageParam`）留 fe-core 经 `ConnectorContext`。

---

## 5. 验证标准

### 5.1 每 task 必跑
- FE 编译该模块（maven **用绝对 `-f`**；paimon 模块需 `-am package -Dassembly.skipAssembly=true`，因 shade jar 携带 HiveConf）。
- checkstyle 0 违规（用 `fe-code-style` 技能）。
- 相关 UT 全绿（**注意 maven build-cache 会跳过 surefire → BUILD SUCCESS ≠ 测试跑了**；用 `-Dmaven.build.cache.enabled=false` 确保真跑）。
- 后台 task 的退出码以输出里的 `BUILD SUCCESS/FAILURE` 行为准（非 echo 的 exit code）。

### 5.2 阶段验收测试（强制，设计文档 §5）
- **T1**（P1，**DV-002 修订**）：S3/OSS/COS/OBS/HDFS 代表输入下，新 `toHadoopConfigurationMap()`/`toBackendProperties().toMap()` 与 paimon 现走 fe-property 旧产物在**常见静态凭据路径**（配齐 endpoint/region/AK/SK，无 role/无 vended）下 key/value **全等**（含默认调优值分叉 S3=50/3000/1000 vs OSS/COS/OBS=100/10000/10000）；fe-filesystem 超集差异（S3 role/anon、endpoint 无条件、BE 多 AWS_BUCKET/ROOT_PATH/CREDENTIALS_PROVIDER_TYPE）作有意记录，非漂移。
- **T2**（P2）：HMS(simple/kerberos)/DLF/REST/JDBC/filesystem 下，共享 `*MetastoreBackend.parse` 产物与 fe-core 旧 `Paimon*MetaStoreProperties` 一致（HiveConf key 集 + ParamRules 报错文案）。
- **T3**：依赖图守门（CI gate + 可加 ArchUnit）。
- **T4**：docker `enablePaimonTest=true` 跑 paimon 5 flavor（filesystem/hms/rest/jdbc/dlf）+ vended(REST/DLF) + Kerberos HMS。

### 5.3 docker/e2e 说明
- T4 是 docker-gated；若本次环境不部署，**明确标注"未跑 e2e"**（项目 CLAUDE.md Rule 12 失败/跳过要发声），不得把"编译过"当"验证过"。

---

## 6. 决策 / 偏差 / 风险（沿用 ../README §3）

- **决策（D-NNN）**：事前选择，进 `decisions-log.md` 顶部。本项目 4 个核心决策已记 D-001..D-004，范围决策 D-005。
- **偏差（DV-NNN）**：原设计落地中发现不可行/不必要，**事后**记 `deviations-log.md`，并回写设计文档对应节加 `（DV-NNN 修订）`。**禁止 silently 改设计**。
- **风险（R-NNN）** vs **问题（Issue）**：可能发生→`risks.md` 滚动；已发生→记在 task 块 blocker。
- 编号仅本子项目内有效（与上层 plan-doc 独立）。

---

## 7. 文档维护规则（沿用 ../README §4，裁剪）

| 触发 | 动作 |
|---|---|
| 完成一个 task | `tasks.md` 状态翻转 + 日期/commit；更新 `PROGRESS.md`；阶段日志追加一行 |
| 产生新决策 | 先写 `decisions-log.md` 顶部分配 D-NNN；如改设计则回写并加脚注 |
| 发现偏差 | 先写 `deviations-log.md`（DV-NNN：原计划位置/为何不可行/新方案/影响）；再改设计 |
| **每完成一个 task/阶段 或 session 结束**（用户 2026-06-17 立规） | **覆盖更新 `HANDOFF.md`**（完成了什么 / 下一步含真实代码 review 要点 / 未决 / 红线）**并 commit**（随该 task commit 或单独 doc commit）。下次接手不靠记忆、只靠 HANDOFF。 |
| 每个 commit | 第一行 `[Pn-Tnn] <subject>`；merge 后立即按上面流程更新状态 |

---

## 8. 不在范围（沿用 ../README §6）

本流程**不**涵盖：代码评审（GitHub PR）、缺陷管理（Issues）、CI 状态（Actions）、工时/KPI。文档只追踪"本子项目的设计与进度"，不追踪人。

---

## 9. 实现顺序建议（来自设计文档 §4）

```
P0（准备，可与 P1 并行起步）
 └ P0-T01 确认 fe-filesystem-api 已够用   ∥  P0-T02 fe-core 绑定入口

P1（paimon storage 收口；纯新增/迁移）
 P1-T01 ConnectorContext.getStorageProperties()      ← 解锁 fe-connector→fe-filesystem-api 边
 P1-T02 DefaultConnectorContext 实现（限 paimon 路径）  [依赖 P1-T01,P0-T02]
 P1-T03 PaimonCatalogFactory storage 改走 api          [依赖 P1-T01]
 P1-T04 PaimonScanPlanProvider BE 凭据改走 api          [依赖 P1-T01]
 P1-T05 断开 paimon→fe-property 依赖边                  [依赖 P1-T03,T04]
 P1-T06 验证（UT + docker 5 flavor + T1）              [依赖 P1-T02..T05]

P2（metastore SPI + paimon adapter；纯新增/迁移）
 P2-T01 新建 fe-connector-metastore-api
 P2-T02 新建 fe-connector-metastore-spi（共享后端解析器） [依赖 P2-T01]
 P2-T03 paimon adapter 改走共享解析器                   [依赖 P2-T02]
 P2-T04 paimon pom + gate 核对                          [依赖 P2-T03]
 P2-T05 验证（UT + docker 5 flavor + T2 + vended + kerberos） [依赖 P2-T03,T04]
```
