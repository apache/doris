# 🤝 Session Handoff —— 每语句表加载归属者 · 移植/重构

> **滚动文档**：每 session 结束覆盖式更新，只留下一个 session 必须的上下文。
> 开场必读顺序、模板、铁律见 [`README.md`](./README.md)；进度见 [`progress.md`](./progress.md)；状态见 [`tasklist.md`](./tasklist.md)。

---

# 🆕 本轮（2026-07-20 session 8）已完成：**缓存隔离——list≠load 越权修复（RD-4）+ 读写重构主线四步收官**

## 一句话结果
- **背景**：某些 iceberg 目录（REST + `iceberg.rest.session=user`）按登录用户逐个鉴权，鉴权发生在**加载表的远端往返里**；而"能列表名"与"能加载表"是两套独立授权。此前若干**跨查询缓存**命中就返回、**不走加载表**→"能列不能载"的用户经缓存读到别人才该见的元数据（真实越权，用户已确认 list≠load）。
- **grounding 纠偏（对抗验证）**：真正**活跃**的越权只 2 个——iceberg `latestSnapshotCache` + fe-core schema 缓存；`partitionCache`/`formatCache` 靠上游 per-user loadTable 已护（脆弱，非活跃漏）；**异构 HMS 网关是"潜在"非"活跃"洞**（兄弟被强制 hms flavor、永不 session=user）；原计划"分片 + 传属主标签"两点被证伪。
- **用户签字（三决策）**：①**禁用路线**（session=user 下把授权敏感缓存置空，非身份分片）——无需新 SPI、**无撤权陈旧窗口**、贴合 Doris 现状（session=user 本就每次现载）、镜像已有 tableCache/commentCache 先例；②**三投影缓存统一禁用**（成"session=user ⇒ iceberg 无活跃跨查询元数据缓存"不变量）；③**网关加 fail-loud 守卫**。
- **已落地（全绿，4 独立 commit）**：
  - **4a**（`9f88aa4`）iceberg `latestSnapshotCache`/`partitionCache`/`formatCache` 在 `isUserSessionEnabled()` 置空 + `beginQuerySnapshot` null 回退（`loadLatestSnapshotPin`）+ `invalidate*` 三处 null 守卫。
  - **4b**（`92289e3`）fe-core `shouldBypassSchemaCache(SessionContext)`（默认 false）+ `PluginDrivenExternalCatalog` override（判据同库/表名缓存 bypass）+ `ExternalTable.getSchemaCacheValue()` bypass 现读（覆盖 MVCC latest 路径）。
  - **4c**（`c40af11`）hive `getOrCreateIcebergSibling` 建成兄弟若含 `SUPPORTS_USER_SESSION` 即 fail-loud（今天恒不触发，守未来）。
  - **4d**（`5cb5fb9`）防漂移门禁 `tools/check-authz-cache-sharding.sh`（marker：`authz-cache-session-user-disabled`/`authz-cache-exempt`）+ 自测 + 挂 fe-connector pom validate。
- **验证**：116 目标单测全绿；checkstyle 0；三门禁 exit 0；**clean-room 对抗复审 0 blocker/major**（安全本体全 clean；仅 2 findings 针对门禁本身，minor 已修=正则放宽覆盖 raw `Cache<`/`LoadingCache<`/静态形态）。
- **未动**：iceberg 表 side-car（正交）；`getIdentityShardKey()` SPI 不新增（禁用路线不需要）；hive/paimon/hudi 无 session=user 轴不动。

---

# ➡️ 下一个 session = **统一补 e2e（读写重构主线的唯一欠账）**

> 读写重构主线 **RD-1→RD-4 / STEP 1–4 全部完成**（读取键石 + HMS 兄弟扇出 + 写入共用 + 缓存隔离）。剩下的是各步一路"择机统一补"的 e2e 欠账，宜一次补齐。

## 第一件事（先读）
1. 读 `progress.md` session 6/7/8 尾（各步欠的 e2e 清单）+ 架构记忆 `hms-iceberg-delegation-needs-e2e`。
2. 读 `designs/P4-cache-isolation-implementation-design.md` §4 + `designs/P3-write-sharing-implementation-design.md` §4（欠账 e2e 范围）。

## e2e 待补清单（落 `regression-test/suites/external_table_p2/refactor_catalog_param`）
- **异构 HMS 网关**（RD-2/RD-3 欠）：异构目录跑 INSERT/DELETE/MERGE/ALTER/EXECUTE，断言与独立 iceberg 目录同表同结果（对齐 `hms-iceberg-delegation-needs-e2e`）。
- **越权**（RD-4 欠）：造 can-list-cannot-load 用户，断 REST session=user 目录下命中不泄漏别人的 schema/snapshot/分区。
- 环境依赖真实 REST catalog（session=user）+ 异构 HMS，需先确认 docker 编排是否支持；不支持则先补编排或标注前置。

## 铁律 / 闸门提醒
- 用户 2026-07-19 已豁免铁律 A（fe-core 只减不增）。仍守：连接器 connector-agnostic、**fe-connector-* 不得 import fe-core**（门禁）、新 SPI 用 `getUser()`/主体字节不解析凭证令牌。
- **动码前探测并发活动**（git log/status + maven 进程 + 近 90s mtime），发现活跃即停手（`concurrent-sessions-shared-worktree-hazard`）。
- e2e 沿用"择机统一补"，但主线已收官，宜作为独立收尾专项立项。

---

# 🗂 遗留 / 关联
- 分期定稿：`designs/expanded-scope-phasing-and-security-decisions.md`（§3 缓存隔离、§4 分期、§6 残留风险）。
- as-built：`designs/P4-cache-isolation-implementation-design.md`（缓存隔离）、`designs/P3-write-sharing-implementation-design.md`（写入共用）。
- 门禁：`tools/check-fecore-metadata-funnel.sh`、`tools/check-connector-imports.sh`、`tools/check-authz-cache-sharding.sh`（新）。
- 架构记忆：`iceberg-table-resolution-cache-scoping`、`catalog-spi-session-user-no-live-crossquery-cache`（新）、`hms-iceberg-delegation-needs-e2e`、`catalog-spi-plugin-tccl-classloader-gotcha`。
