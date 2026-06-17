# PROGRESS — 属性体系重构（paimon 优先）

> 人类 + agent 入口。每完成 task / 阶段切换 / 重要变更后更新。上次更新：**2026-06-17**。

---

## 总体状态

| 阶段 | 进度 | 状态 |
|---|---|---|
| Research（调研） | ██████████ 100% | ✅ 完成（8-agent + grep；+ 3-agent recon 复核 D-006/7/8） |
| Design（设计） | ██████████ 100% | ✅ 完成（设计文档 + **7 决策** D-001..D-008，范围已收窄） |
| **Implement（实现）** | █░░░░░░░░░ ~7% | 🚧 **进行中**（范围 P0+P1 已获批；P0-T01 ✅） |

任务计数：**1 / 14** 完成（P0: 1/2 ｜ P1: 0/6 ｜ P2: 0/5 ｜ **P3a: 0/1**）｜ + **P3b**（全量去重 follow-up，范围外占位）。

---

## 当前活跃 task
- **下一个：`P0-T02`**（fe-core `FileSystemPluginManager.bindAll`，D-009）+ `P1-T01`（ConnectorContext.getStorageProperties 默认方法，可并行）。
- P0-T01 ✅ 已完成（recon + 定向）：见下「待决」与 DV-001/D-009。

## 阻塞 / 待决
- ✅ 范围已获批（2026-06-17）= **P0+P1（storage 收口），做到 P1-T06 gate 停**。
- ✅ **DV-001/D-009（2026-06-17）**：P0-T01 recon 证伪「fe-filesystem-api 已够、唯一 fe-core 改动」——产出 fe-filesystem typed StorageProperties 须新增 bind-all（仓内不存在）。用户定 **机制 A**：fe-core `FileSystemPluginManager` 加 additive `bindAll`，`getStorageProperties()` 经 `getOrigProps()` 取 raw map、不碰构造点。**fe-core 改动 = 2 文件**（DefaultConnectorContext + FileSystemPluginManager，均纯新增），白名单已 +1。
- ⚠️ **R-001 等价性**：fe-filesystem 为新事实源，较 fe-property 略**超集**（S3 role/anon；OSS/COS/OBS endpoint 无条件）；T1 须钉常见路径全等 + 记超集差异。

---

## 最近动态（最近 7 天）
- 2026-06-17 **进入 Implement（范围 P0+P1 获批）**；**P0-T01 ✅**（4-agent recon 取证三套 StorageProperties + 连接器 seam）：(1) F1 等价性=非阻塞（fe-filesystem 与 paimon 现 fe-property 路常见静态凭据键全等、为超集）；(2) F2 可行性=阻塞（无 bind-all 入口，证伪白名单「唯一 fe-core 改动」）→ **DV-001**；用户定 **机制 A** → **D-009**（fe-core `FileSystemPluginManager.bindAll` + `getStorageProperties()` 经 `getOrigProps()`，白名单 +1）。已回写设计/WORKFLOW/decisions/risks/tasks。
- 2026-06-17 **3 设计点定稿（D-006/7/8）**（3-agent recon + 直读复核）：**D-006** MetaStore 后端用 `MetaStoreProvider.supports()` 自识别 + ServiceLoader（镜像 `FileSystemProvider`），api 层**去掉** `MetaStoreType` 枚举；**D-007** Kerberos 抽**顶层中立叶子 `fe-kerberos`**（否决 fe-connector-auth：破 fe-filesystem↛fe-connector gate + fe-common 层级倒挂），分 P3a（paimon-local）/P3b（全量去重 follow-up）；**D-008** vended 边界=连接器只抽取、fe-core 单点归一（现状已符合）。设计文档 §0/§2.3/§3.1/§3.2/§3.3/§3.5/依赖图已更新。
- 2026-06-17 调研完成（current state：paimon metastore 已与 fe-core 解耦、仅剩 storage 对 fe-property 一条边；三套同名 StorageProperties；fe-core metastore 28 文件 3624 LOC 矩阵；kerberos 三处实现）。
- 2026-06-17 设计定稿 + 4 决策（①新建 metastore-api/spi ②混合去重 ③fe-core 绑定下发 typed storage ④复用 @ConnectorProperty）。
- 2026-06-17 范围收窄（用户）：纯新增/迁移、只动 paimon、不删 fe-core 两包、不动其它连接器、fe-property 不物理删。
- 2026-06-17 建立本跟踪目录 + 开发流程（WORKFLOW.md）+ 任务清单（13 tasks）。

---

## 关键链接
- 设计：[`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md)
- 背景（fe-filesystem StorageProperties 现状评审）：[`../reviews/fe-filesystem-storage-spi-review-2026-06-17.md`](../reviews/fe-filesystem-storage-spi-review-2026-06-17.md)
- 流程：[`WORKFLOW.md`](./WORKFLOW.md) ｜ 任务：[`tasks.md`](./tasks.md) ｜ 决策：[`decisions-log.md`](./decisions-log.md)
