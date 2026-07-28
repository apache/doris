# 📦 任务空间 — 清理 fe-core `datasource/property/{common,metastore}`

> **独立任务空间**，与 catalog-spi 主线（`plan-doc/HANDOFF.md`）并行但**不混流**。
> 目标：按架构目标「**fe-core 不持有任何属性解析**」，处置 fe-core
> `org.apache.doris.datasource.property` 下的 `common/`（AWS 凭证模式）与 `metastore/`（元存储属性）两个包。

---

## 🚩 一句话结论（2026-07-28 基线 `3468d905eb3`，8 路侦察 + 3 路独立设计 + 6 项对抗验证）

**两个包的答案不一样，别当成一件事做。**

| 包 | 裁决 | 一句话理由 |
|---|---|---|
| `metastore/`（4 文件 333 行） | **整体删除**（连带孤儿 `ConnectionProperties.java`） | 运行期**不可达**；接班人 `fe-connector-metastore-api` 早已上线跑着 → **没有任何东西需要搬** |
| `common/`（2 文件 237 行） | **留在 fe-core，砍掉死的一半** | 它服务的是**内部存储**（冷存 StoragePolicy / 云上 StorageVault），**不是外部数据源**；三个候选目的地全部堵死；「复用 fe-filesystem 现成那份」是**行为变更**不是重构 |

**⚠️ 最容易踩的坑**：`common/` 看起来和 `fe-filesystem-s3-base` 的
`S3CredentialsProviderType`/`S3CredentialsProviderFactory` 是重复造轮子 —— **调研初判就是这么错的**。
两份实现有**两条活的行为差异**（见 `design.md` §3.3），且全仓**没有任何测试**钉住发出的凭证串，
换掉会绿着上线一个回归。

---

## 📂 本空间文件

| 文件 | 用途 | 更新方式 |
|---|---|---|
| [`design.md`](./design.md) | **设计文档** —— 两个包的判据、证据链、被否方案、风险 | 稳定文档，改动需在 progress 留痕 |
| [`tasklist.md`](./tasklist.md) | **Task list** —— 唯一进度清单，`FPC-NN` 勾选 | 每完成一项随 commit 勾 `[x]` |
| [`open-decisions.md`](./open-decisions.md) | **待拍板** —— 动手前需要用户定的事 | 拍板后就地标 ✅ 并写明结论 |
| [`HANDOFF.md`](./HANDOFF.md) | **交接文档** —— 只写「下一个 session 第一件事做什么」 | 每 session 结束**覆盖式**更新 |
| [`progress.md`](./progress.md) | **进度记录** —— append-only 日志（日期 / commit / 结论 / 踩坑） | 只追加，不覆盖 |

---

## ▶️ 新 session 开场流程（必须遵守）

```
1. Read plan-doc/fecore-property-cleanup/HANDOFF.md      ← 上次留言 + 下一步
2. Read plan-doc/fecore-property-cleanup/tasklist.md     ← 勾到哪了
3. Read plan-doc/fecore-property-cleanup/open-decisions.md ← 有没有还没拍板的
4. 需要背景/为什么时才 Read design.md（别默认全读，它是稳定参考不是状态）
5. 用一句话向用户复述："上次做完了 X，下一步是 FPC-NN，对吗？"
6. 等用户确认后开始
```

**⚠️ 行号信 HEAD 不信文档** —— 本空间所有 `file:line` 是 **2026-07-28 / `3468d905eb3`** 基线，
代码动了就以 `grep` 为准。

---

## 🔗 与其它空间的关系

- **主线** = `plan-doc/HANDOFF.md`（catalog-spi 迁移）。本任务是主线「fe-core 去属性解析」的收尾一环。
- **`../metastore-storage-refactor/`（已 CLOSED）** = 本任务的**前置**：正是那条子线**生产**出了
  `fe-connector-metastore-api` / `fe-connector-metastore-spi`（`MetaStoreProviders.bind` + 5 个 provider）。
  本任务只是把它留在 fe-core 的**旧壳**扫掉。⛔ 那个目录是历史留存，别去读它的规划文档。
- **继承主线两条铁律**：**fe-core 只出不进**（铁律 A） + **禁 deletion-scaffolding 式搬迁**（铁律 B）。
  本任务全程 fe-core **只减不增**，两条天然满足。
- 协作规范沿用 [`../AGENT-PLAYBOOK.md`](../AGENT-PLAYBOOK.md)。

---

## 📌 范围边界（误判比漏判贵）

**在范围内**：
- `fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/`（4 文件）
- `fe/fe-core/src/main/java/org/apache/doris/datasource/property/ConnectionProperties.java`（删 metastore 后成孤儿）
- `fe/fe-core/src/main/java/org/apache/doris/datasource/property/common/`（只砍死代码，**不搬迁**）
- `CatalogProperty.java` 的连带瘦身

**明确不在范围**（理由见 `design.md` §6）：
- `datasource/property/constants/`、`datasource/property/fileformat/` —— **压根不是外部数据源逻辑**
- `StorageAdapter.checkAzureOauth2OnlyForIcebergRest()` —— 是真的架构违规，但需**单独一刀 + e2e**
- 把 `common/` 搬去任何模块 —— 三条独立理由否决，见 `design.md` §3
