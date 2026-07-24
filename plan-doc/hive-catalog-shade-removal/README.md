# 📦 任务空间 — 剔除 `hive-catalog-shade` 冗余依赖

> **独立任务空间**，与 catalog-spi 主线（`plan-doc/HANDOFF.md`）并行但**不混流**。
> 目标：让 **fe-core / fe-common 不再依赖 `org.apache.doris:hive-catalog-shade`**，从而把这个 **127 MB** 的
> jar 及其连带的三个历史 hack（commons-lang 2.x / fastutil shade 覆盖 / central repo 条目）从 `fe/lib` 剔除。

---

## 🚩 一句话结论（2026-07-14 基线，10-agent 对抗验证）

**今天删不掉。** `fe/fe-core/pom.xml:437-440` **不是**冗余：fe-core `src/main` 里还有 **26 个文件**在 import
hive 类，`fe-common` 还有一处 `HiveConf`（而且是 `provided`，会**编译绿、运行炸**）。真正的工作量是把这些
**残留的源码搬回插件**，pom 那 4 行只是症状。

**它跟 `fe-connector-paimon-hive-shade` 没有依赖关系** —— 后者是同一招数的「插件私有版复制品」。

---

## 📂 本空间文件

| 文件 | 用途 | 更新方式 |
|---|---|---|
| [`design.md`](./design.md) | **设计文档** —— shade 是什么 / 为什么删不掉 / 方案 / 风险 / 待拍板决策 | 稳定文档，改动需在 progress 留痕 |
| [`tasklist.md`](./tasklist.md) | **Task list** —— 唯一进度清单，`HCS-NN` 勾选 | 每完成一项随 commit 勾 `[x]` |
| [`HANDOFF.md`](./HANDOFF.md) | **交接文档** —— 只写「下一个 session 第一件事做什么」 | 每 session 结束**覆盖式**更新 |
| [`progress.md`](./progress.md) | **进度记录** —— append-only 日志（日期 / commit / 结论 / 踩坑） | 只追加，不覆盖 |

---

## ▶️ 新 session 开场流程（必须遵守）

```
1. Read plan-doc/hive-catalog-shade-removal/HANDOFF.md   ← 上次留言 + 下一步
2. Read plan-doc/hive-catalog-shade-removal/tasklist.md   ← 勾到哪了
3. 需要背景/为什么时才 Read design.md（别默认全读，它是稳定参考不是状态）
4. 用一句话向用户复述："上次做完了 X，下一步是 HCS-NN，对吗？"
5. 等用户确认后开始
```

**⚠️ 行号信 HEAD 不信文档** —— 本空间所有 `file:line` 是 2026-07-14 基线，代码动了就以 `grep` 为准。

---

## 🔗 与主线的关系

- 主线 = `plan-doc/HANDOFF.md`（HMS 翻闸 → 删遗留代码），**删除阶段主体已完成**。
- 本任务是主线 `⏭ 单列后续` 的自然延伸：主线删掉了 fe-core 的 hive/iceberg/hudi **业务代码**，
  但**没碰** vendored 的 Glue/DLF metastore 客户端和 HiveConf 属性解析 —— 那正是 shade 还赖在 fe-core 的原因。
- 本任务**继承主线的两条铁律**（见 design.md §6）：**fe-core 只出不进** + **禁 deletion-scaffolding 式搬迁**。
- 协作规范沿用 [`../AGENT-PLAYBOOK.md`](../AGENT-PLAYBOOK.md)。
