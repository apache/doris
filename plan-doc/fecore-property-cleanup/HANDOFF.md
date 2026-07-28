# 🤝 Session Handoff — 清理 fe-core `datasource/property/{common,metastore}`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) ·
> 清单 [`tasklist.md`](./tasklist.md) · 待拍板 [`open-decisions.md`](./open-decisions.md)

---

# 🆕 下一个 session 第一件事 = **拿 OD-1 的拍板**（`open-decisions.md`）

## 状态：**调研已完成，代码一行未动。**

**基线 HEAD** = `3468d905eb3`（分支 `catalog-spi-review-21`，2026-07-28）。
本空间是**新建**的，尚无任何本任务的 commit。

---

## 📍 你现在需要知道的三件事

### 1️⃣ 两个包的答案不一样，别当成一件事做

| 包 | 裁决 |
|---|---|
| `metastore/`（4 文件 333 行）+ `ConnectionProperties.java`（140 行） | **整体删除** —— 运行期不可达，接班人 `fe-connector-metastore-api` 早已上线，**没有东西需要搬** |
| `common/`（2 文件 237 行） | **留在 fe-core** —— 它服务的是**内部存储**（冷存 StoragePolicy / 云上 StorageVault），不是外部数据源；只砍掉死的一半 |

### 2️⃣ 🔴 一个必须记住的「别再犯」

调研初判说 `common/` 和 `fe-filesystem-s3-base` 的
`S3CredentialsProviderType`/`S3CredentialsProviderFactory` 是重复造轮子、可以直接替换 ——
**这个判断被对抗验证两轮推翻了**。两份实现有**两条活的行为差异**（`design.md` §3.3）：

1. 发给 hadoop 的凭证串会**多出** `ProfileCredentialsProvider`
2. 模式串接受面会**放宽**（空串 / `ENVIRONMENT` / `WEB_IDENTITY_TOKEN_FILE` 从抛异常变成接受）

而**全仓没有任何测试钉住那个串** ⇒ 换掉会**绿着上线一个回归**。
**下次看到「这两个类长得一样，合并掉吧」的念头，先来读 `design.md` §3.3。**

### 3️⃣ ⛔ 卡在哪：OD-1 没拍板

`open-decisions.md` **OD-1**：删掉 metastore 后，`resolveDerivedStorageDefaults()` 的
null-supplier 分支要 **fail-loud（`throw`，推荐）** 还是 **fail-silent（`return emptyMap()`）**？

这条**直接决定 FPC-03 的代码怎么写**，不定不能开工。
（OD-2 是「FPC-02 做不做」，不阻塞任何东西，可以随时定。）

---

## ▶️ 建议的执行顺序

```
1. 拿 OD-1 拍板                      ← 就是这一步，先做
2. FPC-02（删 AWS 死构造臂，~146 行）  ← 可选、解耦，先做也行、跳过也行
3. FPC-03（主删除：5 文件 473 行 + CatalogProperty 瘦身 ~45 行）  ← 依赖 OD-1
4. FPC-04（可选清扫，另起提交）
```

---

## ⚠️ 开工前必读的三条纪律（本仓库已知踩坑）

1. **删除类改动不能只信增量编译** —— `fe-core/target/classes` 里确实躺着无源文件的陈旧 `.class`。
   每步先 `rm -rf fe-core/target/{classes,test-classes}`。
2. **全反应堆必须含测试源**（禁 `-Dmaven.test.skip=true`），且必须 `-Dcheckstyle.skip=true`
   （否则 checkstyle 扫 generated-sources 退化成平方级，构建卡死 60+ 分钟）；
   checkstyle 改为**只对改动模块**单独跑 `checkstyle:check`。
3. **`fe-connector-api` 的录制基线要显式跑** ——
   `mvn -pl fe-connector/fe-connector-api test`。全反应堆 `test-compile` **不跑 surefire**，
   这是本分支已经红过好几批没人发现的盲区。
   本任务**预期不需要刷基线**（该模块不依赖 fe-core），**红了就停手，别顺手刷。**

---

## 🔎 尚未验证（如实声明，别当成已完成）

- **没跑过任何 maven 构建** —— `tasklist.md` 里的验证命令是方子，不是结果
- **没查 apache/doris master** 是否有 `StorageAdapter.getAwsCredentialsProvider()` 的调用者
  （FPC-02 的 rebase 冲突风险是陈述不是实测）
- **没跑 e2e**（需要集群）
- `ExternalCatalog.buildHadoopConfiguration(Map)` 的调用者没枚举 ⇒ FPC-04 明确排除它

---

## 📎 参考：完整英文调研报告

29k 字的原始报告（逐条 `file:line` 证据 + 三份独立设计的分歧点 + 六项对抗验证的两轮判决）
在调研 session 的 scratchpad：
`/tmp/claude-1000/-mnt-disk1-yy-git-wt-catalog-spi/6983e5ef-36cf-4f14-a048-139ffc1c1b51/scratchpad/property-common-metastore-report.md`

⚠️ **scratchpad 是 session 级的，可能已经不在了。** 本空间的 `design.md` 已经把其中**结论性、
需要长期保留**的部分中文化落盘 —— 以 `design.md` 为准，那份报告只是溯源用。
