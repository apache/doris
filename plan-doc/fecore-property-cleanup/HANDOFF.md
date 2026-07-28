# 🤝 Session Handoff — 清理 fe-core `datasource/property/{common,metastore}`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) ·
> 清单 [`tasklist.md`](./tasklist.md) · 待拍板 [`open-decisions.md`](./open-decisions.md)

---

# 🆕 下一个 session = **FPC-02**（删 AWS 死构造臂），先过 OD-2

## 状态：**主删除 FPC-03 已完成并验证通过。`metastore/` 目录已不存在。**

| 阶段 | commit | 结果 |
|---|---|---|
| 文档空间 | `938d38c7425` | 6 份文档落盘 |
| FPC-01 + FPC-03 | 见 `git log` | **删 5 文件 473 行** + `CatalogProperty` 净减 ~45 行；全反应堆绿 / checkstyle 0 violations / 95+110 单测全过 |

`fe/fe-core/.../datasource/property/` 现在只剩 `common` / `constants` / `fileformat`。

---

## ⏳ 一件待用户追认的事（**别忘了问**）

**OD-1 用户始终没表态**，我按文档推荐值 **A（fail-loud）** 落地了：
`CatalogProperty.resolveDerivedStorageDefaults()` 的 null-supplier 分支
`throw new IllegalStateException(...)`，并配了守卫测试
`CatalogPropertyPluginStorageDerivationTest.unwiredSupplierFailsLoudInsteadOfDerivingNothing`
（已做变异验证）。

**要翻成 B（`return Collections.emptyMap()`）只需改一行 + 删该用例。**
开场时向用户确认一句即可，不必重新论证。

---

## 📋 下一步：FPC-02（`tasklist.md` 阶段 2）

删 `StorageAdapter.getAwsCredentialsProvider()` + 两个私有 helper，
以及 `AwsCredentialsProviderFactory` 的 `createV2` / `createDefaultV2` / 单参 `getV2ClassName`，
共 **~146 行零调用者代码，零行为变更**。

**动手前先过 OD-2**：grep 一次上游 `apache/doris` master 看有没有
`getAwsCredentialsProvider()` 的调用者（这段是上游 `f499c78c67c` / #66004 整体带进来的，
若上游有调用者，下次 rebase 会 modify/delete 冲突）。

⚠️ `tasklist.md` FPC-02 里的**「必须保留」清单要逐条对**——
`getAwsCredentialsProviderMode()` 和 `s3CredentialsMode` 字段是**活的**（喂 BE 的
`AWS_CREDENTIALS_PROVIDER_TYPE`，且被 `AzureGuessRoutingParityTest` 钉着），删了会炸。

🟢 FPC-02 **可以整项丢弃**，不影响已完成的 FPC-03。

---

## ⚠️ 四条验证纪律（**第 4 条是这轮实测新增的**）

1. 删除类改动**不能只信增量编译** → 每步先 `rm -rf fe-core/target/{classes,test-classes}`。
2. 全反应堆**必须含测试源**（禁 `-Dmaven.test.skip=true`），且必须 `-Dcheckstyle.skip=true`。
3. checkstyle `UnusedImports` 是**阻塞门禁** → **只对改动模块**单独跑 `checkstyle:check`。
4. 🆕 **`-pl` 必须配 `-am`**（否则兄弟模块 `${revision}` 解析不了，报出**像真错的假错**），
   且 surefire 2.22.2 认 **`-DfailIfNoTests=false`**（不是 `-DfailIfNoSpecifiedTests`）。
   **这两条我这轮都实际踩了** —— `tasklist.md` 里的命令已修正，照抄即可。

---

## 🔴 一个必须记住的「别再犯」

调研初判说 `common/` 和 `fe-filesystem-s3-base` 的
`S3CredentialsProviderType`/`S3CredentialsProviderFactory` 是重复造轮子、可以直接替换 ——
**被对抗验证两轮推翻**。两条活的行为差异（`design.md` §3.3）：
① 发给 hadoop 的凭证串会**多出** `ProfileCredentialsProvider`；
② 模式串接受面**放宽**（空串 / `ENVIRONMENT` / `WEB_IDENTITY_TOKEN_FILE` 从抛异常变成接受）。
而**全仓没有任何测试钉住那个串** ⇒ 换掉会**绿着上线一个回归**。

**下次看到「这两个类长得一样，合并掉吧」的念头，先来读 `design.md` §3.3。**

---

## 🔎 尚未验证（如实声明）

- **没跑 e2e**（需要集群）。FPC-03 是纯删除不可达代码 + Gson 回放测试已过，风险低；
  但真正的存储绑定路径（iceberg hadoop `warehouse → fs.defaultFS`）只有单测覆盖。
- **没查 apache/doris master** 是否有 `getAwsCredentialsProvider()` 调用者 → 这正是 OD-2。
- `ExternalCatalog.buildHadoopConfiguration(Map)` 的调用者没枚举 ⇒ FPC-04 明确排除它。
