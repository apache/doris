# 🤝 Session Handoff — 清理 fe-core `datasource/property/{common,metastore}`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) ·
> 清单 [`tasklist.md`](./tasklist.md) · 待拍板 [`open-decisions.md`](./open-decisions.md)

---

# ✅ 本任务核心工作已全部完成。只剩可选的 FPC-04。

**没有待拍板事项**（OD-1 / OD-2 均已由用户 2026-07-28 拍板并执行完毕）。

| 阶段 | commit | 结果 |
|---|---|---|
| 文档空间 | `938d38c7425` | 6 份文档 |
| FPC-01 + FPC-03 主删除 | `ac2d931ee3a` | 删 5 文件 473 行 + `CatalogProperty` 净减 ~45 行 |
| OD-2 反向发现 | `6d245a524d3` | 纯文档，无代码 |
| FPC-02 删死构造臂 | 见 `git log` | 实删 159 行 |

**fe-core `datasource/property/` 现状**：`metastore/` 已不存在；只剩
`common`（已瘦身到只有活代码）/ `constants` / `fileformat`。

---

## 📌 两条拍板结论（**已执行，勿再动摇**）

- **OD-1 = 抛异常。** `CatalogProperty.resolveDerivedStorageDefaults()` 的 null-supplier 分支
  `throw new IllegalStateException(...)`，守卫测试
  `CatalogPropertyPluginStorageDerivationTest.unwiredSupplierFailsLoudInsteadOfDerivingNothing`
  （已做变异验证）。
- **OD-2 = 直接删**（用户推翻了我的推荐）。**已知并接受的代价**：
  `upstream-apache/master` 上 `StorageAdapter.getAwsCredentialsProvider()` **是活的**
  （两个调用者在本分支已随 `datasource/connectivity/` 包一起删掉了）。
  ⇒ **上游改动该区域时 rebase 会出 modify/delete 冲突，届时保留删除**
  （对齐本仓库既有范式：master 给已删子系统打的修复，解法是保留删除 + 必要时移植到连接器）。

---

## ⏭ 剩下的唯一任务：FPC-04（可选）

清扫 fe-core 已死的 storage 门：`ExternalCatalog.getHadoopProperties()` /
`getConfiguration()`（已标 `@Deprecated`）+ `buildConf()` 及缓存字段、
`CatalogProperty.getBackendStorageProperties()` / `getOrderedStorageAdapters()`。

- **动手前必须重新逐符号 grep 确认零调用者**（别信这份文档的旧结论）。
- **✋ 不要碰** `ExternalCatalog.buildHadoopConfiguration(Map)` —— 它的调用者从未枚举过。
- 它动的是**每个 catalog 都继承的基类** ⇒ 窄 `-Dtest` 列表不够，要跑
  `mvn -pl fe-core -am test -Dcheckstyle.skip=true -DfailIfNoTests=false --fail-at-end`。
- 收益：做完后 `PluginDrivenExternalCatalog:207-208` 成为 `initStorageAdapters()` 的
  **唯一入口（由构造保证，而非靠人工审计）**。

其余单列后续见 `tasklist.md` 末尾的 **SEP-1 ~ SEP-4**（都不属于本任务）。

---

## ⚠️ 五条验证纪律（第 4、5 条是这两轮实测新增的）

1. 删除类改动**不能只信增量编译** → 每步先 `rm -rf fe-core/target/{classes,test-classes}`。
2. 全反应堆**必须含测试源**（禁 `-Dmaven.test.skip=true`），且必须 `-Dcheckstyle.skip=true`。
3. checkstyle `UnusedImports` 是**阻塞门禁** → **只对改动模块**单独跑 `checkstyle:check`。
4. **`-pl` 必须配 `-am`**（否则兄弟模块 `${revision}` 解析不了，报出**像真错的假错**），
   且 surefire 2.22.2 认 **`-DfailIfNoTests=false`**（不是 `-DfailIfNoSpecifiedTests`）。
5. **但 `-am test` 对「依赖链经过 shade 模块」的连接器跑不通**（如 iceberg → hms：
   报 `package org.apache.hadoop.hive.metastore.api does not exist`，因为 shaded jar 只在
   `package` 阶段产出）。**这是既有怪癖，已 stash 到干净 HEAD 复现确认。**
   这类模块用全反应堆 `test-compile` 覆盖；`fe-connector-api` 不在该链上，`-am test` 正常。

---

## 🔴 一个必须记住的「别再犯」

调研初判说 `common/` 和 `fe-filesystem-s3-base` 的
`S3CredentialsProviderType`/`S3CredentialsProviderFactory` 是重复造轮子、可以直接替换 ——
**被对抗验证两轮推翻**。两条活的行为差异（`design.md` §3.3）：
① 发给 hadoop 的凭证串会**多出** `ProfileCredentialsProvider`；
② 模式串接受面**放宽**（空串 / `ENVIRONMENT` / `WEB_IDENTITY_TOKEN_FILE` 从抛异常变成接受）。
而**全仓没有任何测试钉住那个串** ⇒ 换掉会**绿着上线一个回归**。

**FPC-02 删的是「构造 provider 实例」那一臂，不是这个。** `common/` 里
`AwsCredentialsProviderMode` 全保留、`AwsCredentialsProviderFactory.getV2ClassName(mode, boolean)`
保留 —— 它们喂的正是上面那条**活的** hadoop/BE 串。**下次别顺手把它们也合并掉。**

---

## 🔎 尚未验证（如实声明）

- **没跑 e2e**（需要集群）。两次删除都是删不可达代码，且 Gson 回放 + 存储适配对齐测试已过，
  但真正的存储绑定路径（iceberg hadoop `warehouse → fs.defaultFS`）只有单测覆盖。
- `ExternalCatalog.buildHadoopConfiguration(Map)` 的调用者没枚举 ⇒ FPC-04 明确排除它。
