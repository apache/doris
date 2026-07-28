# 🤝 Session Handoff — 清理 fe-core `datasource/property/{common,metastore}`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) ·
> 清单 [`tasklist.md`](./tasklist.md) · 待拍板 [`open-decisions.md`](./open-decisions.md)

---

# ✅ 本任务空间核心工作（FPC-01 ~ FPC-04）已全部完成

**没有待拍板事项**（OD-1 / OD-2 均已由用户 2026-07-28 拍板并执行完毕）。

| 阶段 | commit | 结果 |
|---|---|---|
| 文档空间 | `938d38c7425` | 6 份文档 |
| FPC-01 + FPC-03 主删除 | `ac2d931ee3a` | 删 5 文件 473 行 + `CatalogProperty` 净减 ~45 行 |
| OD-2 反向发现 | `6d245a524d3` | 纯文档 |
| FPC-02 删死构造臂 | `a824cd81ac1` | 实删 159 行 |
| FPC-04 清扫死 storage 门 | 见 `git log` | 纯删除 135 行，零新增 |

`fe/fe-core/.../datasource/property/` 现在只剩 `common`（已瘦身到只有活代码）/ `constants` / `fileformat`；
`initStorageAdapters()` 的入口收敛为 `PluginDrivenExternalCatalog:207-208` 两处，**由构造保证**。

---

## ⚠️ FPC-04 的验证有一处缺口，接手时请知悉

完整 fe-core 套件**未跑完**：跑到 **3h29m / 1232 个测试类**时由用户指示**主动终止**
（套件耗时问题已单独立档 [`../fe-core-ut-runtime-problem.md`](../fe-core-ut-runtime-problem.md)）。

- 已绿：残留 grep = 0 · 全反应堆 `test-compile`（含测试源）· `checkstyle:check` 0 violations
- 终止时唯一失败：`http.ForwardToMasterTest.testAddBeDropBe`
  （`ClassCastException: JSONObject → JSONArray`），**已用 stash 回干净 HEAD 复现 ⇒ 既有失败，与本改动无关**
- **口径**：「已执行的 1232 个类中除 1 个既有失败外无失败」，**不是**「全套件通过」

⇒ 若要补齐，重跑一次完整套件即可（注意耗时）。

---

## ⏭ 本任务空间已完结

剩下的都是**单列后续，不属于本空间**，见 `tasklist.md` 末尾 **SEP-1 ~ SEP-4**：
- **SEP-1** `StorageAdapter.checkAzureOauth2OnlyForIcebergRest()` 在存储路径读 metastore 命名空间键
  —— 真架构违规，但带上游 #66004 刻意的大小写敏感怪癖，需单独一刀 + e2e
- **SEP-2** 把 `S3CredentialsProviderType` 上提 `fe-filesystem-api` → 调和 `hadoopClassName` → 删 `common/`
  —— 架构终局，但会改线上串，需用户先就 `Config.aws_credentials_provider_version` v1 分支拍板
- **SEP-3** `BaseProperties.getCloudCredential()` 零调用者
- **SEP-4** `fe-connector-metastore-api/pom.xml:64` 那句注释是过时的假话

另有一条**独立线**：主线 `plan-doc/HANDOFF.md` 的 scope 是「修 TeamCity **CI 997422**
（Doris_External_Regression）失败用例」。**其当前红绿状态本 session 未查证。**

---

## ⚠️ 五条验证纪律

1. 删除类改动**不能只信增量编译** → 每步先 `rm -rf fe-core/target/{classes,test-classes}`。
2. 全反应堆**必须含测试源**（禁 `-Dmaven.test.skip=true`），且必须 `-Dcheckstyle.skip=true`。
3. checkstyle `UnusedImports` 是**阻塞门禁** → **只对改动模块**单独跑 `checkstyle:check`。
4. **`-pl` 必须配 `-am`**（否则兄弟模块 `${revision}` 解析不了，报出**像真错的假错**），
   且 surefire 2.22.2 认 **`-DfailIfNoTests=false`**（不是 `-DfailIfNoSpecifiedTests`）。
5. **但 `-am test` 对「依赖链经过 shade 模块」的连接器跑不通**（如 iceberg → hms：
   报 `package org.apache.hadoop.hive.metastore.api does not exist`，因为 shaded jar 只在
   `package` 阶段产出）。**既有怪癖，已 stash 到干净 HEAD 复现确认。**
   这类模块用全反应堆 `test-compile` 覆盖；`fe-core` / `fe-connector-api` 不在该链上，`-am test` 正常。

**删字段时额外注意**（FPC-04 实证）：先 grep 字段名的**全部**出现位置，
方法外的使用（cache reset / 反序列化后处理）最容易漏，只删声明会编译不过。

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

- **完整 fe-core 套件未跑完**（见上方 ⚠️ 段），口径是「已执行的 1232 类中除 1 个既有失败外无失败」。
- **没跑 e2e**（需要集群）。四次删除都是删不可达代码，Gson 回放 + 存储适配对齐测试已过，
  但真正的存储绑定路径（iceberg hadoop `warehouse → fs.defaultFS`）只有单测覆盖。
- **主线 CI 997422 的当前状态未查证。**
- OD-2 已知代价：`getAwsCredentialsProvider()` 在上游是活的 ⇒ 上游改动该区域时 rebase 会出
  modify/delete 冲突，**届时保留删除**。
