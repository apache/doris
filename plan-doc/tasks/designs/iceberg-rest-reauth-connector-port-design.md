# 设计：把 #64966（Iceberg REST 401 自愈）移植到 fe-connector-iceberg

日期：2026-07-17　范围：用户拍板「完整对齐」（plain REST + session=user 默认路径）

## 背景 / 问题
- upstream apache/master #64966 给 Iceberg REST catalog 加了「401 时重建客户端并重试一次」的自愈能力，实现类
  `ReauthenticatingRestSessionCatalog`（`BaseViewSessionCatalog`，包住 `RESTSessionCatalog`）+ 单测，接线点在
  fe-core `IcebergRestProperties`。
- 本分支（catalog SPI 迁移）已删掉整个 fe-core legacy iceberg 子系统，`IcebergRestProperties` 不复存在；rebase 到
  master 后 #64966 的补丁落在被删代码上 → **本分支丢了这个生产修复**。连接器 `fe-connector-iceberg` 目前没有任何
  re-auth / 401 处理。本设计把该能力补进连接器。

## 架构差异（为什么不是照搬接线）
| | fe-core（#64966 的家） | 连接器现状 |
|---|---|---|
| plain REST（**生产 bug 场景**） | 永远建裸 `RESTSessionCatalog` → 包 wrapper → `asCatalog(empty)` | 走 `CatalogUtil.buildIcebergCatalog` → all-in-one `RESTCatalog`（裸 session catalog 藏私有字段，包不住）；view 靠 `catalog instanceof ViewCatalog` |
| session=user | 初始化在用户委派凭证下 → 不包装 | `buildRestSessionCatalogDefault` 建裸 `RESTSessionCatalog`，**用 catalog 自身身份**初始化；逐请求 attach 用户凭证 |

## 目标
1. 连接器所有「以 catalog 自身身份认证的 REST 操作」都能 401 自愈：即 plain REST 的默认路径 + session=user 的默认
   `asCatalog(empty)` 路径。
2. 逐用户委派请求**绝不**触发重建、用户 token **绝不**进共享客户端（wrapper 的 `usesCatalogIdentity(ctx)` 请求级
   闸门 + 重建 supplier 只用 catalog 自身属性 `frozenProps` 兜底）。
3. 原样移植 wrapper 类 + 单测（仅改包名 + 过期 prose 引用）。

## 非目标
- 不改非 REST flavor（hms/glue/jdbc/hadoop/s3tables）的构建路径。
- 不补 e2e 回归（后续单独一步，见 [[hms-iceberg-delegation-needs-e2e]] 的模式）；本步只做单测。

## 实现（4 个文件）
1. **新增** `fe-connector-iceberg/.../ReauthenticatingRestSessionCatalog.java`：#64966 原类，包名改为
   `org.apache.doris.connector.iceberg`；把 `{@code IcebergRestProperties}`/`{@code IcebergMetadataOps}` 等过期
   prose 引用改成连接器 `IcebergConnector`。逻辑一字不改。依赖（iceberg-core 1.10.1 / commons-lang3 / guava /
   log4j2）在本模块全可用。
2. **新增** 同名单测（src/test），#64966 原样，仅改包名。JUnit5，自包含（`FakeRestSessionCatalog extends
   RESTSessionCatalog`，无网络/无 Mockito）。
3. **改** `IcebergSessionCatalogAdapter`：字段/构造参/`requireSessionCatalog()` 的 `RESTSessionCatalog` →
   `BaseViewSessionCatalog`（`asCatalog(ctx)`/`asViewCatalog(ctx)` 是 `BaseViewSessionCatalog` 的方法，改类型后
   逐用户请求即经过 wrapper 的自愈方法，被请求级闸门排除）。
4. **改** `IcebergConnector`：
   - 字段 `restSessionCatalog` 类型 `RESTSessionCatalog` → `BaseViewSessionCatalog`（持 wrapper）。
   - `createCatalog()`：REST flavor（含 plain 与 session=user）统一走 `buildRestSessionCatalogDefault`；非 REST 不变。
   - `buildRestSessionCatalogDefault()`：建裸 `RESTSessionCatalog` → 包 `ReauthenticatingRestSessionCatalog`
     （delegateBuilder = 用 `frozenProps` 重建）→ `this.restSessionCatalog = wrapper`；**仅当 session=user 时**建
     `sessionCatalogAdapter`（持 wrapper）；返回 `wrapper.asCatalog(empty)`。
   - **重建的 TCCL 钉**（连接器特有、#64966 没有的关键点，见 [[catalog-spi-plugin-tccl-classloader-gotcha]]）：重建
     `new RESTSessionCatalog().initialize()` 会反射解析类，须钉插件 loader；故 delegateBuilder 经
     `context.executeAuthenticated(...)` 重建，与首建同一钉子。
   - `newCatalogBackedOps()`（共享 ops）：`restSessionCatalog != null && !isUserSessionEnabled()`（=plain REST）时，
     注入 `wrapper.asViewCatalog(empty)` 作为 viewCatalog（`asCatalog(empty)` 不是 ViewCatalog，不注入则 plain REST
     的 iceberg view 失效）。session=user 共享路径行为保持不变（不注入，逐用户路径管 view）。
   - `close()`：字段现为 `BaseViewSessionCatalog`；`instanceof Closeable` 后 close（wrapper 是 Closeable，会 close
     其 delegate）。

## 边界 / 正确性
- 只有 REST flavor 建裸 wrapper；wrapper 只由本连接器构造 → 非 REST 零影响。
- plain REST 换成 `asCatalog(empty)+asViewCatalog(empty)` 与旧 `RESTCatalog` 行为等价（`RESTCatalog` 内部就是
  `sessionCatalog.asCatalog(empty)`；view facet 用注入补齐）。
- session=user 共享无参 ops 路径**不变**（不注入 view），避免碰 #63068 fail-closed 语义。
- 重建 supplier 用 `frozenProps`（catalog 自身身份的不可变副本）→ 永不捕获用户 token。

## 测试
- 单测：移植 #64966 的 7 个用例（重建+重试1次 / 包裹异常识别 / 重建后仍 401 不死循环 / 非 auth 错误不重试 /
  委派用户会话不自愈 / asCatalog 视图路由经自愈 / close 关闭当前 delegate）。
- 编译：`mvn -pl fe-connector-iceberg -am test-compile`；跑新单测。
- checkstyle：本模块中央配置，import 分组 SAME_PACKAGE(3)→THIRD_PARTY→java，组内字母序、组间空行。

## TODO
- [ ] 移植 wrapper 类（改包 + prose）
- [ ] 移植单测（改包）
- [ ] 改 IcebergSessionCatalogAdapter 类型
- [ ] 改 IcebergConnector（字段/createCatalog/buildRestSessionCatalogDefault+TCCL重建/newCatalogBackedOps/close）
- [ ] 编译 + 跑新单测
- [ ] checkstyle 过
- [ ] 更新 HANDOFF + commit（英文 message）
