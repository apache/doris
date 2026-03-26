# Role Mapping DDL 与 OIDC 登录桥接设计

## 目标

在当前 `rich-master-auth-rolemapping-oidc` 分支上引入一等公民的 `CREATE ROLE MAPPING` / `DROP ROLE MAPPING` DDL，并以独立元数据作为 role mapping 的唯一权威来源。同时补齐最小 MySQL OIDC 登录桥接，使真实 OIDC 登录请求能够进入现有 OIDC plugin，再通过新 role mapping 元数据完成角色求值与 session 注入。

## 范围

本设计覆盖：

- `CREATE ROLE MAPPING` / `DROP ROLE MAPPING` parser 与 command
- 独立的 `RoleMappingMeta` / `RoleMappingMgr`
- role mapping create/drop 的 edit log、replay 与 snapshot
- `Env`、`AuthenticationIntegrationMgr`、`AuthenticationIntegrationRuntime` 对 role mapping 元数据的接线
- `fe-authentication-handler` 中默认 `AuthenticationService` 入口从 property-backed evaluator 切换到 metadata-backed evaluator
- 运行时只从 `RoleMappingMgr` 读取 role mapping，不再从 integration properties 读取 `role_mapping.rule.*`
- MySQL 登录链路对 OIDC 客户端插件的最小识别与 token 凭据桥接
- 针对 parser、metadata、runtime、MySQL OIDC 请求桥接的测试

本设计不覆盖：

- `ALTER ROLE MAPPING`
- `SHOW ROLE MAPPINGS` / `SHOW CREATE ROLE MAPPING`
- 旧的 integration property 内联 role mapping 兼容
- 自动把旧的 property 规则迁移成 metadata
- OIDC plugin 本身的 token 校验规则重写
- 非 MySQL 协议的 OIDC 凭据接入

## 核心决策

### 1. Role Mapping 只走独立元数据

- `ROLE MAPPING` 是一等 FE 元数据对象。
- 运行时不再把 `AUTHENTICATION INTEGRATION` properties 中的 `role_mapping.rule.*` 当作权威来源。
- 认证成功后，只能通过 integration 名称到 `RoleMappingMgr` 查找绑定的 mapping，再做规则求值。

这条决策的目的很直接：语义、存储和管理都放到单独对象里，避免把权限模型埋在 integration properties 这种临时实现细节中。

### 2. 一个 integration 最多绑定一个 mapping

- 一个 mapping 只归属一个 integration。
- 一个 integration 最多只能绑定一个 mapping。
- 一个 mapping 中可以包含多条 `RULE`，最终角色是所有命中规则授予角色的并集。

这延续之前 worktree 的约束，避免运行时再处理多个 mapping 的优先级、合并顺序和运维可观测性问题。

### 3. 先只做 CREATE / DROP，保证最小闭环

这次只迁：

- `CREATE ROLE MAPPING`
- `DROP ROLE MAPPING`

不同时引入 `ALTER` / `SHOW`。原因是当前目标是先打通 OIDC 登录链路，让新的 metadata-backed role mapping 真正参与认证后的授权，而不是一次把 role mapping 管理面做全。

### 4. 真实 OIDC 登录需要补最小 MySQL 凭据桥接

只迁 role mapping DDL 还不足以打通真实 OIDC 登录。当前 MySQL 登录解析只会产出：

- `CLEAR_TEXT_PASSWORD`
- `MYSQL_NATIVE_PASSWORD`

而 OIDC plugin 只接受：

- `OIDC_ID_TOKEN`
- `JWT_TOKEN`

因此还需要在 MySQL 认证请求构造阶段识别 OIDC 客户端插件，把客户端提交的 token 映射为 OIDC 凭据类型，送入现有 OIDC 认证链。

### 5. 这是有意的不兼容变更

本次明确不兼容旧的 `role_mapping.rule.*` property 存储方式。

含义是：

- 升级后，生产路径将不再消费这些 properties
- 现有把 role mapping 写在 integration properties 里的部署不会被自动迁移
- 如果用户要保留这些映射，必须在切换到新版本前或切换后手工用 `CREATE ROLE MAPPING` 重新建模

这不是遗漏，而是设计决定。这样可以避免继续背负两套权威来源，并减少后续 `ALTER` / `SHOW` / 审计语义的复杂度。

## 权威语法

```sql
CREATE ROLE MAPPING [IF NOT EXISTS] <mapping_name>
  ON AUTHENTICATION INTEGRATION <integration_name>
  RULE (
    USING CEL '<condition>'
    GRANT ROLE <role1>, <role2>, ...
  )
  [, RULE (
    USING CEL '<condition>'
    GRANT ROLE <role1>, <role2>, ...
  ) ...]
  [COMMENT '<comment>'];
```

```sql
DROP ROLE MAPPING [IF EXISTS] <mapping_name>;
```

当前不会实现其它草案语法，也不会支持从 integration properties 自动转义或导入。

## 元数据模型

在 `fe-core` 中新增 `RoleMappingMeta`：

- `name`
- `integrationName`
- `rules`
- `comment`
- `createUser`
- `createTime`
- `alterUser`
- `alterTime`

规则元数据采用嵌套或并列的 `RuleMeta` 表达：

- `condition`
- `grantedRoles`

`RoleMappingMeta` 负责持久化与 FE 侧审计信息，并提供到纯领域对象的转换：

- `RoleMappingDefinition`

`RoleMappingDefinition` 仍放在 `fe-authentication-role-mapping` 模块里，作为 CEL evaluator 的输入模型。

## 管理器模型

在 `fe-core` 中新增 `RoleMappingMgr`，职责如下：

- `createRoleMapping(...)`
- `dropRoleMapping(...)`
- `getRoleMapping(String mappingName)`
- `getRoleMappingByIntegration(String integrationName)`
- `hasRoleMapping(String integrationName)`
- replay create/drop
- snapshot read/write

额外约束：

- `AuthenticationIntegrationMgr` 删除 integration 前必须检查该 integration 是否仍绑定 role mapping
- `Env` 需要持有并暴露 `RoleMappingMgr`

## Parser 与 Command 接入

在 `DorisParser.g4` / `DorisLexer.g4` 中加入：

- `CREATE ROLE MAPPING`
- `DROP ROLE MAPPING`
- `RULE ( USING CEL ... GRANT ROLE ... )`

在 `LogicalPlanBuilder` 中新增 visitor：

- `CreateRoleMappingCommand`
- `DropRoleMappingCommand`

command 行为保持与现有 authentication DDL 一致：

- 要求 ADMIN 权限
- create/drop 通过 manager 执行
- create 走 `ForwardWithSync`

## 校验规则

### 创建时强校验

`CREATE ROLE MAPPING` 时必须校验：

- mapping 名称全局唯一
- integration 必须存在
- integration 还没有绑定其它 mapping
- 至少有一条 `RULE`
- 每条规则的 CEL 条件非空
- 每条规则至少授予一个 Doris role
- 所有被授予的 Doris role 都已存在
- 所有 CEL 条件可以成功编译
- 条件返回值必须是 boolean

### 删除时校验

- `DROP ROLE MAPPING IF EXISTS` 幂等
- 普通 `DROP ROLE MAPPING` 在 mapping 不存在时失败

### Replay 与 FE 启动语义

create 阶段做强校验，但 replay / startup 保持惰性：

- replay 只恢复元数据
- 不因坏 mapping 阻止 FE 启动
- 只有真正认证命中该 integration 时，运行时才因坏 mapping 以 `MISCONFIGURED` 失败

## 运行时求值模型

当前分支里基于 `IntegrationPropertyRoleMappingEvaluator` 的生产接线要移除。新的生产模型是：

1. integration 认证成功
2. 根据 integration 名称到 `RoleMappingMgr` 查询唯一 mapping
3. 将 `RoleMappingMeta` 转换成 `RoleMappingDefinition`
4. 使用 metadata-backed evaluator 编译或复用缓存
5. 使用当前 `Principal` 求值
6. 将求值结果与 plugin 自己授予的 roles 做并集
7. 把最终 roles 注入 `AuthenticateResponse` / `ConnectContext`

如果 integration 没有绑定 role mapping，则只保留 plugin 自己授予的 roles。

需要同步调整的生产入口：

- `fe-core` 中的 `AuthenticationIntegrationRuntime`
- `fe-authentication-handler` 中默认构造的 `AuthenticationService`

否则 `fe-authentication-handler` 仍会继续默认走 `IntegrationPropertyRoleMappingEvaluator`，无法真正完成权威来源切换。

## MySQL OIDC 登录桥接

### 现状问题

当前 MySQL 登录链路虽然能读到客户端声明的 `auth plugin name`，但并没有把 OIDC 客户端请求转换成 OIDC 凭据：

- `MysqlAuthPacket` 能读出 `pluginName`
- resolver 最终只构造 `CLEAR_TEXT_PASSWORD` 或 `MYSQL_NATIVE_PASSWORD`
- OIDC plugin 因此无法在真实 MySQL 登录链路里被选择

### 最小桥接策略

本次只做最小可用桥接，不重写整个 MySQL 认证模型：

- 新增一个面向 MySQL 登录的 OIDC token resolver，或在现有 clear-password resolver 上做有界扩展
- 当客户端插件名表明这是 OIDC 客户端登录时，将客户端提交的 token 视为原始 UTF-8 凭据
- 构造 `AuthenticateRequest` 时显式写入 `CredentialType.OIDC_ID_TOKEN`
- 保留 `remoteHost`、`clientType=mysql` 等现有字段

推荐识别信号：

- `MysqlAuthPacket.getPluginName()` 为 OIDC 客户端插件名，例如 `authentication_openid_connect_client`

桥接后的链路：

1. MySQL 客户端用 OIDC token 发起登录
2. FE 识别为 OIDC 客户端插件请求
3. resolver 产出 `OIDC_ID_TOKEN`
4. `AuthenticationIntegrationAuthenticator` 将请求转入 runtime
5. runtime 选中 OIDC plugin 完成 token 校验
6. runtime 从 `RoleMappingMgr` 获取 mapping 并求值
7. session 得到最终认证角色并用于权限检查

### 有意不做的事情

- 不修改 OIDC plugin 支持的 credential type 语义
- 不新增第二套 role 注入链路
- 不把 OIDC token 塞回 password-only 分支里做隐式兼容

## 模块边界

保留在 `fe-core`：

- parser / command
- metadata / manager
- edit log / journal / replay
- `Env` wiring
- MySQL 登录请求桥接
- integration 删除前的 role mapping 约束校验

保留在 `fe-authentication-handler`：

- 默认 `AuthenticationService` 的 metadata-backed evaluator 接线
- 相关 handler 级测试更新

保留在 `fe-authentication-role-mapping`：

- `RoleMappingDefinition`
- `RoleMappingDefinitionProvider`
- metadata-backed evaluator
- CEL engine 与缓存

保留在 `fe-authentication-plugin-oidc`：

- token 校验
- principal 构造

## 错误处理

DDL 错误：

- 重名、integration 不存在、role 不存在、CEL 编译失败，统一在 DDL 阶段返回明确错误

运行时错误：

- mapping 不存在：视为没有额外 role mapping，继续使用 plugin granted roles
- mapping 元数据损坏或 CEL 编译失败：返回 `MISCONFIGURED`
- OIDC token 无效：保持现有 `BAD_CREDENTIAL`
- MySQL OIDC 客户端被识别但未能提供 token：返回 `BAD_CREDENTIAL`

兼容性错误：

- 升级后仍残留的 `role_mapping.rule.*` properties 不再生效
- 本次不提供自动导入或自动告警机制，文档与发布说明必须明确这是 breaking change

## 测试策略

### Parser / Command

- `CREATE ROLE MAPPING` 解析成功
- `DROP ROLE MAPPING` 解析成功
- 多条 `RULE`
- `IF NOT EXISTS` / `IF EXISTS`
- 非法 rule 子句失败

### Manager / Metadata

- create 成功
- drop 成功
- integration 不存在失败
- integration 重复绑定失败
- role 不存在失败
- replay 只恢复元数据
- integration 被引用时删除失败
- snapshot round-trip 后元数据仍完整可读

### Runtime

- metadata-backed mapping 参与 granted roles 合并
- 无 mapping 时只保留 plugin roles
- 坏 mapping 在运行时返回 `MISCONFIGURED`
- `AuthenticationIntegrationRuntime` 默认不再走 property-backed evaluator
- `AuthenticationService` 默认不再走 property-backed evaluator

### Restart / Replay 安全性

- 启动或 replay 时遇到坏 mapping 元数据不会阻止 FE 恢复
- 坏 mapping 只会在首次真实命中时失败
- snapshot 恢复后的坏 mapping 仍满足上述惰性失败语义

### MySQL OIDC 登录桥接

- OIDC 客户端插件请求被识别为 `OIDC_ID_TOKEN`
- 非 OIDC 客户端请求不受影响
- 认证成功后 session 中可见 metadata-backed mapped roles

## 实施顺序

建议按以下顺序落地：

1. 迁 parser / command / plan type
2. 迁 `RoleMappingMeta` / `RoleMappingMgr` / 持久化 / `Env` wiring
3. 把 `AuthenticationIntegrationRuntime` 与 `AuthenticationService` 都切到 metadata-backed evaluator，并移除生产路径上的 integration property evaluator
4. 补 MySQL OIDC token 桥接
5. 补 parser、manager、runtime、restart/replay、MySQL 登录桥接测试

## 预期结果

完成后，当前分支将具备以下闭环：

- 管理员可以通过 `CREATE ROLE MAPPING` 为 OIDC integration 建立角色映射
- Doris 在真实 OIDC MySQL 登录时能正确进入 OIDC plugin
- 认证成功后可从独立 role mapping 元数据求值得到 Doris roles
- session 权限检查使用最终求值结果，而不是 integration properties 中的临时规则
- 升级路径对旧 property 配置的失效是显式且可预期的，而不是隐式兼容
