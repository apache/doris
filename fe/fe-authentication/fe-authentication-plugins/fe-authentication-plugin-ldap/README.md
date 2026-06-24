# LDAP Authentication Plugin

LDAP 认证插件，用于将用户身份验证委托给 LDAP 服务器。

## 功能特性

- LDAP 用户身份验证
- LDAP 组信息提取
- 多 LDAP 实例支持（可配置多个 LDAP Integration）
- 配置热重载

## 架构边界

### 认证层职责（本插件）
- 验证 LDAP 密码
- 提取 LDAP 组信息
- 返回 `Principal{username, externalGroups}`

### 授权层职责（fe-core/Auth）
- ROLE_MAPPING: LDAP 组 → Doris 角色
- 权限检查
- JIT 用户创建

## 配置示例

### 基础配置

```sql
CREATE AUTHENTICATION INTEGRATION corp_ldap
  TYPE = 'ldap'
  WITH (
    'server' = 'ldap://ldap.example.com:389',
    'base_dn' = 'dc=example,dc=com'
  );
```

### 完整配置

```sql
CREATE AUTHENTICATION INTEGRATION corp_ldap
  TYPE = 'ldap'
  WITH (
    -- 必需配置
    'server' = 'ldap://ldap.example.com:389',
    'base_dn' = 'dc=example,dc=com',

    -- 用户查找配置
    'user_base_dn' = 'ou=users,dc=example,dc=com',
    'user_filter' = '(uid={login})',

    -- 组查找配置
    'group_base_dn' = 'ou=groups,dc=example,dc=com',
    'group_filter' = '',  -- 可选，默认使用 member 属性

    -- LDAP 绑定凭据（用于组查询）
    'bind_dn' = 'cn=admin,dc=example,dc=com',
    'bind_password' = 'admin_password'
  );
```

### 配置参数说明

| 参数 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `server` | 是 | 无 | LDAP 服务器地址，格式：`ldap://host:port` |
| `base_dn` | 是 | 无 | LDAP 基础 DN |
| `user_base_dn` | 否 | `ou=users,{base_dn}` | 用户搜索基础 DN |
| `user_filter` | 否 | `(uid={login})` | 用户过滤器，`{login}` 会被替换为用户名 |
| `group_base_dn` | 否 | `ou=groups,{base_dn}` | 组搜索基础 DN |
| `group_filter` | 否 | 空（使用 member） | 组过滤器，支持 `{login}` 占位符 |
| `bind_dn` | 否 | 无 | LDAP 管理员 DN（用于组查询） |
| `bind_password` | 否 | 无 | LDAP 管理员密码 |

## 与 fe-core 功能对齐

| 功能 | fe-core 实现 | 插件实现 | 状态 |
|------|-------------|---------|------|
| 用户查找 | `LdapClient.getUserDn()` | `LdapClient.getUserDn()` | 已对齐 |
| 密码验证 | `LdapClient.checkPassword()` | `LdapClient.checkPassword()` | 已对齐 |
| 组提取 | `LdapClient.getGroups()` | `LdapClient.getGroups()` | 已对齐 |
| 组名解析 | 从 DN 提取 cn | 从 DN 提取 cn | 已对齐 |

## 使用示例

### 1. 创建 LDAP 认证配置

```sql
CREATE AUTHENTICATION INTEGRATION corp_ldap
  TYPE = 'ldap'
  WITH (
    'server' = 'ldap://ldap.corp.com:389',
    'base_dn' = 'dc=corp,dc=com',
    'bind_dn' = 'cn=admin,dc=corp,dc=com',
    'bind_password' = 'admin_pass'
  );
```

### 2. 将用户绑定到 LDAP 认证

```sql
CREATE USER 'alice'@'%' IDENTIFIED WITH corp_ldap;
```

### 3. 用户登录

```bash
mysql -h doris-host -P 9030 -u alice -p
# 输入 LDAP 密码
```

## 认证流程

```
1. 用户输入用户名和密码
   ↓
2. 插件查找用户 DN
   - 使用 user_filter 在 user_base_dn 下搜索
   ↓
3. 插件验证密码
   - 使用用户 DN 和密码尝试 LDAP 绑定
   ↓
4. 插件提取 LDAP 组
   - 使用 bind_dn 在 group_base_dn 下搜索
   - 从组 DN 提取组名（cn=group_name,... → group_name）
   ↓
5. 返回 Principal{username, groups}
   ↓
6. fe-core/Auth 执行 ROLE_MAPPING
   - LDAP 组 → Doris 角色
   ↓
7. 权限检查
```

## 测试

运行单元测试：

```bash
cd fe-authentication-plugins/fe-authentication-plugin-ldap
mvn test
```

注意：完整的集成测试需要运行 LDAP 服务器（如 Docker 中的 OpenLDAP）。

## 限制

1. **连接池**：当前版本未实现连接池，高并发场景可能需要优化
2. **TLS/SSL**：暂不支持 LDAPS，需要后续添加
3. **嵌套组**：暂不支持递归查询嵌套组

## 未来改进

- [ ] 支持 LDAPS（TLS/SSL 加密连接）
- [ ] 连接池优化
- [ ] 嵌套组支持
- [ ] 更详细的错误日志
- [ ] 性能指标监控

## 参考

- [fe-core LDAP 实现](../../fe-core/src/main/java/org/apache/doris/mysql/authenticate/ldap/)
- [Spring LDAP 文档](https://docs.spring.io/spring-ldap/reference/)
- [设计文档](../../design.md)
