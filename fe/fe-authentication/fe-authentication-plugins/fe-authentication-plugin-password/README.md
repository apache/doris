# FE Authentication Plugin - Password

## 概述

Native密码认证插件，支持本地密码验证，内置暴力破解防护和密码复杂度验证。

## 功能特性

### 1. 多种哈希算法支持

- **BCrypt** (推荐) - 自适应哈希算法，带盐值，抗彩虹表攻击
- **SHA-256** - 快速但安全性较低，用于向后兼容
- **PLAIN** - 明文存储（仅用于测试，生产环境禁用）

### 2. 暴力破解防护

- 失败尝试次数限制（默认5次）
- 账户临时锁定（默认5分钟）
- 自动解锁机制

### 3. 密码复杂度验证

-最小长度要求
- 大写字母要求
- 小写字母要求
- 数字要求
- 特殊字符要求

### 4. 自动密码升级

检测旧哈希算法并建议重新哈希。

## 配置说明

### 配置参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `hash.algorithm` | String | BCRYPT | 哈希算法（BCRYPT/SHA256/PLAIN） |
| `brute_force.max_attempts` | Integer | 5 | 最大失败尝试次数 |
| `brute_force.lockout_duration_seconds` | Integer | 300 | 锁定持续时间（秒） |
| `password.min_length` | Integer | 8 | 最小密码长度 |
| `password.require_uppercase` | Boolean | false | 是否要求大写字母 |
| `password.require_lowercase` | Boolean | false | 是否要求小写字母 |
| `password.require_digit` | Boolean | false | 是否要求数字 |
| `password.require_special` | Boolean | false | 是否要求特殊字符 |

### 配置示例

```sql
-- 创建密码认证配置（默认设置）
CREATE AUTHENTICATION INTEGRATION local_password
  TYPE = 'password'
  WITH (
    'hash.algorithm' = 'BCRYPT'
  );

-- 高安全性配置
CREATE AUTHENTICATION INTEGRATION secure_password
  TYPE = 'password'
  WITH (
    'hash.algorithm' = 'BCRYPT',
    'brute_force.max_attempts' = '3',
    'brute_force.lockout_duration_seconds' = '600',
    'password.min_length' = '12',
    'password.require_uppercase' = 'true',
    'password.require_lowercase' = 'true',
    'password.require_digit' = 'true',
    'password.require_special' = 'true'
  );
```

## 使用方式

### 1. 创建用户并设置密码

```sql
-- 创建用户（密码会自动使用BCrypt哈希）
CREATE USER alice IDENTIFIED BY 'SecureP@ssw0rd';

-- 绑定到密码认证配置
CREATE AUTHENTICATION BINDING
  FOR USER alice
  USE AUTHENTICATION INTEGRATION local_password;
```

### 2. 用户登录

```bash
# MySQL客户端登录
mysql -h doris-host -P 9030 -u alice -p
# 输入密码: SecureP@ssw0rd
```

### 3. 修改密码

```sql
-- 用户自己修改密码
ALTER USER alice IDENTIFIED BY 'NewSecureP@ssw0rd';

-- 管理员重置密码
SET PASSWORD FOR alice = PASSWORD('ResetP@ssw0rd');
```

## 安全最佳实践

### 1. 密码策略

```sql
-- 推荐：强密码策略
CREATE AUTHENTICATION INTEGRATION strong_password
  TYPE = 'password'
  WITH (
    'hash.algorithm' = 'BCRYPT',
    'password.min_length' = '12',
    'password.require_uppercase' = 'true',
    'password.require_lowercase' = 'true',
    'password.require_digit' = 'true',
    'password.require_special' = 'true'
  );

-- 不推荐：弱密码策略
CREATE AUTHENTICATION INTEGRATION weak_password
  TYPE = 'password'
  WITH (
    'hash.algorithm' = 'PLAIN',  -- 明文存储
    'password.min_length' = '1'   -- 太短
  );
```

### 2. 暴力破解防护

```sql
-- 推荐：严格的暴力破解防护
CREATE AUTHENTICATION INTEGRATION brute_force_protected
  TYPE = 'password'
  WITH (
    'brute_force.max_attempts' = '3',
    'brute_force.lockout_duration_seconds' = '600'  -- 10分钟
  );
```

### 3. 密码升级

```sql
-- 从SHA-256升级到BCrypt
ALTER AUTHENTICATION INTEGRATION legacy_password
  SET 'hash.algorithm' = 'BCRYPT';
-- 下次用户登录时会自动重新哈希
```

## 密码哈希格式

### BCrypt格式

```
$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy
│  │ │  │                                               │
│  │ │  └─ 盐值（22字符）                                └─ 哈希值（31字符）
│  │ └─ 工作因子（10 = 2^10次迭代）
│  └─ BCrypt次版本
└─ BCrypt主版本
```

### SHA-256格式

```
{SHA256}5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8
│      │
│      └─ Base64编码的SHA-256哈希
└─ 算法标识
```

### PLAIN格式（仅测试用）

```
{PLAIN}mypassword
│      │
│      └─ 明文密码
└─ 算法标识
```

## 故障排查

### 1. 登录失败：账户锁定

**错误信息**:
```
Account temporarily locked due to too many failed login attempts
```

**解决方案**:
- 等待锁定时间到期（默认5分钟）
- 或联系管理员重置失败计数（需要代码支持）

### 2. 密码不符合复杂度要求

**错误信息**:
```
Password does not meet complexity requirements
```

**解决方案**:
```sql
-- 检查当前密码策略
DESC AUTHENTICATION INTEGRATION local_password;

-- 设置符合要求的密码
ALTER USER alice IDENTIFIED BY 'ComplexP@ssw0rd123';
```

### 3. 用户不存在或密码未设置

**错误信息**:
```
User not found or password not set: alice
```

**解决方案**:
```sql
-- 创建用户并设置密码
CREATE USER alice IDENTIFIED BY 'SecureP@ssw0rd';
```

## 性能说明

### BCrypt性能

- **工作因子**: 10 (2^10 = 1024次迭代)
- **验证延迟**: 约50-100ms per password
- **推荐场景**: 正常用户登录（登录频率不高）

### SHA-256性能

- **验证延迟**: < 1ms per password
- **推荐场景**: 高频API认证（但安全性较低，建议使用Token）

## 开发指南

### 单元测试

```bash
cd fe-authentication-plugin-password
mvn test
```

### 集成测试

```bash
mvn verify -Pintegration-test
```

### 覆盖率报告

```bash
mvn test jacoco:report
open target/site/jacoco/index.html
```

## 参考资料

- [BCrypt Algorithm](https://en.wikipedia.org/wiki/Bcrypt)
- [OWASP Password Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)
- [NIST Password Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
