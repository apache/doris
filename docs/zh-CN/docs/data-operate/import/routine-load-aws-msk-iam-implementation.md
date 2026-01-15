# Routine Load AWS MSK IAM 认证实现说明

## 技术实现概述

Doris Routine Load 的 AWS MSK IAM 认证支持基于以下技术栈：

### 前端 (FE) 实现
- **版本**: Doris 1.2+
- **核心组件**: 
  - `KafkaRoutineLoadJob`: 管理 Routine Load 任务
  - `KafkaDataSourceProperties`: 解析和验证 Kafka 数据源配置
  - `KafkaConfiguration`: Kafka 配置参数定义

### 后端 (BE) 实现
- **版本**: Doris 1.2+
- **核心组件**:
  - `KafkaDataConsumer`: 使用 librdkafka 消费 Kafka 数据
  - **librdkafka 版本**: 2.11.0
  - **依赖库**:
    - cyrus-sasl 2.1.27 (SASL 认证框架)
    - AWS SDK C++ 1.11.219 (AWS 服务集成)
    - openssl 1.1.1s (SSL/TLS 加密)
    - kerberos (krb5) 相关库

## 当前支持情况

### ✅ 已支持

1. **SASL/SSL 协议**
   - 完全支持 `security.protocol=SASL_SSL`
   - SSL/TLS 加密连接已启用

2. **SASL 认证机制**
   - librdkafka 已编译 SASL 支持（`--enable-sasl`）
   - 支持以下 SASL 机制：
     - PLAIN
     - SCRAM-SHA-256
     - SCRAM-SHA-512
     - GSSAPI/Kerberos
     - OAUTHBEARER

3. **自定义 Kafka 属性**
   - 支持通过 `property.*` 前缀传递任意 Kafka 配置
   - 配置参数透传到 librdkafka
   - 支持文件配置（如 SSL 证书文件）

4. **配置验证**
   - FE 端验证 AWS MSK IAM 认证配置的完整性
   - 自动检测配置错误并提供友好的错误提示

### ⚠️ 部分支持

1. **AWS_MSK_IAM SASL 机制**
   - **状态**: 依赖外部库
   - **说明**: 
     - librdkafka 原生不支持 `AWS_MSK_IAM` SASL 机制
     - 需要额外的 `aws-msk-iam-auth` 插件库
     - **替代方案**: 使用 `OAUTHBEARER` 机制配合自定义回调

2. **OAUTHBEARER with AWS Credentials**
   - **状态**: 需要额外开发
   - **说明**:
     - librdkafka 支持 OAUTHBEARER 机制
     - 需要实现回调函数来获取 AWS 临时凭证
     - 当前 Doris 未实现该回调

### ❌ 暂不支持

1. **自动 AWS 凭证刷新**
   - 当前无法自动刷新 AWS 临时凭证
   - IAM Role 的临时凭证（通常有效期 1-12 小时）过期后需要重启 Routine Load 任务

2. **跨区域 AWS 凭证**
   - 未测试跨 AWS 区域的 IAM 认证场景

## 实现方案

### 方案一：通过配置传递（当前实现）✅

**原理**: 
- 用户通过 `property.*` 配置参数传递 SASL 和 SSL 设置
- librdkafka 处理底层认证逻辑

**优点**:
- 无需修改 BE 端代码
- 灵活支持各种 SASL 机制

**缺点**:
- AWS_MSK_IAM 机制需要外部插件
- 无法自动刷新凭证

**适用场景**:
- 使用 SASL/PLAIN 或 SASL/SCRAM 认证
- MSK 集群支持多种认证方式

### 方案二：实现 OAUTHBEARER 回调（待开发）⏳

**原理**:
- 在 BE 端实现 librdkafka 的 OAUTHBEARER 回调接口
- 回调函数使用 AWS SDK 获取临时凭证
- 自动刷新过期凭证

**实现步骤**:

```cpp
// 伪代码示例
class AwsIamOAuthCallback : public RdKafka::OAuthBearerTokenRefreshCb {
public:
    void oauthbearer_token_refresh_cb(RdKafka::Handle* handle, 
                                      const std::string& oauthbearer_config) override {
        // 1. 使用 AWS SDK 获取当前 EC2 实例的 IAM 角色凭证
        Aws::Auth::InstanceProfileCredentialsProvider provider;
        auto credentials = provider.GetAWSCredentials();
        
        // 2. 生成 AWS SigV4 签名的 token
        std::string token = generateAwsMskIamToken(credentials);
        
        // 3. 设置 token 到 librdkafka
        handle->oauthbearer_set_token(token, token_lifetime_ms, 
                                      principal_name, extensions);
    }
};
```

**优点**:
- 完全支持 AWS MSK IAM 认证
- 自动刷新凭证
- 无需外部插件

**缺点**:
- 需要修改 BE 端代码
- 增加代码复杂度
- 需要充分测试

**工作量估算**:
- 实现 OAUTHBEARER 回调: 2-3 天
- AWS 凭证集成: 1-2 天
- 测试和调试: 2-3 天
- 总计: 5-8 天

### 方案三：集成 aws-msk-iam-auth 库（高级方案）🚀

**原理**:
- 集成 AWS 官方或社区的 `aws-msk-iam-auth` C++ 库
- 作为 librdkafka 的 SASL 插件
- 支持 `sasl.mechanism=AWS_MSK_IAM`

**实现步骤**:

1. 添加 `aws-msk-iam-auth` 到第三方依赖
2. 编译配置添加插件支持
3. librdkafka 编译时链接插件
4. 配置插件加载路径

**优点**:
- 完全兼容 AWS MSK IAM
- 使用官方实现，稳定可靠
- 支持所有 AWS 凭证提供方式

**缺点**:
- 增加第三方依赖
- 需要维护额外的库
- 构建流程更复杂

**工作量估算**:
- 依赖集成: 3-5 天
- 编译配置: 2-3 天
- 测试验证: 3-5 天
- 文档更新: 1-2 天
- 总计: 9-15 天

## 当前使用限制

### 1. AWS_MSK_IAM 机制限制

如果 librdkafka 不支持 `AWS_MSK_IAM` 机制，用户可能会看到以下错误：

```
Unknown sasl mechanism: AWS_MSK_IAM
```

**解决方案**: 使用 `OAUTHBEARER` 或其他支持的 SASL 机制。

### 2. 凭证刷新限制

IAM Role 临时凭证通常有以下有效期：
- **EC2 Instance Profile**: 6 小时（默认）
- **ECS Task Role**: 1-12 小时（可配置）
- **Lambda**: 执行期间有效

**影响**:
- Routine Load 长时间运行时，凭证可能过期
- 需要手动暂停并恢复任务以获取新凭证

**建议**:
- 设置 Routine Load 的 `max_batch_interval` 和其他超时参数时考虑凭证有效期
- 实施监控和自动重启机制

### 3. 网络要求

- BE 节点必须能够访问 AWS metadata service (169.254.169.254)
- BE 节点必须能够访问 MSK broker（通常在同一 VPC）
- 需要配置正确的安全组规则

## 验证步骤

### 验证 librdkafka SASL 支持

```bash
# 在 BE 节点上运行
ldd /path/to/doris/be/lib/librdkafka.so | grep sasl

# 预期输出应包含:
# libsasl2.so.2 => /path/to/libsasl2.so.2
```

### 验证 AWS SDK 可用性

```bash
# 检查 AWS SDK 库
ls -la /path/to/doris/be/lib/libaws-*.a

# 检查 AWS 凭证（在 EC2 实例上）
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

### 测试 MSK 连接

```bash
# 使用 kafkacat 测试连接（需要单独安装）
kafkacat -b b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=OAUTHBEARER \
  -L
```

## 未来改进计划

### 短期（1-2 个月）
1. 实现 OAUTHBEARER 回调支持 AWS IAM
2. 添加凭证自动刷新机制
3. 完善错误提示和日志

### 中期（3-6 个月）
1. 集成 aws-msk-iam-auth 库
2. 支持 AWS_MSK_IAM SASL 机制
3. 添加更多 AWS 服务集成

### 长期（6-12 个月）
1. 支持多云 Kafka 服务（Azure Event Hubs, GCP Pub/Sub 等）
2. 统一认证配置接口
3. 提供图形化配置工具

## 贡献指南

如果您想贡献 AWS MSK IAM 认证的完整实现，请：

1. 在 GitHub 上创建 Issue 讨论实现方案
2. Fork Doris 仓库并创建特性分支
3. 实现代码并添加测试
4. 提交 Pull Request
5. 参与 Code Review

### 关键文件

**FE 端**:
- `fe/fe-core/src/main/java/org/apache/doris/load/routineload/kafka/KafkaDataSourceProperties.java`
- `fe/fe-core/src/main/java/org/apache/doris/load/routineload/kafka/KafkaConfiguration.java`
- `fe/fe-core/src/main/java/org/apache/doris/load/routineload/KafkaRoutineLoadJob.java`

**BE 端**:
- `be/src/runtime/routine_load/data_consumer.cpp`
- `be/src/runtime/routine_load/data_consumer.h`
- `be/src/runtime/stream_load/stream_load_context.h`

**构建配置**:
- `thirdparty/build-thirdparty.sh`
- `thirdparty/vars.sh`

## 参考资料

### AWS 文档
- [AWS MSK IAM Access Control](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
- [AWS MSK Client Authentication](https://docs.aws.amazon.com/msk/latest/developerguide/msk-authentication.html)

### librdkafka 文档
- [librdkafka SASL Configuration](https://github.com/confluentinc/librdkafka/wiki/Using-SASL)
- [librdkafka Configuration Reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

### 相关库
- [aws-msk-iam-auth (Java)](https://github.com/aws/aws-msk-iam-auth)
- [cyrus-sasl](https://www.cyrusimap.org/sasl/)
- [AWS SDK for C++](https://github.com/aws/aws-sdk-cpp)
