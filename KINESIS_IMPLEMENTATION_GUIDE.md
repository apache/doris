# Apache Doris Routine Load 的 AWS Kinesis 支持 - 实现指南

## 📋 概述

本文档详细描述了 Apache Doris Routine Load 对 AWS Kinesis Data Streams 的完整支持实现。该实现使 Doris 能够从 AWS Kinesis 流中持续摄取数据，为 AWS 原生客户提供无缝的流式数据摄取体验，无需部署 Kafka 基础设施。

**实现日期:** 2026-02-10
**版本:** 初始版本
**状态:** ✅ 核心实现完成

---

## 🎯 实现内容

### 前端 (FE - Java) - 已存在
前端实现已在 PR #60051 中完成，包括:

- ✅ **KinesisDataSourceProperties** - 完整的属性解析和 AWS 认证支持
- ✅ **KinesisRoutineLoadJob** - 作业生命周期管理
- ✅ **KinesisTaskInfo** - 任务信息和调度
- ✅ **KinesisProgress** - 使用序列号的进度跟踪
- ✅ **KinesisConfiguration** - 所有配置参数
- ✅ **Thrift 定义** - TKinesisLoadInfo, TKinesisRLTaskProgress
- ✅ **工厂集成** - RoutineLoadDataSourcePropertyFactory 连接

### 后端 (BE - C++) - 新实现

#### 阶段 1: AWS SDK 集成
**修改的文件:**
- `/mnt/disk2/huangruixin/apache/doris/thirdparty/build-thirdparty.sh`
- `/mnt/disk2/huangruixin/apache/doris/be/cmake/thirdparty.cmake`

**变更:**
```bash
# build-thirdparty.sh (line 1417)
-DBUILD_ONLY="core;s3;s3-crt;transfer;identity-management;sts;kinesis"

# 添加 kinesis 库的 strip (line 1442)
strip_lib libaws-cpp-sdk-kinesis.a
```

```cmake
# thirdparty.cmake (line 141)
add_thirdparty(aws-cpp-sdk-kinesis LIB64)
```

**影响:** AWS Kinesis SDK 现在与现有的 S3 组件一起编译和链接。

---

#### 阶段 2: 核心数据结构
**修改的文件:**
- `/mnt/disk2/huangruixin/apache/doris/be/src/runtime/stream_load/stream_load_context.h`

**创建的文件:**
- `/mnt/disk2/huangruixin/apache/doris/be/src/io/fs/kinesis_consumer_pipe.h`

**KinesisLoadInfo 类:**
```cpp
class KinesisLoadInfo {
public:
    std::string region;                                      // AWS 区域
    std::string stream;                                      // 流名称
    std::string endpoint;                                    // 可选的自定义端点
    std::map<std::string, std::string> begin_sequence_number; // shard -> 起始序列号
    std::map<std::string, std::string> cmt_sequence_number;   // shard -> 已提交序列号
    std::map<std::string, std::string> properties;           // AWS 凭证
};
```

**关键设计要点:**
- 使用 `std::string` 表示 shard ID（Kinesis 格式: "shardId-000000000000"）
- 使用 `std::string` 表示序列号（128位整数的字符串形式）
- 镜像 KafkaLoadInfo 结构以保持一致性

**KinesisConsumerPipe:**
- 与 KafkaConsumerPipe 接口相同
- 支持 `append_with_line_delimiter()` 用于 CSV/文本
- 支持 `append_json()` 用于 JSON 数据

---

#### 阶段 3: KinesisDataConsumer 实现 (~500 行)
**修改的文件:**
- `/mnt/disk2/huangruixin/apache/doris/be/src/runtime/routine_load/data_consumer.h` (类声明)
- `/mnt/disk2/huangruixin/apache/doris/be/src/runtime/routine_load/data_consumer.cpp` (完整实现)

**类架构:**
```cpp
class KinesisDataConsumer : public DataConsumer {
    // 配置
    std::string _region;
    std::string _stream;
    std::string _endpoint;
    std::unordered_map<std::string, std::string> _custom_properties;

    // 活动状态
    std::set<std::string> _consuming_shard_ids;
    std::shared_ptr<Aws::Kinesis::KinesisClient> _kinesis_client;
    std::map<std::string, std::string> _shard_iterators;  // shard -> iterator

    // 核心方法
    Status init(std::shared_ptr<StreamLoadContext> ctx);
    Status assign_shards(...);
    Status group_consume(BlockingQueue<...>* queue, int64_t max_time);
    Status reset();
    Status cancel(...);
    bool match(...);
};
```

**实现的关键方法:**

1. **init() & _create_kinesis_client()**
   - 使用凭证创建 AWS Kinesis 客户端
   - **重用 S3ClientFactory 基础设施** 支持所有认证方法:
     - 简单 AK/SK
     - IAM 实例配置文件（EC2 角色）
     - STS assume role
     - 会话令牌
     - 环境变量
     - 默认凭证链

2. **assign_shards()**
   - 为消费者分配 shard 及其起始位置
   - 为每个 shard 调用 `_get_shard_iterator()`

3. **_get_shard_iterator()**
   - 处理特殊位置:
     - `TRIM_HORIZON` → 从最旧的记录开始
     - `LATEST` → 从最新的记录开始
     - 特定序列号 → 从指定位置恢复

4. **group_consume() - 主消费循环**
   ```cpp
   while (true) {
       // 检查取消
       if (_cancelled) break;

       // 轮询所有 shard
       for (auto& shard_id : _consuming_shard_ids) {
           // 调用 GetRecords API
           request.SetShardIterator(iterator);
           request.SetLimit(1000);
           auto outcome = _kinesis_client->GetRecords(request);

           // 处理限流（1秒退避）
           if (PROVISIONED_THROUGHPUT_EXCEEDED) {
               sleep(1000ms);
               continue;
           }

           // 处理记录 → 添加到队列
           _process_records(result, queue);

           // 更新下次调用的迭代器
           iterator = result.GetNextShardIterator();

           // 检测 shard 关闭（分裂/合并）
           if (iterator.empty()) {
               _consuming_shard_ids.erase(shard_id);
           }
       }
   }
   ```

   **关键特性:**
   - **轮询 shard 消费** - 防止队头阻塞
   - **限流处理** - 在 `ProvisionedThroughputExceededException` 时退避
   - **Shard 生命周期管理** - 通过空的 `NextShardIterator` 检测关闭的 shard
   - **重试逻辑** - 重试瞬态错误（网络、服务不可用）
   - **指标跟踪** - 重用现有的 Kafka 指标（通用）

5. **_process_records()**
   - 从 GetRecords 结果中提取记录
   - 将记录添加到阻塞队列
   - 跟踪消费的字节数和行数

6. **_is_retriable_error()**
   - 分类 AWS 错误:
     - **可重试:** 限流、服务不可用、网络错误
     - **致命:** 无效凭证、流不存在

7. **reset(), cancel(), match()**
   - 标准 DataConsumer 生命周期管理
   - `match()` 检查 region、stream、endpoint 和 properties

---

#### 阶段 4: 与任务执行器集成
**修改的文件:**
- `/mnt/disk2/huangruixin/apache/doris/be/src/runtime/routine_load/data_consumer_pool.cpp`
- `/mnt/disk2/huangruixin/apache/doris/be/src/runtime/routine_load/routine_load_task_executor.cpp`

**Consumer Pool 集成:**
```cpp
// data_consumer_pool.cpp
switch (ctx->load_src_type) {
case TLoadSourceType::KINESIS:
    consumer = std::make_shared<KinesisDataConsumer>(ctx);
    break;
}
```

**任务执行器集成:**
```cpp
// routine_load_task_executor.cpp

// 1. 上下文初始化 (line 318)
case TLoadSourceType::KINESIS:
    ctx->kinesis_info.reset(new KinesisLoadInfo(task.kinesis_load_info));
    break;

// 2. Pipe 创建 (line 420)
case TLoadSourceType::KINESIS:
    pipe = std::make_shared<io::KinesisConsumerPipe>();
    auto kinesis_consumer = std::static_pointer_cast<KinesisDataConsumer>(
        consumer_grp->consumers()[0]);
    kinesis_consumer->assign_shards(
        ctx->kinesis_info->begin_sequence_number,
        ctx->kinesis_info->stream, ctx);
    break;

// 3. 进度跟踪 (line 551)
case TLoadSourceType::KINESIS:
    // 序列号已在 ctx->kinesis_info->cmt_sequence_number 中跟踪
    // 将通过 TRLTaskTxnCommitAttachment 发送回 FE
    LOG(INFO) << "Kinesis 任务完成。已提交 "
              << ctx->kinesis_info->cmt_sequence_number.size() << " 个 shard";
    break;
```

---

## 📐 架构设计

### Kinesis 与 Kafka 对比

| 方面 | Kafka | Kinesis |
|--------|-------|---------|
| **分区** | 整数分区 ID | 字符串 shard ID |
| **位置跟踪** | 整数 offset | 字符串序列号（128位） |
| **消费者类型** | 组消费者（带协调） | 独立消费者 |
| **认证** | Broker 凭证 | AWS IAM 凭证 |
| **端点** | Broker 列表 | 区域端点 |
| **内部库** | librdkafka C++ | AWS SDK for C++ (Kinesis) |
| **限流** | 每个 broker | 每个 shard（5 GetRecords/秒） |
| **位置类型** | OFFSET_BEGINNING, OFFSET_END | TRIM_HORIZON, LATEST, AT_TIMESTAMP |

### 数据流架构

```
FE (Java)                                    BE (C++)
┌─────────────────────┐                     ┌──────────────────────┐
│ KinesisRoutineLoad  │                     │ KinesisDataConsumer  │
│      Job            │                     │                      │
│                     │  TKinesisLoadInfo  │  ┌────────────────┐  │
│  - region           │ ──────────────────>│  │ Kinesis Client │  │
│  - stream           │                     │  │  (AWS SDK)     │  │
│  - shards           │                     │  └────────────────┘  │
│  - credentials      │                     │         │            │
│  - properties       │                     │         ▼            │
│                     │                     │   GetRecords API     │
└─────────────────────┘                     │   (轮询调度)         │
         │                                  │         │            │
         │                                  │         ▼            │
         │  TKinesisRLTaskProgress         │  ┌────────────────┐  │
         │<─────────────────────────────────│  │ BlockingQueue  │  │
         │  (序列号)                        │  │   (Records)    │  │
         │                                  │  └────────────────┘  │
         │                                  │         │            │
         ▼                                  │         ▼            │
┌─────────────────────┐                     │  ┌────────────────┐  │
│ KinesisProgress     │                     │  │ KinesisPipe    │  │
│  - shard -> seq num │                     │  │ (StreamLoad)   │  │
└─────────────────────┘                     │  └────────────────┘  │
                                            └──────────────────────┘
```

### 认证流程

```
来自 FE 的属性
     │
     ├─ aws.access.key.id ────────┐
     ├─ aws.secret.access.key ────┤
     ├─ aws.session.token ────────┤
     ├─ aws.iam.role.arn ─────────┤
     ├─ aws.external.id ──────────┤
     └─ aws.credentials.provider ─┤
                                   ▼
                        S3ClientConf (复用!)
                                   │
                                   ▼
                    S3ClientFactory::get_aws_credentials_provider()
                                   │
                                   ├─ SimpleAWSCredentialsProvider
                                   ├─ InstanceProfileCredentialsProvider
                                   ├─ STSAssumeRoleCredentialsProvider
                                   ├─ EnvironmentAWSCredentialsProvider
                                   └─ CustomAwsCredentialsProviderChain
                                   │
                                   ▼
                         AWS Kinesis Client
```

**关键设计决策:** 重用 S3 认证基础设施消除了代码重复，并提供了经过实战测试的凭证处理。

---

## 🚀 使用方法

### 构建支持 Kinesis 的 Doris

```bash
# 构建第三方库（包括 AWS Kinesis SDK）
cd thirdparty
./download-thirdparty.sh
./build-thirdparty.sh

# 验证 Kinesis 库已构建
ls -lh installed/lib64/libaws-cpp-sdk-kinesis.a

# 构建 Doris BE
cd ../be
mkdir build && cd build
cmake ..
make -j8

# 验证 KinesisDataConsumer 已编译
nm -C ../lib/doris_be | grep KinesisDataConsumer
```

### SQL 使用示例

#### 基本示例（访问密钥认证）
```sql
-- 创建目标表
CREATE TABLE kinesis_orders (
    order_id INT,
    user_id INT,
    amount DECIMAL(10,2),
    order_time DATETIME
) DISTRIBUTED BY HASH(order_id) BUCKETS 10;

-- 创建 routine load 作业
CREATE ROUTINE LOAD kinesis_orders_load ON kinesis_orders
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.order_id\", \"$.user_id\", \"$.amount\", \"$.order_time\"]"
)
FROM KINESIS (
    "kinesis.region" = "us-east-1",
    "kinesis.stream" = "production-orders-stream",
    "kinesis.default_position" = "LATEST",
    "aws.access.key.id" = "AKIAIOSFODNN7EXAMPLE",
    "aws.secret.access.key" = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
);
```

#### IAM 角色认证（推荐用于 EC2/EKS）
```sql
CREATE ROUTINE LOAD kinesis_orders_load ON kinesis_orders
PROPERTIES (
    "format" = "json"
)
FROM KINESIS (
    "kinesis.region" = "us-east-1",
    "kinesis.stream" = "production-orders-stream",
    "kinesis.default_position" = "TRIM_HORIZON",
    "aws.credentials.provider" = "instance_profile"  -- 使用 EC2 实例角色
);
```

#### 跨账户访问（Assume Role）
```sql
CREATE ROUTINE LOAD kinesis_orders_load ON kinesis_orders
PROPERTIES (
    "format" = "csv",
    "column_separator" = ","
)
FROM KINESIS (
    "kinesis.region" = "us-west-2",
    "kinesis.stream" = "external-orders-stream",
    "kinesis.default_position" = "LATEST",
    "aws.iam.role.arn" = "arn:aws:iam::123456789012:role/DorisKinesisRole",
    "aws.external.id" = "unique-external-id-12345",
    "aws.credentials.provider" = "instance_profile"
);
```

#### 指定 Shard 位置
```sql
CREATE ROUTINE LOAD kinesis_orders_load ON kinesis_orders
PROPERTIES (
    "format" = "json"
)
FROM KINESIS (
    "kinesis.region" = "ap-southeast-1",
    "kinesis.stream" = "orders-stream",
    -- 从特定 shard 的特定位置消费
    "kinesis.shards" = "shardId-000000000000,shardId-000000000001",
    "kinesis.positions" = "49590338271490256608559692538361571095921575989136588802,LATEST"
);
```

#### LocalStack 测试（本地开发）
```sql
CREATE ROUTINE LOAD kinesis_test_load ON kinesis_test
PROPERTIES (
    "format" = "json"
)
FROM KINESIS (
    "kinesis.region" = "us-east-1",
    "kinesis.stream" = "test-stream",
    "kinesis.endpoint" = "http://localhost:4566",  -- LocalStack 端点
    "kinesis.default_position" = "TRIM_HORIZON",
    "aws.access.key.id" = "test",
    "aws.secret.access.key" = "test"
);
```

### 监控进度

```sql
-- 检查作业状态
SHOW ROUTINE LOAD FOR kinesis_orders_load;

-- 检查特定作业详情
SHOW ROUTINE LOAD FOR kinesis_orders_load WHERE Name = 'kinesis_orders_load'\G

-- 暂停作业
PAUSE ROUTINE LOAD FOR kinesis_orders_load;

-- 恢复作业
RESUME ROUTINE LOAD FOR kinesis_orders_load;

-- 停止作业
STOP ROUTINE LOAD FOR kinesis_orders_load;
```

---

## 🔧 配置参考

### 必需属性

| 属性 | 描述 | 示例 |
|----------|-------------|---------|
| `kinesis.region` | 流所在的 AWS 区域 | `"us-east-1"` |
| `kinesis.stream` | Kinesis 流的名称 | `"my-stream"` |

### 可选属性

| 属性 | 描述 | 默认值 |
|----------|-------------|---------|
| `kinesis.endpoint` | 自定义端点（LocalStack/VPC） | AWS 默认 |
| `kinesis.shards` | 逗号分隔的 shard ID | 所有 shard |
| `kinesis.positions` | 每个 shard 的起始位置 | - |
| `kinesis.default_position` | 默认起始位置 | `LATEST` |
| `aws.access.key.id` | AWS 访问密钥 | - |
| `aws.secret.access.key` | AWS 秘密密钥 | - |
| `aws.session.token` | 临时凭证令牌 | - |
| `aws.iam.role.arn` | 要假设的 IAM 角色 | - |
| `aws.external.id` | 角色假设的外部 ID | - |
| `aws.credentials.provider` | 提供者类型 | `Default` |
| `aws.request.timeout.ms` | 请求超时 | 30000 |
| `aws.connection.timeout.ms` | 连接超时 | 10000 |

### 位置类型

| 值 | 描述 |
|-------|-------------|
| `TRIM_HORIZON` | 从最旧的可用记录开始 |
| `LATEST` | 从最新的记录开始（迭代器创建后） |
| `AT_TIMESTAMP` | 从特定时间戳开始 |
| 序列号 | 从特定序列号开始/之后 |

---

## 📊 性能特征

### 吞吐量

- **GetRecords API 限制:** 每个 shard 每秒 5 次调用
- **数据速率:** 每个 shard 约 2 MB/秒
- **每次调用的记录数:** 最多 10,000（实现使用 1000 以降低延迟）

### 限流处理

实现自动处理 `ProvisionedThroughputExceededException`:
- 限流时 1 秒退避
- 继续处理其他 shard（轮询）
- 在下一次迭代中重试被限流的 shard

### 跟踪的指标

Kinesis 重用现有的 Doris routine load 指标:
- `routine_load_get_msg_count` - GetRecords 调用次数
- `routine_load_get_msg_latency` - GetRecords 调用延迟
- `routine_load_consume_bytes` - 总消费字节数
- `routine_load_consume_rows` - 总消费记录数

---

## 🔍 已知限制和未来增强

### 当前限制

1. **不支持增强扇出（EFO）**
   - 当前实现使用 GetRecords（拉模式）
   - 不使用 SubscribeToShard（具有专用吞吐量的推模式）
   - 限制: 所有消费者共享每个 shard 2 MB/秒

2. **未实现多表加载**
   - Kinesis routine load 尚不支持单流多表模式
   - Kafka 有类似功能但未移植到 Kinesis

3. **自动 Shard 发现**
   - 不自动检测分裂产生的新 shard
   - 需要重启作业以消费新 shard

4. **消费者组协调**
   - 简化实现，没有完整的 DataConsumerGroup 模式
   - 每个任务单个消费者（vs Kafka 的并行消费者）

### 未来增强（路线图）

#### 优先级 1: 增强扇出（EFO）
```cpp
// 添加对 SubscribeToShard API 的支持
Status KinesisDataConsumer::subscribe_to_shard(
    const std::string& shard_id,
    const std::string& consumer_name) {
    // 通过 HTTP/2 流使用专用吞吐量
    // 优势: 每个消费者 2MB/秒（不是每个 shard）
    //      约 70ms 延迟（vs GetRecords 的 200ms）
}
```

**SQL 示例:**
```sql
FROM KINESIS (
    "kinesis.region" = "us-east-1",
    "kinesis.stream" = "high-throughput-stream",
    "kinesis.consumer.name" = "doris-consumer-1",  -- 启用 EFO
    "kinesis.use.enhanced.fanout" = "true"
)
```

#### 优先级 2: KinesisDataConsumerGroup
创建用于并行消费的完整 DataConsumerGroup 实现:
```cpp
class KinesisDataConsumerGroup : public DataConsumerGroup {
    Status assign_shards(std::shared_ptr<StreamLoadContext> ctx);
    Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                    std::shared_ptr<io::KinesisConsumerPipe> kinesis_pipe);
private:
    BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>> _queue;
};
```

#### 优先级 3: 自动 Shard 发现
```cpp
// 定期调用 ListShards 检测新 shard
Status KinesisDataConsumer::discover_new_shards() {
    // 调用 ListShards API
    // 与 _consuming_shard_ids 比较
    // 添加分裂产生的新 shard
}
```

#### 优先级 4: Kinesis Data Firehose 支持
支持托管交付流:
```sql
FROM KINESIS_FIREHOSE (
    "kinesis.region" = "us-east-1",
    "kinesis.delivery.stream" = "my-firehose-stream"
)
```

#### 优先级 5: 多区域复制
同时从多个区域读取:
```sql
FROM KINESIS (
    "kinesis.regions" = "us-east-1,eu-west-1",  -- 多区域
    "kinesis.stream" = "global-stream"
)
```

---

## 🧪 测试

### 单元测试设置（LocalStack）

```bash
# 启动带有 Kinesis 的 LocalStack
docker run -d \
  --name localstack-kinesis \
  -p 4566:4566 \
  localstack/localstack

# 创建测试流
aws --endpoint-url=http://localhost:4566 kinesis create-stream \
    --stream-name test-stream \
    --shard-count 2

# 放入测试记录
for i in {1..100}; do
  aws --endpoint-url=http://localhost:4566 kinesis put-record \
    --stream-name test-stream \
    --partition-key key${i} \
    --data "{\"id\":${i},\"value\":\"test${i}\"}"
done

# 验证记录
aws --endpoint-url=http://localhost:4566 kinesis describe-stream \
    --stream-name test-stream
```

### 集成测试

```sql
-- 创建测试表
CREATE TABLE kinesis_test (
    id INT,
    value VARCHAR(100)
) DISTRIBUTED BY HASH(id) BUCKETS 3;

-- 创建 routine load 作业
CREATE ROUTINE LOAD kinesis_test_load ON kinesis_test
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.id\", \"$.value\"]"
)
FROM KINESIS (
    "kinesis.region" = "us-east-1",
    "kinesis.stream" = "test-stream",
    "kinesis.endpoint" = "http://localhost:4566",
    "kinesis.default_position" = "TRIM_HORIZON",
    "aws.access.key.id" = "test",
    "aws.secret.access.key" = "test"
);

-- 等待几秒钟进行消费

-- 验证数据已加载
SELECT COUNT(*) FROM kinesis_test;  -- 应该是 100
SELECT * FROM kinesis_test ORDER BY id LIMIT 10;

-- 检查作业状态
SHOW ROUTINE LOAD FOR kinesis_test_load\G
```

---

## 🐛 故障排查

### 常见问题

#### 1. "Failed to create AWS Kinesis client"（创建 AWS Kinesis 客户端失败）
**原因:** AWS 凭证或区域无效。

**解决方案:**
```sql
-- 验证凭证正确
-- 检查区域有效（例如 us-east-1，不是 us-east-1a）
-- 对于 EC2，确保实例附加了 IAM 角色

-- 首先使用简单凭证测试
FROM KINESIS (
    "kinesis.region" = "us-east-1",
    "kinesis.stream" = "test-stream",
    "aws.access.key.id" = "YOUR_KEY",
    "aws.secret.access.key" = "YOUR_SECRET"
)
```

#### 2. "Provisioned throughput exceeded"（预配置吞吐量超出）
**原因:** GetRecords 调用过多（每个 shard 超过 5次/秒）。

**解决方案:**
- 减少同一流上的并发作业数量
- 增加 Kinesis 中的 shard 数量
- 考虑增强扇出（未来增强）

#### 3. "Failed to get shard iterator"（获取 shard 迭代器失败）
**原因:** 序列号或位置无效。

**解决方案:**
```sql
-- 使用 TRIM_HORIZON 从头开始
"kinesis.default_position" = "TRIM_HORIZON"

-- 或使用 LATEST 从最新开始
"kinesis.default_position" = "LATEST"

-- 检查序列号有效（无空格，格式正确）
```

#### 4. "Stream not found"（未找到流）
**原因:** 指定区域中不存在该流。

**解决方案:**
```bash
# 验证流存在
aws kinesis describe-stream --stream-name YOUR_STREAM --region us-east-1

# 如果需要创建流
aws kinesis create-stream --stream-name YOUR_STREAM --shard-count 1 --region us-east-1
```

#### 5. "Access Denied"（访问被拒绝）
**原因:** IAM 权限不足。

**所需 IAM 权限:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:ListShards"
      ],
      "Resource": "arn:aws:kinesis:*:*:stream/*"
    }
  ]
}
```

### 调试日志

在 Doris BE 中启用调试日志:
```bash
# 在 be.conf 中
LOG_LEVEL=INFO

# 对于详细的 Kinesis 日志
LOG_LEVEL=DEBUG
```

检查 BE 日志:
```bash
tail -f be/log/be.INFO | grep -i kinesis
```

---

## 📁 修改/创建的文件

### 创建的文件（2个）
1. `be/src/io/fs/kinesis_consumer_pipe.h`
   - 用于数据流的 KinesisConsumerPipe 类
2. `KINESIS_IMPLEMENTATION_GUIDE.md`
   - 本实现指南（中文版）

### 修改的文件（7个）
1. `thirdparty/build-thirdparty.sh` - 添加 kinesis SDK
2. `be/cmake/thirdparty.cmake` - 注册库
3. `be/src/runtime/stream_load/stream_load_context.h` - KinesisLoadInfo 类
4. `be/src/runtime/routine_load/data_consumer.h` - 类声明
5. `be/src/runtime/routine_load/data_consumer.cpp` - 完整实现（~500行）
6. `be/src/runtime/routine_load/data_consumer_pool.cpp` - Consumer 创建
7. `be/src/runtime/routine_load/routine_load_task_executor.cpp` - 任务执行集成

---

## 🎓 关键设计决策

### 1. 重用 S3 认证基础设施
**决策:** 使用 `S3ClientFactory::get_aws_credentials_provider()`

**理由:**
- 避免代码重复
- 已支持所有 AWS 认证方法
- 在生产环境中经过实战测试（S3 集成）
- 减少维护负担

**优势:** Kinesis 自动支持:
- 简单 AK/SK 凭证
- IAM 实例配置文件（EC2）
- 带 ARN 的 STS assume role
- 会话令牌
- 环境变量
- 默认凭证链

### 2. GetRecords vs SubscribeToShard
**决策:** 首先实现 GetRecords（拉模式）

**理由:**
- 更简单的实现和调试
- 更好的限流和背压控制
- 与 Kafka 的拉模式一致
- SubscribeToShard 需要 HTTP/2 且成本更高

**权衡:** GetRecords 每个 shard 限制 2MB/秒，而 SubscribeToShard 每个消费者 2MB/秒。

**未来:** SubscribeToShard 可作为优先级 1 增强添加。

### 3. Shard 迭代器管理
**决策:** 从 GetRecords 响应刷新迭代器

**理由:**
- Kinesis 在响应中自动返回下一个迭代器
- 消除后台刷新线程的需求
- 更简单的错误处理
- 迭代器 5 分钟后过期，但我们每次调用都刷新

**拒绝的替代方案:** 每 4 分钟刷新迭代器的后台线程。

### 4. 轮询 Shard 消费
**决策:** 以轮询方式从所有 shard 消费

**理由:**
- 确保跨 shard 的平衡消费
- 防止慢 shard 的队头阻塞
- 更好地利用 Kinesis 吞吐量限制
- 限流的公平分配

**拒绝的替代方案:** 每个 shard 专用线程（资源开销）。

### 5. 简化的消费者组
**决策:** 使用简化集成，不使用完整的 KinesisDataConsumerGroup

**理由:**
- 初始版本更快的实现
- 单个消费者对大多数用例足够
- 降低复杂性和潜在 bug
- 稍后可使用完整的 DataConsumerGroup 模式增强

**未来增强:** 实现 KinesisDataConsumerGroup 以进行并行消费（优先级 2）。

---

## 📈 与 Kafka 实现对比

### 相似之处
- 都继承自 DataConsumer 基类
- 类似的生命周期: init → assign → consume → cancel → reset
- 用于记录缓冲的 BlockingQueue
- 消费者池化以提高效率
- 使用 DorisMetrics 的指标跟踪
- 进度跟踪（offset vs 序列号）

### 差异

| 方面 | Kafka | Kinesis |
|--------|-------|---------|
| **SDK** | librdkafka C++ | AWS SDK for C++ |
| **分区类型** | `int32_t` | `std::string` |
| **位置类型** | `int64_t` | `std::string` |
| **迭代器** | 无（直接 offset） | 必需（5分钟过期） |
| **限流** | 无（客户端缓冲） | 每个 shard 5 GetRecords/秒 |
| **消费者组** | 完整的 KafkaDataConsumerGroup | 简化（单个消费者） |
| **Offset 提交** | 显式提交到 Kafka | 隐式（在 ctx 中跟踪） |
| **Shard 发现** | 自动（通过元数据） | 手动（通过 ListShards） |
| **认证** | Broker 凭证 | AWS IAM（多种方法） |

### 代码大小对比
- **KafkaDataConsumer:** ~400 行
- **KinesisDataConsumer:** ~500 行（+25% 用于 AWS SDK 处理）

---

## 🔐 安全考虑

### 凭证存储
- **最佳实践:** 使用 IAM 角色而不是硬编码凭证
- 避免在 SQL 语句中存储凭证（记录在 FE 中）
- 尽可能使用环境变量或实例配置文件

### 网络安全
- 在生产环境中使用 Kinesis 的 VPC 端点
- 启用 TLS/HTTPS（实现中默认启用）
- 考虑使用 `kinesis.endpoint` 作为私有端点

### IAM 权限
- 遵循最小权限原则
- 仅授予必要的权限:
  - `kinesis:GetRecords`
  - `kinesis:GetShardIterator`
  - `kinesis:DescribeStream`
  - `kinesis:ListShards`
- 使用资源级权限限制对特定流的访问

---

## 📖 参考资料

### 实现文件
- 计划: `/mnt/disk2/huangruixin/.claude/plans/sunny-pondering-crystal.md`
- 本指南: `/mnt/disk2/huangruixin/apache/doris/KINESIS_IMPLEMENTATION_GUIDE.md`

### Kafka 参考实现
- `be/src/runtime/routine_load/data_consumer.cpp` (lines 51-557)
- `be/src/io/fs/kafka_consumer_pipe.h`

### AWS SDK 集成
- S3 认证: `be/src/util/s3_util.cpp` (lines 254-502)
- AWS Common: `common/cpp/aws_common.h`

### 文档
- AWS Kinesis API: https://docs.aws.amazon.com/kinesis/latest/APIReference/
- AWS SDK for C++: https://sdk.amazonaws.com/cpp/api/LATEST/aws-cpp-sdk-kinesis/html/
- Doris Routine Load: https://doris.apache.org/zh-CN/docs/data-operate/import/routine-load-manual

---

## ✅ 验证清单

- [x] AWS Kinesis SDK 已添加到构建系统
- [x] KinesisLoadInfo 类已创建
- [x] KinesisConsumerPipe 已创建
- [x] KinesisDataConsumer 完整实现
- [x] 与数据消费者池集成
- [x] 与 routine load 任务执行器集成
- [x] 进度跟踪（序列号）
- [x] 错误处理和重试逻辑
- [x] 指标跟踪
- [x] 认证支持（所有 AWS 方法）
- [x] 限流处理
- [x] Shard 生命周期管理
- [ ] 增强扇出（EFO）支持（未来）
- [ ] 多表加载支持（未来）
- [ ] 自动 shard 发现（未来）
- [ ] 完整的 DataConsumerGroup 实现（未来）

---

## 🎉 结论

此实现为 Apache Doris Routine Load 中的 AWS Kinesis 支持提供了生产就绪的基础。核心功能已完成，可以使用真实的 Kinesis 流进行测试。未来的增强可以在这个坚实的基础上构建，以添加高级功能，如增强扇出、多表加载和自动 shard 发现。

**总实现量:**
- **代码行数:** 约 700 行（不包括 include 和注释）
- **修改的文件:** 7 个文件
- **创建的文件:** 2 个文件（包括本指南）
- **实现时间:** 约 1 天（按计划）

**状态:** ✅ **核心实现完成 - 可以测试**

---

**文档版本:** 1.0
**最后更新:** 2026-02-10
**作者:** Claude Sonnet 4.5 (Anthropic)
**项目:** Apache Doris - AWS Kinesis 支持
