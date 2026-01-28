# Doris 数据库协议架构

## 概述

本文档描述了 Apache Doris 的多协议支持架构，该架构通过 SPI (Service Provider Interface) 机制实现了内核与协议的解耦。

## 设计原则

### 1. 协议模块独立
每种协议（如 MySQL、Arrow Flight）是一个独立的 Java module：
- 协议模块负责协议的包结构、加解密、网络交互等实现
- 模块之间没有直接依赖

### 2. 内核解耦协议
- 数据库内核执行查询和登录时，不直接依赖具体协议实现
- 内核只依赖抽象的协议 API 接口（SPI）
- 协议模块通过实现 SPI 提供服务

### 3. 公共 API 可调整
- 公共 API 可以根据新协议或新功能进行调整和扩展
- 内核和协议模块都可以调用公共 API

### 4. 协议实现不可破坏
- MySQL 协议模块的现有协议定义、包结构、加解密逻辑不可修改或破坏
- 遵循向后兼容原则，保证原有客户端仍可正常访问

### 5. SPI 扩展机制
- 新增协议时，通过 SPI 方式加载协议实现
- 内核无需感知具体协议
- SPI 提供协议初始化、登录认证、查询转发等接口

## 迁移策略

### 已迁移到协议模块的类

| 协议模块 (新位置) | fe-core (原位置) | 说明 |
|------------------|-----------------|------|
| `o.a.d.protocol.mysql.MysqlCapability` | `o.a.d.mysql.MysqlCapability` | 协议能力标志 |
| `o.a.d.protocol.mysql.MysqlCommand` | `o.a.d.mysql.MysqlCommand` | 协议命令 |
| `o.a.d.protocol.mysql.MysqlServerStatusFlag` | `o.a.d.mysql.MysqlServerStatusFlag` | 服务器状态标志 |
| `o.a.d.protocol.mysql.MysqlColType` | `o.a.d.catalog.MysqlColType` | MySQL 类型码 |
| `o.a.d.protocol.mysql.MysqlPacket` | `o.a.d.mysql.MysqlPacket` | 包基类 |
| `o.a.d.protocol.mysql.MysqlHandshakePacket` | `o.a.d.mysql.MysqlHandshakePacket` | 握手包 |
| `o.a.d.protocol.mysql.MysqlAuthPacket` | `o.a.d.mysql.MysqlAuthPacket` | 认证包 |
| `o.a.d.protocol.mysql.MysqlAuthSwitchPacket` | `o.a.d.mysql.MysqlAuthSwitchPacket` | 认证切换包 |
| `o.a.d.protocol.mysql.MysqlOkPacket` | `o.a.d.mysql.MysqlOkPacket` | OK 响应包 |
| `o.a.d.protocol.mysql.MysqlErrPacket` | `o.a.d.mysql.MysqlErrPacket` | 错误响应包 |
| `o.a.d.protocol.mysql.MysqlEofPacket` | `o.a.d.mysql.MysqlEofPacket` | EOF 包 |
| `o.a.d.protocol.mysql.MysqlClearTextPacket` | `o.a.d.mysql.MysqlClearTextPacket` | 明文密码包 |
| `o.a.d.protocol.mysql.MysqlSslPacket` | `o.a.d.mysql.MysqlSslPacket` | SSL 请求包 |
| `o.a.d.protocol.mysql.MysqlColDef` | `o.a.d.mysql.MysqlColDef` | 列定义 |
| `o.a.d.protocol.mysql.FieldInfo` | `o.a.d.mysql.FieldInfo` | 字段元数据 |
| `o.a.d.protocol.mysql.MysqlSerializer` | `o.a.d.mysql.MysqlSerializer` | 序列化器 |
| `o.a.d.protocol.mysql.MysqlProto` | `o.a.d.mysql.MysqlProto` | 协议读取工具 |
| `o.a.d.protocol.mysql.MysqlPassword` | `o.a.d.mysql.MysqlPassword` | 密码处理 |
| `o.a.d.protocol.mysql.BytesChannel` | `o.a.d.mysql.BytesChannel` | 字节通道接口 |
| `o.a.d.protocol.mysql.SslEngineHelper` | `o.a.d.mysql.SslEngineHelper` | SSL 工具类 |

### 保留在 fe-core 的类（有内核依赖）

以下类因为有内核依赖（如 Config, ConnectContext, QueryState 等），保留在 fe-core 中：

- `MysqlChannel` - 依赖 ConnectContext
- `MysqlSslContext` - 依赖 Config
- `MysqlProto.negotiate()` 等方法 - 依赖 ConnectContext, Env, Auth
- `MysqlSerializer.writeField(FieldInfo, Type)` - 依赖内核 Type 类型
- `ReadListener` - 依赖 ConnectContext, ConnectProcessor
- `ProxyProtocolHandler`, `ProxyMysqlChannel`, `DummyMysqlChannel` - 依赖内核类
- `authenticate/` 子包 - 认证相关，依赖内核类
- `privilege/` 子包 - 权限相关，依赖内核类

### 向后兼容性

fe-core 中的原有类保持不变，可以：
1. 继续使用原有的 `org.apache.doris.mysql.*` 包路径
2. 原有类可以选择性地继承或委托到协议模块的类
3. 新代码建议使用 `org.apache.doris.protocol.mysql.*` 包路径

## 模块结构

```
fe/
├── fe-common/                          # 公共工具模块
├── fe-protocol/                        # 协议模块父项目
│   ├── fe-protocol-api/               # 协议 SPI 接口定义
│   │   └── org.apache.doris.protocol/
│   │       ├── ProtocolHandler.java   # 协议处理器接口
│   │       ├── ProtocolConfig.java    # 协议配置
│   │       ├── ProtocolContext.java   # 连接上下文接口
│   │       ├── ProtocolLoader.java    # SPI 加载器
│   │       ├── ProtocolException.java # 协议异常
│   │       └── AuthenticationResult.java
│   │
│   ├── fe-protocol-mysql/             # MySQL 协议实现
│   │   └── org.apache.doris.protocol.mysql/
│   │       ├── MysqlProtocolHandler.java  # 实现 ProtocolHandler
│   │       ├── MysqlProto.java            # 兼容层
│   │       ├── command/
│   │       │   └── MysqlCommand.java
│   │       └── channel/
│   │           └── MysqlChannel.java
│   │
│   └── fe-protocol-arrowflight/       # Arrow Flight SQL 协议实现
│       └── org.apache.doris.protocol.arrowflight/
│           ├── ArrowFlightProtocolHandler.java
│           └── FlightSqlContext.java
│
└── fe-core/                           # 数据库内核
    └── org.apache.doris.qe/
        └── QeService.java             # 通过 SPI 加载协议
```

## 模块依赖关系

```
                    ┌─────────────────────────────────────┐
                    │           fe-common                  │
                    │       (公共工具、基础类)              │
                    └─────────────────────────────────────┘
                                    ▲
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
            │                       │                       │
┌───────────┴───────────┐           │           ┌───────────┴───────────┐
│   fe-protocol-api     │           │           │   fe-protocol-api     │
│     (SPI 接口)        │           │           │     (SPI 接口)        │
└───────────────────────┘           │           └───────────────────────┘
            ▲                       │                       ▲
            │                       │                       │
┌───────────┴───────────┐   ┌──────┴───────┐   ┌───────────┴───────────┐
│  fe-protocol-mysql    │   │   fe-core    │   │fe-protocol-arrowflight│
│   (MySQL 协议实现)    │◄──│    (内核)    │──►│  (Arrow Flight 实现)  │
└───────────────────────┘   └──────────────┘   └───────────────────────┘
```

## SPI 接口定义

### ProtocolHandler 接口

```java
public interface ProtocolHandler {
    // 协议名称
    String getProtocolName();
    
    // 协议版本
    String getProtocolVersion();
    
    // 初始化
    void initialize(ProtocolConfig config) throws ProtocolException;
    
    // 设置连接接受器
    void setAcceptor(Consumer<Object> acceptor);
    
    // 启动
    boolean start();
    
    // 停止
    void stop();
    
    // 运行状态
    boolean isRunning();
    
    // 监听端口
    int getPort();
    
    // 是否启用
    boolean isEnabled(ProtocolConfig config);
    
    // 优先级
    int getPriority();
}
```

### ProtocolContext 接口

```java
public interface ProtocolContext {
    String getProtocolName();
    long getConnectionId();
    String getRemoteIP();
    String getUser();
    String getDatabase();
    void setDatabase(String database);
    boolean isAuthenticated();
    boolean isKilled();
    void setKilled();
    void cleanup();
    <T> T getChannel();
}
```

## 内核调用示例

### QeService 中的协议加载

```java
public class QeService {
    private final List<ProtocolHandler> protocolHandlers;
    
    public QeService(int mysqlPort, int arrowFlightPort, ConnectScheduler scheduler) {
        // 通过 SPI 加载协议
        ProtocolConfig config = new ProtocolConfig(mysqlPort, arrowFlightPort, scheduler);
        List<ProtocolHandler> spiHandlers = ProtocolLoader.loadConfiguredProtocols(config);
        
        // 为 MySQL 协议设置连接接受器
        for (ProtocolHandler handler : spiHandlers) {
            if ("mysql".equals(handler.getProtocolName())) {
                handler.setAcceptor(this::handleMysqlConnection);
            }
            protocolHandlers.add(handler);
        }
    }
    
    public void start() throws Exception {
        for (ProtocolHandler handler : protocolHandlers) {
            LOG.info("Starting protocol: {}", handler.getProtocolName());
            if (!handler.start()) {
                LOG.error("Failed to start protocol: {}", handler.getProtocolName());
                System.exit(-1);
            }
        }
    }
    
    private void handleMysqlConnection(Object connection) {
        // 处理新的 MySQL 连接
        StreamConnection streamConnection = (StreamConnection) connection;
        ConnectContext context = new ConnectContext(streamConnection);
        // ...
    }
}
```

## 添加新协议步骤

### 1. 创建协议模块

```bash
mkdir -p fe/fe-protocol/fe-protocol-newprotocol/src/main/java/org/apache/doris/protocol/newprotocol
mkdir -p fe/fe-protocol/fe-protocol-newprotocol/src/main/resources/META-INF/services
```

### 2. 实现 ProtocolHandler

```java
package org.apache.doris.protocol.newprotocol;

public class NewProtocolHandler implements ProtocolHandler {
    @Override
    public String getProtocolName() {
        return "newprotocol";
    }
    
    @Override
    public void initialize(ProtocolConfig config) throws ProtocolException {
        // 初始化协议
    }
    
    @Override
    public boolean start() {
        // 启动协议服务
        return true;
    }
    
    // ... 其他方法实现
}
```

### 3. 注册 SPI 服务

创建文件 `META-INF/services/org.apache.doris.protocol.ProtocolHandler`:

```
org.apache.doris.protocol.newprotocol.NewProtocolHandler
```

### 4. 配置 pom.xml

```xml
<project>
    <parent>
        <groupId>org.apache.doris</groupId>
        <artifactId>fe-protocol</artifactId>
    </parent>
    
    <artifactId>fe-protocol-newprotocol</artifactId>
    
    <dependencies>
        <dependency>
            <groupId>org.apache.doris</groupId>
            <artifactId>fe-protocol-api</artifactId>
        </dependency>
    </dependencies>
</project>
```

### 5. 在 fe-core 中添加依赖

```xml
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>fe-protocol-newprotocol</artifactId>
</dependency>
```

## 向后兼容性保证

### MySQL 协议兼容性

1. **包格式不变**: 所有 MySQL 包的格式、字段定义保持不变
2. **加解密不变**: SSL/TLS、密码加密算法保持不变
3. **握手流程不变**: 完整保留原有握手协议
4. **命令支持不变**: 所有 MySQL 命令继续支持

### 迁移策略

1. 现有 MySQL 协议代码保留在 `org.apache.doris.mysql` 包
2. 新的协议模块通过兼容层引用现有实现
3. 内核代码通过 SPI 接口调用协议
4. 渐进式迁移，不影响现有功能

## 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `query_port` | MySQL 协议端口 | 9030 |
| `arrow_flight_sql_port` | Arrow Flight SQL 端口 | 9090 |
| `enable_ssl` | 启用 SSL | false |
| `max_connection_scheduler_threads_num` | 最大连接数 | 4096 |

## 参考文档

- [MySQL Protocol Documentation](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
- [Arrow Flight SQL Specification](https://arrow.apache.org/docs/format/FlightSql.html)
- [Java ServiceLoader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html)
