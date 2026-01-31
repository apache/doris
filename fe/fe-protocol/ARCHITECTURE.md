# Doris Protocol Architecture

## Overview

This document describes Apache Doris multi-protocol support. The architecture uses SPI
(Service Provider Interface) to decouple protocol implementations from the kernel.

## Core Design

### SPI Loading Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           QeService (fe-core)                            │
│                                                                          │
│  1. Build ProtocolConfig (from Config + FrontendOptions)                 │
│  2. ProtocolLoader.loadConfiguredProtocols(config)                       │
│  3. Set protocol acceptor callbacks                                      │
│  4. handler.start() to launch protocol servers                           │
└────────────────────────────────────────────────────────────┬─────────────┘
                                                             │
                              ServiceLoader discovery         │
                                                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              META-INF/services/org.apache.doris.protocol.ProtocolHandler │
│                                                                          │
│  org.apache.doris.mysql.MysqlProtocolHandler                             │
│  org.apache.doris.protocol.arrowflight.ArrowFlightProtocolHandler        │
└────────────────────────────────────────────────────────────┬─────────────┘
                                                             │
                              instantiate + initialize       │
                                                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     MysqlProtocolHandler (fe-protocol-mysql)             │
│                                                                          │
│  - initialize(config): read port, ioThreads, backlog, etc                │
│  - setAcceptor(callback): receive fe-core connection handling callback   │
│  - start(): create XNIO server, start listening                           │
│  - on new connection: acceptor.accept(connection)                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### Configuration Flow

`fe-core` builds a `ProtocolConfig` and passes it into protocol handlers:

```java
// QeService.java
private ProtocolConfig buildProtocolConfig() {
    ProtocolConfig config = new ProtocolConfig(mysqlPort, arrowFlightPort, scheduler);

    // From Config
    config.set(KEY_MYSQL_IO_THREADS, Config.mysql_service_io_threads_num);
    config.set(KEY_MYSQL_BACKLOG, Config.mysql_nio_backlog_num);
    config.set(KEY_MYSQL_KEEP_ALIVE, Config.mysql_nio_enable_keep_alive);

    // From FrontendOptions
    config.set(KEY_MYSQL_BIND_IPV6, FrontendOptions.isBindIPV6());

    // External thread pool
    config.set(KEY_MYSQL_TASK_EXECUTOR, ThreadPoolManager.newDaemon...());

    return config;
}
```

### Configuration Key Mapping

| fe-core parameter | ProtocolConfig key | Notes |
|------------------|--------------------|-------|
| `Config.mysql_service_io_threads_num` | `mysql.io.threads` | IO threads (default 4) |
| `Config.mysql_nio_backlog_num` | `mysql.backlog` | backlog size (default 1024) |
| `Config.mysql_nio_enable_keep_alive` | `mysql.keep.alive` | TCP keep-alive |
| `FrontendOptions.isBindIPV6()` | `mysql.bind.ipv6` | bind IPv6 |
| `Config.max_mysql_service_task_threads_num` | `mysql.max.task.threads` | max task threads |
| `ThreadPoolManager` executor | `mysql.task.executor` | external thread pool |

## Design Principles

1. **Protocol modules are independent**
   - Each protocol (MySQL, Arrow Flight) is a standalone Java module.
   - Modules do not depend on each other.

2. **Kernel is decoupled from protocols**
   - Kernel uses abstract SPI interfaces only.
   - Protocol modules implement SPI and provide services.

3. **Shared API can evolve**
   - SPI and shared API can expand for new protocols/features.

4. **Protocol compatibility must not break**
   - MySQL protocol packet format, crypto, and handshake must remain compatible.

5. **SPI extension mechanism**
   - New protocols are discovered via SPI; kernel does not hardcode implementations.

## Migration Status

### Classes already moved to protocol modules

| Protocol module class | Original fe-core class | Notes |
|-----------------------|------------------------|-------|
| `o.a.d.protocol.mysql.MysqlCapability` | `o.a.d.mysql.MysqlCapability` | capability flags |
| `o.a.d.protocol.mysql.MysqlCommand` | `o.a.d.mysql.MysqlCommand` | command enum |
| `o.a.d.protocol.mysql.MysqlServerStatusFlag` | `o.a.d.mysql.MysqlServerStatusFlag` | server status |
| `o.a.d.protocol.mysql.MysqlColType` | `o.a.d.catalog.MysqlColType` | MySQL type codes |
| `o.a.d.protocol.mysql.MysqlPacket` | `o.a.d.mysql.MysqlPacket` | base packet |
| `o.a.d.protocol.mysql.MysqlHandshakePacket` | `o.a.d.mysql.MysqlHandshakePacket` | handshake |
| `o.a.d.protocol.mysql.MysqlAuthPacket` | `o.a.d.mysql.MysqlAuthPacket` | auth packet |
| `o.a.d.protocol.mysql.MysqlAuthSwitchPacket` | `o.a.d.mysql.MysqlAuthSwitchPacket` | auth switch |
| `o.a.d.protocol.mysql.MysqlOkPacket` | `o.a.d.mysql.MysqlOkPacket` | OK packet |
| `o.a.d.protocol.mysql.MysqlErrPacket` | `o.a.d.mysql.MysqlErrPacket` | error packet |
| `o.a.d.protocol.mysql.MysqlEofPacket` | `o.a.d.mysql.MysqlEofPacket` | EOF packet |
| `o.a.d.protocol.mysql.MysqlClearTextPacket` | `o.a.d.mysql.MysqlClearTextPacket` | clear text |
| `o.a.d.protocol.mysql.MysqlSslPacket` | `o.a.d.mysql.MysqlSslPacket` | SSL request |
| `o.a.d.protocol.mysql.MysqlColDef` | `o.a.d.mysql.MysqlColDef` | column definition |
| `o.a.d.protocol.mysql.FieldInfo` | `o.a.d.mysql.FieldInfo` | field metadata |
| `o.a.d.protocol.mysql.MysqlSerializer` | `o.a.d.mysql.MysqlSerializer` | serializer |
| `o.a.d.protocol.mysql.MysqlProto` | `o.a.d.mysql.MysqlProto` | protocol utils |
| `o.a.d.protocol.mysql.MysqlPassword` | `o.a.d.mysql.MysqlPassword` | password crypto |
| `o.a.d.protocol.mysql.BytesChannel` | `o.a.d.mysql.BytesChannel` | channel interface |
| `o.a.d.protocol.mysql.SslEngineHelper` | `o.a.d.mysql.SslEngineHelper` | SSL utilities |

### Classes still in fe-core (kernel dependencies)

These remain in fe-core due to heavy coupling with kernel classes (Config,
ConnectContext, QueryState, Auth, etc):

| Class | Dependencies | Reason |
|------|--------------|--------|
| `MysqlChannel` | ConnectContext | connection context needed |
| `MysqlSslContext` | Config | SSL configuration |
| `MysqlProto.negotiate()` | ConnectContext, Env, Auth | auth/handshake logic |
| `MysqlSerializer.writeField()` | Type | result serialization with kernel types |
| `ReadListener` | ConnectContext, ConnectProcessor | query pipeline |
| `ProxyProtocolHandler` | kernel classes | proxy protocol support |
| `authenticate/` | privileges | auth logic |
| `privilege/` | privileges | permission checks |

### Why full decoupling is still hard

1. **Connection lifecycle**
   - `ConnectContext` owns session state, variables, transaction state.
2. **Auth and privilege checks**
   - MySQL handshake calls `Env.getAuth()` directly.
3. **Query execution**
   - `ConnectProcessor` drives parsing, planning, execution.
4. **Result serialization**
   - `MysqlSerializer.writeField()` maps kernel `Type` to MySQL types.

### Current Decoupling Strategy

```
┌─────────────────────────────────────────────────────────────────────────┐
│  fe-protocol-mysql (decoupled)                                           │
│                                                                          │
│  OK  Network layer: XNIO server, acceptor                                │
│  OK  Protocol definitions: packets, fields, command codes                │
│  OK  Base serialization and crypto                                       │
│  OK  Handshake/Auth packets, OK/Err/EOF packets                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↑
                           acceptor callback
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│  fe-core (still coupled)                                                 │
│                                                                          │
│  WARN Connection handling: MysqlChannel, ReadListener                    │
│  WARN Protocol negotiation: MysqlProto.negotiate()                       │
│  WARN Query processing: ConnectProcessor                                 │
│  WARN Result serialization: MysqlSerializer.writeField(Type)             │
│  WARN Auth/privilege: authenticate/, privilege/                           │
└─────────────────────────────────────────────────────────────────────────┘
```

## Code-Level Coupling Observations (current code)

- **ProtocolConfig is an object bag**: it carries raw kernel objects like
  `connect.scheduler`, `arrowflight.producer`, and `arrowflight.authenticator`.
  This bypasses type-safe SPI boundaries and ties protocol modules to kernel
  runtime instances.
- **Thread pool ownership remains in fe-core**: `QeService` creates the MySQL
  task executor via `ThreadPoolManager` and injects it into `ProtocolConfig`;
  `MysqlProtocolHandler` depends on that external executor.
- **MySQL config leakage**: `MysqlAuthPacket` reads `Config.proxy_auth_magic_prefix`
  directly, and `MysqlProtocolHandler` uses an ad-hoc key
  `mysql.proxy.protocol.enabled` plus a placeholder proxy-protocol hook. Actual
  proxy-protocol handling still lives in fe-core.
- **Arrow Flight acceptor is not wired**: `ArrowFlightProtocolHandler` stores an
  acceptor but never invokes it; it starts `DorisFlightSqlService` directly, so
  the kernel’s acceptor-based lifecycle is bypassed.
- **Context mismatch for Arrow Flight**: the protocol module provides
  `FlightSqlContext`, but `QeService` expects `ConnectContext` for Arrow Flight
  connections, so there is no unified context bridge today.

## Module Layout

```
fe/
├── fe-common/                          # shared utilities
├── fe-protocol/                        # protocol parent
│   ├── fe-protocol-api/                # SPI interfaces
│   │   └── org.apache.doris.protocol/
│   │       ├── ProtocolHandler.java
│   │       ├── ProtocolConfig.java
│   │       ├── ProtocolContext.java
│   │       ├── ProtocolLoader.java
│   │       ├── ProtocolException.java
│   │       └── AuthenticationResult.java
│   │
│   ├── fe-protocol-mysql/              # MySQL protocol implementation
│   │   └── org.apache.doris.mysql/
│   │       ├── MysqlProtocolHandler.java
│   │       ├── MysqlProto.java
│   │       ├── MysqlCommand.java
│   │       ├── MysqlAuthPacket.java
│   │       └── ... (packets, serializer, crypto, helpers)
│   │
│   └── fe-protocol-arrowflight/        # Arrow Flight SQL implementation
│       └── org.apache.doris.protocol.arrowflight/
│           ├── ArrowFlightProtocolHandler.java
│           └── FlightSqlContext.java
│
└── fe-core/                            # kernel
    └── org.apache.doris.qe/
        └── QeService.java              # loads protocols via SPI
```

## Module Dependencies

```
                    ┌─────────────────────────────────────┐
                    │           fe-common                  │
                    │       (shared utilities)             │
                    └─────────────────────────────────────┘
                                    ▲
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
            │                       │                       │
┌───────────┴───────────┐           │           ┌───────────┴───────────┐
│   fe-protocol-api     │           │           │   fe-protocol-api     │
│       (SPI)           │           │           │       (SPI)           │
└───────────────────────┘           │           └───────────────────────┘
            ▲                       │                       ▲
            │                       │                       │
┌───────────┴───────────┐   ┌────────┴────────┐   ┌───────────┴───────────┐
│  fe-protocol-mysql    │   │    fe-core      │   │ fe-protocol-arrowflight│
│   (MySQL impl)        │◄──│   (kernel)      │──►│  (Arrow Flight impl)   │
└───────────────────────┘   └─────────────────┘   └───────────────────────┘
```

## SPI Interfaces

### ProtocolHandler

```java
public interface ProtocolHandler {
    String getProtocolName();
    default String getProtocolVersion() { return "1.0"; }
    void initialize(ProtocolConfig config) throws ProtocolException;
    void setAcceptor(Consumer<Object> acceptor);
    boolean start();
    void stop();
    boolean isRunning();
    int getPort();
    default boolean isEnabled(ProtocolConfig config) { return getPort() > 0; }
    default int getPriority() { return 0; }
}
```

### ProtocolContext

```java
public interface ProtocolContext {
    String getProtocolName();
    long getConnectionId();
    String getRemoteIP();
    String getRemoteHostPortString();
    String getUser();
    String getDatabase();
    void setDatabase(String database);
    boolean isAuthenticated();
    boolean isKilled();
    void setKilled();
    boolean isSslEnabled();
    void cleanup();
    long getStartTime();
    void setStartTime(long startTime);
    <T> T getChannel();
}
```

## Kernel Usage Example

QeService loads protocol handlers via SPI and registers acceptors:

```java
public class QeService {
    public QeService(int mysqlPort, int arrowFlightPort, ConnectScheduler scheduler) {
        ProtocolConfig config = buildProtocolConfig();
        List<ProtocolHandler> handlers = ProtocolLoader.loadConfiguredProtocols(config);

        for (ProtocolHandler handler : handlers) {
            if ("mysql".equalsIgnoreCase(handler.getProtocolName())) {
                handler.setAcceptor(this::handleMysqlConnection);
            } else if ("arrowflight".equalsIgnoreCase(handler.getProtocolName())) {
                handler.setAcceptor(this::handleArrowFlightConnection);
            }
            protocolHandlers.add(handler);
        }
    }
}
```

## Add a New Protocol

1. Create a new module:

```bash
mkdir -p fe/fe-protocol/fe-protocol-newprotocol/src/main/java/org/apache/doris/protocol/newprotocol
mkdir -p fe/fe-protocol/fe-protocol-newprotocol/src/main/resources/META-INF/services
```

2. Implement `ProtocolHandler`:

```java
package org.apache.doris.protocol.newprotocol;

public class NewProtocolHandler implements ProtocolHandler {
    @Override
    public String getProtocolName() { return "newprotocol"; }
    @Override
    public void initialize(ProtocolConfig config) throws ProtocolException { }
    @Override
    public boolean start() { return true; }
}
```

3. Register SPI service:

`META-INF/services/org.apache.doris.protocol.ProtocolHandler`

```
org.apache.doris.protocol.newprotocol.NewProtocolHandler
```

4. Add module pom and dependency on `fe-protocol-api`.

5. Add dependency in `fe-core` to load the new protocol.

## Arrow Flight: Current Coupling

Arrow Flight SQL is still tightly coupled with `fe-core`:

- QeService constructs Arrow Flight objects (`FlightTokenManagerImpl`,
  `DorisFlightSqlProducer`, `FlightBearerTokenAuthenticator`) and injects them
  into `ProtocolConfig`, which mixes protocol-specific runtime objects into the
  kernel config path.
- `DorisFlightSqlService` is still launched by QeService when SPI handler is not
  present (legacy fallback).
- `FlightSqlConnectProcessor` and `FlightSqlConnectContext` still depend on
  `ConnectContext`, `ConnectScheduler`, `Auth`, and execution pipeline classes.
- `ConnectScheduler` owns `FlightSqlConnectPoolMgr`, used by session/token logic
  and by connection limit enforcement.

## Backward Compatibility

### MySQL Protocol Compatibility

1. Packet formats unchanged
2. Crypto/SSL unchanged
3. Handshake flow unchanged
4. Command coverage unchanged

### Migration Strategy

1. Keep existing `org.apache.doris.mysql.*` package as-is
2. Protocol modules can delegate to legacy implementation
3. Kernel calls protocols via SPI
4. Gradual migration without breaking clients

## Configuration Parameters

| Parameter | Description | Default |
|----------|-------------|---------|
| `query_port` | MySQL protocol port | 9030 |
| `arrow_flight_sql_port` | Arrow Flight SQL port | 9090 |
| `enable_ssl` | enable SSL | false |
| `max_connection_scheduler_threads_num` | max connections | 4096 |

## References

- MySQL Protocol Documentation
- Arrow Flight SQL Specification
- Java ServiceLoader

## TODO / Next Steps

The current protocol split is incomplete. Coupling remains high in user/session
management, configuration wiring, and connection pool management. The following
items are the prioritized next steps:

1. **Separate user/session management from protocol handlers**
   - Introduce a kernel-facing `AuthenticationService` / `SessionService` SPI so
     protocol modules do not call `Env.getAuth()` or access user limits directly.
   - Move user identity, session variables, and per-user limits into a protocol-
     neutral service. Protocols should only pass credentials and connection info.

2. **Decouple configuration and parameter wiring**
   - Replace direct `Config`/`FrontendOptions` reads inside `QeService` with a
     dedicated `ProtocolConfigFactory` that builds protocol-scoped configs.
   - Avoid injecting protocol-specific runtime objects (token managers, producers,
     executors) into `ProtocolConfig`. Instead, let protocol modules create and
     own these objects behind SPI boundaries, or supply them via dedicated SPI
     providers.

3. **Extract connection pool management**
   - Move `ConnectPoolMgr` and `FlightSqlConnectPoolMgr` behind a unified
     `ConnectionPoolService` in `fe-core`.
   - Protocol handlers should register/unregister connections through the SPI
     service rather than reaching into `ConnectScheduler` directly.

4. **Define connection lifecycle SPI**
   - Standardize `onConnect`, `onAuthenticate`, `onQuery`, `onClose` hooks so
     MySQL and Arrow Flight share a consistent lifecycle, and kernel code owns
     the execution pipeline.

5. **Finish Arrow Flight migration**
   - Move `DorisFlightSqlService` startup and token/session management into
     `fe-protocol-arrowflight`.
   - Remove the legacy fallback path from `QeService` after migration.

6. **Reduce kernel type leakage**
   - Introduce a protocol-neutral type mapping layer to reduce direct dependency
     on `Type` in serializers.

7. **Add tests for SPI wiring**
   - Validate handler discovery, config mapping, and connection lifecycle for
     each protocol module.
