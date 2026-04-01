# fe-filesystem-broker 详细设计文档

## 1. 问题陈述

### 1.1 当前状态

`BrokerUtil.java`（fe-core）是最后一个使用旧 `RemoteFileSystem` API 的主要调用方。
它有两处 `FileSystemFactory.get()` 调用：

```java
// parseFile — 通过旧 API 列举 broker 路径下的文件
try (RemoteFileSystem fileSystem = FileSystemFactory.get(brokerDesc.getStorageProperties())) {
    Status st = fileSystem.globList(path, rfiles, false);
}

// deleteDirectoryWithFileSystem — 通过旧 API 删除目录
try (RemoteFileSystem fileSystem = FileSystemFactory.get(brokerDesc.getStorageProperties())) {
    Status st = fileSystem.deleteDirectory(path);
}
```

以及 `Repository.java`（Backup Domain）通过 `FileSystemFactory.get()` 重建 broker 连接。

### 1.2 关键约束分析

Broker 文件系统的核心挑战是 **broker 地址解析**：

```
broker name (String) ──► BrokerMgr.getBroker(name, localIP) ──► FsBroker(host, port)
```

`BrokerMgr` 是 `fe-core` 的运行时组件（依赖 `Env`、`FrontendOptions`）。

#### FileSystemProvider SPI 的关键设计

通过研究现有代码，发现 `FileSystemProvider` SPI 使用的是 **`Map<String, String>`** 接口，而非 `StorageProperties`：

```java
// FileSystemProvider.java (fe-filesystem-spi)
public interface FileSystemProvider {
    boolean supports(Map<String, String> properties);
    FileSystem create(Map<String, String> properties) throws IOException;
}

// StoragePropertiesConverter.java (fe-core) 负责转换
public static Map<String, String> toMap(StorageProperties props) {
    // 对 BrokerProperties 注入: "_STORAGE_TYPE_" = "BROKER"
}

// FileSystemPluginManager.java (fe-core) 做调度
public FileSystem createFileSystem(Map<String, String> properties) throws IOException {
    for (FileSystemProvider provider : providers) {
        if (provider.supports(properties)) { return provider.create(properties); }
    }
}
```

**关键洞察**：`BrokerFileSystemProvider` 只需检查 `map.get("_STORAGE_TYPE_").equals("BROKER")`，
完全不需要导入 `BrokerProperties`（fe-core 类），因此**无循环依赖**！

#### 解耦方案

fe-core 在调用 SPI 前完成 broker name → host:port 解析，将解析结果直接注入 map：

```
BrokerUtil (fe-core)
  ├── BrokerMgr.getBroker(name, localIP) → FsBroker(host, port)    [fe-core 内部]
  └── FileSystemFactory.getBrokerFileSystem(host, port, params)
        └── FileSystemPluginManager.createFileSystem({
                "_STORAGE_TYPE_": "BROKER",
                "BROKER_HOST": host,
                "BROKER_PORT": port,
                ...brokerParams
            })
              └── BrokerFileSystemProvider.create(map)  [fe-filesystem-broker, 零 fe-core 依赖]
                    └── new BrokerSpiFileSystem(host, port, params)
```

---

## 2. 设计目标

| 目标 | 说明 |
|------|------|
| **零 fe-core 依赖** | `fe-filesystem-broker` 只依赖 `fe-filesystem-spi`、`fe-thrift`、`libthrift`、`commons-pool2` |
| **完整 spi.FileSystem** | 实现所有 spi.FileSystem 接口方法，支持 list、delete、rename、read、write |
| **统一 SPI 路由** | `BrokerFileSystemProvider` 注册为标准 SPI，通过 `FileSystemPluginManager` 统一调度 |
| **连接池复用** | 模块内维护自己的 Thrift 客户端连接池，不依赖 fe-core 的 `ClientPool` |
| **向后兼容** | 旧 `BrokerFileSystem`（fe-core）保持不变，新实现并行存在直到迁移完成 |

---

## 3. 架构设计

### 3.1 模块层次图

```
┌─────────────────────────────────────────────────────────────┐
│  fe-core                                                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ BrokerUtil / Repository / BackupHandler              │  │
│  │  1. FsBroker broker = BrokerMgr.getBroker(name, ip) │  │
│  │  2. FileSystemFactory.getBrokerFileSystem(           │  │
│  │         broker.host, broker.port, params)            │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ FileSystemFactory.getBrokerFileSystem()              │  │
│  │   builds Map {_STORAGE_TYPE_=BROKER,                 │  │
│  │               BROKER_HOST, BROKER_PORT, ...params}   │  │
│  │   calls FileSystemPluginManager.createFileSystem()   │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
         │ depends on
         ▼
┌─────────────────────────────────────────────────────────────┐
│  fe-filesystem-broker                                       │
│  ┌──────────────────────────────────┐                      │
│  │ BrokerFileSystemProvider         │ ◄── SPI registered   │
│  │   supports: _STORAGE_TYPE_=BROKER│   via ServiceLoader  │
│  │             + BROKER_HOST exists │                      │
│  │   create(map) → BrokerSpiFileSystem                    │
│  ├──────────────────────────────────┤                      │
│  │ BrokerSpiFileSystem              │                      │
│  │   implements spi.FileSystem      │                      │
│  │   - String host                  │                      │
│  │   - int port                     │                      │
│  │   - Map<String,String> params    │                      │
│  │   - BrokerClientPool             │ (self-managed)       │
│  ├──────────────────────────────────┤                      │
│  │ BrokerClientPool                 │                      │
│  │  GenericKeyedObjectPool<         │                      │
│  │    TNetworkAddress,              │                      │
│  │    TPaloBrokerService.Client>    │                      │
│  └──────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────┘
         │ depends on
         ▼
┌─────────────────────────────────────────────────────────────┐
│  fe-filesystem-spi                                          │
│  spi.FileSystem, Location, FileEntry, FileIterator...       │
├─────────────────────────────────────────────────────────────┤
│  fe-thrift                                                  │
│  TPaloBrokerService, TBrokerListPathRequest, ...            │
├─────────────────────────────────────────────────────────────┤
│  libthrift  (org.apache.thrift:libthrift)                   │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 关键设计决策：使用 FileSystemProvider SPI + 预解析地址

与 S3、HDFS 等模块不同，broker 的地址解析发生在 **SPI 调用之前**（在 fe-core 完成），
解析结果通过 map 传递。`BrokerFileSystemProvider` 是一个普通 SPI 实现，通过 `ServiceLoader` 发现。

这样实现了用户期望的**统一 SPI 路由**：所有存储类型（包括 broker）都经过 `FileSystemPluginManager.createFileSystem(map)`，
区别仅在于 broker 的 map 是由 fe-core 手动构建（含预解析地址），而非通过 `StoragePropertiesConverter.toMap()` 转换。

---

## 4. 接口与实现

### 4.1 `BrokerFileSystemProvider`（SPI 注册，在 `fe-filesystem-broker`）

```java
package org.apache.doris.filesystem.broker;

import org.apache.doris.filesystem.spi.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;

/**
 * SPI provider for the Doris Broker filesystem.
 *
 * <p>Recognizes maps that contain "_STORAGE_TYPE_" = "BROKER" plus a resolved
 * "BROKER_HOST" key. The host and port are pre-resolved by fe-core before
 * calling FileSystemPluginManager, avoiding any dependency on BrokerMgr here.
 *
 * <p>Registered via:
 * META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider
 */
public class BrokerFileSystemProvider implements FileSystemProvider {

    private static final String KEY_TYPE    = "_STORAGE_TYPE_";
    private static final String KEY_HOST    = "BROKER_HOST";
    private static final String KEY_PORT    = "BROKER_PORT";

    @Override
    public boolean supports(Map<String, String> properties) {
        return "BROKER".equals(properties.get(KEY_TYPE))
                && properties.containsKey(KEY_HOST);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        String host = properties.get(KEY_HOST);
        int port;
        try {
            port = Integer.parseInt(properties.get(KEY_PORT));
        } catch (NumberFormatException e) {
            throw new IOException("Invalid BROKER_PORT: " + properties.get(KEY_PORT));
        }
        // Remaining keys are broker-specific params (username, password, hadoop config, ...)
        Map<String, String> params = extractBrokerParams(properties);
        return new BrokerSpiFileSystem(host, port, params);
    }

    private Map<String, String> extractBrokerParams(Map<String, String> properties) {
        Map<String, String> params = new java.util.HashMap<>(properties);
        params.remove(KEY_TYPE);
        params.remove(KEY_HOST);
        params.remove(KEY_PORT);
        return Map.copyOf(params);
    }

    @Override
    public String name() { return "Broker"; }
}
```

### 4.2 `BrokerSpiFileSystem`（核心实现）

```java
package org.apache.doris.filesystem.broker;

import org.apache.doris.filesystem.spi.FileSystem;
import org.apache.doris.filesystem.spi.Location;
import org.apache.doris.filesystem.spi.FileEntry;
import org.apache.doris.filesystem.spi.FileIterator;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * spi.FileSystem backed by a Doris Broker Thrift RPC.
 *
 * <p>The broker endpoint (host, port) is resolved before construction by fe-core.
 * This class has zero dependency on fe-core, BrokerMgr, or BrokerProperties.
 */
public class BrokerSpiFileSystem implements FileSystem {

    private final TNetworkAddress endpoint;
    private final Map<String, String> brokerParams;
    private final BrokerClientPool clientPool;

    BrokerSpiFileSystem(String host, int port, Map<String, String> brokerParams) {
        this.endpoint = new TNetworkAddress(host, port);
        this.brokerParams = Map.copyOf(brokerParams);
        this.clientPool = new BrokerClientPool();
    }

    // ... 见 Section 5 完整方法列表
}
```

### 4.3 `BrokerClientPool`（内部连接池）

```java
package org.apache.doris.filesystem.broker;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

/**
 * Thread-safe pool of TPaloBrokerService Thrift clients, keyed by TNetworkAddress.
 * Manages connection lifecycle independently of fe-core's ClientPool.
 */
class BrokerClientPool implements AutoCloseable {
    private final GenericKeyedObjectPool<TNetworkAddress, TPaloBrokerService.Client> pool;

    BrokerClientPool() {
        GenericKeyedObjectPoolConfig<TPaloBrokerService.Client> config =
                new GenericKeyedObjectPoolConfig<>();
        config.setMaxIdlePerKey(16);
        config.setMinIdlePerKey(0);
        config.setMaxTotalPerKey(-1);
        config.setMaxTotal(-1);
        config.setMaxWaitMillis(500);
        this.pool = new GenericKeyedObjectPool<>(new BrokerClientFactory(), config);
    }

    TPaloBrokerService.Client borrow(TNetworkAddress addr) throws Exception {
        return pool.borrowObject(addr);
    }

    void returnGood(TNetworkAddress addr, TPaloBrokerService.Client client) {
        try { pool.returnObject(addr, client); } catch (Exception ignored) { }
    }

    void invalidate(TNetworkAddress addr, TPaloBrokerService.Client client) {
        try { pool.invalidateObject(addr, client); } catch (Exception ignored) { }
    }

    @Override
    public void close() {
        pool.close();
    }
}
```

---

## 5. spi.FileSystem 方法映射

| spi.FileSystem 方法 | Thrift RPC | 备注 |
|----------------------|-----------|------|
| `list(Location)` | `listPath(TBrokerListPathRequest{recursive=false})` | 返回 FileIterator |
| `listFiles(Location)` | `listLocatedFiles(TBrokerListPathRequest{onlyFiles=true})` | 返回 `List<FileEntry>` |
| `listFilesRecursive(Location)` | `listLocatedFiles(TBrokerListPathRequest{recursive=true,onlyFiles=true})` | 深度递归 |
| `listDirectories(Location)` | `listPath(TBrokerListPathRequest{recursive=false})` 过滤 isDir | 返回 `Set<String>` |
| `exists(Location)` | `checkPathExist(TBrokerCheckPathExistRequest)` | |
| `mkdirs(Location)` | — | Broker 不支持 mkdirs，no-op（broker 按需创建父目录）|
| `delete(Location, false)` | `deletePath(TBrokerDeletePathRequest)` | 删除单文件 |
| `delete(Location, true)` | `deletePath(TBrokerDeletePathRequest)` | 递归删除目录 |
| `rename(Location, Location)` | `renamePath(TBrokerRenamePathRequest)` | |
| `newInputFile(Location)` | `openReader` + `pread` + `closeReader` | 返回 BrokerInputFile |
| `newOutputFile(Location)` | `openWriter` + `pwrite` + `closeWriter` | 返回 BrokerOutputFile |

### 5.1 globList（list with glob pattern）

`BrokerUtil.parseFile()` 需要 glob 语义（`path` 可包含 `*`、`?`）。
Broker 的 `listPath` 支持 glob（broker 进程底层调用 Hadoop FileSystem.globStatus）。
因此 `globListWithLimit()` 可通过 `listPath` + 客户端过滤 startAfter 实现：

```java
@Override
public GlobListing globListWithLimit(Location path, String startAfter,
        long maxBytes, long maxFiles) throws IOException {
    // Broker handles glob natively via Hadoop
    List<FileEntry> entries = doListPath(path.uri(), false);  // glob-aware
    // Apply startAfter + size/count limits client-side
    return applyLimits(entries, startAfter, maxBytes, maxFiles);
}
```

### 5.2 mkdirs 处理

Broker 不提供 `mkdirs` RPC。通常 broker 通过 `openWriter` 自动创建父目录。
对于 `BrokerUtil` 中的使用场景（`parseFile`、`deleteDirectory`），不需要 `mkdirs`。
实现为 no-op（返回即可，不抛异常）。

---

## 6. fe-core 集成方式

### 6.1 FileSystemFactory 新增工厂方法

```java
// fe/fe-core/src/main/java/org/apache/doris/fs/FileSystemFactory.java

/**
 * Creates a BrokerSpiFileSystem with a pre-resolved broker endpoint.
 *
 * <p>The caller is responsible for resolving broker name → host:port via BrokerMgr
 * before calling this method. This keeps BrokerMgr coupling in fe-core only.
 *
 * @param host         live broker host (already resolved from BrokerMgr)
 * @param port         live broker Thrift port
 * @param brokerParams broker-specific params (username, password, hadoop config, ...)
 */
public static FileSystem getBrokerFileSystem(String host, int port,
        Map<String, String> brokerParams) throws IOException {
    Map<String, String> props = new HashMap<>(brokerParams);
    props.put("_STORAGE_TYPE_", "BROKER");
    props.put("BROKER_HOST", host);
    props.put("BROKER_PORT", String.valueOf(port));
    return PLUGIN_MANAGER.createFileSystem(props);
}
```

### 6.2 BrokerUtil 迁移后的代码

```java
// BEFORE:
try (RemoteFileSystem fileSystem = FileSystemFactory.get(brokerDesc.getStorageProperties())) {
    Status st = fileSystem.globList(path, rfiles, false);
    if (!st.ok()) { throw new UserException(st.getErrMsg()); }
}

// AFTER:
BrokerProperties brokerProps = (BrokerProperties) brokerDesc.getStorageProperties();
try {
    String localHost = FrontendOptions.getLocalHostAddress();
    FsBroker broker = Env.getCurrentEnv().getBrokerMgr()
            .getBroker(brokerProps.getBrokerName(), localHost);
    try (FileSystem fs = FileSystemFactory.getBrokerFileSystem(
            broker.host, broker.port, brokerProps.getBrokerParams())) {
        List<FileEntry> entries = fs.listFiles(Location.of(path));
        // convert FileEntry → TBrokerFileStatus for callers
        for (FileEntry e : entries) {
            rfiles.add(new TBrokerFileStatus(e.location().uri(),
                    e.isDirectory(), e.length(), e.isDirectory()));
        }
    }
} catch (AnalysisException e) {
    throw new UserException("Failed to get broker: " + e.getMessage(), e);
}
```

---

## 7. Maven 模块结构

### 7.1 目录结构

```
fe/fe-filesystem/fe-filesystem-broker/
├── pom.xml
└── src/
    └── main/
        ├── java/org/apache/doris/filesystem/broker/
        │   ├── BrokerFileSystemProvider.java  (implements FileSystemProvider — SPI entry)
        │   ├── BrokerSpiFileSystem.java       (implements spi.FileSystem)
        │   ├── BrokerClientPool.java          (Thrift client pool)
        │   ├── BrokerClientFactory.java       (PooledObjectFactory for Thrift clients)
        │   ├── BrokerInputFile.java           (implements spi.DorisInputFile)
        │   └── BrokerOutputFile.java          (implements spi.DorisOutputFile)
        └── resources/META-INF/services/
            └── org.apache.doris.filesystem.spi.FileSystemProvider
                  (contains: org.apache.doris.filesystem.broker.BrokerFileSystemProvider)
```

### 7.2 pom.xml（关键依赖）

```xml
<artifactId>fe-filesystem-broker</artifactId>
<packaging>jar</packaging>

<dependencies>
    <!-- SPI interfaces only — no fe-core -->
    <dependency>
        <groupId>${project.groupId}</groupId>
        <artifactId>fe-filesystem-spi</artifactId>
        <version>${project.version}</version>
    </dependency>

    <!-- Thrift generated classes (TPaloBrokerService, TBrokerListPathRequest, ...) -->
    <dependency>
        <groupId>${project.groupId}</groupId>
        <artifactId>fe-thrift</artifactId>
        <version>${project.version}</version>
    </dependency>

    <!-- Thrift runtime -->
    <dependency>
        <groupId>org.apache.thrift</groupId>
        <artifactId>libthrift</artifactId>
    </dependency>

    <!-- Connection pool for Thrift clients -->
    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-pool2</artifactId>
    </dependency>
</dependencies>
```

### 7.3 fe-core pom.xml 追加依赖

```xml
<dependency>
    <groupId>${project.groupId}</groupId>
    <artifactId>fe-filesystem-broker</artifactId>
    <version>${project.version}</version>
</dependency>
```

### 7.4 fe-filesystem/pom.xml 追加子模块

```xml
<modules>
    <!-- ... existing modules ... -->
    <module>fe-filesystem-broker</module>
</modules>
```

---

## 8. 数据流：BrokerUtil.parseFile() 迁移后

```
BrokerUtil.parseFile(path, brokerDesc, statuses)
  │
  ├── (BrokerProperties) brokerDesc.getStorageProperties()
  │
  ├── BrokerMgr.getBroker(name, localIP) ──► FsBroker(host, port)  [fe-core 内部]
  │
  ├── FileSystemFactory.getBrokerFileSystem(host, port, brokerParams)
  │     └── map = {_STORAGE_TYPE_=BROKER, BROKER_HOST=host, BROKER_PORT=port, ...params}
  │     └── FileSystemPluginManager.createFileSystem(map)
  │           └── BrokerFileSystemProvider.supports(map) == true
  │           └── BrokerFileSystemProvider.create(map)
  │                 └── new BrokerSpiFileSystem(host, port, params)
  │
  └── fs.listFiles(Location.of(path))
        │
        ├── BrokerSpiFileSystem.listFiles()
        │     ├── clientPool.borrow(endpoint)
        │     │     └── BrokerClientPool ──► new TPaloBrokerService.Client(transport)
        │     │
        │     ├── client.listLocatedFiles(TBrokerListPathRequest{
        │     │       path, recursive=false, onlyFiles=true, properties=brokerParams})
        │     │     └── Thrift RPC ──► Broker Process
        │     │
        │     └── TBrokerListResponse ──► List<FileEntry>
        │
        └── for each FileEntry: convert to TBrokerFileStatus, add to statuses
```

---

## 9. 异常处理

| Thrift 状态码 | 语义 | spi.FileSystem 行为 |
|--------------|------|---------------------|
| `OK (0)` | 成功 | 正常返回 |
| `FILE_NOT_FOUND (501)` | 路径不存在 | `exists()` 返回 false；`list()` 返回空 |
| `NOT_AUTHORIZED (401)` | 认证失败 | throw `IOException("Broker access denied: ...")` |
| `OPERATION_NOT_SUPPORTED (503)` | 操作不支持 | throw `UnsupportedOperationException` |
| `END_OF_FILE (301)` | 读完 | `newInput().read()` 返回 -1 |
| 其他 / TException | 网络/未知错误 | throw `IOException(opStatus.getMessage())` |

---

## 10. 迁移步骤（TODO）

| 步骤 | 内容 | 文件 |
|------|------|------|
| T1 | 新建 `fe-filesystem/fe-filesystem-broker/` Maven 模块 | `pom.xml` × 3 |
| T2 | 实现 `BrokerFileSystemProvider`（SPI 注册） | `fe-filesystem-broker` |
| T3 | 实现 `BrokerClientFactory` + `BrokerClientPool` | `fe-filesystem-broker` |
| T4 | 实现 `BrokerSpiFileSystem.listFiles()` + `globList()` + `delete()` + `rename()` | `fe-filesystem-broker` |
| T5 | 实现 `BrokerInputFile`（read path）| `fe-filesystem-broker` |
| T6 | 实现 `BrokerOutputFile`（write path）| `fe-filesystem-broker` |
| T7 | 在 `fe-core` 中新增 `FileSystemFactory.getBrokerFileSystem()` 工厂方法 | `FileSystemFactory.java` |
| T8 | 迁移 `BrokerUtil.parseFile()` | `BrokerUtil.java` |
| T9 | 迁移 `BrokerUtil.deleteDirectoryWithFileSystem()` | `BrokerUtil.java` |
| T10 | 迁移 `Repository.java` I/O 路径 | `Repository.java` |
| T11 | 迁移 `BackupHandler.java` repository 创建 | `BackupHandler.java` |
| T12 | 删除旧 `BrokerFileSystem`（fe-core）| `fs/remote/BrokerFileSystem.java` |
| T13 | 构建验证 + 回归测试 | — |

---

## 11. 非目标（Out of Scope）

- 不修改 Thrift 协议（`PaloBrokerService.thrift` 保持不变）
- `BrokerUtil.readFile()` 的 Thrift 直接调用路径（非 FileSystem 接口）不在本设计范围内
- 不处理多 broker 实例的负载均衡（由 BrokerMgr 已实现，host:port 已是最终结果）

---

## 12. 风险与注意事项

| 风险 | 缓解措施 |
|------|---------|
| `fe-thrift` 中的 Thrift 生成类版本变化 | fe-thrift 与 fe-core 同步发布，版本一致 |
| Broker 不支持 mkdirs | BrokerUtil 调用场景不需要 mkdirs，no-op 即可 |
| 连接池配置差异 | 参考 `ClientPool.brokerPool` 配置（maxIdle=128, maxTotal=-1, wait=500ms）|
| Repository 序列化兼容性 | Repository.fileSystem 的 GSON 反序列化需额外处理，在 T10 中专项设计 |
| BrokerProperties 地址未解析 | `StoragePropertiesConverter.toMap(BrokerProperties)` 不含 host/port，不能用于 getBrokerFileSystem；调用方必须先手动解析地址 |
