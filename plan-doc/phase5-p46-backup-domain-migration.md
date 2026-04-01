# P4.6 设计文档：Backup Domain（Repository/BackupJob/RestoreJob）迁移到 SPI

## 一、背景与目标

### 1.1 现状

Backup/Restore 域的 4 个核心文件仍依赖旧的 `PersistentFileSystem`：

| 文件 | 依赖的旧 API |
|------|------------|
| `Repository.java` | `PersistentFileSystem` 字段（`fileSystem`），`getRemoteFileSystem()` |
| `BackupHandler.java` | `repo.getRemoteFileSystem().getProperties()` |
| `BackupJob.java` (L440, L824) | `repo.getRemoteFileSystem().getStorageProperties().getBackendConfigProperties()` <br> `repo.getRemoteFileSystem().getThriftStorageType()` |
| `RestoreJob.java` (L448, L2005) | 同上 |

### 1.2 BE-FE 通信机制

Backup/Restore 的核心是：FE 将存储配置打包成 **Thrift 消息**发给 BE，
BE 据此初始化存储客户端并执行文件上传/下载。

完整调用链如下：

```
Repository.getRemoteFileSystem()
    └─ .getStorageProperties().getBackendConfigProperties()
           └─ → Map<String, String>  (broker_prop 字段)
    └─ .getThriftStorageType()
           └─ PersistentFileSystem.getThriftStorageType()
                └─ FsStorageTypeAdapter.toThrift(type)
                     └─ StorageBackend.StorageType.toThrift()
                          └─ → TStorageBackendType  (storage_backend 字段)

UploadTask / DownloadTask
    └─ toThrift()
         └─ TUploadReq / TDownloadReq
               ├─ broker_prop: Map<String,String>    ← getBackendConfigProperties()
               ├─ storage_backend: TStorageBackendType ← getThriftStorageType()
               └─ location: String                    ← repo.getLocation()
```

### 1.3 阻塞原因

| 缺失能力 | 说明 |
|---------|------|
| `PersistentFileSystem.getThriftStorageType()` | 返回 `StorageBackend.StorageType`（可调 `.toThrift()` 得 `TStorageBackendType`）；SPI `FileSystem` 无此方法 |
| `StorageProperties.getBackendConfigProperties()` | 返回 BE 所需的 `Map<String,String>`（含 AK/SK 等）；SPI `FileSystem` 无此方法 |

两者都是**从 FE 侧元数据提取配置**的操作，不涉及实际 I/O，
因此应由 **`FileSystemDescriptor`**（已有的轻量描述符 POJO）来承担。

### 1.4 目标

- 在 `FileSystemDescriptor` 中新增 `getThriftStorageType()` 和 `getBackendConfigProperties()` 方法
- 将 `BackupJob` / `RestoreJob` / `BackupHandler` 中的
  `repo.getRemoteFileSystem().*` 调用替换为 `repo.getFileSystemDescriptor().*`
- `Repository` 保留 `fileSystem`（`PersistentFileSystem`）仅用于实际 I/O（upload/download/list），
  不再用于元数据提取

---

## 二、架构设计

### 2.1 核心思路

将"存储配置的元数据"和"实际 I/O"分离：

```
┌─────────────────────────────────────────────┐
│ Repository                                  │
│                                             │
│  fileSystemDescriptor ─────────────────────►│ 元数据查询
│    .getThriftStorageType()                  │ （不需要 I/O）
│    .getBackendConfigProperties()            │
│                                             │
│  fileSystem (spi.FileSystem) ──────────────►│ 实际 I/O
│    .list() / .delete() / download/upload    │ （需要连接）
└─────────────────────────────────────────────┘
```

`FileSystemDescriptor` 已存储 `FsStorageType` 和原始 `properties`，
完全具备推导出上述两个方法的能力，无需持有 `PersistentFileSystem` 实例。

### 2.2 方案选择

| 方案 | 说明 | 优缺点 |
|------|------|--------|
| **方案 A（推荐）** | 在 `FileSystemDescriptor` 新增两个方法；`BackupJob`/`RestoreJob` 改用 `descriptor` | 完全去除 `PersistentFileSystem` 依赖；清晰 |
| 方案 B | 保留 `Repository.fileSystem`（`PersistentFileSystem`）仅用于 BE 通信 | 不彻底，新旧代码共存 |

**选择方案 A**。

---

## 三、`FileSystemDescriptor` 扩展

### 3.1 当前状态

```java
// 现有 FileSystemDescriptor
public class FileSystemDescriptor {
    @SerializedName("fs_type")
    private final FsStorageType storageType;   // 枚举，如 S3, HDFS, BROKER...

    @SerializedName("fs_name")
    private final String name;

    @SerializedName("fs_props")
    private final Map<String, String> properties;  // 原始配置（AK/SK 等）

    // 已有方法
    public FsStorageType getStorageType() { return storageType; }
    public String getName() { return name; }
    public Map<String, String> getProperties() { return properties; }

    public static FileSystemDescriptor fromPersistentFileSystem(PersistentFileSystem fs) {
        return new FileSystemDescriptor(fs.getStorageType(), fs.getName(), fs.getProperties());
    }
}
```

### 3.2 新增方法

#### 3.2.1 `getThriftStorageType()`

```java
/**
 * Returns the Thrift-compatible storage type enum, used when constructing
 * {@code TUploadReq}/{@code TDownloadReq} messages for backend communication.
 *
 * <p>This replaces the deprecated {@code PersistentFileSystem.getThriftStorageType()}.
 *
 * @return the Thrift storage type corresponding to this descriptor's {@link FsStorageType}
 */
public StorageBackend.StorageType getThriftStorageType() {
    return FsStorageTypeAdapter.toThrift(storageType);
}
```

`FsStorageTypeAdapter.toThrift()` 已存在，映射关系：

| `FsStorageType` | `StorageBackend.StorageType` |
|----------------|------------------------------|
| `S3` | `S3` |
| `HDFS` | `HDFS` |
| `OSS_HDFS` | `HDFS` |
| `BROKER` | `BROKER` |
| `AZURE` | `AZURE` |
| `OFS` | `OFS` |
| `JFS` | `JFS` |
| `LOCAL` | `LOCAL` |

#### 3.2.2 `getBackendConfigProperties()`

```java
/**
 * Returns the backend configuration properties map, used as the {@code broker_prop}
 * field in {@code TUploadReq}/{@code TDownloadReq} Thrift messages.
 *
 * <p>The returned map contains storage-specific parameters (AK/SK, endpoint, region, etc.)
 * in the format that the Doris backend expects for initializing its storage client.
 *
 * <p>This replaces {@code PersistentFileSystem.getStorageProperties().getBackendConfigProperties()}.
 *
 * @return an immutable map of backend configuration properties
 */
public Map<String, String> getBackendConfigProperties() {
    StorageProperties sp = StorageProperties.createPrimary(properties);
    return sp.getBackendConfigProperties();
}
```

`StorageProperties.createPrimary(Map)` 已存在，根据 `properties` 中的 `type` key
选择正确的 `StorageProperties` 子类（`S3Properties`, `HdfsProperties` 等）。

#### 3.2.3 带运行时覆盖的重载

部分场景（如 vended credentials）需在 base 配置上叠加临时凭证：

```java
/**
 * Returns backend config properties merged with the provided runtime overrides.
 * Runtime overrides take precedence over base properties.
 *
 * @param runtimeProperties additional/override properties (e.g., vended STS credentials)
 * @return merged properties map
 */
public Map<String, String> getBackendConfigProperties(Map<String, String> runtimeProperties) {
    Map<String, String> base = new HashMap<>(getBackendConfigProperties());
    if (runtimeProperties != null) {
        base.putAll(runtimeProperties);
    }
    return Collections.unmodifiableMap(base);
}
```

---

## 四、`Repository.java` 变更

### 4.1 `fileSystem` 字段从 `PersistentFileSystem` → `spi.FileSystem`

```java
// 变更前
@Deprecated @SerializedName("fs")
private PersistentFileSystem legacyFileSystem;

@SerializedName("fs_descriptor")
private FileSystemDescriptor fileSystemDescriptor;

private transient PersistentFileSystem fileSystem;  // 旧类型

// 变更后
@Deprecated @SerializedName("fs")
private PersistentFileSystem legacyFileSystem;  // 仅用于向后兼容反序列化，不再使用

@SerializedName("fs_descriptor")
private FileSystemDescriptor fileSystemDescriptor;

private transient org.apache.doris.filesystem.spi.FileSystem fileSystem;  // 新 SPI 类型
```

### 4.2 `gsonPostProcess()` 变更

```java
@Override
public void gsonPostProcess() {
    // 优先使用新 descriptor，fallback 到旧 legacyFileSystem（向后兼容）
    if (fileSystemDescriptor == null && legacyFileSystem != null) {
        fileSystemDescriptor = FileSystemDescriptor.fromPersistentFileSystem(legacyFileSystem);
    }
    if (fileSystemDescriptor == null) return;

    Map<String, String> props = fileSystemDescriptor.getProperties();
    try {
        // 变更前：FileSystemFactory.get(storageProperties) 返回 PersistentFileSystem
        // 变更后：FileSystemFactory.getFileSystem(props) 返回 spi.FileSystem
        this.fileSystem = FileSystemFactory.getFileSystem(props);
    } catch (Exception e) {
        LOG.warn("Failed to init SPI FileSystem for repository {}, falling back to broker", name, e);
        // Fallback：Broker 场景（fe-filesystem-broker 模块提供）
        Map<String, String> brokerProps = new HashMap<>(props);
        brokerProps.put("type", "BROKER");
        brokerProps.put(BrokerProperties.BROKER_NAME_KEY, fileSystemDescriptor.getName());
        try {
            this.fileSystem = FileSystemFactory.getFileSystem(brokerProps);
        } catch (Exception ex) {
            LOG.error("Failed to init Broker FileSystem for repository {}", name, ex);
        }
    }
}
```

### 4.3 方法变更

```java
// 保留（类型改变）
public org.apache.doris.filesystem.spi.FileSystem getFileSystem() {
    return fileSystem;
}

// 保留（类型改变，已在 P4.6e 提交中新增）
public FileSystemDescriptor getFileSystemDescriptor() {
    return fileSystemDescriptor;
}

// 废弃（用 getFileSystem() 替代）
@Deprecated
public PersistentFileSystem getRemoteFileSystem() {
    // 临时向后兼容：若有调用方，打印 deprecation 日志
    LOG.warn("getRemoteFileSystem() is deprecated, use getFileSystemDescriptor() for metadata "
           + "and getFileSystem() for I/O");
    throw new UnsupportedOperationException(
        "Use getFileSystemDescriptor() for storage metadata, getFileSystem() for I/O");
}
```

> **注**：`getRemoteFileSystem()` 若有编译时调用方则编译报错，确保所有调用方均已迁移。

### 4.4 `validateAndNormalizeUri()`

当前使用 `getStorageProperties()` 做 URI 校验：

```java
// 当前（需调查具体代码）
StorageProperties sp = repo.getRemoteFileSystem().getStorageProperties();
String normalizedUri = sp.validateAndNormalizeUri(uri);
```

迁移方式：

```java
// 变更后
StorageProperties sp = StorageProperties.createPrimary(
    repo.getFileSystemDescriptor().getProperties());
String normalizedUri = sp.validateAndNormalizeUri(uri);
```

---

## 五、`BackupJob.java` 变更

### 5.1 L440：更新 broker properties（UPLOADING 状态重试）

```java
// 变更前（L440 附近）
((UploadTask) task).updateBrokerProperties(
    repo.getRemoteFileSystem().getStorageProperties().getBackendConfigProperties());

// 变更后
((UploadTask) task).updateBrokerProperties(
    repo.getFileSystemDescriptor().getBackendConfigProperties());
```

### 5.2 L824-825：创建 `UploadTask`

```java
// 变更前
UploadTask task = new UploadTask(null, beId, signature, jobId, dbId, srcToDest,
    brokers.get(0),
    repo.getRemoteFileSystem().getStorageProperties().getBackendConfigProperties(),
    repo.getRemoteFileSystem().getThriftStorageType(),   // ← PersistentFileSystem.getThriftStorageType()
    repo.getLocation());

// 变更后
UploadTask task = new UploadTask(null, beId, signature, jobId, dbId, srcToDest,
    brokers.get(0),
    repo.getFileSystemDescriptor().getBackendConfigProperties(),
    repo.getFileSystemDescriptor().getThriftStorageType(),  // ← FileSystemDescriptor 新方法
    repo.getLocation());
```

### 5.3 `UploadTask` 参数类型变更

`UploadTask` 构造函数第 9 个参数原类型为 `StorageBackend.StorageType`，不变；
`FileSystemDescriptor.getThriftStorageType()` 返回同类型，无需修改 `UploadTask`。

---

## 六、`RestoreJob.java` 变更

### 6.1 L448：更新 broker properties（DOWNLOADING 状态重试）

```java
// 变更前
((DownloadTask) task).updateBrokerProperties(
    repo.getRemoteFileSystem().getStorageProperties().getBackendConfigProperties());

// 变更后
((DownloadTask) task).updateBrokerProperties(
    repo.getFileSystemDescriptor().getBackendConfigProperties());
```

### 6.2 L2005-2006：创建 `DownloadTask`

```java
// 变更前
return new DownloadTask(null, beId, signature, jobId, dbId, srcToDest,
    brokerAddr,
    repo.getRemoteFileSystem().getStorageProperties().getBackendConfigProperties(),
    repo.getRemoteFileSystem().getThriftStorageType(),
    repo.getLocation(),
    "");

// 变更后
return new DownloadTask(null, beId, signature, jobId, dbId, srcToDest,
    brokerAddr,
    repo.getFileSystemDescriptor().getBackendConfigProperties(),
    repo.getFileSystemDescriptor().getThriftStorageType(),
    repo.getLocation(),
    "");
```

---

## 七、`BackupHandler.java` 变更

`BackupHandler` 使用 `repo.getRemoteFileSystem().getProperties()` 做属性合并：

```java
// 变更前
private Map<String, String> mergeProperties(Repository repo, Map<String, String> newProps) {
    Map<String, String> combined = new HashMap<>(repo.getRemoteFileSystem().getProperties());
    combined.putAll(newProps);
    return combined;
}

// 变更后
private Map<String, String> mergeProperties(Repository repo, Map<String, String> newProps) {
    Map<String, String> combined = new HashMap<>(repo.getFileSystemDescriptor().getProperties());
    combined.putAll(newProps);
    return combined;
}
```

---

## 八、Repository 实际 I/O 路径

Repository 的 I/O 操作（upload/download/list/delete）当前由
`fileSystem`（`PersistentFileSystem` / `RemoteFileSystem`）执行：

```java
// Repository 内部方法，如 upload()
Status st = fileSystem.upload(localPath, remotePath);
```

迁移后，这些调用改用新 SPI 方法，通过 `FileSystemTransferUtil`（Phase 4.0 已建立）：

| 旧调用 | 新调用 |
|--------|--------|
| `fileSystem.upload(local, remote)` | `FileSystemTransferUtil.upload(fileSystem, local, Location.of(remote))` |
| `fileSystem.downloadWithFileSize(remote, local, size)` | `FileSystemTransferUtil.download(fileSystem, Location.of(remote), local, size)` |
| `fileSystem.directUpload(content, remote)` | `FileSystemTransferUtil.directUpload(fileSystem, content, Location.of(remote))` |
| `fileSystem.globList(path, result, false)` | `FileSystemTransferUtil.globList(fileSystem, path, false)` |
| `fileSystem.delete(path)` | `fileSystem.delete(Location.of(path), false)` |
| `fileSystem.deleteDirectory(path)` | `fileSystem.delete(Location.of(path), true)` |
| `fileSystem.exists(path)` | `fileSystem.exists(Location.of(path))` |

---

## 九、序列化兼容性

`Repository` 使用 Gson 序列化存储元数据到 FE 的 image 文件。
变更点：

### 9.1 序列化格式

`fileSystemDescriptor` 字段（`@SerializedName("fs_descriptor")`）已从 Phase 4.6e 开始写入。
旧的 `legacyFileSystem`（`@SerializedName("fs")`）保留反序列化，新写入时不再包含该字段。

迁移后 `fileSystem` 字段为 `transient`（不序列化），在 `gsonPostProcess()` 中重建，
**序列化格式不变**。

### 9.2 向后兼容

- 旧 image：含 `fs` 字段（`PersistentFileSystem`），无 `fs_descriptor`
  → `gsonPostProcess()` 从 `legacyFileSystem` 构建 `fileSystemDescriptor`，正常运行
- 新 image：含 `fs_descriptor` 字段，无 `fs`
  → 直接使用 `fileSystemDescriptor`，无需 fallback

---

## 十、文件变更清单

### `fe-core` — 核心变更

| 文件 | 变更 |
|------|------|
| `fs/FileSystemDescriptor.java` | **修改** — 新增 `getThriftStorageType()`, `getBackendConfigProperties()`, `getBackendConfigProperties(Map)` |
| `backup/Repository.java` | **修改** — `fileSystem` 类型 `PersistentFileSystem` → `spi.FileSystem`；`gsonPostProcess()` 改用 `getFileSystem()`；`getRemoteFileSystem()` 废弃 |
| `backup/BackupJob.java` | **修改** — L440, L824-825：`getRemoteFileSystem().*` → `getFileSystemDescriptor().*` |
| `backup/RestoreJob.java` | **修改** — L448, L2005-2006：同上 |
| `backup/BackupHandler.java` | **修改** — `getRemoteFileSystem().getProperties()` → `getFileSystemDescriptor().getProperties()` |

### `fe-core` — Repository I/O 路径（次要）

| 文件 | 变更 |
|------|------|
| `backup/Repository.java`（I/O 方法） | **修改** — 内部 I/O 调用从旧 API 迁移到 `FileSystemTransferUtil` + `spi.FileSystem` |

---

## 十一、次要阻塞：`RepositoryMgr.java`（已完成）

P4.6e 已完成 `instanceof S3FileSystem` / `instanceof AzureFileSystem` 替换为
`fileSystemDescriptor.getStorageType() == FsStorageType.S3`，此项无需重复处理。

---

## 十二、测试策略

### 12.1 单元测试

```java
// FileSystemDescriptorTest
@Test
void testGetThriftStorageType() {
    FileSystemDescriptor desc = new FileSystemDescriptor(FsStorageType.S3, "test-repo", props);
    assertEquals(StorageBackend.StorageType.S3, desc.getThriftStorageType());
}

@Test
void testGetBackendConfigProperties() {
    Map<String, String> props = Map.of(
        "type", "S3",
        "s3.access_key", "AK",
        "s3.secret_key", "SK",
        "s3.endpoint", "s3.us-east-1.amazonaws.com",
        "s3.region", "us-east-1",
        "s3.bucket", "my-bucket"
    );
    FileSystemDescriptor desc = new FileSystemDescriptor(FsStorageType.S3, "test", props);
    Map<String, String> beProps = desc.getBackendConfigProperties();
    assertEquals("AK", beProps.get("AWS_ACCESS_KEY"));  // BE key format
    assertEquals("SK", beProps.get("AWS_SECRET_KEY"));
}
```

### 12.2 集成测试

- 备份任务创建并正确发送 `TUploadReq` 到 BE（验证 `broker_prop` 和 `storage_backend`）
- 恢复任务正确发送 `TDownloadReq`
- 旧 image（含 `legacyFileSystem` 序列化数据）能正常反序列化并恢复
- 新旧版本 FE 的 image 格式互相兼容（滚动升级场景）

### 12.3 验证清单

- [ ] `BackupJob` 发出的 `TUploadReq.storage_backend` 与 repository 类型匹配（S3 repo → `TStorageBackendType.S3`）
- [ ] `BackupJob` 发出的 `TUploadReq.broker_prop` 含有 BE 可识别的 AK/SK/endpoint
- [ ] `RestoreJob` 对称验证
- [ ] 旧 image 格式（含 `PersistentFileSystem` 序列化）升级后正常加载
- [ ] `Repository.getFileSystemDescriptor()` 在 `BackupHandler.mergeProperties()` 中返回正确 properties

---

## 十三、依赖关系与排期

| 步骤 | 说明 | 依赖 |
|------|------|------|
| 1 | 扩展 `FileSystemDescriptor`（新增 2 个方法） | — |
| 2 | 修改 `BackupJob` / `RestoreJob`（改用 descriptor） | 步骤 1 |
| 3 | 修改 `BackupHandler` | 步骤 1 |
| 4 | 修改 `Repository`（`fileSystem` 类型变更、`gsonPostProcess` 更新） | 步骤 2、3，`fe-filesystem-broker` 模块（P4.5a） |
| 5 | `Repository` I/O 路径迁移（upload/download/list） | `FileSystemTransferUtil`（已完成） |
| 6 | 运行 Backup/Restore 回归测试 | 步骤 1-5 |

> **注**：步骤 4 中 `gsonPostProcess` 改用 `FileSystemFactory.getFileSystem()`，
> 需要 `fe-filesystem-broker` 已注册（P4.5a），否则 Broker 类型 Repository 无法加载。
> 因此 P4.6 与 P4.5a 有运行时依赖。

---

## 十四、涉及的架构图（变更后）

```
              ┌──────────────────────────────────────┐
              │  Repository                          │
              │                                      │
              │  fileSystemDescriptor ──────────────►│──► getThriftStorageType()
              │    (FsStorageType + properties)       │──► getBackendConfigProperties()
              │                                      │
              │  fileSystem (spi.FileSystem) ────────►│──► list / delete / exists
              │    (建立实际连接，via ServiceLoader)   │──► FileSystemTransferUtil.upload/download
              └──────────────────────────────────────┘
                           │                │
                           │ BackupJob      │ RestoreJob
                           ▼                ▼
              ┌──────────────────────────────────────┐
              │  UploadTask / DownloadTask            │
              │    .toThrift()                        │
              │    ├─ broker_prop  ← descriptor.getBackendConfigProperties()
              │    ├─ storage_backend ← descriptor.getThriftStorageType().toThrift()
              │    └─ location    ← repo.getLocation()
              └──────────────────────────────────────┘
                           │
                           ▼
              ┌──────────────────────────────────────┐
              │  BE: TUploadReq / TDownloadReq       │
              │  初始化 S3/HDFS/OFS 客户端并执行      │
              └──────────────────────────────────────┘
```
