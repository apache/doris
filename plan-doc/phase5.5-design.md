# Phase 5.5 设计文档 — 迁移纯 SPI 工具类到 fe-filesystem-spi

## 目标

将 `fs/` 包中 6 个无 fe-core 依赖（或可消除依赖）的文件迁移到更合适的位置，
进一步减少 `fs/` 包中的混杂内容，使 fe-core 的 `fs/` 只保留真正依赖 fe-core 的桥接层代码。

---

## 迁移对象

### 第一组：迁移到 `fe-filesystem-spi` 主代码

#### 1. `RemoteIterator.java`
- **现状：** `org.apache.doris.fs.RemoteIterator`，依赖 `FileSystemIOException`（同包）
- **目标：** `org.apache.doris.filesystem.spi.RemoteIterator`
- **注意：** 必须与 `FileSystemIOException` 一起迁移（存在相互引用）

#### 2. `FileSystemIOException.java`
- **现状：** `org.apache.doris.fs.FileSystemIOException`，依赖 `org.apache.doris.backup.Status.ErrCode`
- **目标：** `org.apache.doris.filesystem.spi.FileSystemIOException`
- **注意：** `ErrCode` 是可选字段（`@Nullable` 模式），迁移后 fe-core 的 backup 模块
  import 路径变更，不影响逻辑
- **SPI 模块约束：** `fe-filesystem-spi` 声明"Zero third-party external dependencies"，
  `ErrCode` 来自 fe-core 内部类，不是第三方库。但引入 `org.apache.doris.backup` 会使
  SPI 模块依赖 fe-core，违反模块独立性。
  **解决方案：** 将 `ErrCode` 字段改为 `String errCode`，或直接去掉 ErrCode 构造器——
  检查是否有调用方使用了带 ErrCode 的构造器。

#### 3. `SimpleRemoteIterator.java`
- **现状：** `org.apache.doris.fs.SimpleRemoteIterator`，实现 `RemoteIterator`
- **目标：** `org.apache.doris.filesystem.spi.SimpleRemoteIterator`
- **注意：** 随 `RemoteIterator` 一起迁移即可

#### 4. `FileSystemType.java`
- **现状：** `org.apache.doris.fs.FileSystemType`，纯枚举，零依赖
- **目标：** `org.apache.doris.filesystem.spi.FileSystemType`
- **调用方：** `SchemaTypeMapper`（fe-core）、`LocationPath`（fe-core）

#### 5. `FileSystemUtil.java`
- **现状：** `org.apache.doris.fs.FileSystemUtil`，有 `org.apache.hadoop.fs.Path` 依赖
- **目标：** `org.apache.doris.filesystem.spi.FileSystemUtil`
- **处理 Hadoop 依赖：** 唯一使用 `Path` 的场景是路径拼接，等价于：
  ```java
  // 替换前
  Path source = new Path(origFilePath, fileName);
  source.toString()
  // 替换后（等价）
  origFilePath.endsWith("/") ? origFilePath + fileName : origFilePath + "/" + fileName
  ```
  替换后无任何 Hadoop 依赖，可安全迁移到 SPI 模块。
- **调用方：** `HMSTransaction`（fe-core）

### 第二组：在 fe-core 内从主代码移到测试代码

#### 6. `MemoryFileSystem.java`
- **现状：** `fe-core/src/main/java/org/apache/doris/fs/MemoryFileSystem.java`（生产代码！）
- **目标：** `fe-core/src/test/java/org/apache/doris/fs/MemoryFileSystem.java`（测试代码）
- **理由：** 仅被 `MemoryFileSystemTest` 使用，不应出现在生产 JAR 中
- **注意：** 无需修改包名或导入；`MemoryFileSystemTest` 在同一模块同一包，仍可访问
- **不迁移到 SPI 测试源的原因：** 如放在 `fe-filesystem-spi/src/test/java`，
  `fe-core` 测试将无法引用（需要 test-jar 依赖，复杂度高）

---

## FileSystemIOException 的 ErrCode 处理

先确认是否有调用方使用了 `ErrCode` 参数的构造器：

```java
// 需要检查的调用方
grep -rn "FileSystemIOException.*ErrCode\|new FileSystemIOException" --include="*.java" fe/
```

如果没有调用方使用带 `ErrCode` 的构造器，则直接去掉该构造器，
使 `FileSystemIOException` 无 fe-core 依赖。

如果有调用方使用，则将 `ErrCode errCode` 字段改为 `String errCode`（用 `ErrCode.name()` 转换），
SPI 模块不引入 `ErrCode` 类型即可。

---

## 调用方更新清单

迁移后需要更新以下文件的 import：

| 文件 | 需更新的 import |
|------|----------------|
| `fs/DirectoryLister.java` | `RemoteIterator`, `FileSystemIOException` |
| `fs/FileSystemDirectoryLister.java` | `SimpleRemoteIterator` |
| `fs/TransactionScopeCachingDirectoryLister.java` | `RemoteIterator`, `FileSystemIOException`, `SimpleRemoteIterator` |
| `fs/TransactionScopeCachingDirectoryListerFactory.java` | （无，不直接用这三个） |
| `datasource/hive/HiveExternalMetaCache.java` | `RemoteIterator`, `FileSystemIOException` |
| `datasource/hive/HMSTransaction.java` | `FileSystemUtil` |
| `common/util/LocationPath.java` | `FileSystemType` |
| `fs/SchemaTypeMapper.java` | `FileSystemType` |

---

## 步骤

1. 检查 `FileSystemIOException` ErrCode 调用方
2. 迁移 `FileSystemIOException` → SPI（去掉或改造 ErrCode 依赖）
3. 迁移 `RemoteIterator` → SPI（更新包声明，去掉同包 import）
4. 迁移 `SimpleRemoteIterator` → SPI（同上）
5. 迁移 `FileSystemType` → SPI
6. 迁移 `FileSystemUtil` → SPI（替换 Hadoop Path 为字符串拼接）
7. 更新所有调用方 import
8. 移动 `MemoryFileSystem` 到 `fe-core/src/test/java`
9. 编译验证：`./build.sh --fe -j16`
10. 运行测试：`MemoryFileSystemTest`、`FileSystemTransferUtilTest`、`SchemaTypeMapperTest`
11. 提交

---

## 风险评估

- **低风险：** `MemoryFileSystem` 移到测试目录——无逻辑变更，仅文件位置变化
- **低风险：** `FileSystemType`、`SimpleRemoteIterator` 纯重命名包
- **中风险：** `FileSystemIOException` 需处理 `ErrCode` 依赖
- **低风险：** `FileSystemUtil` 的 Hadoop Path 替换——逻辑等价，有充分测试
