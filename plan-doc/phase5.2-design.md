# Phase 5.2 开发设计文档 — 删除旧版 fe-core fs 接口体系

**前置：** Phase 5.1 已完成（`operations/`、`FileSystemSpiProvider`、`ParsedPath` 全部删除）  
**目标：** 重写 `MemoryFileSystem` 实现 `spi.FileSystem`，然后删除旧版 fe-core fs 接口层的 6 个文件。

---

## 调查结论

### 旧版接口实际外部 caller 统计（import 级别精确 grep）

| 旧版接口 | 外部 caller 数 | 实际 caller |
|---------|--------------|-------------|
| `org.apache.doris.fs.FileSystem` | 0 | 无外部引用 |
| `org.apache.doris.fs.FileIterator` | 0 | 无外部引用 |
| `org.apache.doris.fs.FileEntry` | 1 | `remote/RemoteFile.java`（Phase 5.3 删除）|
| `org.apache.doris.fs.Location` | 3 | `RemoteFile.java`、`io/DorisInputFile.java`、`io/DorisOutputFile.java` |
| `org.apache.doris.fs.io.DorisInputFile` | 2 | `FileSystem.java`（定义）、`MemoryFileSystem.java` |
| `org.apache.doris.fs.io.DorisInput` | 1 | `MemoryFileSystem.java` |
| `org.apache.doris.fs.io.DorisInputStream` | 1 | `MemoryFileSystem.java` |
| `org.apache.doris.fs.io.DorisOutputFile` | 2 | `FileSystem.java`（定义）、`MemoryFileSystem.java` |

**结论：production 代码早已完全切换到 SPI 接口。** 旧版接口层是一个自闭合孤岛：`FileSystem.java` → `MemoryFileSystem.java` → `io/*`，外部零依赖。

### RemoteFile 处理策略（调整）

原设计 5.2.1 要求先清理 S3ObjStorage/AzureObjStorage 中的 RemoteFile 引用。  
**调整：** S3ObjStorage/AzureObjStorage 将在 Phase 5.3 中整包删除，提前迁移 RemoteFile 只是中间状态，无实质价值。  
因此将 `RemoteFile.java`、`FileEntry.java`、`Location.java` 三个文件推迟到 Phase 5.3 一并删除。

---

## 子阶段划分

### Phase 5.2.1 — 重写 MemoryFileSystem + 更新 MemoryFileSystemTest

### Phase 5.2.2 — 删除旧版 fs 接口层（6 个文件）

---

## Phase 5.2.1 详细设计

### 1. 旧接口 vs SPI 方法对照表

| `fs.FileSystem` 旧方法 | `spi.FileSystem` 对应 | 备注 |
|----------------------|--------------------|------|
| `deleteFile(Location)` | `delete(Location, false)` | SPI 统一为带 recursive 参数 |
| `deleteDirectory(Location)` | `delete(Location, true)` | 同上 |
| `deleteFiles(Collection<Location>)` | 新增 `default deleteFiles()` 到 SPI | 见下 |
| `renameFile(Location, Location)` | `rename(Location, Location)` | 直接对应 |
| `renameDirectory(Location, Location)` | `rename(Location, Location)` | SPI 将文件/目录重命名统一 |
| `createDirectory(Location)` | `mkdirs(Location)` | 改名 |
| `listFiles(Location, false)` | `list(Location)` | SPI `list()` 总是非递归 |
| `listFiles(Location, true)` | `listFilesRecursive(Location)` | SPI 默认实现递归 |
| `listDirectories(Location) → Set<Location>` | `listDirectories(Location) → Set<String>` | 返回类型变 |
| `newInputFile(Location)` | `newInputFile(Location) throws IOException` | 方法签名一致（增加 throws） |
| `newOutputFile(Location)` | `newOutputFile(Location) throws IOException` | 同上 |

### 2. 需要对 SPI 补充的内容

#### 2a. `spi.FileSystem` 新增 `deleteFiles()` default 方法

```java
// 在 spi.FileSystem 中新增
default void deleteFiles(Collection<Location> locations) throws IOException {
    for (Location location : locations) {
        delete(location, false);
    }
}
```

#### 2b. `spi.FileEntry` 新增 `isFile()` + `name()` 便利方法

当前 `spi.FileEntry` 只有 `isDirectory()`。测试和生产代码中常用 `isFile()` 和 `name()`：

```java
// 在 spi.FileEntry 中新增
public boolean isFile() {
    return !isDirectory;
}

public String name() {
    String uri = location.uri();
    int end = uri.endsWith("/") ? uri.length() - 1 : uri.length();
    int start = uri.lastIndexOf('/', end - 1);
    return uri.substring(start + 1, end);
}
```

### 3. MemoryFileSystem.java 重写要点

```java
// 之前
public class MemoryFileSystem implements FileSystem {
// 之后
public class MemoryFileSystem implements org.apache.doris.filesystem.spi.FileSystem {
```

**方法变更列表：**

| 变更 | 说明 |
|------|------|
| `deleteFile()` + `deleteDirectory()` → 合并为 `delete(loc, recursive)` | |
| `createDirectory()` → `mkdirs()` | |
| `renameFile()` + `renameDirectory()` → 合并为 `rename(src, dst)` | |
| `listFiles(loc, recursive)` → `list(loc)`（只实现非递归）| 递归由 SPI default `listFilesRecursive()` 处理 |
| `listDirectories()` 返回类型 `Set<Location>` → `Set<String>` | 取 `entry.location().uri()` |
| 删除 `listDirectories()` override | SPI default 通过 `list()` 实现，等价逻辑 |
| 添加 `iteratorOf(List<FileEntry>)` 私有工具方法 | SPI `FileIterator` 无 `ofList()` 工厂 |
| `FileEntry.builder(loc)...build()` → `new FileEntry(loc, len, isDir, 0L, null)` | SPI `FileEntry` 构造器直接使用 |

**内部类变更：**

| 内部类 | 变更 |
|--------|------|
| `MemoryInputFile implements DorisInputFile` | 改为 `implements spi.DorisInputFile`；删除 `newInput()` 方法 |
| `MemorySeekableInputStream extends DorisInputStream` | 改为 `extends spi.DorisInputStream`；`getPosition()` → `getPos()` |
| `MemoryOutputFile implements DorisOutputFile` | 改为 `implements spi.DorisOutputFile` |

**不变：** 核心内存逻辑（`store` HashMap、读写流、目录遍历逻辑）全部保留，仅方法名和类型更新。

### 4. MemoryFileSystemTest.java 更新要点

| 旧调用 | 新调用 |
|--------|--------|
| `fs.deleteFile(loc)` | `fs.delete(loc, false)` |
| `fs.deleteDirectory(loc)` | `fs.delete(loc, true)` |
| `fs.createDirectory(loc)` | `fs.mkdirs(loc)` |
| `fs.renameFile(src, dst)` | `fs.rename(src, dst)` |
| `fs.renameDirectory(src, dst)` | `fs.rename(src, dst)` |
| `fs.listFiles(loc, false)` | `drain(fs.list(loc))` |
| `fs.listFiles(loc, true)` | `fs.listFilesRecursive(loc)` |
| `Set<Location> dirs` | `Set<String> dirs` |
| `d.toString().contains(...)` | `d.contains(...)` |
| `e.isFile()` | `!e.isDirectory()`（或使用新增的 `e.isFile()`） |
| `e.name()` | `e.name()`（使用新增方法；SPI FileEntry 无此方法，需新增）|
| `Location.of(...)` | 改为 `spi.Location.of(...)` 导入 |
| `FileEntry` 类型 | 改为 `spi.FileEntry` |
| `FileIterator` 类型 | 改为 `spi.FileIterator` |
| `fs.deleteFiles(toDelete)` | 保持不变（SPI 新增 `deleteFiles()` default） |

---

## Phase 5.2.2 详细设计 — 删除 6 个文件

Phase 5.2.1 完成后，以下文件的 caller 为零：

| 文件 | 删除条件 |
|------|---------|
| `fs/FileSystem.java` | 无外部引用（MemoryFileSystem 已不实现它）|
| `fs/FileIterator.java` | 无外部引用（MemoryFileSystem 已用 spi 版）|
| `fs/io/DorisInputFile.java` | 无外部引用（FileSystem.java 删除后）|
| `fs/io/DorisInput.java` | 无外部引用（MemoryFileSystem 已不使用）|
| `fs/io/DorisInputStream.java` | 无外部引用（MemoryFileSystem 已不使用）|
| `fs/io/DorisOutputFile.java` | 无外部引用（FileSystem.java 删除后）|

```bash
git rm fe/fe-core/src/main/java/org/apache/doris/fs/FileSystem.java
git rm fe/fe-core/src/main/java/org/apache/doris/fs/FileIterator.java
git rm fe/fe-core/src/main/java/org/apache/doris/fs/io/DorisInputFile.java
git rm fe/fe-core/src/main/java/org/apache/doris/fs/io/DorisInput.java
git rm fe/fe-core/src/main/java/org/apache/doris/fs/io/DorisInputStream.java
git rm fe/fe-core/src/main/java/org/apache/doris/fs/io/DorisOutputFile.java
```

**注意**：`fs/io/` 目录下还有 `package-info.java`（如果存在），需检查后一并删除。

---

## 不在本阶段处理（推迟至 Phase 5.3）

| 文件 | 原因 |
|------|------|
| `fs/remote/RemoteFile.java` | S3ObjStorage/AzureObjStorage 仍在使用，Phase 5.3 整包删除 |
| `fs/FileEntry.java` | RemoteFile.java 导入它 |
| `fs/Location.java` | RemoteFile.java 导入它 |

---

## 验证检查清单

- [ ] `./build.sh --fe -j16` 编译通过
- [ ] `run-fe-ut.sh --run org.apache.doris.fs.MemoryFileSystemTest` 全部通过
- [ ] `grep -rn "import org.apache.doris.fs.io\." --include="*.java" fe/` → 0 结果
- [ ] `grep -rn "import org.apache.doris.fs.FileSystem;" --include="*.java" fe/` → 0 结果
- [ ] `grep -rn "import org.apache.doris.fs.FileIterator;" --include="*.java" fe/` → 0 结果

---

## Commit 信息

```
[chore](fe) Rewrite MemoryFileSystem for spi.FileSystem; delete old fs interface layer

### What problem does this PR solve?

Problem Summary: Continue fe-core fs/ cleanup (Phase 5.2).
- Add isFile()/name()/deleteFiles() convenience helpers to spi.FileEntry and spi.FileSystem
- Rewrite MemoryFileSystem to implement filesystem.spi.FileSystem instead of the old fs.FileSystem
- Update MemoryFileSystemTest accordingly
- Delete 6 dead files: fs/FileSystem.java, fs/FileIterator.java,
  fs/io/DorisInputFile.java, fs/io/DorisInput.java, fs/io/DorisInputStream.java,
  fs/io/DorisOutputFile.java

### Release note

None

### Check List (For Author)

- Test: FE unit tests (MemoryFileSystemTest)
- Behavior changed: No
- Does this need documentation: No

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
```
