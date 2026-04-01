# Phase 5.4 设计文档 — 迁移 FileSystemTransferUtil 到 fe-filesystem-spi

**目标：** 将 `fe-core` 中的 `fs/FileSystemTransferUtil.java` 移动到 `fe-filesystem-spi` 模块，更新其 caller 的 import，并删除原文件。

---

## 现状分析

### FileSystemTransferUtil 的现状

**当前位置：** `fe/fe-core/src/main/java/org/apache/doris/fs/FileSystemTransferUtil.java`  
**当前包：** `org.apache.doris.fs`

**依赖分析：**
- 所有 import 均来自 `org.apache.doris.filesystem.spi.*`（SPI 包）
- 无 fe-core 特定依赖（无 Catalog、Config、Env 等引用）
- 只使用 JDK 标准库 + SPI 接口

→ 完全适合迁移到 `fe-filesystem-spi` 模块。

### 目标位置

由于 `fe-filesystem-spi` 当前使用平铺结构（无子目录），直接放入主包：

**目标位置：** `fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/FileSystemTransferUtil.java`  
**目标包：** `org.apache.doris.filesystem.spi`

### Caller 一览

**生产代码（2 个文件）：**

| Caller | 调用方式 |
|--------|--------|
| `datasource/hive/AcidUtil.java` | `FileSystemTransferUtil.globList(...)` × 3 处 |
| `fs/FileSystemDirectoryLister.java` | `FileSystemTransferUtil.globList(...)` × 1 处 |

**测试代码（1 个文件）：**

| 文件 | 变更方式 |
|------|--------|
| `fs/FileSystemTransferUtilTest.java` | 更新 import（留在 fe-core 测试目录） |

> **注：** 测试文件依赖 `LocalFileSystemProvider`（`fe-filesystem-local` 模块），该模块已是 fe-core 的测试依赖。测试保留在 fe-core，仅更新 import，无需移动测试类。

---

## 执行步骤

### 5.4.1 — 在 fe-filesystem-spi 中创建 FileSystemTransferUtil

在 `fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/` 下新建 `FileSystemTransferUtil.java`：
- 内容与原文件完全相同
- 仅修改首行 `package` 声明：`org.apache.doris.fs` → `org.apache.doris.filesystem.spi`

### 5.4.2 — 更新 AcidUtil.java

```java
// 旧 import
import org.apache.doris.fs.FileSystemTransferUtil;

// 新 import
import org.apache.doris.filesystem.spi.FileSystemTransferUtil;
```

调用代码无需改动（方法签名相同）。

### 5.4.3 — 更新 FileSystemDirectoryLister.java

```java
// 旧 import
import org.apache.doris.fs.FileSystemTransferUtil;

// 新 import
import org.apache.doris.filesystem.spi.FileSystemTransferUtil;
```

### 5.4.4 — 更新 FileSystemTransferUtilTest.java

```java
// 旧 import（类所在的 package 是 org.apache.doris.fs）
package org.apache.doris.fs;
// 对 FileSystemTransferUtil 的引用是 implicit（同包）

// 新 import（package 不变，显式 import 新位置的类）
import org.apache.doris.filesystem.spi.FileSystemTransferUtil;
```

> **注意：** 测试类的 `package org.apache.doris.fs` 保持不变，仅需添加 import，因为迁移后两者不再同包。

### 5.4.5 — 删除原文件

```bash
git rm fe/fe-core/src/main/java/org/apache/doris/fs/FileSystemTransferUtil.java
```

---

## 验证计划

1. **编译验证：** `./build.sh --fe -j16` → `BUILD SUCCESS`
2. **Import 扫描：**
   ```bash
   grep -rn "import org.apache.doris.fs.FileSystemTransferUtil" fe/ --include="*.java"
   ```
   应为 0 结果
3. **单元测试：**
   ```bash
   ./run-fe-ut.sh --run org.apache.doris.fs.FileSystemTransferUtilTest
   ```
   所有测试通过

---

## fs/ 包最终状态

完成 Phase 5.4 后，`fe-core` 的 `org.apache.doris.fs` 包将仅剩：

| 文件 | 留存原因 |
|------|--------|
| `FileSystemFactory.java` | fe-core 与 SPI plugin 系统的桥梁；涉及 `FileSystemPluginManager`（fe-core 类），无法移出 |
| `MemoryFileSystem.java` | in-memory 测试实现；被 fe-core 测试和部分测试基础设施使用 |

这两个文件是合理的 fe-core 保留项，不需要迁移。

---

## 总结

Phase 5.4 是最简单的阶段：
- **新建** 1 个文件（SPI 模块）
- **更新** 3 个文件（import 变更）
- **删除** 1 个文件（原 fe-core 文件）
- 无逻辑变更，零风险
