# Phase 5.1 开发设计文档 — 删除完全死亡代码

**阶段目标：** 删除 fe-core `fs/` 下已确认无任何外部 caller 的死代码，编译零风险。

---

## 删除范围

### Group A：`fs/operations/` 整包（6 个文件）

| 文件 | 说明 |
|------|------|
| `operations/FileOperations.java` | 接口，无任何 production caller |
| `operations/BrokerFileOperations.java` | Broker 实现，无外部 caller |
| `operations/HDFSFileOperations.java` | HDFS 实现，无外部 caller |
| `operations/BrokerOpParams.java` | 参数类，无外部 caller |
| `operations/HDFSOpParams.java` | 参数类，无外部 caller |
| `operations/OpParams.java` | 参数基类，无外部 caller |

**验证结果：** `grep -r "import org.apache.doris.fs.operations"` → **0 结果**。包外无任何引用。

---

### Group B：`fs/spi/FileSystemSpiProvider.java`（1 个文件）

- 已被 `fe-filesystem-spi` 中的 `org.apache.doris.filesystem.spi.FileSystemProvider` 完全取代
- **验证结果：** `grep -r "import org.apache.doris.fs.spi.FileSystemSpiProvider"` → **0 结果**

---

### Group C：`fs/io/ParsedPath.java`（1 个文件）+ 两个接口的 deprecated 方法

**ParsedPath.java** 标注 `@Deprecated`，外部 production/test caller 为零。

但 `fs/io/DorisInputFile.java` 和 `fs/io/DorisOutputFile.java` 各有一个废弃的 `default path()` 方法在接口内部使用它：

```java
// DorisInputFile.java（第 38-45 行）
/**
 * Returns the path of this file as a ParsedPath object.
 * @deprecated use {@link #location()} instead
 * @return the ParsedPath representing the file location
 */
@Deprecated
default ParsedPath path() {
    return new ParsedPath(location().toString());
}

// DorisOutputFile.java（第 62-69 行）
// 同上结构
```

**验证结果：** 整个 fe 代码库中，`DorisInputFile.path()` / `DorisOutputFile.path()` 均没有任何 caller（grep `.path()` 结果均属于 Iceberg/Paimon 等第三方对象的 `.path()`，与 Doris 这两个接口无关）。

**操作方案：** 在删除 `ParsedPath.java` 的同时，一并从 `DorisInputFile.java` 和 `DorisOutputFile.java` 中删除对应的废弃 `path()` default 方法及其 import。这两个接口本身仍保留（Phase 5.2 处理）。

---

## 执行步骤

```
Step 1：git rm operations/ 整包（6 文件）
Step 2：git rm spi/FileSystemSpiProvider.java
Step 3：编辑 DorisInputFile.java，删除 @Deprecated path() 方法和 ParsedPath import
Step 4：编辑 DorisOutputFile.java，删除 @Deprecated path() 方法和 ParsedPath import
Step 5：git rm io/ParsedPath.java
Step 6：./build.sh --fe -j${DORIS_PARALLELISM} 验证编译通过
Step 7：run-fe-ut.sh 相关 UT 验证
```

---

## 风险评估

| 风险项 | 评估 |
|--------|------|
| operations/ 删除造成编译错误 | 无风险：grep 确认 0 外部引用 |
| FileSystemSpiProvider.java 删除造成编译错误 | 无风险：grep 确认 0 外部引用 |
| ParsedPath.java 删除造成编译错误 | 低风险：需同步删除 DorisInputFile/DorisOutputFile 中的 deprecated path() 方法 |
| 删除 deprecated path() 造成运行时行为变化 | 无风险：确认无任何 caller |

---

## Commit 信息

```
[chore](fe) Delete dead fs code: operations/, FileSystemSpiProvider, ParsedPath

### What problem does this PR solve?

Problem Summary: Remove completely dead code from fe-core fs/ package.
- fs/operations/ (6 files): Broker/HDFS file operations with zero external callers.
- fs/spi/FileSystemSpiProvider.java: Superseded by fe-filesystem-spi module, zero callers.
- fs/io/ParsedPath.java: @Deprecated, zero callers; also remove the @Deprecated path()
  default methods from DorisInputFile and DorisOutputFile that referenced it.

### Release note

None

### Check List (For Author)

- Test: FE unit tests (fs-related)
- Behavior changed: No
- Does this need documentation: No

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
```
