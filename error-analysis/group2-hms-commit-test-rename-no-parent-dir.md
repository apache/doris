# Group 2 — HmsCommitTest: LocalFileSystem.rename() Fails When Parent Directory Missing

## Affected Tests (10 failures)
All 10 methods in `HmsCommitTest`:
- `testCommitInsert_*`
- `testCommitDropPartition_*`
- etc.

## Symptom
```
java.lang.RuntimeException: src -> dst
    at LocalFileSystem.rename(LocalFileSystem.java:92)
Caused by: java.nio.file.NoSuchFileException: /tmp/test_write_xxx/{uuid}/c3=a -> /tmp/test_warehouse_xxx/{uuid}/c3=a
    at LocalFileSystem.rename(LocalFileSystem.java:91)
```

## Root Cause

**File:** `fe/fe-filesystem/fe-filesystem-local/src/main/java/org/apache/doris/filesystem/local/LocalFileSystem.java`

`LocalFileSystem.rename()` calls `Files.move(src, dst)` without creating the parent directories of `dst` first:

```java
@Override
public void rename(Location src, Location dst) throws IOException {
    try {
        Files.move(toPath(src), toPath(dst));   // ← fails if dst parent doesn't exist
    } catch (IOException e) {
        throw new IOException(src + " -> " + dst, e);
    }
}
```

`Files.move()` on Java's local filesystem requires that the **parent directory of `dst`** already exists. In the test, the destination warehouse directory structure is not pre-created, so moving into it fails with `NoSuchFileException`.

## Call Chain
```
HmsCommitTest → HMSTransaction.commit()
  → HMSTransaction.renameDirectory()
  → SpiSwitchingFileSystem.renameDirectory()
  → default FileSystem.renameDirectory() [renames path-by-path]
  → LocalFileSystem.rename(src, dst)
  → Files.move(srcPath, dstPath)      ← FAILS: dstPath parent doesn't exist
```

## Fix

Create parent directories before calling `Files.move()`:

```java
@Override
public void rename(Location src, Location dst) throws IOException {
    try {
        Path dstPath = toPath(dst);
        Files.createDirectories(dstPath.getParent());
        Files.move(toPath(src), dstPath);
    } catch (IOException e) {
        throw new IOException(src + " -> " + dst, e);
    }
}
```

## Impact
Production code bug. In production S3/HDFS renames, the parent is implicitly created by the cloud API. For `LocalFileSystem` (used in unit tests), it must be explicit. This means all HMS commit tests that do directory renames on the local filesystem will fail until fixed.
