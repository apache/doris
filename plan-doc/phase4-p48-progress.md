# P4.8 Legacy Class Deletion — Progress & Remaining Work

**Branch:** `phase4-p48-deletion`  
**Design doc:** `plan-doc/phase4-p48-legacy-class-deletion-design.md`  
**Status: ALL PHASES COMPLETE ✅**

---

## All Phases Complete

| Phase | Commit | Summary |
|-------|--------|---------|
| A | `13dc273b565` | Delete `LegacyToNewFsAdapter`, `LegacyFileSystemAdapter`; remove stale `@see` javadoc |
| B | `86486c85076` | Migrate `DirectoryLister` from legacy `RemoteFile` to SPI `FileEntry`; delete `RemoteFileRemoteIterator`, `RemoteFiles`; add `modificationTime` to `FileEntry`/`RemoteObject` |
| C | `216454f1996` | Delete `MultipartUploadCapable` interface (no callers after P4.6) |
| D | `3e90dc92d0e` | Migrate `HdfsStorageVault.checkConnectivity()` to SPI; delete 5 legacy HDFS IO wrappers (`HdfsInputFile` etc.); remove dead `DFSFileSystem.newInputFile()/newOutputFile()` overrides |
| E | `9d539779e26` | Delete dead provider chain: `SwitchingFileSystem`, `FileSystemProviderImpl`, `LegacyFileSystemProviderFactory`, `FileSystemLookup` (all zero production callers) |
| F | `59884ca553c` | Delete `BrokerFileSystem`, `S3FileSystem`, `AzureFileSystem` (legacy fe-core), `StorageTypeMapper`; remove `FileSystemFactory.get()` legacy overloads; remove dead `instanceof BrokerFileSystem` branch in `HiveUtil.isSplittable()` |
| G.1+G.2 | `a844c736ccc` | Delete `OSSHdfsFileSystem`, `JFSFileSystem`, `OFSFileSystem` (zero callers); delete `ObjFileSystem` (legacy, no subclasses); fix `StageUtilTest` to use SPI `ObjFileSystem` import |
| G.3 | `8733d0e8013` | Delete `DFSFileSystem`, `DFSFileSystemPhantomReference`, `RemoteFSPhantomManager`; inline `PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH` constant and `HdfsConfiguration` creation in `ExternalCatalog`; update `HMSExternalCatalog`; delete `IcebergHadoopCatalogTest` (@Ignore, no assertions) |
| G.4–G.6 + H.1–H.2 | `c6eae8d0fbf` | Delete entire legacy hierarchy: `RemoteFileSystem`, `PersistentFileSystem`, `LegacyFileSystemApi`, `LocalDfsFileSystem`, `GlobListResult`; clean up `Repository` (remove `legacyFileSystem` field), `FileSystemDescriptor` (remove `fromPersistentFileSystem()`), `GsonUtils` (remove `buildLegacyFileSystemAdapterFactory()`); move `GlobListResult` to private inner class of `S3ObjStorage`; migrate `HiveAcidTest` file-creation fixtures to Java NIO |

---

## Files Deleted (Total Across All Phases)

### Production source
| File | Phase |
|------|-------|
| `fs/LegacyToNewFsAdapter.java` | A |
| `fs/LegacyFileSystemAdapter.java` | A |
| `fs/RemoteFileRemoteIterator.java` | B |
| `fs/RemoteFiles.java` | B |
| `fs/remote/MultipartUploadCapable.java` | C |
| `fs/io/hdfs/HdfsInputFile.java` | D |
| `fs/io/hdfs/HdfsOutputFile.java` | D |
| `fs/io/hdfs/HdfsInputStream.java` | D |
| `fs/io/hdfs/HdfsOutputStream.java` | D |
| `fs/io/hdfs/HdfsInput.java` | D |
| `fs/FileSystemLookup.java` | E |
| `fs/FileSystemProviderImpl.java` | E |
| `fs/LegacyFileSystemProviderFactory.java` | E |
| `fs/remote/SwitchingFileSystem.java` | E |
| `fs/StorageTypeMapper.java` | F |
| `fs/remote/BrokerFileSystem.java` | F |
| `fs/remote/S3FileSystem.java` | F |
| `fs/remote/AzureFileSystem.java` | F |
| `fs/remote/dfs/OSSHdfsFileSystem.java` | G.1 |
| `fs/remote/dfs/JFSFileSystem.java` | G.1 |
| `fs/remote/dfs/OFSFileSystem.java` | G.1 |
| `fs/remote/ObjFileSystem.java` | G.2 |
| `fs/remote/dfs/DFSFileSystem.java` | G.3 |
| `fs/remote/dfs/DFSFileSystemPhantomReference.java` | G.3 |
| `fs/remote/dfs/RemoteFSPhantomManager.java` | G.3 |
| `fs/remote/RemoteFileSystem.java` | G.4 |
| `fs/PersistentFileSystem.java` | G.5 |
| `fs/LegacyFileSystemApi.java` | G.5 |
| `fs/LocalDfsFileSystem.java` | G.5 |
| `fs/GlobListResult.java` | G.6 |

### Test source
| File | Phase |
|------|-------|
| `backup/BrokerStorageTest.java` | F |
| `fs/obj/S3FileSystemTest.java` | F |
| `external/iceberg/IcebergHadoopCatalogTest.java` | G.3 |
| `fs/remote/RemoteFileSystemTest.java` | G.4 |

---

## Key Design Decisions Made During Implementation

### Phase D blocker resolution (G.3)
`ExternalCatalog` and `HMSExternalCatalog` used `DFSFileSystem.PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH`
and `DFSFileSystem.getHdfsConf(boolean)`. Because `fe-core` does not depend on `fe-filesystem-hdfs`,
these could not be moved to `HdfsConfigBuilder`. Resolution: **inlined the constant string** 
`"ipc.client.fallback-to-simple-auth-allowed"` and the 3-line `HdfsConfiguration` building
directly in `ExternalCatalog.buildConf()`, with `HdfsConfiguration` added to imports.

### GlobListResult (G.6)
Only used internally by `S3ObjStorage` after `LegacyFileSystemApi` was deleted. Rather than
a risky refactor to remove the pagination struct, it was moved as a **private static inner class**
of `S3ObjStorage`, preserving all existing behavior.

### HiveAcidTest migration (G.5)
`LocalDfsFileSystem.createFile()` was replaced with a private static `createFile(String fileUri)`
helper using `java.nio.file.Files` — `createDirectories()` + `createFile()`. The test already
imported `java.nio.file.Files` and `java.nio.file.Path`.

### Repository backward-compat (H.1)
The `@SerializedName("rfs") PersistentFileSystem legacyFileSystem` backward-compat field was
removed. GSON silently ignores unknown JSON fields, so old metadata with a `"rfs"` key will
now skip the legacy deserialization path. Clusters at this version will have already written
`"fs_descriptor"` format in any new repository operations.


---
