# Group 3 — SPI Provider Not Found During Tests (6 failures)

## Affected Tests (6 failures)
- `BackupJobTest.testRunNormal`
- `BackupJobTest.testRunWithTableDroppedDuringSnapshoting`
- `BrokerLoadPendingTaskTest.testExecuteTask`
- `CopyLoadPendingTaskTest.testParseFileForCopyJob`
- `CopyLoadPendingTaskTest.testParseFileForCopyJobV2`
- `CreateRepositoryCommandTest.testS3RepositoryPropertiesConverter`

## Common Symptom
```
FileSystemFactory: loaded 1 SPI provider(s): LOCAL
...
No FileSystemProvider supports the given properties: S3. Registered providers: [LOCAL]
```
or similar IOException about provider not found.

In FE unit tests, only `fe-filesystem-local` is on the classpath (LOCAL provider). No S3/OSS/etc. provider exists in the test classpath.

---

## Sub-cause A: Repository Constructor — `CreateRepositoryCommandTest` (1 test)

### Root Cause
`Repository(id, name, readOnly, location, storageProperties)` constructor (line 179–186) calls `FileSystemFactory.getFileSystem(storageProperties)` unconditionally for non-BROKER repositories:
```java
if (fileSystemDescriptor.getStorageType() != FsStorageType.BROKER) {
    try {
        this.spiFs = FileSystemFactory.getFileSystem(storageProperties);  // ← fails with S3
    } catch (IOException e) {
        throw new IllegalStateException(...);
    }
}
```
`ping()` (line 503) has a `FeConstants.runningUnitTest` bypass, but the **constructor** does not. The test fails before `ping()` is ever called.

### Fix
Add `FeConstants.runningUnitTest` bypass in the constructor:
```java
if (fileSystemDescriptor.getStorageType() != FsStorageType.BROKER
        && !FeConstants.runningUnitTest) {
    try {
        this.spiFs = FileSystemFactory.getFileSystem(storageProperties);
    } catch (IOException e) {
        throw new IllegalStateException(...);
    }
}
```

---

## Sub-cause B: BrokerLoadPendingTaskTest — stale mock (1 test)

### Root Cause
The test mocks `BrokerUtil.parseFile()`, but after the SPI refactoring the code now calls `FileSystemFactory.getFileSystem(brokerDesc)` for non-broker paths (or the underlying code calls into the SPI factory). The `BrokerUtil.parseFile` mock is now dead code and the real code fails when it tries to get an S3/OSS provider.

### Fix
Add a `MockUp<FileSystemFactory>` in the test or adjust the mock to use the new code path. The simplest fix is to mock the `FileSystemFactory` to return a mock filesystem for the test input.

---

## Sub-cause C: BackupJobTest — Repository mock doesn't cover new method (2 tests)

### Root Cause
`BackupJob.runNormal()` calls `repo.getFileSystemDescriptor().getBackendConfigProperties()`. The existing `MockUp<Repository>` only mocks `upload()` and `getBrokerAddress()`. The `getBackendConfigProperties()` call goes to real code which calls `StorageProperties.createPrimary(properties)`, which fails because the empty BrokerProperties map doesn't match any storage type.

### Fix
Add a mock for `Repository.getFileSystemDescriptor()` to return a stub descriptor (or mock `FileSystemDescriptor.getBackendConfigProperties()` directly) that returns an empty map.

---

## Sub-cause D: CopyLoadPendingTaskTest — `FileSystemFactory.getFileSystem()` fails before ObjFileSystem mock (2 tests)

### Root Cause
The test sets up `MockUp<ObjFileSystem>` to mock `listObjectsWithPrefix()`. However, the code calls `FileSystemFactory.getFileSystem(storageProperties)` which tries to find an OSS/S3 `FileSystemProvider`. Since only `LOCAL` is on the classpath, this throws an IOException before any `ObjFileSystem` is ever created — so the mock is never reached.

### Fix
Add a `MockUp<FileSystemFactory>` to short-circuit `getFileSystem()` and return a concrete `ObjFileSystem` stub, OR move the test to use dependency injection so the factory can be controlled without SPI.
