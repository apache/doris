# FIX-HIVEFS-4 — connector read-scan file listing: bare Hadoop `FileSystem` → engine-injected `org.apache.doris.filesystem.FileSystem`

> Substep of FIX-HIVEFS (parent design `FIX-HIVEFS-design.md`, task list `task-list-HIVEFS.md`). Scope = the **non-ACID read file-listing path only** (`HiveFileListingCache` + its 3 callers). ACID (`planAcidScan:272` / `HiveAcidUtil`) is HIVEFS-5; write path is HIVEFS-6.
> Line numbers verified against HEAD (`14169366b76`).

## Goal / success criteria

- `HiveFileListingCache.listFromFileSystem` no longer calls bare `org.apache.hadoop.fs.FileSystem.get` / `listStatus`; it lists through the engine-injected `org.apache.doris.filesystem.FileSystem` (a per-catalog `SpiSwitchingFileSystem`).
- Byte-parity of the produced `HiveFileStatus` list (path string, length, mtime; dir + `_`/`.` filter; zero-length kept) with the old lister.
- The two failure semantics preserved: SYSTEMIC (unresolvable filesystem/scheme) → loud `DorisConnectorException`; LOCAL (this partition dir missing/unreadable) → skippable `HiveDirectoryListingException`.
- `HIVEFS-4 + HIVEFS-3 + redeploy` turns the failing regression `test_string_dict_filter` green.
- Build + 0 checkstyle + targeted UT (adapted `HiveFileListingCacheTest`) all green, all RED-able.

## Surface (verified against HEAD)

`fileListingCache.listDataFiles(db, table, location, Configuration)` has exactly 3 callers, all with `ConnectorSession` in scope:
1. `HiveScanPlanProvider.listAndSplitFiles:449` (non-ACID scan hot path). Provider does NOT hold `ConnectorContext`.
2. `HiveConnectorMetadata.listFileSizes:846` (`ANALYZE ... WITH SAMPLE`; loud on error). Holds `context`.
3. `HiveConnectorMetadata.sumCachedFileSizes:963` ← `estimateDataSizeByListingFiles:798` (row-count estimate; best-effort `-1`). Holds `context`.

`ConnectorContext.getFileSystem(ConnectorSession)` (HIVEFS-2) returns the per-catalog cached `SpiSwitchingFileSystem`, or `null` when the catalog has no storage (HIVEFS-3).

## Design decisions

### D1 — Seam signature: `Configuration` → `org.apache.doris.filesystem.FileSystem`
- `DirectoryLister.list(String location, Configuration)` → `list(String location, FileSystem fs)`.
- `listDataFiles(db, table, location, Configuration)` → `listDataFiles(db, table, location, FileSystem fs)`.
- The `FileSystem` is the per-catalog switching FS; passed by each caller from `context.getFileSystem(session)`. It is NOT part of the cache key (key stays `(db, table, location)`) — there is exactly one FS per catalog, constant across calls, so it never varies for a given key.

### D2 — `listFromFileSystem` new body (v2, red-team folded in): literal `list()` + split resolution/listing + lazy-systemic re-raise
```java
static List<HiveFileStatus> listFromFileSystem(String location, FileSystem fs) {
    if (fs == null) {                                        // D4 null-FS guard (systemic, loud)
        throw new DorisConnectorException("No filesystem configured for " + location);
    }
    Location loc = Location.of(location);
    FileSystem resolved;
    try {
        resolved = fs.forLocation(loc);                      // SYSTEMIC boundary: scheme/storage resolution + FS construction (no I/O)
    } catch (IOException | RuntimeException e) {              // [Minor-1] broadened: FactoryFactory may throw RuntimeException
        throw new DorisConnectorException("Failed to resolve filesystem for " + location, e);
    }
    try {
        List<HiveFileStatus> files = new ArrayList<>();
        // resolved.list(loc) — the LITERAL, non-glob listing (== old fs.listStatus). NOT listFiles(loc):
        // DFSFileSystem/S3CompatibleFileSystem override listFiles with a glob-aware branch that would treat a
        // location containing '[','*','?' as a PATTERN. [Major-1] Old listStatus never glob-expanded.
        try (FileIterator it = resolved.list(loc)) {
            while (it.hasNext()) {
                FileEntry e = it.next();
                if (e.isDirectory()) {                       // dir filter (== old !status.isDirectory())
                    continue;
                }
                String name = e.name();
                if (name.startsWith("_") || name.startsWith(".")) {   // hive hidden/marker filter
                    continue;
                }
                files.add(new HiveFileStatus(e.location().uri(), e.length(), e.modificationTime()));
            }
        }
        return files;
    } catch (IOException e) {
        // [Major-2] A lazily-surfaced "No FileSystem for scheme X" (UnsupportedFileSystemException) is a SYSTEMIC
        // storage/packaging error affecting every partition — the migration's OWN failure class — and must stay
        // LOUD (old FileSystem.get threw it loud). Everything else (dir missing/unreadable/perm) is LOCAL/skippable.
        if (isSystemicResolutionFailure(e)) {
            throw new DorisConnectorException("Failed to resolve filesystem for " + location, e);
        }
        throw new HiveDirectoryListingException("Failed to list files under " + location, e);
    }
}

// Walks the cause chain (robust to authenticator.doAs wrapping) for the scheme-not-registered systemic class.
private static boolean isSystemicResolutionFailure(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
        if (c instanceof UnsupportedFileSystemException) {
            return true;
        }
        String msg = c.getMessage();
        if (msg != null && msg.contains("No FileSystem for scheme")) {
            return true;
        }
        if (c.getCause() == c) {                              // guard self-referential cause
            break;
        }
    }
    return false;
}
```
**Why `forLocation` then `list` (not a single `listFiles`)**: on `SpiSwitchingFileSystem`, `listFiles(dir)` = `forLocation(dir).listFiles(dir)` — one call, so it cannot distinguish a resolution failure from a listing failure. Splitting maps `forLocation` (= `forPath` → `LocationPath.of` + `FileSystemFactory.getFileSystem`, i.e. resolution + FS *construction*, **no I/O**) to the old `FileSystem.get` SYSTEMIC boundary, and `resolved.list` (= `DFSFileSystem.list` → `getHadoopFs` → the actual Hadoop `FileSystem.get` + `listStatusIterator`) to the old `listStatus` LOCAL boundary. **Residual (accepted, documented)**: bad-namenode-host / connect failures surface at `list` = LOCAL/skippable. Old code's classification of these was Hadoop-version-dependent (connect is lazy at first RPC), so this is not a clear regression; only the *deterministic scheme-not-registered* class is force-classified systemic (via `isSystemicResolutionFailure`).

### D3 — filter + field parity
- `resolved.listFiles(loc)` = non-recursive, directories excluded (contract; default impl iterates `list()` filtering `isDirectory`) — matches old `listStatus` + `!isDirectory` filter.
- `_`/`.` filter applied on `FileEntry.name()` (last path segment) — matches old `status.getPath().getName()` prefix check.
- Zero-length files kept (splitter skips size==0; estimate adds 0) — `listFiles` returns them.
- Path string: `FileEntry.location().uri()` == `HdfsFileIterator`'s `Location.of(status.getPath().toString())` == old `status.getPath().toString()`. **`Path.toString()`, not `toUri().toString()`** → no `%3A` double-encoding regression for hive timestamp partition dirs. `length()`/`modificationTime()` map 1:1.

### D4 — null-FS guard (fail loud, systemic)
`context.getFileSystem(session)` returns `null` only for a catalog with no storage (HIVEFS-3). A plain-hive catalog always has storage, but guard defensively: `null` FS → loud `DorisConnectorException` (systemic — affects every partition). For the estimate path this is caught by `estimateDataSize`'s `catch (RuntimeException) → -1` (best-effort, unchanged); for scan / `listFileSizes` it fails the query loud (Rule 12), never a silent empty scan.

### D5 — plumbing
- `HiveScanPlanProvider`: add `ConnectorContext context` ctor arg (3rd, mirroring `HiveConnectorMetadata`; HiveConnector already holds it). Resolve `FileSystem fs = context.getFileSystem(session)` once in `planScan` / `planScanForPartitionBatch`, thread `fs` down to `listAndSplitFiles` → `listDataFiles`. `buildHadoopConf()` STAYS (still used by `planAcidScan:272`, HIVEFS-5) — so `HiveScanPlanProvider`'s `Configuration`/`FileStatus`/`FileSystem`/`Path` imports also STAY. It is no longer passed to `listAndSplitFiles`. In `planScanForPartitionBatch` (non-ACID only) the local `hadoopConf` becomes unused → drop that line.
- `HiveConnectorMetadata.listFileSizes`: resolve `FileSystem fs = context.getFileSystem(session)` under the EXISTING TCCL pin (ANALYZE path, loud by contract — resolving outside the estimate try is fine), pass to `listDataFiles`.
- `HiveConnectorMetadata.estimateDataSizeByListingFiles`: **[Minor-3]** resolve the FS INSIDE `estimateDataSize`'s protected region — pass `context.getFileSystem(session)` inside the size lambda (`location -> sumCachedFileSizes(hiveHandle, location, context.getFileSystem(session))`) so any throw from `getFileSystem` degrades to `-1` (statistics must not fail a query), not loud. `getFileSystem` is a cached field-return so per-location calls are cheap.
- `buildHadoopConf()` in `HiveConnectorMetadata` is used only by these two methods → remove both usages, the orphan private `buildHadoopConf()` method, AND **[Minor-2]** the now-unused `import org.apache.hadoop.conf.Configuration;` (checkstyle UnusedImports). `sumCachedFileSizes` param type flips `Configuration` → `org.apache.doris.filesystem.FileSystem`.

### D6 — TCCL: keep the existing metadata-layer pins (rationale corrected per red-team)
`listFileSizes` / `estimateDataSizeByListingFiles` already pin the TCCL to the plugin loader (the stats thread is not pinned by fe-core). KEEP those pins. **Corrected rationale [Minor-4]**: the pin protects TCCL-*reflective* config-class loading (Hadoop `ServiceLoader`/`Configuration` reads the TCCL). The engine `DFSFileSystem`/`UserGroupInformation`/`SecurityUtil` classes are resolved by the **fe-filesystem-hdfs** plugin loader (their *defining* loader), not the hive-plugin loader the caller thread is pinned to — so the pin does not itself pick the hadoop copy. The construction path (first `forLocation` lazily builds a `DFSFileSystem` via `SpiSwitchingFileSystem` → `FileSystemFactory`) is **identical to what paimon/iceberg and every other engine FileSystem consumer already trigger in production today** — HIVEFS-4 adds no new classloader locus. `DFSFileSystem.getHadoopFs` additionally self-pins the fe-filesystem-hdfs loader for hdfs/viewfs around the actual `FileSystem.get` (the scheme-reflection point). Simple-auth `createRemoteUser` does no TCCL class reflection (benign); the kerberos-HDFS / custom `hadoop.security.dns` paths ride the same shared construction path already exercised. No NEW connector pin is added.

## Test plan (connector module has NO Mockito → hand-written recording fake)

**[Major-3] Full blast radius — 6 test files touch the changed seam/ctor; ALL must compile+pass for "build green":**
- New shared helper `FakeFileSystem` (package-private test class): a recording `org.apache.doris.filesystem.FileSystem` — `forLocation` returns `this` or throws (configurable); `list(Location)` returns a canned in-memory `FileIterator` over set `FileEntry`s or throws (configurable IOException, incl. an `UnsupportedFileSystemException` for the systemic case); all other methods `throw new UnsupportedOperationException`. Reused across the tests below.
- `HiveConnectorInvalidateTest`: replace the `CONF`/`@TempDir`+`Files.write` real-local-listing with a `FakeFileSystem` (list succeeds → leaves a cache entry; `size()` assertions unchanged). `listDataFiles(...,CONF)` → `listDataFiles(...,fakeFs)`.
- `HiveScanBatchModeTest`: `CountingLister.list(String,Configuration)` → `(String,FileSystem)`; `HiveScanPlanProvider` ctor gains `context` (a `new FakeConnectorContext()` — null FS is fine, the fake lister ignores it).
- `HiveConnectorMetadataFileListStatsTest`: `ThrowingFileListingCache.listDataFiles(...,Configuration)` override → `(...,FileSystem)`.
- `HiveReadTransactionTest`: `HiveScanPlanProvider` ctor gains `context` (ACID path; context unused for HIVEFS-4).

Adapt `HiveFileListingCacheTest`:
- Replace the `CONF` constant + `CountingLister.list(String, Configuration)` with a `FileSystem`-typed seam; `CountingLister` becomes a `DirectoryLister` returning canned `HiveFileStatus` (unchanged — it is above the FS seam).
- Add a hand-written fake `org.apache.doris.filesystem.FileSystem` (a "RecordingFs") for the `listFromFileSystem`-level tests: override `forLocation` (return this or throw) + `listFiles` (return canned `FileEntry`s / throw `IOException`); all other methods throw `UnsupportedOperationException`.
- Rework the 4 real-lister tests to drive `listFromFileSystem(location, fakeFs)`:
  - filter test: fake `listFiles` returns `[data1, data2, _SUCCESS, .hidden]` → assert only `data1,data2` (300 bytes) survive.
  - `forLocation` throws IOException → assert loud `DorisConnectorException` (NOT `HiveDirectoryListingException`).
  - `listFiles` throws IOException → assert skippable `HiveDirectoryListingException`.
  - through-cache variants: assert type survives the cache boundary + `size()==0` (failure not cached).
- The scan-integration tests (`scanSkipsOnlyTheFailedPartition...`, `scanFailsLoudOnSystemicFilesystemFailure`, `scanProviderServesRepeatedScansFromTheCache`) update the `HiveScanPlanProvider` ctor to pass a fake `ConnectorContext` whose `getFileSystem` returns the fake FS (or null for a null-FS test).
- `estimateDataSizeIsServedFromTheCache`: `FakeConnectorContext.getFileSystem` returns the fake FS.
- Every assertion must be RED-able (revert the mutation → red).

## Residual / deferred
- ACID `planAcidScan:272` + `HiveAcidUtil` bare Hadoop → HIVEFS-5.
- S3/OSS exception-split behavior: their FS construction may connect/validate creds eagerly (→ bad creds surface at `forLocation` = SYSTEMIC loud, which is *more* correct than old lazy behavior, not a regression). Flag for the read red-team.
- per-user identity: `getFileSystem(session)` still catalog-level (session ignored) — unchanged from HIVEFS-2/3.
