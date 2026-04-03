# Group 1 — HiveAcidTest: Directory Entries Never Returned from globList

## Affected Tests (9 failures)
All 9 methods in `HiveAcidTest`:
- `testGetAcidState_fullAcid_*`
- `testGetAcidState_insertOnly_*`
- etc.

## Symptom
All 9 tests throw `AssertionError`. Example:
```
java.lang.AssertionError: expected:<2> but was:<0>
    at HiveAcidTest.testGetAcidState_fullAcid_base(HiveAcidTest.java:89)
```

## Root Cause

**File:** `fe/fe-filesystem/fe-filesystem-api/src/main/java/org/apache/doris/filesystem/FileSystemTransferUtil.java`

`collectEntries()` (lines 109–126) **never adds directory entries to the result list**:

```java
private static void collectEntries(FileSystem fs, Location base,
        Pattern pattern, boolean recursive,
        List<FileEntry> result) throws IOException {
    try (FileIterator iter = fs.list(base)) {
        while (iter.hasNext()) {
            FileEntry entry = iter.next();
            if (entry.isDirectory()) {
                if (recursive) {
                    collectEntries(fs, entry.location(), pattern, true, result);
                }
                // BUG: directory entries are NEVER added to result, even when matching the pattern
            } else {
                if (pattern == null || pattern.matcher(entry.location().uri()).matches()) {
                    result.add(entry);
                }
            }
        }
    }
}
```

`AcidUtil.getAcidState()` calls:
```java
List<FileEntry> entries = FileSystemTransferUtil.globList(fileSystem, partitionPath + "/*", false);
```

It then iterates `entries` and checks `entry.isDirectory()` (at `AcidUtil.java:266`) to find `base_` and `delta_` subdirectories. Because `collectEntries` never adds directories to `result`, the list is always empty, causing the assertions to fail.

## Fix

In `collectEntries`, add directory entries to the result when the pattern matches (same as files):

```java
if (entry.isDirectory()) {
    if (pattern == null || pattern.matcher(entry.location().uri()).matches()) {
        result.add(entry);   // ← add: directories matching pattern belong in result
    }
    if (recursive) {
        collectEntries(fs, entry.location(), pattern, true, result);
    }
} else {
    if (pattern == null || pattern.matcher(entry.location().uri()).matches()) {
        result.add(entry);
    }
}
```

## Impact
Production code bug; this would also affect the real Hive ACID scan path.
