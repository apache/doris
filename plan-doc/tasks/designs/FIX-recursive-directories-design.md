# FIX — restore `hive.recursive_directories` (recursive partition-dir listing)

**Status:** design → (red-team) → implement → UT → commit
**Scope:** connector-local (`fe-connector-hive`), **zero fe-core change**, zero SPI change.
**Suite:** `external_table_p0/hive/hive_config_test` tags `2` and `21` (and the ENV tag `1`, separate).

---

## 1. Problem / root cause (HEAD-verified)

After the hms flip, a plugin-driven hive catalog lists partition data files through
`HiveFileListingCache.listFromFileSystem` (fe-connector-hive). That loader is **unconditionally
non-recursive**: it iterates `resolved.list(loc)` once, `continue`s on every directory entry
(`HiveFileListingCache.java:196`), and never descends. It also **never reads** the catalog property
`hive.recursive_directories`.

Legacy fe-core honored it: `HiveExternalMetaCache.getFileCache:390-397` read
`catalog.getProperties().getOrDefault("hive.recursive_directories", "true")` (**default true**) and
passed it to `directoryLister.listFiles(fs, isRecursiveDirectories, …)` → `FileSystemDirectoryLister`
→ `FileSystemTransferUtil.globList(fs, location, recursive)` → `collectEntries(…, recursive, …)`
(`FileSystemTransferUtil.java:109-130`), which recurses into subdirectories when `recursive`.

Consequence on a table whose data lives in subdirectories: the connector returns only the top-level
files → **silent missing rows** (data loss), not an error.

### Acceptance criterion (the test)
`hive_recursive_directories_table` (built by `hive_config_test.groovy` via `INTO OUTFILE`) has data at
three levels — top-level `exp_*`, subdir `1/exp_*`, subdir `2/exp_*` (subdir names `1`,`2` and file
names `exp_*` are **non-hidden**). Golden `hive_config_test.out`:

| tag | property | expected rows |
|---|---|---|
| `1`  | `hive.recursive_directories=false` | 2 (top-level only) — **already correct today** |
| `2`  | `hive.recursive_directories=true`  | 6 (top + `1/` + `2/`) — **broken today, returns 2** |
| `21` | (default, absent → true)           | 6 — **broken today, returns 2** |

So: the fix must make `recursive=true`/default descend into subdirectories; `recursive=false` must stay
byte-identical to today (top-level only).

---

## 2. Design

All three readers (`HiveScanPlanProvider.listAndSplitFiles`, `HiveConnectorMetadata.sumCachedFileSizes`
for `estimateDataSizeByListingFiles`, and the cache itself) go through the **same** shared
`HiveFileListingCache.listDataFiles` → `DirectoryLister.list` → `listFromFileSystem`. The recursive flag
is a **per-catalog constant**, so bake it into the cache instance and they are consistent by
construction — no per-consumer change, no signature change on the hot path.

### 2.1 Parse the flag once, in the cache ctor
`HiveFileListingCache.java`:
- Add `static final String RECURSIVE_DIRECTORIES_PROPERTY = "hive.recursive_directories";`
- Public ctor `HiveFileListingCache(Map<String,String> properties)` builds the production lister as a
  lambda that captures the parsed flag:
  ```java
  public HiveFileListingCache(Map<String, String> properties) {
      this(properties, defaultLister(properties));
  }
  private static DirectoryLister defaultLister(Map<String, String> properties) {
      Map<String, String> props = properties == null ? Collections.emptyMap() : properties;
      boolean recursive = Boolean.parseBoolean(props.getOrDefault(RECURSIVE_DIRECTORIES_PROPERTY, "true"));
      return (location, fs) -> listFromFileSystem(location, fs, recursive);
  }
  ```
  `Boolean.parseBoolean` matches legacy `Boolean.valueOf` semantics (only "true" case-insensitive → true;
  default string "true").
- The `DirectoryLister` functional interface stays `(location, fs)` — test injection unchanged.

### 2.2 Recursive-aware loader
Introduce `listFromFileSystem(String location, FileSystem fs, boolean recursive)` and keep the existing
2-arg `listFromFileSystem(String, FileSystem)` delegating to `(…, false)` (the 5 existing direct-call
tests assert non-recursive dir-skipping and use a flat, non-tree fake — they must keep testing exactly
today's behavior; recursion through that flat fake would loop).

The top-level failure classification (systemic-loud vs local-skippable, null-fs) is **unchanged**; only
the inner listing loop is factored into a recursive helper so a sub-listing `IOException` bubbles to the
SAME outer classifier (a failure anywhere under the partition → the partition's one systemic/local
verdict, exactly as before):

```java
static List<HiveFileStatus> listFromFileSystem(String location, FileSystem fs, boolean recursive) {
    // ... unchanged null-fs guard + forLocation resolution (systemic) ...
    try {
        List<HiveFileStatus> files = new ArrayList<>();
        collectFiles(resolved, loc, recursive, files);
        return files;
    } catch (IOException e) {
        // ... unchanged isSystemicResolutionFailure split ...
    }
}

private static void collectFiles(FileSystem resolved, Location dir, boolean recursive,
        List<HiveFileStatus> out) throws IOException {
    try (FileIterator it = resolved.list(dir)) {
        while (it.hasNext()) {
            FileEntry entry = it.next();
            String name = entry.name();
            if (entry.isDirectory()) {
                if (recursive && !isHidden(name)) {
                    collectFiles(resolved, entry.location(), true, out);   // reuse resolved FS (same scheme)
                }
                continue;
            }
            if (isHidden(name)) {
                continue;
            }
            out.add(new HiveFileStatus(entry.location().uri(), entry.length(), entry.modificationTime()));
        }
    }
}

private static boolean isHidden(String name) {
    return name.startsWith("_") || name.startsWith(".");
}
```

Notes:
- `entry.name()` returns the last path component (handles the dir trailing slash) — correct for both
  file and dir hidden checks (`FileEntry.java:72`).
- Descent reuses the already-resolved `resolved` FS (the subdir shares scheme/authority) — cheaper and
  avoids a second `forLocation`.
- **Non-recursive path is byte-identical**: with `recursive=false`, directories hit `continue` (no
  descent), files pass the same `_`/`.` filter — the exact current loop, just extracted.

### 2.3 Hidden-directory policy (deliberate) — EXACT legacy net parity (red-team verified)
When recursing, **skip hidden subdirectories** (`_`/`.` prefixed — e.g. `_temporary`, `.hive-staging`),
mirroring the existing leaf-file filter. This is **exact net parity with legacy**, not a deviation:
- Legacy `HiveExternalMetaCache.getFileCache` → `globList`/`collectEntries` **does** descend into ALL
  directories (including hidden), BUT every returned file then passes the mandatory post-filter
  `isFileVisible` → `containsHiddenPath` (`HiveExternalMetaCache.java:1005-1025`), which drops any file
  whose **FULL path** contains a `_`/`.`-prefixed component (`startsWith(".")||("_")`, or any `/.`/`/_`).
  So legacy's NET file set already excludes every file under a hidden subdirectory. Skipping hidden dirs
  up front produces the identical net set.
- **Descend-all is REGRESSED here, not more-legacy.** The connector filters by the **leaf**
  `entry.name()` only (`HiveFileListingCache.java:199-202`), never the full path. So "descend into all
  dirs + leaf-only filter" would surface `_temporary/part-0` / `.hive-staging/…` files that legacy
  suppresses — a real data-correctness regression. Skip-hidden-dirs is the parity-safe choice.
- The test does not pin this either way (its subdirs `1`,`2` are non-hidden), so the choice is
  golden-safe; §4 adds a test that pins it.

---

## 3. Consumers / parity (why one change suffices)
`HiveConnector` builds a single `HiveFileListingCache` (`:116`) and injects the SAME instance into the
scan provider (`:173`) and metadata (`:139-141`). Both `HiveConnectorMetadata` ctors parse the flag from
the same catalog `properties`. Therefore:
- **scan split** (`HiveScanPlanProvider:473` `listAndSplitFiles` → `listDataFiles`) — recursive.
- **size estimate** (`HiveConnectorMetadata:1003` `sumCachedFileSizes` → `listDataFiles`) — recursive,
  consistent with scan (so cardinality/bytes match the rows actually scanned).
- **stats sampling** (`HiveConnectorMetadata:886` `listFileSizes` → `listDataFiles`, the
  `ANALYZE … WITH SAMPLE` / legacy `getChunkSizes` port) — also becomes recursive via the same shared
  instance. Legacy parity holds: `getChunkSizes → getFilesForPartitions → getFileCache` honored
  `hive.recursive_directories` too, so scan and stats stay consistent.
- **cache** — keyed by `(db, table, location)`; the loader now returns the recursive listing; invalidation
  unchanged.

Out of scope (unchanged): the **ACID** path (`HiveAcidUtil` — its own base/delta descent, legacy
`recursive_directories` never applied to it) and the **write** path (`HiveConnectorTransaction`).

---

## 4. Tests (`HiveFileListingCacheTest`)
Add a **tree-aware** capability to `FakeFileSystem`. Two **load-bearing invariants** (drop either and the
existing flat-fake subdir test recurses INFINITELY, because flat `list()` re-returns the same entries for
any location):
- **(a)** the retained 2-arg `listFromFileSystem(String,FileSystem)` MUST delegate to `(…, false)`.
- **(b)** `FakeFileSystem.list()` MUST consult the tree map ONLY when it is populated; otherwise return
  the flat `entries` iterator **unchanged** (existing tests untouched).
Also add a **per-location** list error to the tree fake (the current single `listError` fails EVERY
`list()`, so it cannot model "top succeeds, one subdir fails"). New imports (`Map`/`HashMap`) go in the
`CustomImportOrder` alphabetical slot (`checkstyle.xml`).

New tests:
1. `recursiveDescendsIntoSubdirectories` — tree: top has `exp_a` + dirs `1`,`2`; `1` has `exp_b`; `2` has
   `exp_c`. `listFromFileSystem(top, fs, true)` → 3 files (a,b,c).
2. `nonRecursiveListsTopLevelOnly` — same tree, `recursive=false` → 1 file (`exp_a`). Pins the tag-`1`
   behavior and that `false` never descends.
3. `recursiveSkipsHiddenSubdirectoriesAndFiles` — tree with `_temporary/` (containing a file) and a
   top-level `.hidden` file → both excluded; only real files survive. Pins the §2.3 policy.
4. `defaultIsRecursive` / `recursiveFlagFalseFromProperty` — via the public ctor:
   `new HiveFileListingCache(emptyMap())` and `new HiveFileListingCache(props("hive.recursive_directories","false"))`
   drive a tree through `listDataFiles`; assert 3 vs 1 files. **This is the genuine RED-against-literal-HEAD
   guarantee**: today's non-recursive lister returns 1 where the default-true assertion expects 3. Pins
   default=true (tag `21`) and the property wiring (tag `2` vs tag `1`).
5. `recursiveSubdirListingFailureIsSkippable` — healthy top-level dir containing a subdir whose `list()`
   throws `FileNotFoundException` → assert the skippable `HiveDirectoryListingException` propagates,
   proving a **nested-descent** failure reaches the single outer classifier
   (`HiveFileListingCache.java:208-219`). Pins red-team seed #2 (a future swallowing/reclassifying catch
   around the recursion is caught RED).

RED-ability note: tests #1–#3 and #5 call the NEW 3-arg `listFromFileSystem` overload (absent at HEAD), so
they RED only against a signature-added-but-non-recursive stub — standard TDD for a new overload. Test #4
is the one that REDs against literal HEAD through the public ctor + `listDataFiles`.

All existing `HiveFileListingCacheTest` cases remain (the flat-fake, non-recursive direct-call tests keep
pinning current behavior). fe-connector-hive full UT must stay green; 0 checkstyle.

---

## 5. Iron-rule / architecture compliance
- **fe-core untouched**, **SPI untouched** — pure connector-local behavior restoration.
- No `if(hive)` anywhere; the flag is a hive catalog property parsed only inside the hive connector.
- Trino parity: Trino's `CachingDirectoryLister` / `hive.recursive-directories` is likewise a
  connector-side listing knob; fe-core stays connector-agnostic.

## 6. Red-team seeds (challenge these)
- Is skip-hidden-dirs (§2.3) a golden-safe / correct parity choice, or should recursion match legacy
  `collectEntries` (descend all dirs)?
- Does factoring the loop into `collectFiles` keep the systemic-vs-local failure split byte-identical for
  BOTH the top-level and a sub-directory failure?
- Any consumer of `listFromFileSystem`/`listDataFiles` NOT covered (ACID? write? stats sampling
  `listFileSizes`)? Confirm ACID/write are genuinely out of scope for this property.
- `FakeFileSystem` tree-mode must not change the semantics the existing flat-mode tests rely on.
- Does `entry.location()` round-trip through `list()` in production (nested descent target correct for
  real per-scheme FS, not just the fake)?
