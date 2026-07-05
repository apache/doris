# P6.2-T04 — iceberg merge-on-read delete files → `TIcebergDeleteFileDesc`

> Branch `catalog-spi-10-iceberg`. Mirrors legacy `IcebergScanNode.setIcebergParams` delete loop
> (`:315-368`) + `getDeleteFileFilters` (`:1072`) + `IcebergDeleteFileFilter`. Predecessors: T01 skeleton,
> T02 predicate/split, T03 typed range-params (`designs/P6-T03-iceberg-scan-rangeparams-design.md`).
> **0 new SPI** (the single T03 `isPartitionBearing` exception aside). Iceberg stays out of
> `SPI_READY_TYPES`; nothing reaches BE this phase — parity verified by offline UT until P6.6.

## 1. Goal / scope

Fill the `TIcebergFileDesc.delete_files` list that T03 deliberately left unset (T03 review refuted the
"unset == empty is unsafe" worry for no-delete tables; T04 now makes it byte-faithful for **both** the
no-delete and the with-delete case).

In scope:
- v2+ : emit `delete_files` (a possibly-empty list, legacy always calls `setDeleteFiles(new ArrayList<>())`).
- Per delete file → `TIcebergDeleteFileDesc`: **POSITION** (`content=1`, position bounds) / **deletion
  vector / PUFFIN** (`content=3`, `content_offset`+`content_size_in_bytes`, bounds carried too) /
  **EQUALITY** (`content=2`, `field_ids`). Per-delete `file_format` (parquet/orc; **unset for PUFFIN**).
- Delete-path normalization via the engine seam (legacy `LocationPath.of(path,config).toStorageLocation()`).
- `getDeleteFiles(TTableFormatFileDesc)` override → delete paths for the VERBOSE per-backend EXPLAIN
  (`deleteFileNum`/`deleteSplitNum`), verbatim port of legacy `IcebergScanNode.getDeleteFiles(:398-421)`.

Out of scope (later tasks / paths):
- COUNT pushdown's `getCountFromSnapshot` equality/position-delete handling → **T05**.
- The node-level `deleteFilesByReferencedDataFile` / `deleteFilesDescByReferencedDataFile` maps
  (`IcebergScanNode:167-168`, `:353-367`): consumed **only** by `IcebergRewritableDeletePlanner`
  (iceberg MOR DELETE / rewrite = **write path, P6.3+**), never by the read scan path. **Not ported.**
- field-id **data** schema dictionary (`history_schema_info`) → **T06**. Note: equality-delete `field_ids`
  here come straight from `DeleteFile.equalityFieldIds()` (delete-file metadata) and are correct by
  construction — independent of the T06 data-schema dictionary risk.
- vended-credential-aware path normalization (2-arg `normalizeStorageUri(uri,token)`) → **T09** (T04 uses
  the 1-arg static-map form, same as the main data path today).

## 2. Design decision: typed carriers (not `ConnectorDeleteFile.properties`)

The P6.2 recon (`research/p6.2-iceberg-scan-recon.md` §2 delete row) sketched encoding delete metadata into
the existing `ConnectorScanRange.getDeleteFiles() → List<ConnectorDeleteFile>` (`.properties`). **That plan
is superseded** by the pattern T03 already established and verified:

- The generic `PluginDrivenScanNode.setScanParams(:932-947)` builds the thrift **only** by calling
  `scanRange.populateRangeParams(...)`. It does **not** consume `ConnectorScanRange.getDeleteFiles()`
  (that default-empty method is vestigial — no generic-node code reads it for thrift).
- Routing delete metadata through `ConnectorDeleteFile` would force the generic node to translate it into
  the iceberg-specific `TIcebergDeleteFileDesc` — **iceberg-specific code in the engine-neutral node**,
  which the project rule forbids (`catalog-spi-plugindriven-no-source-specific-code`).
- T03 emits **every** iceberg per-file field as a typed carrier consumed in `populateRangeParams`;
  `getProperties()` stays empty. Delete files follow the same shape — consistent, node-agnostic, testable.

So: `IcebergScanRange` carries a typed `List<DeleteFile>`; `populateRangeParams` emits the
`TIcebergDeleteFileDesc` list directly. **Flag for cleanup:** recon §2 delete row + the
`ConnectorScanRange.getDeleteFiles()`/`ConnectorDeleteFile` SPI default are now dead for iceberg.

## 3. Implementation

### 3.1 `IcebergScanRange` — typed delete carrier + emission
- New immutable `Serializable` nested class `DeleteFile` with factory methods mirroring legacy
  `IcebergDeleteFileFilter` subclasses (the `type()` ids are the legacy literals 1/2/3):
  - `positionDelete(path, TFileFormatType fmt, Long lower, Long upper)` → `content=1`
  - `deletionVector(path, Long lower, Long upper, long offset, long size)` → `content=3`, `fileFormat=null`
  - `equalityDelete(path, TFileFormatType fmt, List<Integer> fieldIds)` → `content=2`
  - `toThrift()` → `TIcebergDeleteFileDesc`: `setPath`; set `fileFormat`/bounds/`fieldIds`/`contentOffset`/
    `contentSizeInBytes` **only when non-null** (legacy sets bounds only `if present`, sets format only for
    parquet/orc); `setContent` last.
- New builder field `deleteFiles` (default empty, never null).
- `populateRangeParams`: split the v1 branch into v1/v2+:
  - `formatVersion < 2` → `setContent(FileContent.DATA.id())` (unchanged).
  - else → `fileDesc.setDeleteFiles(<list, possibly empty>)` (legacy parity; **always set** for v2+).

### 3.2 `IcebergScanPlanProvider` — classify + normalize + EXPLAIN read-back
- `buildRange(...)` adds `.deleteFiles(buildDeleteFiles(task))`.
- `buildDeleteFiles(FileScanTask)` → maps `task.deletes()` through `convertDelete`. Empty → emptyList.
- `convertDelete(org.apache.iceberg.DeleteFile)` (package-private, unit-tested) — port of
  `getDeleteFileFilters` + `IcebergDeleteFileFilter.create*` + `setIcebergParams`:
  - normalize path via `normalizeDeletePath`.
  - `POSITION_DELETES` + `format()==PUFFIN` → `deletionVector(path, lower, upper, contentOffset(),
    contentSizeInBytes())`; else → `positionDelete(path, fmt, lower, upper)`.
  - `EQUALITY_DELETES` → `equalityDelete(path, fmt, equalityFieldIds())`.
  - else → `throw IllegalStateException("Unknown delete content: " + content)` (legacy `:1082`, defensive —
    delete files are only position/equality).
- `readPositionBound(Map<Integer,ByteBuffer>)` — port of `createPositionDelete`: decode
  `bounds.get(MetadataColumns.DELETE_FILE_POS.fieldId())` via `Conversions.fromByteBuffer(DELETE_FILE_POS
  .type(), buf)`; absent **or sentinel `-1`** → `null` (legacy `orElse(-1L)` + `getPositionLowerBound()`
  returns empty on -1 → only emitted when present).
- `deleteFileFormat(FileFormat)` — port of `setDeleteFileFormat`: PARQUET→`FORMAT_PARQUET`, ORC→`FORMAT_ORC`,
  else `null` (legacy leaves PUFFIN/other unset).
- `normalizeDeletePath(raw)` = `context != null ? context.normalizeStorageUri(raw) : raw`
  (`DefaultConnectorContext.normalizeStorageUri` = `LocationPath.of(raw, staticMap).toStorageLocation()`,
  byte-equivalent to legacy delete-path normalization; null context = offline UT → raw, paimon parity).
- `getDeleteFiles(TTableFormatFileDesc)` override — verbatim port of legacy `getDeleteFiles(:398-421)` /
  paimon `getDeleteFiles(:1286)`: read back every `iceberg_params.delete_files[*].path` (incl. equality)
  for the VERBOSE EXPLAIN count. Null/empty guards mirror legacy exactly.

## 4. Parity nuances (write down to avoid re-deriving)
- **DV is a position delete with PUFFIN format**: `DeleteFile.content() == POSITION_DELETES`; the DV-vs-plain
  split is by `format()==PUFFIN`, **not** by content. The DV thrift carries bounds (if present) **and**
  offset/size **and** `content=3` (legacy sets content=1 then overrides to 3).
- **v2+ always emits the list** (empty when no deletes) — strict legacy parity (T03 left it unset; review
  said BE treats unset==empty, so this is a safe tightening, not a behavior change for BE).
- **Equality field-ids are inherently correct** (from delete-file metadata), so the "field-id loss"
  highest-risk item (T06) does not apply to equality deletes; only the data-schema dict does.
- **Path normalization owner**: the parent normalizes only the *main* range path (`PluginDrivenSplit
  .buildPath`); delete paths live entirely inside `iceberg_params`, so the **connector** must normalize
  them (legacy does `LocationPath...toStorageLocation`; connector does `context.normalizeStorageUri`).

## 5. Deviations (UT-invisible, P6.6 docker-only)
- 1-arg `normalizeStorageUri` (static map) vs T09's 2-arg vended form — REST object-store delete paths
  normalize correctly only after T09 wires the vended token (same gap as the main data path today).
- DV `contentOffset()`/`contentSizeInBytes()` auto-unbox to `long` (legacy parity — legacy also NPEs if a
  PUFFIN delete lacked them; in iceberg 1.10.1 DVs always set both).

## 6. Tests (TDD, no Mockito, fail-loud fakes)
- `IcebergScanRangeTest`: `populateRangeParams` v2 with each carrier kind (position/DV/equality) →
  exact `TIcebergDeleteFileDesc` fields; v2 no-delete → `delete_files` set + empty + no DATA content;
  v1 → DATA content + `delete_files` unset (extend existing v1 test).
- `IcebergScanPlanProviderTest`: `convertDelete` for position (with/without bounds), DV (offset/size +
  format unset), equality (field-ids); path normalized via a recording context; unknown-content throw is
  documented as unreachable (delete files are only position/equality, can't be built otherwise);
  `getDeleteFiles` read-back (incl equality) + null/empty guards; one end-to-end `planScan` test that
  commits a real position delete via `RowDelta` and asserts it reaches `delete_files` (proves
  `task.deletes()` wiring).

## 7. Acceptance gate
fe-connector-iceberg UT green (164 → +12 = 176, 1 skip), fe-core PluginDriven* unaffected, checkstyle 0,
import-gate net, iceberg NOT in `SPI_READY_TYPES`, plugin-zip still has no fe-thrift. Adversarial parity
review (workflow `wf_d530fdbf-2bf`, 4 dims × skeptic-verify) vs legacy `setIcebergParams`/
`IcebergDeleteFileFilter`.

## 8. Adversarial review outcome (1 confirmed, 0 refuted)
The review confirmed the T04 delete logic is **byte-correct** (classification / bounds / content ids /
format / EXPLAIN read-back / path normalization all match legacy). It surfaced **1 real cross-task gap**
(medium, high-confidence): the connector emits the **main data-file path raw** (`dataFile.path()` →
`ConnectorScanRange.getPath()` → `PluginDrivenSplit` 1-arg `LocationPath.of` → no scheme normalization),
whereas legacy `createIcebergSplit:852` normalizes it via the 2-arg `LocationPath.of(path,
storagePropertiesMap)` (and paimon normalizes in-connector, FIX-URI-NORMALIZE). On an object-store
warehouse (`oss://`/`cos://`/`obs://`/`s3a://`) this diverges at the P6.6 cutover: the delete path becomes
`s3://` but the data path stays `oss://` → BE's scheme-dispatched S3 factory cannot open the data file.
This is **data-path scope (T02/T03)**, not delete scope; a comment T04 wrote asserting "the main data path
is normalized by the parent" was false and is corrected. **Per the user's decision it is fixed in a
SEPARATE follow-up commit** (split the range's single `path` into a normalized `path` for BE-open + a raw
`originalPath` for `original_file_path`, normalize the data path in `buildRange` via
`context.normalizeStorageUri`, mirroring paimon + legacy `createIcebergSplit`; `original_file_path` stays
raw — BE matches position-deletes against the raw iceberg path, legacy `setOriginalFilePath:304`).
