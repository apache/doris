# P6.2-T10 — iceberg scan parity-UT coverage audit + gap-fill

> **Task nature**: T10 is **not** from-zero test writing — T02–T09 each landed fairly complete parity tests
> (270/0/1 at T10 start). T10 = **audit coverage completeness** against the P6.2 acceptance gate
> (`P6-iceberg-migration.md:90`) vs legacy `IcebergScanNode`, fill any **parity-by-omission** gaps, run green.
> **Result**: 270 → **278/0/0 (1 skipped)**; **0 SPI / fe-core / paimon / pom / production changes** (only 3
> test files); checkstyle 0; import-gate net; iceberg still **not** in `SPI_READY_TYPES`.

## Method — 10-dimension audit workflow (`wf_9d88fe61-5c7`)

Canonical Review pattern: one auditor per acceptance-gate dimension reads the **legacy** source (ground truth
for expected VALUES), the **connector** impl, and the **connector tests**, then classifies each legacy-observable
behavior as value-asserted / weak (class-name/non-null only) / untested. Every reported gap was then independently
**adversarially verified** (refute-by-default, filtered against the signed-off deferred deviations from T02–T09).

- **Dimensions (10)**: predicate-pushdown / partition-pruning / native-jni+path_partition_keys / delete-files /
  count-pushdown / format-version-rangeparams / field-id-dictionary / mvcc-time-travel / vended+static-creds /
  e2e-combination+select-star.
- **Assertion-quality by dimension**: **8 value-parity**, 2 mixed (predicate-pushdown, e2e-combination).
- **Verdict**: 12 gaps reported → **10 confirmed, 2 refuted**. Two confirmed gaps were duplicates (G2 == E2E-1)
  and E2E-2 folds into them → **8 tests** fill the 10 confirmed gaps.

## Confirmed gaps filled (8 tests)

| # | gap | test (file) | what it pins (legacy ref) |
|---|-----|-------------|---------------------------|
| PRED-1 | only EQ value-asserted; GT/LT/GE/LE/NE→Operation unpinned | `comparisonOperatorsMatchLegacyOperations` (`IcebergPredicateConverterTest`) | GT→GT, LT→LT, GE→GT_EQ, LE→LT_EQ, **NE→not(equal) i.e. NOT-over-EQ** (legacy `IcebergUtils.convertToIcebergExpr`; connector `IcebergPredicateConverter:201-214`). The EQ-only grid asserts pushability, not the per-op Operation. |
| PP-1 | default split heuristic + `max_file_split_num` cap never count-asserted | `planScanDefaultSplitHeuristicTilesAndMaxFileSplitNumCapCollapses` (`IcebergScanPlanProviderTest`) | default 32MB target tiles a 96MB file; `max_file_split_num=1` raises target to whole-file → 1 range (`determineTargetFileSplitSize` cap `Math.max(result, minSplitSizeForMaxNum)`). |
| NF-1 | partition-bearing range with empty identity map (T03 Bug2) not e2e | `planScanPartitionBearingFileWithNoIdentityValuesEmitsNoColumnsFromPath` | bucket-only spec → `isPartitionBearing()==true` + empty `getPartitionValues()` + no columns-from-path (`buildRange:356-372`). |
| G1 | stored `-1L` position bound (sentinel) vs absent map not distinguished | `convertDeletePositionDeleteTreatsStoredMinusOneBoundAsUnset` | `readPositionBound:483` `value == -1L → null`; the no-bounds test takes the null-map early return, never reaching this arm. |
| MVCC-1 | schema-only ALTER divergence (latest-schema vs snapshot-schema) untested | `beginQuerySnapshotPinsLatestSchemaAfterSchemaOnlyAlter` (`IcebergConnectorMetadataMvccTest`) | `beginQuerySnapshot` pins `table.schema().schemaId()` (latest) not `currentSnapshot().schemaId()` (lagging) — legacy `getLatestIcebergSnapshot`. The existing test has the two ids coincide. |
| MVCC-2 | `FOR TIME AS OF '<datetime string>'` path never run through `resolveTimeTravel` | `resolveTimestampDatetimeStringResolvesSnapshot` | non-digital `isDigital==false` → `IcebergTimeUtils.datetimeToMillis(sessionZone)` → `SnapshotUtil.snapshotIdAsOfTime`; the existing test only drives the digital epoch-millis `parseLong` branch. |
| VC-2 | vended credential-wins-on-collision merge ordering not pinned | `extractVendedTokenCredentialWinsOnKeyCollision` | `extractVendedToken` seeds `io.properties()` then `putAll(credential.config())` → credential wins on a duplicate key (existing merge test uses disjoint keys). |
| G2+E2E-1+E2E-2 | no single planScan combines partition + delete + predicate + normalization | `planScanCombinesPartitionPruneDeleteAndPathNormalizeOnOneRange` | partitioned v2 oss:// table, `WHERE p=1` prunes p=2; the one surviving range carries TOGETHER: scheme-normalized data path + raw `original_file_path`, columns-from-path, and its scheme-normalized position delete. The existing delete e2e tests all use unpartitioned tables. |

## Refuted (correctly, by the adversarial pass)

- **FIDX-1** (field-id dict asserts names but not ids): refuted — the rename-safe field id IS already
  value-asserted (`IcebergSchemaUtilsTest.renamePreservesFieldIdAcrossEvolution` +
  `topLevelFieldsCarryIcebergFieldIdsAndLowercasedNames`).
- **VC-1** (vended whole-map REPLACE vs key-level MERGE): refuted — the divergence is unreachable (a REST-vended
  catalog's static storage map is empty by design, so replace == merge); already examined in the T09 review.

## Notable corrections made during implementation (did NOT trust the audit's suggestions blindly)

- **PP-1 cap test**: the audit proposed `max_file_split_num=2 → 2 ranges` on an **offset-bearing** 96MB file.
  Empirically wrong — iceberg's *offset-aware* splitter cuts at every row-group offset and ignores a larger
  target, so the cap has **no observable count effect** with split offsets (got 3, not 2). Fixed by using a
  **no-offset** file (fixed-size splitter, where the target directly controls count) + `max_file_split_num=1`
  → whole-file → exactly 1 range. SDK-version-robust, deterministic.
- **MVCC-2 session**: the audit proposed an awkward inline `ConnectorSession` stub. Unnecessary — a `null`
  session resolves to UTC via `resolveSessionZone`, and zone-correctness is already covered by
  `IcebergTimeUtilsTest`; MVCC-2 only needs to prove the datetime branch is wired through `resolveTimeTravel`.

## Verification (Rule 9 — tests must be able to fail)

- Full suite: **278/0/0 (1 skipped env-gated live)**; `IcebergScanPlanProviderTest` 52→57,
  `IcebergPredicateConverterTest` 16→17, `IcebergConnectorMetadataMvccTest` 17→19.
- **Mutation check (PRED-1)**: swapping GE↔GT in `IcebergPredicateConverter` reddened
  `comparisonOperatorsMatchLegacyOperations` (`GT operation expected: <GT> but was: <GT_EQ>`) — the exact
  transposition the old EQ-only grid could not catch. Reverted clean.
- checkstyle 0, import-gate net, iceberg not in `SPI_READY_TYPES`, only 3 test files changed.

## Conclusion

P6.2 scan parity coverage is **complete** against the acceptance gate (predicate pushdown / partition-pruning
counts / native·JNI / position+equality+DV delete / SELECT* no-predicate / MVCC AS OF·VERSION·TAG·BRANCH·
TIMESTAMP-string / vended REST round-trip), with the parity-by-omission gaps closed and a representative
end-to-end combination scan pinned. Remaining UT-invisible deviations (GLOBAL_ROWID flip-gate blocker,
Kerberized auth, vended live round-trip, manifest-cache/profile drops) stay registered for **P6.6 docker**.
T10 closes; next = **T11** (final design summary + connector validation gate, mostly already landed).
