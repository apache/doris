# Hudi time travel (FOR TIME AS OF) — connector-side design (HD-C2)

Authoritative, code-grounded design for the hudi `FOR TIME AS OF` step. HEAD = branch `catalog-spi-11-hive`
(after HD-C1 `bbe6cfcd647`). Recon = `wf_e0b62364-d20` (5 HEAD-grounded readers + reconciliation critic, all
line anchors re-verified). This design is the mirror of the paimon time-travel template and the
byte-faithful reproduction of legacy `HudiScanNode` FOR TIME AS OF. **No fe-core changes.**

---

## 0. Scope

After the HMS-catalog cutover a hudi-on-HMS table is a generic `PluginDrivenMvccExternalTable` served by
`fe-connector-hudi` as a sibling. This step implements **`FOR TIME AS OF`** connector-side so a hudi table
served through the generic scan path reads at the pinned instant with **no regression** vs legacy
`HudiScanNode`. It also establishes the **query-instant pin spine on `HudiTableHandle`** that HD-C3
(incremental read) reuses.

Out of scope (documented deferrals): schema-at-instant (`FOR TIME AS OF` on a schema-*evolved* table still
reads the LATEST schema) → HD-C4; `@incr` incremental read → HD-C3; `@tag`/`@branch` → hudi has no tags/branches.

## 1. The mechanism (how the pin reaches planScan) — verified

The pin flows through the generic MVCC seam exactly like paimon. NO fe-core edit is needed; all four SPI hooks
already dispatch generically:

1. fe-core `PluginDrivenMvccExternalTable.loadSnapshot` (`:347`) calls `metadata.resolveTimeTravel(session,
   handle, spec)` for an explicit `FOR TIME AS OF` and stores the resolved `PluginDrivenMvccSnapshot` in the
   statement context (`StatementContext.loadSnapshots`, keyed by table + version selector). The `pinnedHandle`
   it computes at `:375` is **discarded** (used only for `getTableSchema`); the surviving carrier is the opaque
   `ConnectorMvccSnapshot`.
2. At scan time `PluginDrivenScanNode.pinMvccSnapshot` (`:749`) does a **version-aware** lookup
   (`MvccUtil.getSnapshotFromContext` keyed by *this scan's* `getQueryTableSnapshot()` selector), so a
   `FOR TIME AS OF` query retrieves the **resolveTimeTravel-resolved** snapshot (my property), while a normal
   read retrieves the query-begin latest-pin snapshot (empty properties).
3. `applyMvccSnapshotPin` (`:732`) unwraps `getConnectorSnapshot()` and calls
   `metadata.applySnapshot(session, currentHandle, connectorSnapshot)`; the result becomes `currentHandle`
   (`:768`), which is exactly the handle passed to `scanProvider.planScan(session, currentHandle, ...)`
   (`:998-1000`).

**Consequence:** the pin must live on the `ConnectorMvccSnapshot` (a String property) and be re-derived by
`HudiConnectorMetadata.applySnapshot` onto the `HudiTableHandle`; a value stamped only on `loadSnapshot`'s
handle would never reach `planScan`. `ConnectorMvccSnapshot.getProperties()` is **not** serialized to BE for a
plugin scan (fe-core only feeds it to `applySnapshot`); the instant reaches BE **only** via the per-range
`THudiFileDesc.instantTime` that `planScan` already stamps for MOR-JNI slices. So carrying the instant as a
handle field is both necessary and sufficient.

## 2. Decisions

### D1 — TIMESTAMP is permissive; NO timeline validation. **(plan correction)**
Legacy `HudiScanNode` never validates a `FOR TIME AS OF` value against the timeline: it only does
`value.replaceAll("[-: ]", "")` (`HudiScanNode.java:211`) and `getLatest{BaseFiles,MergedFileSlices}BeforeOrOn`
(`:419`/`:435`) — **before-or-on** file selection. A value after all commits reads the latest slice; a value
before the earliest commit reads **empty** (0 rows). Legacy **never errors** on a well-formed FOR TIME AS OF
(only the VERSION rejection). The written HD-C2 plan text ("validate the instant exists … empty ⇒
notFoundMessage") is a paimon-ism that would ADD an error where legacy read empty = a regression. **Corrected:**
`resolveTimeTravel(TIMESTAMP)` normalizes and **always returns a non-empty pin** for a well-formed value; it
never returns empty and never throws for not-found. This is byte-faithful, keeps the method fully offline
unit-testable (no live metaClient, zero extra round-trip), and preserves before-or-on at `planScan` unchanged.

### D2 — SNAPSHOT_ID / VERSION_REF fail loud by THROWING the byte-for-byte legacy message. **(mechanism correction)**
Legacy rejects `FOR VERSION AS OF` for hudi (`HudiScanNode.java:208-209`). fe-core's `toTimeTravelSpec` maps a
digital `FOR VERSION AS OF` → `SNAPSHOT_ID` and a non-digital one → `VERSION_REF`
(`PluginDrivenMvccExternalTable.java:404-407`); **both** must be rejected. Returning `Optional.empty()` would
surface fe-core's WRONG-DOMAIN `notFoundMessage` ("can't find snapshot by id" / "…by tag"). To surface the
exact legacy string the connector must **throw** a `DorisConnectorException` (unchecked; propagates verbatim —
no try/catch at `loadSnapshot :347-350`):

> ``Hudi does not support `FOR VERSION AS OF`, please use `FOR TIME AS OF` `` (verbatim incl. both backtick pairs)

### D3 — Digital flag ignored (legacy parity).
Legacy strips `[-: ]` regardless of `digital`; it does **no** epoch-millis conversion and **no** session
time-zone parse (unlike paimon). `resolveTimeTravel(TIMESTAMP)` ignores `spec.isDigital()` and just normalizes
the string. (A digital epoch-millis value passes through verbatim and reads before-or-on lexically — a faithful
legacy quirk.)

### D4 — Pin carrier = MVCC property → handle field.
`resolveTimeTravel(TIMESTAMP)` stashes the normalized instant STRING in
`ConnectorMvccSnapshot.property(HUDI_QUERY_INSTANT_PROPERTY, normalized)` (FE-internal transport, paimon
scan-options model). `applySnapshot` reads that property and stamps
`hudiHandle.toBuilder().queryInstant(v).build()` — **`toBuilder` preserves `prunedPartitionPaths`** (applyFilter
runs before applySnapshot, so a pruned time-travel scan must not lose its pruning). Do **not** overload
`snapshotId` (opaque to fe-core for the TT pin; planScan needs the string; property-presence cleanly
discriminates explicit-TT from latest-pin). `snapshotId` left default — the TT pin never enters an MTMV
comparison (MTMV uses the query-begin pin).

### D5 — applySnapshot is a no-op for the latest pin (no-regression guard).
`applySnapshot` is also invoked at scan time on the **query-begin** (latest) pin, which carries **empty**
properties (`beginQuerySnapshot` sets only `snapshotId`). When the `HUDI_QUERY_INSTANT_PROPERTY` is absent (or
snapshot null) `applySnapshot` returns the handle **unchanged** → `planScan` falls back to
`timeline.lastInstant()` → **byte-identical** to today for every ordinary read. Mirrors paimon's
`snapshotId<0 ⇒ return handle` guard.

### D6 — planScan single switch point.
`HudiScanPlanProvider.java:139` changes from `String queryInstant = lastInstant.get().requestedTime();` to
`hudiHandle.getQueryInstant() != null ? hudiHandle.getQueryInstant() : lastInstant.get().requestedTime()`.
All three downstream consumers read this one local — COW `getLatestBaseFilesBeforeOrOn` (`:250`), MOR
`getLatestMergedFileSlicesBeforeOrOn` (`:279`), MOR-JNI `builder.instantTime(queryInstant)` (`:311` →
`THudiFileDesc.instantTime`) — so FE file selection and the BE merge instant stay consistent. The
empty-timeline early-return (`:135-138`) is kept: `lastInstant.get()` is only touched in the null-pin branch
(guaranteed present past `:138`), so no NPE; a pinned instant on a never-committed table still early-returns
empty (correct). Schema/columns (`:144-157`) already resolve LATEST with no instant arg → NOT a switch site
(schema-at-instant is HD-C4).

### D7 — TAG / BRANCH / INCREMENTAL → `Optional.empty()` (unchanged SPI-default behavior).
The `resolveTimeTravel` override handles TIMESTAMP (pin) and SNAPSHOT_ID/VERSION_REF (throw); every other kind
returns `Optional.empty()` = the same result as not overriding, so no regression. `INCREMENTAL` is HD-C3 (which
will add that case); hudi has no `@tag`/`@branch`. Dormant until HD-B2, so no live behavior depends on this.

## 3. Implementation checklist (ordered)

1. **`HudiTableHandle.java`** — add `private final String queryInstant;` + `getQueryInstant()`; Builder field +
   `queryInstant(String)` fluent setter; copy in the private ctor and in `toBuilder()` (mirroring
   `prunedPartitionPaths`). **No** equals/hashCode (handle has none; keep reference identity, matching paimon's
   intent to exclude the pin from identity).
2. **`HudiConnectorMetadata.java`** — override `resolveTimeTravel` (TIMESTAMP → pin property; SNAPSHOT_ID +
   VERSION_REF → throw D2 message; default → empty) and `applySnapshot` (property present → stamp via
   `toBuilder().queryInstant(...)`; else unchanged). Add the `HUDI_QUERY_INSTANT_PROPERTY` constant. Do **not**
   override 3-arg `getTableSchema` (clean latest-schema deferral to HD-C4).
3. **`HudiScanPlanProvider.java:139`** — honor `handle.getQueryInstant()` (D6).

## 4. Test plan (offline, same-loader; mirror `HudiConnectorPartitionListingTest`)

- T1 `resolveTimeTravel(TIMESTAMP "2024-01-01 12:00:00")` → pin property == `"20240101120000"`; no TZ shift.
- T2 `resolveTimeTravel(SNAPSHOT_ID)` → throws `DorisConnectorException` with the byte-for-byte D2 message.
- T3 `resolveTimeTravel(VERSION_REF)` → throws the same byte-for-byte message.
- T4 `resolveTimeTravel(TIMESTAMP)` is permissive/offline: non-empty pin, ZERO metaClient interaction (D1).
- T5 `applySnapshot` with the property → `handle.getQueryInstant()==normalized` AND `prunedPartitionPaths`
  preserved (set pruning on the input handle, assert it survives — guards toBuilder-not-rebuild).
- T6 `applySnapshot` with the empty-properties latest pin (`beginQuerySnapshot` output) → handle UNCHANGED,
  `getQueryInstant()==null` (the no-regression guard).
- T7 `HudiTableHandle.toBuilder().queryInstant(x).build()` round-trips and preserves every other field.

**e2e owed (flip-time, per `hms-iceberg-delegation-needs-e2e`):** `planScan` honoring the pin is not
offline-provable (it builds a live metaClient). A real `FOR TIME AS OF` regression test on actual COW + MOR
hudi tables (schema-stable = assert byte-faithful data pin; schema-evolved = assert/document the HD-C4
latest-schema gap) is required before the flip. Offline tests prove routing/normalization/handle-threading only.

## 5. Residuals
- **Schema-at-instant (HD-C4):** FOR TIME AS OF an OLD instant on a schema-EVOLVED table reads the LATEST
  schema/columns (`getTableAvroSchema()` no-arg). No-op for non-evolved tables (legacy's fallback also ignores
  the timestamp — it only reads schema-at-instant when `hoodie.schema.on.read.enable=true`). Real gap only for
  schema-on-read evolved tables → HD-C4.
- **Partition-set-at-instant (deferred; surfaced by HD-C2 review, not a HD-C2 code defect):** HD-C2 pins the
  DATA instant for file selection, but the partition *universe* is still resolved at LATEST
  (`resolvePartitions` → `listAllPartitionPaths` = current partitions; `HudiConnectorMetadata.collectPartitions`
  already flags "explicit time-travel (non-latest) partition listing is a later step"). Legacy pinned the
  at-instant write-partition set (`getPartitionNamesBeforeOrEquals(queryInstant)`). Consequence: a partition
  **dropped after** the pinned instant could be silently omitted from a `FOR TIME AS OF` result (row loss),
  IF Hudi's `getAllPartitionPaths` tombstones it (unverified library semantic). Partitions ADDED after the
  instant are harmless (before-or-on yields no files). This is a no-regression item for time-travel
  completeness to close/validate before the flip (candidate: fold into the schema/partition-at-instant work
  or a small follow-up), tracked here so the row-loss edge is **not silent**. Validated via the same COW/MOR
  `FOR TIME AS OF` e2e (build a dropped-partition-then-travel-back fixture).
- **Pin spine reuse:** the `queryInstant` field + `planScan` honoring is the spine HD-C3 extends with the
  incremental window/hoodie params.
