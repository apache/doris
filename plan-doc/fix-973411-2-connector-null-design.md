# FIX-2 — test_mysql_mtmv: connector-null NPE during mv_infos scan (collateral)

## Problem
CI 973411 `mtmv_p0/test_mysql_mtmv.groovy:63` fails with
`[INTERNAL_ERROR] Cannot invoke Connector.getMetadata(...) because "connector" is null`. The MySQL MTMV test
itself is healthy — it is collateral.

## Root Cause
`getJobName` runs an `mv_infos`/`jobs` metadata scan → `MetadataGenerator.mtmvMetadataResult` loops over ALL
MTMVs in the db → `MTMVPartitionUtil.isMTMVSync` → the related paimon table's
`PluginDrivenMvccExternalTable.materializeLatest:122` dereferences a **null** `connector`
(`pluginCatalog.getConnector()`), throwing NPE that aborts the whole metadata query.

Why null: `PluginDrivenExternalCatalog.connector` is `transient volatile` (line 71). `onClose()` (549-559)
sets `connector = null` but does NOT reset the inherited `objectCreated` flag. `dropCatalog` cleanup calls
`catalog.onClose()` **directly** (`CatalogMgr.cleanupRemovedCatalog:144`), not `resetToUninitialized()` (which
*does* reset `objectCreated`, :625). So a just-dropped catalog object is left `objectCreated=true,
connector=null`; a concurrent stale access calls `makeSureInitialized()` → `initLocalObjects()` skips
`initLocalObjectsImpl()` (the only place the connector is recreated) because `objectCreated` is still true →
`getConnector()` returns null. FE log: catalog drop 21:15:44,724; NPE 21:15:44,748 (24 ms race),
fe.log:83972. Legacy master `PaimonExternalCatalog.onClose()` closed the client but never nulled the field, so
this NPE could not occur. Classification: **SPI regression** (lifecycle), surfaced by a concurrency race.

A null connector after `makeSureInitialized()` is reachable ONLY in this dropped-catalog state: on a healthy
catalog, `initLocalObjectsImpl` THROWS if it cannot create a connector (:115-119) — so the guard cannot mask a
real init failure.

## Design
Guard the null connector at the NPE site in `materializeLatest`: if `connector == null`, return a valid empty
pin (snapshot id -1, empty partition maps), exactly mirroring the existing dropped-**table** branch (:125-130).
Smallest change at the actual failure point; connector-agnostic; keeps `getConnector()`'s contract unchanged.
A stale dropped-catalog MTMV access then yields a benign empty result instead of aborting the scan.

(Not chosen: re-creating the connector in `onClose` — wrong for a genuinely dropped catalog. Optional separate
defense-in-depth, pre-existing generic MTMV code: per-MTMV try/catch in `MetadataGenerator.mtmvMetadataResult`
so one bad MTMV can't fail the whole scan — out of scope for this SPI-regression fix.)

## Implementation Plan
`PluginDrivenMvccExternalTable.materializeLatest`: after `Connector connector = pluginCatalog.getConnector();`,
add `if (connector == null) return new PluginDrivenMvccSnapshot(emptySnapshot(), Collections.emptyMap(),
Collections.emptyMap());`.

## Risk Analysis
Minimal. Empty maps → `isPartitionInvalid()==false` → `getPartitionColumns` returns the cached static columns
(no NPE). Cannot mask a genuine init failure (that path throws). No effect on the healthy path.

## Test Plan
### Unit Tests
`PluginDrivenMvccExternalTableTest.testMaterializeLatestNullConnectorDegradesToEmptyPin`: build a table over a
catalog whose `getConnector()` returns null, call `loadSnapshot(empty, empty)`, assert it returns an empty pin
(snapshot id -1, empty maps) and does not NPE. RED before / GREEN after.

### E2E Tests
Race-dependent; covered indirectly by the existing mtmv_p0 paimon suites under docker enablePaimonTest=true.
