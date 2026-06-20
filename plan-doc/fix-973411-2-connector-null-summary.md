# FIX-2 Summary — connector-null NPE during mv_infos scan (collateral)

## Problem
CI 973411 `test_mysql_mtmv:63` failed with `Connector.getMetadata(...) because "connector" is null`. The MySQL
test is collateral: its `getJobName` runs an `mv_infos` scan that iterates all MTMVs.

## Root Cause
A concurrent catalog DROP: `PluginDrivenExternalCatalog.onClose()` nulls the transient `connector` but does not
reset `objectCreated`; `dropCatalog` calls `onClose()` directly (not `resetToUninitialized`), so a stale
metadata access finds `getConnector()==null` (makeSureInitialized skips re-init). `materializeLatest:122`
dereferenced it → NPE aborted the whole metadata query. Legacy `onClose` never nulled the field.

## Fix
`PluginDrivenMvccExternalTable.materializeLatest`: if `connector == null`, return a valid empty pin
(snapshot id -1, empty maps), mirroring the existing dropped-table (no-handle) branch. Connector-agnostic;
`getConnector()` contract unchanged. Cannot mask a real init failure (that path throws).

## Tests
`PluginDrivenMvccExternalTableTest.testMaterializeLatestNullConnectorDegradesToEmptyPin`: table over a
null-connector catalog → `loadSnapshot(empty,empty)` returns the empty pin instead of NPE. The RED run threw
the exact production NPE; GREEN after. Full class 36/36; fe-core checkstyle clean.

## Result
Fixed (offline UT reproduces + verifies). Optional pre-existing defense-in-depth (per-MTMV try/catch in
`MetadataGenerator.mtmvMetadataResult`) left out of scope.
