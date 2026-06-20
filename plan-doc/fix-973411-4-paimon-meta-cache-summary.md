# FIX-4 Summary — restore paimon table cache (data snapshot + schema TTL)

## Problem
CI 973411 `test_paimon_table_meta_cache` fails on two assertions: L79 (with-cache catalog sees an external
INSERT immediately) and L112 (no-cache catalog serves stale schema). The SPI migration split the legacy single
`meta.cache.paimon.table.ttl-second` knob (which covered data snapshot AND schema) and dropped the data cache.

## Root Cause
`beginQuerySnapshot` read the latest snapshot id live every query (no cross-query pin); the schema cache TTL is
the generic `schema.cache.ttl-second`, unaffected by the paimon knob. SPI regression (test unchanged from master).

## Fix (two axes, fe-core stays connector-agnostic)
**Axis A (data):** new `PaimonLatestSnapshotCache` on `PaimonConnector` (TTL = `meta.cache.paimon.table.ttl-second`,
default 86400, `<=0` disables); `beginQuerySnapshot` serves the id through it (the id flows to `scan.snapshot-id`
via `applySnapshot`, confirmed end-to-end). New `Connector.invalidateTable/invalidateAll` SPI no-ops; paimon
overrides them; `RefreshManager.refreshTableInternal` invalidates any `PluginDrivenExternalCatalog`'s connector
(REFRESH CATALOG already rebuilds it).
**Axis B (schema):** new `Connector.schemaCacheTtlSecondOverride()` SPI (paimon returns the knob); new generic
`ExternalCatalog.overlayMetaCacheConfig` hook (PluginDrivenExternalCatalog delegates to the connector);
`ExternalMetaCacheMgr.findCatalogProperties` applies it to its EPHEMERAL copy (no SHOW CREATE leak). REFRESH
TABLE already invalidates the schema cache.
`ttl-second` removed from the "dead keys" warning; `enable`/`capacity` remain not-wired (still reported ignored).

## Tests
- `PaimonLatestSnapshotCacheTest` 5/5 (cache within TTL, ttl=0 bypass, invalidate, expiry via injected clock).
- `PaimonConnectorCacheTest` 4/4 (`schemaCacheTtlSecondOverride` mapping).
- Regression: PaimonConnectorMetadataMvccTest 40/40, ValidateProperties 14/14; fe-core compile +
  PluginDrivenMvccExternalTableTest (FIX-2) + ListPartitionItemTest (FIX-3).

## Result
Offline UTs + compile verified. The cross-query data cache + schema TTL + refresh behavior is gated by the
docker e2e (`enablePaimonTest=true` rerun of test_paimon_table_meta_cache), currently RED, expected GREEN.
