# FIX-4 — test_paimon_table_meta_cache: restore paimon table cache (data snapshot + schema TTL)

## Problem
CI 973411 `test_paimon_table_meta_cache` fails. Two independent assertions break because the SPI migration split
the legacy single paimon table cache (one `meta.cache.paimon.table.ttl-second` knob covered BOTH data snapshot
AND schema) across two SPI mechanisms with different knobs:
- **L79 (with-cache, data):** SPI has NO snapshot cache, so the with-cache catalog sees an external INSERT
  immediately (expected 1, got 2).
- **L112 (no-cache, schema):** SPI routes paimon schema to the generic schema cache keyed by
  `schema.cache.ttl-second` (default 86400), which `meta.cache.paimon.table.ttl-second=0` does NOT disable, so
  the no-cache catalog serves stale schema (expected 3, got 2).

## Root Cause
`PaimonConnectorProvider` marked `meta.cache.paimon.table.*` "dead" at cutover. `beginQuerySnapshot` reads the
LATEST snapshot id live every query (no cross-query pin), and the schema cache TTL is the generic
`schema.cache.ttl-second`, unaffected by the paimon knob. SPI regression (the unchanged test encodes master).

## Design (two axes, connector-agnostic fe-core)
Confirmed end-to-end that the query-begin snapshot id controls normal reads:
`materializeLatest -> beginQuerySnapshot -> PluginDrivenScanNode.pinMvccSnapshot -> applySnapshot ->
scan.snapshot-id -> resolveScanTable Table.copy`.

**Axis A — data snapshot cache:**
- New `PaimonLatestSnapshotCache` (per-catalog, on the long-lived `PaimonConnector`): TTL cache of latest
  snapshot id keyed by `Identifier(db,table)`, sized by `meta.cache.paimon.table.ttl-second` (legacy default
  86400; `<= 0` disables -> always live = the no-cache catalog). Access-based expiry; injected into
  `PaimonConnectorMetadata` (5-arg ctor; 3/4-arg ctors get a disabled cache so existing tests are unchanged).
- `beginQuerySnapshot` serves the id through the cache (live read only on a miss).
- New `Connector.invalidateTable(db,tbl)` / `invalidateAll()` SPI default no-ops; `PaimonConnector` overrides
  them to invalidate the cache (keyed by REMOTE names, matching the handle).
- `RefreshManager.refreshTableInternal` calls `connector.invalidateTable(db.getRemoteName(),
  table.getRemoteName())` for any `PluginDrivenExternalCatalog` (generic; no source-specific code). REFRESH
  CATALOG already rebuilds the connector (cache gone).

**Axis B — schema cache TTL:**
- New `Connector.schemaCacheTtlSecondOverride()` SPI default `OptionalLong.empty()`; `PaimonConnector` returns
  `meta.cache.paimon.table.ttl-second` when set.
- New generic `ExternalCatalog.overlayMetaCacheConfig(props)` no-op hook; `PluginDrivenExternalCatalog`
  overrides it to set `schema.cache.ttl-second` = the connector override (only if the user didn't set it).
- `ExternalMetaCacheMgr.findCatalogProperties` calls the hook on its EPHEMERAL property copy (no persisted
  mutation -> no SHOW CREATE leak). REFRESH TABLE already invalidates the schema cache entry.

`meta.cache.paimon.table.{enable,capacity}` remain not-wired (still reported ignored); `ttl-second` is removed
from the "dead keys" warning since it again takes effect.

## Risk Analysis
Snapshot pinning stability across queries (within TTL) is the legacy behavior restored — a deliberate, faithful
semantic. fe-core stays connector-agnostic (virtual dispatch; base no-ops). The overlay never mutates persisted
properties. `connector`-field reads are null-guarded (dropped/uninitialized -> engine default). Only fully
verifiable via docker e2e (cross-query cache + external writes); offline UTs cover the cache + the override map.

## Test Plan
### Unit
- `PaimonLatestSnapshotCacheTest`: caches within TTL, ttl=0 bypasses, invalidate/invalidateAll clear, expiry
  (injectable clock). RED/GREEN on the cache logic.
- `PaimonConnectorCacheTest`: `schemaCacheTtlSecondOverride()` maps the knob (absent->empty, 0->of(0),
  N->of(N), garbage->empty).
- Regression: PaimonConnectorMetadataMvccTest (beginQuerySnapshot), ValidateProperties, fe-core compile +
  PluginDrivenMvccExternalTableTest / ListPartitionItemTest.

### E2E
`test_paimon_table_meta_cache.groovy` under docker `enablePaimonTest=true` (currently RED; expected GREEN) —
the real gate for the cross-query data cache + schema TTL + refresh.
