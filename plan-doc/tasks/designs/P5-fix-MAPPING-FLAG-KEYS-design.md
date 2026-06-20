# P5-fix #5 — FIX-MAPPING-FLAG-KEYS (M-crit, MAJOR)

> Finding: `reviews/P5-paimon-rereview2-2026-06-11.md` M-crit (critic-surfaced; **re-verified independently** before this design — see "Re-confirmation" below).
> Scope decision: **paimon-only** + cross-connector follow-up logged ([D-051], [DV-030]). User-signed 2026-06-11.

## Problem

The paimon connector's two type-mapping toggles are **silently dead**: even when the user enables them at `CREATE CATALOG`, the connector never honors them.

- `enable.mapping.varbinary=true` → Paimon `BINARY`/`VARBINARY` columns should map to Doris `VARBINARY`; instead they stay `STRING`.
- `enable.mapping.timestamp_tz=true` → Paimon `TIMESTAMP_WITH_LOCAL_TIME_ZONE` (LTZ) should map to Doris `TIMESTAMPTZ`; instead it stays `DATETIMEV2`.

This is a **cutover regression**: the legacy in-tree paimon path honors both flags.

## Root Cause

A transcription drift introduced during the SPI cutover. fe-core writes/reads **dotted** catalog keys; the connector reads **underscore** keys that are never present in the property map.

- The canonical user-facing CREATE-CATALOG keys are **dotted**: `enable.mapping.varbinary` / `enable.mapping.timestamp_tz` (`CatalogProperty.java:50,52`). `ExternalCatalog.setDefaultPropsIfMissing():302-306` writes **only** those dotted keys (default `false`); `HIDDEN_PROPERTIES` hides them; the legacy paimon/hive/iceberg paths and the JDBC connector all read the dotted keys.
- `PluginDrivenExternalCatalog.createConnectorFromProperties():143-151` hands the connector the **raw** `catalogProperty.getProperties()` map — which therefore contains only the dotted keys (`getProperties()` is a verbatim copy, `CatalogProperty.java:100-101`).
- The connector reads **underscore** keys (`PaimonConnectorProperties.java:39,42` → `PaimonConnectorMetadata.buildTypeMappingOptions:1017-1027`): `enable_mapping_binary_as_varbinary` / `enable_mapping_timestamp_tz`. Those keys are never in the map → `getOrDefault(..., "false")` returns `false` unconditionally → flags permanently off.
- The binary key is **doubly** drifted: not only `.`→`_` but the token was renamed `varbinary`→`binary_as_varbinary`. A generic dots→underscores normalizer would still miss it.

No normalization layer exists anywhere between the catalog property map and the connector read (verified by grep across `fe/`). The underscore keys legitimately exist only in `FileFormatConstants` for the **TVF** path (`hdfs()`/`s3()` functions) — a different namespace, the likely copy-paste source.

### Re-confirmation (M-crit was critic-surfaced, not 3-lens-gated)

Independent 5-angle scout + adversarial synthesizer (workflow `wf_a3626c54-0db`) → **REAL_BUG, high confidence**, false-positive steelman rejected:
- Canonical key is dotted, proven by original feature PRs `c1eaede1260` (#57821), `a22da676bb0` (#59720); by every regression `CREATE CATALOG` (paimon/iceberg/hive/jdbc all use dotted — e.g. `test_paimon_catalog_timestamp_tz.groovy:26-32`, `.out:4` expects `timestamptz(3)`); by legacy parity (`PaimonExternalTable.java:350`); and by the JDBC connector (migrated in the **same** SPI PR) correctly keeping dotted (`JdbcConnectorProperties.java:66-67`).
- Failure manifests end-to-end (no normalization; single read site at ctor line 88).
- **Cross-connector**: NEW hive (`enable_mapping_binary_as_string` — a misnomer, not a semantic inversion) and iceberg (`enable_mapping_varbinary`) share the identical class of bug. **Out of scope here** ([DV-030]).

### Why this is FE-only (no BE, no SPI)

The **BE scan-param** side already reads the dotted key: `PluginDrivenScanNode extends FileQueryScanNode` and does **not** override `getEnableMappingVarbinary()/getEnableMappingTimestampTz()`, which read the dotted catalog getter and feed `params.setEnableMappingVarbinary/Tz` (`FileQueryScanNode.java:192-193,635-678`). Today the FE column-type side (connector) and the BE scan-param side **diverge** when the flag is set; re-pointing the connector's read to the dotted key makes both consistent again. No BE change; no new/changed SPI surface (the connector already receives the raw catalog map and already has the read site — only the key literals change).

## Design

Re-point the two connector constants to the **canonical dotted catalog keys**, fixing both the separator and (for binary) the renamed token, and align the binary constant name with the cross-connector convention (`CatalogProperty`/`Jdbc`/`Iceberg` all use `ENABLE_MAPPING_VARBINARY`).

`PaimonConnectorProperties.java`:
- `ENABLE_MAPPING_BINARY_AS_VARBINARY = "enable_mapping_binary_as_varbinary"` → **`ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary"`**
- `ENABLE_MAPPING_TIMESTAMP_TZ = "enable_mapping_timestamp_tz"` → **`ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz"`**

`PaimonConnectorMetadata.buildTypeMappingOptions` (the single usage site): update the constant reference `ENABLE_MAPPING_BINARY_AS_VARBINARY` → `ENABLE_MAPPING_VARBINARY`. **No logic change** — the `Options(mapBinaryToVarbinary, mapTimestampTz)` arg order is already correct (binary first), and the read order is unchanged.

### Why this approach (vs fe-core normalizer)

Rejected: a dots→underscores normalizer in `PluginDrivenExternalCatalog.createConnectorFromProperties`. It is **broader-blast** (mutates the shared map all connectors + image/replay/SHOW CREATE see), would **break JDBC** (already reads dotted), and is **insufficient** (paimon's renamed token would still be missed). The constant re-point is the minimal, parity-correct fix and converges paimon with the JDBC/legacy dotted convention.

## Implementation Plan

1. `PaimonConnectorProperties.java:38-42` — rename + re-value the two constants (with clarifying Javadoc that these are the canonical dotted CREATE-CATALOG keys mirroring `CatalogProperty`).
2. `PaimonConnectorMetadata.java:~1020` — update the one constant reference.
3. Add fail-before/pass-after UTs (below).

## Risk Analysis

- **Blast radius**: two string literals + one reference, single connector. No SPI, no BE, no fe-core.
- **Behavior change is intended and parity-restoring**: only catalogs that **set** the flag change (latent until enabled — default `false` renders identically to before, so default-config catalogs are unaffected).
- **Misnamed-constant trap avoided**: do NOT invert any boolean (hive's `binary_as_string` is a misnomer, but that is out of scope here anyway).
- **No existing test pins the broken behavior** (verified) → no test churn beyond the net-new coverage.

## Test Plan

### Unit Tests (`PaimonConnectorMetadataTest.java`, offline harness)

Build a `FakePaimonTable` whose `RowType` has a `BINARY` and a `TIMESTAMP_WITH_LOCAL_TIME_ZONE` column; drive `getTableHandle` → `getTableSchema` and assert the mapped `ConnectorType` names.

1. **Bug-catcher** (`...HonorsDottedMappingKeys`): construct the metadata with `{"enable.mapping.varbinary":"true","enable.mapping.timestamp_tz":"true"}` → assert BINARY→`VARBINARY` and LTZ→`TIMESTAMPTZ`.
   - *Fail-before* (underscore constants): both flags read `false` → `STRING` / `DATETIMEV2` → assertions red. *Pass-after*: green.
2. **Default guard** (`...DefaultsMappingFlagsOff`): construct with no mapping keys → assert BINARY→`STRING` and LTZ→`DATETIMEV2` (default-off preserved). Green both states — guards against accidentally flipping defaults.

Each test documents WHY (the dotted catalog key is the user-facing contract fe-core sets/hides/defaults; reading the underscore key silently drops the flag) and the MUTATION that reddens it.

### E2E Tests

`test_paimon_catalog_varbinary.groovy` / `test_paimon_catalog_timestamp_tz.groovy` (the `.out` expects `timestamptz(3)`) already encode the dotted-key contract — but are **CI-gated** (`enablePaimonTest=false` in committed HEAD + external Paimon/HDFS fixture). Note as gated; do not claim to run them.
