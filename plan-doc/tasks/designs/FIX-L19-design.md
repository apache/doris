# FIX-L19 — `partition_columns` reserved magic-key collision with source properties

> **⚠ SUPERSEDED.** The producer-side silent `remove()` approach below (commit `01668779fd9`) was rejected by
> the user (silent deletion discards user data with no signal). It is replaced by
> **`DESIGN-reserved-connector-keys-framework.md`** (commit `2c58d8342c1`): namespace every reserved control
> key under `__internal.` so collision is impossible by construction and a user's bare `partition_columns`
> flows through untouched. This doc is kept for the problem analysis (mechanism / FE-only / three-connector
> scope) which the rename builds on.

## Problem (HEAD-verified)
The SPI carries partition info from connector → fe-core through a stringly-typed reserved key
`"partition_columns"` **inside the same properties map** that also carries the source table's own
pass-through properties. fe-core's sole partition-derivation source is
`PluginDrivenExternalTable.toSchemaCacheValue:513`
(`tableSchema.getProperties().get("partition_columns")`) — a non-empty value ⇒ the table is modeled
as partitioned (shared by the latest path and the MVCC AS-OF path `PluginDrivenMvccExternalTable:387`).

Three producers do a source-property pass-through **before** conditionally stamping the reserved key,
and never remove a pre-existing one:
- iceberg `IcebergConnectorMetadata:411-412` — `tableProps.putAll(table.properties())`, then `put("partition_columns",…)` **only** when `!spec.isUnpartitioned()` (`:430-447`).
- hive `HiveConnectorMetadata:449-450` — `new HashMap<>(tableInfo.getParameters())`, then `put(PARTITION_COLUMNS_PROPERTY,…)` **only** when `!partitionKeys.isEmpty()` (`:455-458`).
- paimon `PaimonConnectorMetadata:304` — `schemaProps.putAll(coreOptions().toMap())`, then `put("partition_columns"/"primary_keys",…)` when the respective key list is non-empty (`:309-322`).

(hudi/maxcompute build a fresh map — no pass-through, unaffected.)

**Failure:** a **non-partitioned** table whose source properties literally contain a key named
`partition_columns` (e.g. iceberg `ALTER TABLE … SET TBLPROPERTIES('partition_columns'='id')`) skips the
stamp branch, so the user value survives the `putAll` and reaches fe-core, which treats it as the
partition CSV → the non-partitioned table is **misdetected as partitioned** (wrong pruning / row-count /
`EXPLAIN partition=N/M` / MTMV / analyze). Partitioned tables are safe (the connector value overwrites at
the stamp). Reachability is narrow: the source key must be exactly the bare reserved name AND its CSV must
match real column names (`toSchemaCacheValue:526` `if (col != null)` filters non-matches → benign).

`primary_keys` (paimon `:322`) is behaviorally benign in fe-core — it is only **stripped** for SHOW CREATE
(`PluginDrivenExternalTable:731`), never functionally consumed — so a collision there just hides it from
rendering (already the case). We strip it too for defensive symmetry with paimon's own stamp.

## Fix (producer-side; the only workable locus)
fe-core cannot defend: once merged, connector-emitted and user-passthrough `partition_columns` are
byte-identical strings in one map, and distinguishing them would require fe-core to parse connector key
semantics (iron-rule violation). So each producer that pass-through-merges must ensure the reserved key
reflects **only its own determination** — remove it right after the `putAll`, before the conditional stamp:
- iceberg after `:412` — `tableProps.remove("partition_columns");`
- hive after `:450` — `tableProperties.remove(PARTITION_COLUMNS_PROPERTY);`
- paimon after `:304` — `schemaProps.remove("partition_columns"); schemaProps.remove("primary_keys");`

No fe-core change; connectors already emit these literals; no source-name branching. No user-facing
regression: `getTableProperties():731` already unconditionally strips both keys from SHOW CREATE PROPERTIES,
so a user's literally-named property is invisible today regardless — the only behavior that changes is the
removal of the incorrect partition misdetection.

## Iron rule
Clean. Entirely in connector modules, which already own/emit these reserved key names. No new fe-core
import, no `instanceof HMSExternal*`, no property parsing pushed into fe-core.

## Tests (RED-able, connector modules, recording fakes)
- iceberg: unpartitioned `db1.t1` with `table.properties()={"partition_columns":"id"}` (a real column) →
  emitted `ConnectorTableSchema.getProperties()` must NOT contain `partition_columns` (RED: leaks via `:412`).
  Guard: a genuinely partitioned (identity `name`) table with a colliding `{"partition_columns":"id"}`
  still emits the spec-derived `partition_columns=name` (fix doesn't over-strip; connector value wins).
- hive: `HiveTableInfo` with NO partition keys but parameters `{"partition_columns":"c1"}` → emitted props
  lack `partition_columns`. Guard: partitioned table still emits its key list.
- paimon: **no dedicated unit test** — the leak vector is `((DataTable) table).coreOptions().toMap()`, but
  the module's `FakePaimonTable implements Table` (NOT `DataTable`), so the `putAll`+`remove` block is
  skipped for the fake and a real `DataTable` fake (many abstract methods) is disproportionate for a 2-line
  defensive fix identical in shape to the iceberg/hive fixes (both RED-tested above). The paimon path is
  verified by inspection + covered by the same pattern; a real paimon table `WITH('partition_columns'=…)`
  collision is **E2E/live-gated**. Registered, not silently skipped (Rule 12).

## Blast radius
Producer-local (isolated `remove()` calls each immediately before an existing conditional put). Normal
tables (no bare `partition_columns`/`primary_keys` source property) are byte-identical before/after. Per
connector by necessity — no shared producer base for buildTableSchema.

## Note (out of scope — design debt)
The root smell is bare-named reserved keys (`partition_columns`/`primary_keys`) vs the namespaced ones
(`show.*`, `connector.*`) that source properties cannot realistically hit. Namespacing them (constant in
`fe-connector-api ConnectorTableSchema`) would be collision-resistant by construction but touches all
producers + the consumer + both strip sites — larger than this minimal defensive fix. Tracked as D-MAGICKEY.
