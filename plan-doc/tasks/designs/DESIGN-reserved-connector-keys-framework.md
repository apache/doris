# DESIGN — namespace all reserved connector control keys under `__internal.` (supersedes L19 silent-strip)

> **Status: IMPLEMENTED (user-directed).** Supersedes the L19 fix `01668779fd9` (per-connector silent
> `remove("partition_columns")`), which the user rejected: silent deletion discards user data with no signal,
> and a future developer adding a new reserved keyword would get no feedback.

## Decision (user, this session)
1. **Namespace every FE-internal reserved control key under `__internal.`** — not just the two bare ones
   (`partition_columns` / `primary_keys`) but also the already-namespaced `show.*` / `connector.*` keys, so
   the whole reserved namespace is uniform and distinctive.
2. **Rename only — NO validation / fail-loud.** A distinctive `__internal.` prefix makes a collision with a
   real user table property practically impossible (this is exactly how the pre-existing `show.*` /
   `connector.*` keys already avoided collision), so a runtime check is unnecessary. Consistent with those
   five keys, which have no fail-loud either.

## Why this is correct + minimal
- `partition_columns` (and every reserved control key) is **FE-only** — verified: none is emitted by any
  connector's `getScanNodeProperties`, and the table-properties map is never serialized into the BE thrift
  scan request. BE gets partition columns via the SEPARATE `path_partition_keys` scan property. So renaming
  has **zero BE impact**.
- The reserved keys are **not GSON/editlog-persisted** (`PluginDrivenSchemaCacheValue` has no serialization;
  the schema cache is rebuilt on demand), so there is **no persisted-state migration**.
- No regression golden `.out` file references any reserved key (they are all stripped from SHOW CREATE).
- The collision is eliminated **by construction**: a user's own bare `partition_columns` property now simply
  flows through as a normal user property (preserved, rendered in SHOW CREATE) — no silent strip, no failure,
  no misdetection. This is strictly better than both the pre-L19 collision and the L19 silent strip.

## Change
### `ConnectorTableSchema` (fe-connector-api) — single source of truth
- New `INTERNAL_KEY_PREFIX = "__internal."`.
- Every reserved key is `INTERNAL_KEY_PREFIX + <old value>`:
  `__internal.partition_columns`, `__internal.primary_keys`, `__internal.show.location`,
  `__internal.show.partition-clause`, `__internal.show.sort-clause`,
  `__internal.connector.per-table-capabilities`, `__internal.connector.distribution-columns`.
- `partition_columns` / `primary_keys` promoted from scattered bare literals to the constants
  `PARTITION_COLUMNS_KEY` / `PRIMARY_KEYS_KEY`.
- New `RESERVED_CONTROL_KEYS` set for the fe-core strip.

### Producers — reference the central constants (no more bare literals)
iceberg / maxcompute / paimon: `put(ConnectorTableSchema.PARTITION_COLUMNS_KEY, …)` (paimon also
`PRIMARY_KEYS_KEY`). hive / hudi: their local `PARTITION_COLUMNS_PROPERTY` alias now `=
ConnectorTableSchema.PARTITION_COLUMNS_KEY`. The `show.*` / `connector.*` producers already used the
constants, so their key VALUES update centrally. **The L19 per-connector `remove()` strips are deleted**
(iceberg / hive / paimon) — no longer needed.

### fe-core consumer
- `PluginDrivenExternalTable.toSchemaCacheValue` reads `ConnectorTableSchema.PARTITION_COLUMNS_KEY`.
- `getTableProperties()` strips `ConnectorTableSchema.RESERVED_CONTROL_KEYS.contains(key)` (replaces the
  hardcoded key list → a future reserved key is stripped automatically).

## Behavior change
A source table (created anywhere) whose user properties contain a bare `partition_columns` / `primary_keys`
(or the old `show.location`, …) is now treated as a plain user property: preserved, flows through to SHOW
CREATE, never mistaken for a control key. Normal tables are byte-identical. No failure path.

## Iron rule / blast radius
- All logic in `fe-connector-api` (constants) + the connectors (reference constants) + the fe-core strip
  swap. No source-name branching, no property parsing added to fe-core. Wire format unchanged (still a map).
- FE-only rename → no BE / thrift / GSON / replay change.

## Tests
- Connector metadata tests assert the connector emits under `PARTITION_COLUMNS_KEY` (the constant), and NEW
  tests assert a user's bare `partition_columns` property **coexists** with the reserved key (partitioned) or
  **flows through** while the table stays unpartitioned (no collision) — replacing the L19 "stripped" asserts.
- fe-core `getTableProperties` strips the `RESERVED_CONTROL_KEYS` and passes a colliding bare user key through.

## Note
Realizes design debt **D-MAGICKEY**'s "promote the bare keys to declared constants" — now every reserved key
is a declared, namespaced constant in one place.
