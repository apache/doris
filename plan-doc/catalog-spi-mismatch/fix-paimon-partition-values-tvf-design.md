# Fix — Paimon partition_values() TVF renders raw spec (DATE/null broken)

## Problem
`SELECT * FROM partition_values(...)` over a paimon table with a DATE partition **fails the
whole query**; a null partition renders the wrong value:
- DATE partition (default `partition.legacy-name=true`): raw value is the epoch-day integer
  string (e.g. `"19723"`); the TVF consumer calls `convertStringToDateV2("19723")` which
  throws `DateTimeParseException` (no `-`), caught in `MetadataGenerator` and turned into an
  error result → the entire TVF query fails. Typed-null partitions throw likewise.
- null partition: raw value is paimon's sentinel `"__DEFAULT_PARTITION__"`, which the
  consumer's null-check (against `__HIVE_DEFAULT_PARTITION__`) does not recognize → rendered
  as the literal sentinel string instead of SQL NULL.

## Root Cause
`ConnectorPartitionInfo` carries two representations: a raw value map
(`getPartitionValues()`, by remote column name) and rendered values
(`getOrderedPartitionValues()` + null flags, name-segment order). The active TVF feeder
`PluginDrivenExternalTable.getNameToPartitionValues` (`:870/:892`) reads the **raw** map.

Paimon's `collectPartitions` puts the un-rendered `partition.spec()` into the raw map
(DATE=epoch-day, null=`__DEFAULT_PARTITION__`) and only renders into `orderedValues`. Every
other connector's raw map already holds canonical strings: hive stores
`__HIVE_DEFAULT_PARTITION__` and date-strings; iceberg stores the decoded strings (and Java
null for null). **Paimon is the outlier** putting raw native values in the map.

The partition **pruning** path is unaffected: `PluginDrivenMvccExternalTable` overrides
`getNameToPartitionItems` to consume the rendered `getOrderedPartitionValues()`/null flags
(`:293-295`), not the raw map.

## Design
Fix at paimon's render point (connector-side, **zero fe-core delta**): build the
`ConnectorPartitionInfo` value map with the SAME rendered/normalized values already computed
into `orderedValues` — DATE via `DateTimeUtils.formatDate`, null →
`ConnectorPartitionValues.HIVE_DEFAULT_PARTITION` — keyed by the raw remote key
(`entry.getKey()`). Only the **values** change; keys stay the remote column names so the
`getNameToPartitionValues` remote-name lookup is unaffected. This aligns paimon's map with
hive's already-canonical convention.

### Why not switch the fe-core consumer to `getOrderedPartitionValues()` (rejected)
`getNameToPartitionValues` is shared by ALL connectors. Iceberg's `orderedValues` renders a
null partition as the literal string `"null"` (`String.valueOf(null)`) with **empty** null
flags, while its raw map holds Java-null → the current TVF yields SQL NULL. Switching would
regress iceberg null partitions to literal `"null"` (or parse errors for typed columns);
MaxCompute supplies **empty** `orderedValues` and would break entirely. Ownership belongs in
the paimon connector.

## Implementation Plan
In `PaimonConnectorMetadata.collectPartitions` per-partition loop (`~:1132-1184`):
1. Declare `Map<String, String> renderedValues = new LinkedHashMap<>(spec.size());`.
2. In each of the three branches, `renderedValues.put(entry.getKey(), <rendered>)` with the
   same value appended to `orderedValues`.
3. Pass `renderedValues` (not `spec`) as the 2nd `ConnectorPartitionInfo` arg; update the
   `// partitionValues = RAW spec` comment.

## Risk Analysis
- No production consumer needs paimon's raw value map: base `getNameToPartitionItems` (raw)
  is overridden for paimon (MVCC); paimon's own `listPartitionValues` (also reads the map) is
  dormant (no fe-core production caller) and wants rendered values too; paimon sys tables are
  unpartitioned (empty partition columns → early return) so the base raw path never runs.
- Keys unchanged → `getNameToPartitionValues` lookup unaffected.
- Reuses `DateTimeUtils.formatDate(...)` output already computed for `orderedValues`; adds no
  new parse/throw surface.

## Test Plan
### Unit Tests (must update — these pin the OLD, buggy raw contract)
- `PaimonConnectorMetadataPartitionTest.legacyNameTrueRendersDateKeyAndCarriesStats:118` —
  flip the assertion from raw epoch-day to the rendered date; update the WHY comment.
- `PaimonConnectorMetadataPartitionTest.listPartitionValuesUsesRequestedColumnOrderWithRawValues`
  (`:175`) — flip to rendered values; the current comment ("TVF contract requires RAW values,
  the epoch-day int, never the rendered date") encodes the buggy contract and must be
  corrected/renamed.
- Add a new test asserting `getPartitionValues()` holds the rendered date and, for a null
  partition, `HIVE_DEFAULT_PARTITION` — i.e. exactly what the TVF consumer expects (SQL NULL
  and a parseable date).

### E2E Tests
Deferred with the batch's TVF e2e (paimon not yet in `SPI_READY_TYPES`; unit coverage pins
the behavior now).
