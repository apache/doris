# Summary — Paimon partition_values() TVF renders raw spec

## Problem
`SELECT * FROM partition_values(...)` over a paimon table with a DATE partition failed the
whole query (`convertStringToDateV2("19723")` threw → error result); a null partition
rendered the literal sentinel `__DEFAULT_PARTITION__` instead of SQL NULL.

## Root Cause
`PaimonConnectorMetadata.collectPartitions` put the un-rendered `partition.spec()`
(DATE=epoch-day, null=`__DEFAULT_PARTITION__`) into `ConnectorPartitionInfo`'s value map,
which the active TVF feeder `PluginDrivenExternalTable.getNameToPartitionValues` reads by
remote name. Every other connector's value map already holds decoded canonical strings
(hive: `__HIVE_DEFAULT_PARTITION__` + date-strings); paimon was the outlier.

## Fix
Connector-side, zero fe-core delta: build the `ConnectorPartitionInfo` value map with the
SAME rendered/normalized values already computed into `orderedValues` (DATE via
`DateTimeUtils.formatDate`, null → `ConnectorPartitionValues.HIVE_DEFAULT_PARTITION`), keyed
by the raw remote column name. Only the values change; keys stay remote names.

Rejected the alternative of switching the shared fe-core consumer to
`getOrderedPartitionValues()` — it would regress iceberg null partitions (rendered as literal
`"null"`) and break MaxCompute (empty ordered values).

## Tests
- Updated `PaimonConnectorMetadataPartitionTest.legacyNameTrueRendersDateKeyAndCarriesStats`:
  the value map now asserts the rendered date (was raw epoch-day); comment corrected.
- Renamed/updated `listPartitionValuesUsesRequestedColumnOrderWith{Raw→Rendered}Values`: the
  dormant `listPartitionValues` SPI now returns rendered values; comment (which encoded the
  buggy "TVF wants raw epoch-day" contract) corrected.
- Added `partitionValueMapCarriesRenderedValuesForTvf`: asserts the value map holds the
  rendered date for a DATE partition and `HIVE_DEFAULT_PARTITION` for a genuine-null DATE
  partition — the exact contract the TVF consumer needs (parseable date + SQL NULL).

## Result
`PaimonConnectorMetadataPartitionTest`: 11 tests, 0 failures. Also ran the other three
partition-touching classes (`PaimonScanPlanProviderCapabilityTest`,
`PaimonConnectorMetadataReadAuthTest`, `PaimonConnectorMetadataPartitionViewCacheTest`) —
35 tests total, 0 failures, BUILD SUCCESS. Checkstyle clean. The `collectPartitions`
change regresses no scan / read-auth / partition-view-cache behavior.
