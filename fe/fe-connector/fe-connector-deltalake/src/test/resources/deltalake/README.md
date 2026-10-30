# Delta Kernel metadata fixtures

Unmodified `_delta_log` files (including Parquet checkpoints and V2 sidecars) from
the Delta Lake project's Apache-2.0-licensed golden tables. Data files and Hadoop
`.crc` files are omitted: these tests exercise snapshot replay and scan planning.

- Release: v4.4.1
- Commit: `cebdc44dc07d331383e72811177af4ffc8638990`
- Source: `connectors/golden-tables/src/main/resources/golden`
- Tables: `dv-partitioned-with-checkpoint`, `v2-checkpoint-json`, `v2-checkpoint-parquet`
- Upstream: https://github.com/delta-io/delta/tree/cebdc44dc07d331383e72811177af4ffc8638990/connectors/golden-tables
- Source archive SHA-256: `a9b81e3e0a10c4b32afec823b5a2803eab57c06f542664478ecd438b2b982339`

Expected latest snapshots: version 15 with 15 files, 37 physical records and two
deleted positions for the classic checkpoint table; version 2 with four files
and ten records for each V2 checkpoint table.
