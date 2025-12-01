## Summary

This is a tracking issue for implementing the remaining Apache Iceberg procedures in Doris. Currently, Doris has implemented 6 out of 20 Iceberg procedures. This issue tracks the implementation status and provides a roadmap for the remaining procedures.

**Important**: Doris does not include Spark dependencies. Therefore, procedures that rely on Spark-specific APIs (like `SparkActions`, `SparkTableUtil`, Spark `Dataset` operations) require re-implementing the distributed processing logic using Doris execution engine, which significantly increases implementation complexity. Procedures that can use Iceberg Core API directly are much easier to implement.

## Reference Documentation

- **Iceberg Procedures Documentation**: https://iceberg.apache.org/docs/latest/spark-procedures/
- **Doris Iceberg Catalog Documentation**: https://doris.apache.org/docs/3.x/lakehouse/catalogs/iceberg-catalog#iceberg-table-actions

## Current Syntax

Doris uses the `ALTER TABLE EXECUTE` syntax to execute Iceberg procedures:

```sql
ALTER TABLE [catalog_name.][database_name.]table_name 
EXECUTE procedure_name 
(property_key = property_value, ...)
[PARTITION (partition_name, ...)]
[WHERE condition]
```

### Examples

```sql
-- Rollback to a specific snapshot
ALTER TABLE iceberg_db.my_table EXECUTE rollback_to_snapshot ("snapshot_id" = "123456789");

-- Rollback to a timestamp
ALTER TABLE iceberg_db.my_table EXECUTE rollback_to_timestamp ("timestamp" = "2024-01-01T00:00:00");

-- Set current snapshot
ALTER TABLE iceberg_db.my_table EXECUTE set_current_snapshot ("snapshot_id" = "123456789");

-- Cherry-pick snapshot
ALTER TABLE iceberg_db.my_table EXECUTE cherrypick_snapshot ("snapshot_id" = "123456789");

-- Fast-forward branch
ALTER TABLE iceberg_db.my_table EXECUTE fast_forward ("branch" = "feature_branch", "snapshot_id" = "123456789");

-- Rewrite data files
ALTER TABLE iceberg_db.my_table EXECUTE rewrite_data_files (
    "target-file-size-bytes" = "536870912",
    "min-input-files" = "5",
    "max-file-size-bytes" = "1073741824"
);
```

For more details, see the [Doris Iceberg Catalog documentation](https://doris.apache.org/docs/3.x/lakehouse/catalogs/iceberg-catalog#iceberg-table-actions).

## Implementation Status

### ✅ Implemented Procedures (6/20)

The following procedures have been implemented in Doris:

1. ✅ **rollback_to_snapshot** - Rollback table to a specific snapshot
   - PR: https://github.com/apache/doris/pull/56257
   - Implementation: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergRollbackToSnapshotAction.java`
   - Syntax: `ALTER TABLE table_name EXECUTE rollback_to_snapshot ("snapshot_id" = "...")`

2. ✅ **rollback_to_timestamp** - Rollback table to a specific timestamp
   - PR: https://github.com/apache/doris/pull/56257
   - Implementation: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergRollbackToTimestampAction.java`
   - Syntax: `ALTER TABLE table_name EXECUTE rollback_to_timestamp ("timestamp" = "...")`

3. ✅ **set_current_snapshot** - Set the current snapshot for a table
   - PR: https://github.com/apache/doris/pull/56257
   - Implementation: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergSetCurrentSnapshotAction.java`
   - Syntax: `ALTER TABLE table_name EXECUTE set_current_snapshot ("snapshot_id" = "...")`

4. ✅ **cherrypick_snapshot** - Cherry-pick a snapshot to the current branch
   - PR: https://github.com/apache/doris/pull/56257
   - Implementation: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergCherrypickSnapshotAction.java`
   - Syntax: `ALTER TABLE table_name EXECUTE cherrypick_snapshot ("snapshot_id" = "...")`

5. ✅ **fast_forward** - Fast-forward a branch to a specific snapshot
   - PR: https://github.com/apache/doris/pull/56257
   - Implementation: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergFastForwardAction.java`
   - Syntax: `ALTER TABLE table_name EXECUTE fast_forward ("branch" = "...", "snapshot_id" = "...")`

6. ✅ **rewrite_data_files** - Rewrite data files to optimize file layout
   - PR: https://github.com/apache/doris/pull/56638
   - Implementation: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergRewriteDataFilesAction.java`
   - Syntax: `ALTER TABLE table_name EXECUTE rewrite_data_files ("target-file-size-bytes" = "...", ...)`

**Factory Class**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergExecuteActionFactory.java`

### Focus Procedures

#### ✅ Easy Wins (Core API Available)

1. **`ancestors_of`**  
   - **Description**: Reports the live snapshot IDs of the ancestors of a specified snapshot. If no snapshot ID is provided, it uses the current snapshot. This procedure helps trace the snapshot lineage and understand the commit history of a table.
   - **Use Cases**: Locate rollback targets, debug commit relationships in multi-branch/multi-tag scenarios, understand table evolution history.
   - **Available APIs**: `SnapshotUtil.ancestorIdsBetween()`, `Table.snapshot()` (Iceberg Core).  
   - **Implementation Notes**: Read-only operation that queries metadata directly. This is the easiest procedure to implement in Doris.

2. **`publish_changes`**  
   - **Description**: Publishes staged changes to a table. In a Write-Audit-Publish (WAP) workflow, this procedure applies changes from a snapshot created with a `wap_id` to the main table, making the changes visible atomically.
   - **Use Cases**: Write data first, then publish after audit approval to ensure data quality. Enables safe data staging and validation before making changes visible to users.
   - **Available APIs**: `Table.manageSnapshots().cherrypick()`, `WapUtil.stagedWapId()` (Iceberg Core).  
   - **Implementation Notes**: Similar flow to the existing `cherrypick_snapshot`, with an additional step to search for snapshots by `wap_id`.

3. **`register_table`**  
   - **Description**: Registers an existing Iceberg table with an Iceberg catalog. This allows tables that were created externally (e.g., using Spark or other tools) to be managed through the catalog.
   - **Use Cases**: Bring externally created Iceberg tables under Doris Catalog management for easier querying and maintenance. Useful when migrating tables or integrating with existing Iceberg deployments.
   - **Available APIs**: `Catalog.registerTable()`, `Table.currentSnapshot()`, `SnapshotSummary` (Iceberg Core).  
   - **Implementation Notes**: Core functionality is calling the Catalog native interface. Doris needs to ensure external Catalogs (e.g., HMS, REST) support the register capability.

#### ⚙️ Important Table Optimization Procedures (Medium Complexity - Core Action Implementation Required)

3. **`expire_snapshots`** ⚠️ (Partially Implemented - Core Logic Pending)
   - **Description**: Removes old snapshots and their associated data files that are no longer needed. This procedure helps manage storage by removing outdated snapshots while preserving recent history based on age or count criteria.
   - **Use Cases**: Reduce storage costs, improve metadata performance, maintain table history within specified retention policies. Essential for production environments with frequent commits.
   - **Available APIs**: `RemoveSnapshots` class (Iceberg Core) - can be instantiated directly. Supports `expireOlderThan()`, `retainLast()`, `expireSnapshotId()` methods.
   - **Implementation Notes**: Framework and parameter validation already implemented. Need to complete `executeAction()` method to create `RemoveSnapshots` instance, apply parameters, and execute deletion. Returns statistics about deleted files.

4. **`remove_orphan_files`**  
   - **Description**: Removes files in the table's data directory that are not referenced by any snapshot. These orphan files can accumulate over time due to failed writes, manual deletions, or other operations, consuming storage space unnecessarily.
   - **Use Cases**: Reduce storage costs and prevent accumulation of old version data. Essential for maintaining clean storage and optimizing costs in production environments.
   - **Available APIs**: `BaseDeleteOrphanFiles` (must implement the specific Action in Doris).  
   - **Implementation Notes**: Need to handle `older_than` safety threshold (minimum 24 hours), dry-run mode, prefix matching, etc. Can reference Iceberg Core's Action interface.

5. **`rewrite_manifests`**  
   - **Description**: Rewrites manifest files to optimize metadata layout. This procedure merges small manifest files into larger ones, reducing the number of metadata files that need to be read during query planning and execution.
   - **Use Cases**: Improve query planning speed and reduce metadata overhead. Works together with `rewrite_data_files` to form a complete table optimization strategy. Particularly important for tables with many small files or frequent writes.
   - **Available APIs**: `BaseRewriteManifests`.  
   - **Implementation Notes**: Similar flow to Doris's already implemented `rewrite_data_files`, can reuse parallel scheduling and task partitioning frameworks.

6. **`rewrite_position_delete_files`**  
   - **Description**: Rewrites position delete files to optimize delete file layout. This procedure merges small position delete files into larger, more efficient ones, reducing the overhead of reading delete files during queries.
   - **Use Cases**: Maintain high read performance in frequent delete scenarios. Forms a complete optimization chain with `rewrite_data_files` and `rewrite_manifests`. Essential for tables using row-level deletes (MERGE, UPDATE, DELETE operations).
   - **Available APIs**: `BaseRewritePositionalDeleteFiles`.  
   - **Implementation Notes**: Doris already has position delete parsing and execution logic, can implement Core Action based on this foundation.

### 🚧 Other High Complexity Procedures (Not Currently Planned)

| Procedure | Reason / Dependencies | Notes |
| --- | --- | --- |
| `snapshot` | Requires implementing `BaseSnapshotTable`, covers cross-table copy semantics | Lower priority than the optimization procedures above |
| `migrate` | Requires implementing `BaseMigrateTable` + Hive Metastore integration | Depends on external Hive environment |
| `add_files` | Heavily depends on `SparkTableUtil`/`Spark3Util`, needs to be rewritten using Doris execution engine | Complex logic, high cost |
| `create_changelog_view` | Depends on Spark Dataset (repartition/sort/mapPartitions) | Needs to rewrite CDC view construction in Doris |
| `compute_table_stats` / `compute_partition_stats` | Official implementation not open-sourced or version differences, requires deep research into Iceberg Core | Currently lacks clear API path |
| `rewrite_table_path` | Involves directly rewriting metadata.json/manifest files | Requires deep understanding of Iceberg metadata structure |

> The above high complexity procedures are not currently planned for implementation. Will be re-evaluated after core capabilities are completed.

## Implementation Guidelines

### Implementation Pattern

All procedures should follow the existing implementation pattern:

1. Register the new procedure in `IcebergExecuteActionFactory`
2. Create corresponding Action class extending `BaseIcebergAction`
3. Implement `registerIcebergArguments()` to register parameters
4. Implement `executeAction()` to execute the specific logic
5. Add unit tests and integration tests

### Reference Files

- **Factory Class**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergExecuteActionFactory.java`
- **Base Class**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/BaseIcebergAction.java`
- **Example Implementation**: `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/IcebergExpireSnapshotsAction.java`
- **Test File**: `regression-test/suites/external_table_p0/iceberg/action/test_iceberg_execute_actions.groovy`

## Progress Tracking

- [ ] **Low Complexity (3 procedures)** - Direct Core API
  - [ ] `ancestors_of` ✅ Core API
  - [ ] `publish_changes` ✅ Core API
  - [ ] `register_table` ✅ Core API

- [ ] **Medium Complexity (6 procedures)** - Core Action Interface
  - [ ] `expire_snapshots` ⚠️ Core API (Partially Implemented - Core Logic Pending)
  - [ ] `snapshot` ⚠️ Core Action Interface
  - [ ] `remove_orphan_files` ⚠️ Core Action Interface
  - [ ] `rewrite_manifests` ⚠️ Core Action Interface
  - [ ] `rewrite_position_delete_files` ⚠️ Core Action Interface
  - [ ] `migrate` ⚠️ Core Action Interface + Hive