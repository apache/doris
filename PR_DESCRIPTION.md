### What problem does this PR solve?

Problem Summary:

Doris can currently use Colocate Join when the join equality conditions directly cover all Hash distribution keys of two tables in the same stable Colocate Group.

However, some data models contain determinant columns that consistently map to distribution keys across tables. For example, if both tables are distributed by `tenant_id`, and each `user_id` always belongs to exactly one `tenant_id`, joining the tables by `user_id` is also colocated when the mapping from `user_id` to `tenant_id` is consistent across both tables.

Previously, Doris could not declare or use this cross-table mapping relationship. Such queries therefore required Shuffle Join even though matching rows were already located in corresponding Buckets.

This PR introduces a `COLOCATE MAPPING` constraint and allows Nereids to use the declared mapping when proving that a Join can run as a Colocate Join.

The implementation supports:

- A mapping determinant covering one or more ordered distribution-key positions.
- Multiple mappings jointly covering a composite distribution key.
- A combination of direct distribution-key equalities and mapping-derived coverage.
- Additional equality predicates that do not affect an already complete colocate proof.
- Conservative propagation through Project and supported ordinary non-DISTINCT Aggregate plans when the underlying natural Bucket locality remains valid, even if the final output removes original distribution-key columns.
- Conservative invalidation across runtime placement barriers that cannot truthfully preserve the original storage Bucket-to-task locality.
- Automatic fallback to another valid join distribution strategy, such as Shuffle Join or Broadcast Join, when a mapping proof is unavailable.
- Table-local metadata persistence and ADD, DROP, and SHOW lifecycle management for the new constraint.

The optimization is controlled by the session variable:

```sql
SET enable_colocate_mapping_constraint = true;
```

It is disabled by default. When disabled, scan distribution properties, join property requests, and Colocate Join decisions retain their original behavior.

The constraint is declared as `NOT ENFORCED`. Doris trusts the mapping supplied by the user and does not validate it during INSERT, UPDATE, load, compaction, or schema change. Declaring an incorrect mapping may produce incorrect query results when the optimization is enabled.

The mapping metadata is supported only for internal, non-temporary OLAP tables with Hash distribution. It is stored in the physical `OlapTable` object rather than in the global name-keyed constraint index. This keeps lifecycle behavior aligned with physical table ownership and avoids adding mapping-specific repair logic to external Catalog, HMS event, Rename, Replace, Recover, and recycle-bin paths.

This change is implemented entirely in FE. It reuses the existing Colocate Hash Join execution path and does not change BE Hash Join semantics or add FE-BE protocol fields.

### Release note

Added an experimental `COLOCATE MAPPING` constraint that allows Nereids to derive Colocate Join eligibility from user-declared mappings between join columns and Hash distribution keys. Mapping-enabled queries fall back to ordinary distribution planning when a valid locality proof is unavailable. DISTINCT/MultiDistinct and pure deduplication Aggregates do not propagate mapping proofs, while ordinary non-DISTINCT Aggregates remain supported under conservative proof conditions. Atomic Restore rejects a selected table when its backup metadata contains mappings; use a non-atomic Restore or create a backup without mappings.

### Applicable Scenarios

This feature is useful when:

- Tables are in the same stable Colocate Group.
- Tables use compatible single-column or composite Hash distribution layouts.
- Business-key columns consistently determine one or more distribution-key positions across the involved tables.
- Queries commonly join by those business keys rather than directly by every distribution key.

A typical example is:

```text
user_id -> tenant_id
```

where both tables are distributed by `tenant_id`, but queries frequently join by `user_id`.

This feature should only be used when the declared mapping has identical semantics across every participating table. The mapping ID, determinant order, and target distribution-key positions are part of that cross-table contract.

### Usage

Create two internal OLAP tables in the same Colocate Group:

```sql
CREATE TABLE orders (
    tenant_id BIGINT,
    user_id BIGINT,
    order_id BIGINT
)
DUPLICATE KEY(tenant_id, user_id)
DISTRIBUTED BY HASH(tenant_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "tenant_group"
);

CREATE TABLE users (
    tenant_id BIGINT,
    user_id BIGINT,
    user_name STRING
)
DUPLICATE KEY(tenant_id, user_id)
DISTRIBUTED BY HASH(tenant_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "tenant_group"
);
```

Declare the same logical mapping on both tables:

```sql
ALTER TABLE orders
ADD CONSTRAINT orders_user_mapping
COLOCATE MAPPING tenant_by_user (user_id)
DETERMINES DISTRIBUTION KEY (tenant_id)
NOT ENFORCED;

ALTER TABLE users
ADD CONSTRAINT users_user_mapping
COLOCATE MAPPING tenant_by_user (user_id)
DETERMINES DISTRIBUTION KEY (tenant_id)
NOT ENFORCED;
```

The constraint name is table-local, while the mapping identifier must match across tables:

```text
Constraint names: orders_user_mapping, users_user_mapping
Mapping identifier: tenant_by_user
```

Enable the optimization:

```sql
SET enable_colocate_mapping_constraint = true;
```

A Join using the determinant columns can then use Colocate Join:

```sql
SELECT *
FROM orders o
JOIN users u
  ON o.user_id = u.user_id;
```

Use `EXPLAIN` to verify the selected distribution strategy:

```sql
EXPLAIN
SELECT *
FROM orders o
JOIN users u
  ON o.user_id = u.user_id;
```

The plan should contain:

```text
INNER JOIN(COLOCATE)
```

The constraints can be removed with:

```sql
ALTER TABLE orders
DROP CONSTRAINT orders_user_mapping;

ALTER TABLE users
DROP CONSTRAINT users_user_mapping;
```

### Metadata Lifecycle and Operational Behavior

A `COLOCATE MAPPING` constraint is stored as a complete JSON snapshot under the reserved `__distribution_mapping_constraints` key in the `TableProperty.properties` map owned by its `OlapTable`. It is not inserted into the global, qualified-name-keyed `ConstraintManager.constraintsMap` used by PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints.

This table-local ownership defines the lifecycle behavior:

- Renaming a table preserves the same physical table object and therefore preserves its mappings without rewriting a secondary name index.
- Renaming or recovering a database preserves its table objects and their mappings.
- `TRUNCATE TABLE` preserves the table object and table properties, so mappings remain present after the partitions are replaced.
- A non-force table or database Drop keeps the physical object in the recycle bin. Recovering that object restores the same mappings.
- Creating a new table with the same name as a dropped table creates a different physical object and does not inherit mappings from the recycled object.
- During `REPLACE TABLE ... PROPERTIES("swap"="false")`, the replacement table object takes the target name and keeps the replacement object's mappings. The replaced object and its mappings follow the normal replacement lifecycle.
- During `REPLACE TABLE ... PROPERTIES("swap"="true")`, the two physical table objects exchange names while each object's mappings remain attached to that object.
- Backup copies the `OlapTable` metadata, including mappings.
- A non-atomic Restore of a selected table whose backup metadata contains mappings validates FE-version and schema compatibility before changing target-table state or creating replicas.
- Atomic Restore rejects a selected table whose backup metadata contains mappings. It is rejected before the destination table enters `in_atomic_restore`, before staging metadata is prepared, and before replicas are created, even when the destination currently has no conflicting constraint. Use a non-atomic Restore or create a backup without mappings.
- Atomic Restore when none of the selected backup tables contains mappings keeps the existing whole-table replacement semantics. Mappings that exist only on the current destination table do not survive a successful replacement.
- `CREATE TABLE LIKE` and CTAS create new physical table objects and do not copy mappings. A `NOT ENFORCED` business invariant cannot be inferred from schema similarity.

ADD binds each determinant and target distribution column to its current name, type, stable column unique ID, and base schema version. Planning and non-atomic Restore revalidate that binding before consuming the mapping. Query planning ignores an incompatible mapping and falls back to the ordinary distribution alternatives, while non-atomic Restore rejects it before changing target state. This prevents a same-name replacement column or an incompatible replayed schema change from silently producing a stale proof.

The following operations are rejected when they directly affect a determinant or target distribution column referenced by a mapping:

- `DROP COLUMN`.
- `RENAME COLUMN`.
- `MODIFY COLUMN`.
- Converting the table from Hash distribution to Random distribution.

For legacy tables whose referenced columns do not have stable column unique IDs, a base schema-version change invalidates the mapping conservatively. Drop and recreate the mapping after completing the schema change.

ADD and DROP use database read lock -> table write lock -> `ConstraintManager` lock ordering. The in-memory mutation and journal submission occur while metadata is protected. The journal `await()` runs only after database, table, and manager locks have been released.

While a table is participating in an Atomic Restore, both Mapping ADD and Mapping DROP are rejected by the normal ALTER-state fence. After cancellation, the fence is removed and the original table and mappings remain available. After successful replacement, the restored table owns exactly the mappings present in the backup; because selected backup tables containing mappings are rejected for Atomic Restore, the replacement table has no mappings.

Image persistence uses the reserved `__distribution_mapping_constraints` entry in the existing `TableProperty.properties` map. Each ADD or DROP serializes the complete mapping set in deterministic constraint-name order; dropping the final mapping persists `[]` instead of removing the key. The same one-entry properties map is journaled through the existing `OP_MODIFY_TABLE_PROPERTIES` envelope. An older FE can replay and checkpoint the opaque property without understanding the feature, while a supporting FE decodes the snapshot into its derived in-memory mapping map. Supporting FE versions do not publish these records as ordinary table-property binlogs.

SQL cache publication is disabled only after a Scan constructs at least one usable mapping proof. A table merely containing a mapping does not disable SQL cache when the session switch is off or when no proof is constructed. The feature changes physical distribution planning only and does not add mapping-specific MTMV rewrite-cache lifecycle hooks.

### Rolling Upgrade Restrictions

Mapping ADD and a non-atomic Restore of selected tables whose backup metadata contains mappings require every registered FE to report the exact current `version-shortHash`. Query planning uses mappings only under the same condition; if an FE has not reported a version or reports a different version, the query ignores mappings and falls back to the ordinary distribution alternatives. Atomic Restore of such a table remains unsupported even after all FE versions converge.

The restrictions are:

- Do not create mappings or non-atomically restore selected tables whose backup metadata contains mappings until every registered FE has completed the upgrade and reports the same exact build as the current FE.
- The session switch does not need to be forcibly disabled during rolling upgrade. If mappings already exist and the switch is enabled, mixed or unknown FE versions cause query planning to ignore the mappings and use the original planning behavior.
- A schema-incompatible mapping is handled the same way during queries: all mappings on that table are ignored and a rate-limited warning identifies the table and stale constraint. ADD and non-atomic Restore remain strict.
- DROP remains available during mixed-version operation so mappings can be removed before downgrade or topology changes involving an unsupported FE, unless the table is currently fenced by an Atomic Restore.
- Adding a same-version FE does not require deleting mappings. ADD and non-atomic Restore of selected tables containing mappings remain unavailable until the new FE is registered and reports the exact current version; queries automatically resume mapping optimization after convergence.

Recommended rolling-upgrade sequence:

1. Upgrade every FE. Existing sessions may keep `enable_colocate_mapping_constraint` enabled; queries use ordinary planning while versions are mixed.
2. Confirm that all registered FEs report the same exact `version-shortHash`.
3. Create mappings or non-atomically restore selected tables containing mappings only after version convergence.
4. Existing mappings become eligible for optimization automatically after convergence.

Downgrading an FE to a version that does not implement this feature while mappings remain is unsupported. Drop all mappings with a supporting FE version before starting the downgrade.

### External Catalog Constraint Consistency

`COLOCATE MAPPING` is deliberately unsupported for external Catalogs, HMS tables, `RemoteOlapTable`, and temporary tables. An ADD attempt on these table types fails instead of creating metadata that would need asynchronous reconciliation.

Consequently, this PR does not change external Catalog refresh, HMS notification, external Rename/Drop, connector event cursor, Catalog source-transition, or MTMV invalidation behavior. Existing PRIMARY KEY, FOREIGN KEY, and UNIQUE constraint behavior for external objects is unchanged by this feature.

This scope is intentional. External metadata can change outside Doris and is identified through Catalog-specific names, IDs, refreshes, and event streams. Supporting a user-trusted physical Bucket mapping there would require a separate identity, persistence, reconciliation, and failure model. Rejecting the feature at the DDL boundary avoids a large lifecycle patch surface unrelated to the core internal-OLAP optimization.

### Limitations

- The constraint is `NOT ENFORCED`; Doris does not verify mapping consistency during writes.
- Mapping-based optimization applies only to the underlying natural Hash distribution of internal, non-temporary OLAP tables.
- Both inputs must have compatible Hash layouts and must satisfy the existing stable Colocate Group checks.
- Mapping IDs have cluster-local user-defined semantics. Doris checks the ID, determinant arity, target positions, schema binding, and Join equalities, but cannot verify that the business mapping is truthful.
- Determinant and target-column order is significant.
- Distribution target columns must be an ordered subset of the table's distribution columns.
- Mapping propagation is conservative across projections. Direct Slots, simple aliases, and non-truncating character widening casts are supported. Other expressions or casts discard the affected proof.
- Mapping locality is discarded across runtime placement barriers, including Generate/LATERAL VIEW, Window and PartitionTopN, Nested Loop Join, and Broadcast Hash Join. An outer Join falls back to another valid distribution strategy instead of reusing the original storage locality proof.
- A selected rollup must expose the required determinant provenance. A rollup that removes the determinant or cannot preserve a complete natural-layout proof does not use the mapping.
- Aggregate propagation is proof-based and conservative. Only ordinary non-DISTINCT Aggregates are supported, and only when their physical child still carries natural Bucket locality, Group By uses direct Slots, and direct distribution keys plus complete mapping determinants cover every distribution-key position.
- DISTINCT aggregate functions, MultiDistinct phases, and pure deduplication Aggregates do not request or propagate mapping properties. The query remains supported, but an upper Join cannot rely on a mapping proof across that boundary and retains the ordinary distribution alternatives.
- Repeat/Grouping Sets, expression-based Group By, aggregation after a non-natural redistribution, incomplete composite determinants, uncovered distribution-key positions, and outputs that remove determinants required by an upper Join do not propagate a usable mapping proof.
- Union, Intersect, Except, multi-hop mapping closure, mapping-closure inference, and expression-based determinants are not supported.
- A mapping requirement is non-enforceable. The optimizer cannot insert an Exchange to manufacture it or degrade it into a one-sided Bucket Shuffle.
- Atomic Restore of a selected table whose backup metadata contains `COLOCATE MAPPING` constraints is unsupported. Non-atomic Restore remains supported after FE-version and schema validation.

The following examples use `orders` and `users` distributed by `tenant_id`, with `user_id` declared as the determinant of `tenant_id`.

An ordinary Aggregate can preserve the mapping when the complete determinant covers the distribution-key position:

```sql
SELECT user_id, SUM(amount)
FROM orders
GROUP BY user_id;
```

A query containing a DISTINCT aggregate function remains executable, but the Aggregate is a mapping-proof barrier. An upper Join that would need the proof to cross this Aggregate therefore uses ordinary distribution planning:

```sql
SELECT user_id, COUNT(DISTINCT order_id)
FROM orders
GROUP BY user_id;
```

The same conservative fallback applies to MultiDistinct phases and pure deduplication such as `SELECT DISTINCT user_id FROM orders`. This boundary prevents a DISTINCT plan from incorrectly forwarding a locality proof derived for a different row grouping.

The following Aggregate shapes do not propagate a usable mapping proof:

```sql
-- Repeat/Grouping Sets can produce grouping rows that do not contain the determinant.
SELECT tenant_id, user_id, SUM(amount)
FROM orders
GROUP BY GROUPING SETS ((tenant_id, user_id), (tenant_id));

-- Expression-based Group By is not the declared direct-Slot determinant.
SELECT user_id + 0, SUM(amount)
FROM orders
GROUP BY user_id + 0;
```

For a table distributed by `HASH(tenant_id, region_id)`, where only `user_id -> tenant_id` is declared, the following Group By leaves the `region_id` Bucket position uncovered:

```sql
SELECT user_id, SUM(amount)
FROM orders
GROUP BY user_id;
```

For a composite determinant `(country_id, user_id) -> tenant_id`, both determinant columns must be present in the proof. Joining or grouping only by `user_id` is insufficient.

An Aggregate may group by a determinant without returning it, but an upper Join cannot use that determinant after it has been removed from the Aggregate output:

```sql
SELECT *
FROM (
    SELECT SUM(amount) AS total_amount
    FROM orders
    GROUP BY user_id
) o
JOIN users u
  ON o.total_amount = u.user_id;
```

An Aggregate also does not propagate the mapping if its input has already been changed from the table's natural Bucket locality by an Exchange. For example, if the Join below requires Shuffle on `region_id`, the Aggregate above it cannot recover the original `tenant_id` Bucket locality:

```sql
SELECT o.tenant_id, o.user_id, SUM(o.amount)
FROM orders o
JOIN regions r ON o.region_id = r.region_id
GROUP BY o.tenant_id, o.user_id;
```

`UNION ALL` does not preserve mapping locality even when each input independently has a valid mapping:

```sql
SELECT user_id FROM current_orders
UNION ALL
SELECT user_id FROM archived_orders;
```

Multi-hop closure is not derived. Declaring or knowing `email -> user_id` and `user_id -> tenant_id` does not let Doris infer `email -> tenant_id`; a determinant must map directly to the distribution-key positions in a supported constraint.

Expression-based determinants are not accepted. For example, the following conceptual declaration is unsupported; determinants must be column Slots:

```sql
COLOCATE MAPPING tenant_by_email (LOWER(email))
DETERMINES DISTRIBUTION KEY (tenant_id)
NOT ENFORCED
```

### Upgrade and compatibility considerations

Mapping metadata is encoded for backward readability:

- The complete mapping set is serialized as JSON in the reserved `__distribution_mapping_constraints` entry of the `TableProperty.properties` map owned by the `OlapTable`, separate from the global polymorphic constraint map.
- ADD and DROP rewrite that complete snapshot and reuse the existing `OP_MODIFY_TABLE_PROPERTIES` journal record. The final DROP writes `[]`, so replay and checkpoint cannot resurrect an older mapping set.
- An older FE treats the entry as an opaque table property and preserves it through journal replay and image checkpoint. A supporting FE recognizes the key, rebuilds the derived mapping map, and skips ordinary table-property binlog publication for these records.

Backward readability prevents an older FE from failing merely because an image or journal contains the reserved property and preserves the opaque snapshot across checkpoint. It does not make the feature supported on that FE: an older FE cannot show or use mappings and does not enforce mapping-specific DDL safeguards.

The exact-version gate therefore remains required:

- Do not execute Mapping ADD or non-atomic Restore of selected tables containing mappings until all registered FEs report the exact current build.
- A missing reported version fails the ADD/non-atomic-Restore gate and makes queries fall back to ordinary planning.
- The gate compares reported version strings; it is not a capability-negotiation protocol. Custom binaries that report the same version string remain the operator's responsibility.
- Avoid changing FE membership concurrently with Mapping ADD or non-atomic Restore of selected tables containing mappings. Complete the membership change, wait for the FE to appear and report its version, and then retry the operation.
- Atomic Restore of a selected table containing mappings is rejected independently of this version gate.

Required downgrade procedure:

1. Stop enabling `enable_colocate_mapping_constraint`.
2. DROP all `COLOCATE MAPPING` constraints while a supporting FE version is still running.
3. Confirm cleanup with `SHOW CONSTRAINTS` on the affected tables.
4. Start the FE downgrade only after cleanup.

Do not complete a downgrade while mappings remain. Although an unsupported FE preserves the opaque snapshot, it cannot use or manage the feature and does not enforce mapping-specific DDL safeguards.

The same rules apply in Cloud mode. Wait until every expected FE is visible in the registered FE set and reports the exact current build before Mapping ADD or non-atomic Restore of selected tables containing mappings. Mapping-enabled queries fall back to ordinary planning while versions are mixed and resume the optimization after convergence. DROP remains the recovery path when versions are mixed, except while a table is fenced by an Atomic Restore.

### Check List (For Author)

- Test <!-- At least one of them must be included. -->
    - [x] Regression test
    - [x] Unit Test
    - [ ] Manual test (add detailed scripts or steps below)
    - [ ] No need to test or manual test. Explain why:
        - [ ] This is a refactor/code format and no logic has been changed.
        - [ ] Previous test can cover this change.
        - [ ] No code files have been changed.
        - [ ] Other reason <!-- Add your reason?  -->

- Behavior changed:
    - [ ] No.
    - [x] Yes. Adds the experimental COLOCATE MAPPING constraint and mapping-based Colocate Join inference for internal OLAP tables. The planner optimization is disabled by default and falls back to ordinary planning when its proof is unavailable, crosses a DISTINCT/MultiDistinct/pure-deduplication Aggregate, or crosses another unsupported runtime placement barrier. Atomic Restore rejects selected tables whose backup metadata contains mappings; non-atomic Restore remains supported after compatibility validation.

- Does this need documentation?
    - [ ] No.
    - [x] Yes. <!-- Add document PR link here. eg: https://github.com/apache/doris-website/pull/1214 -->

### Check List (For Reviewer who merge this PR)

- [ ] Confirm the release note
- [ ] Confirm test cases
- [ ] Confirm document
- [ ] Add branch pick label <!-- Add branch pick label that this PR should merge into -->
