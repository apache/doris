# COLOCATE MAPPING Constraint Design Document

## Document Scope

This document describes the purpose, architecture, module design, query and DDL flows, lifecycle behavior, compatibility strategy, operational constraints, and test scope of `COLOCATE MAPPING Constraint`.

The feature is implemented in FE metadata and the Nereids optimizer. It adds no BE operator and changes no FE-BE execution protocol.

## Terminology

| Term | Meaning |
|---|---|
| `COLOCATE MAPPING Constraint` | A user declaration that a business-key column set determines one or more physical distribution columns. Nereids can use it to prove that some joins do not require a Shuffle. |
| `determinant` | The ordered business column set on the left side of a mapping. It need not be unique, but equal determinant values must always imply equal target distribution-key values. |
| `mapping` | A determinant-to-distribution-position relationship. `DistributionMappingConstraint` is the persisted object and `DistributionMapping` is the planning-time object. |
| `mapping ID` | The identifier used to match the same business mapping across tables. Matching also requires the same determinant arity and target distribution positions. |
| Constraint name | A table-local name used by ADD, DROP, and SHOW. It is not the cross-table semantic identifier. |
| `NOT ENFORCED` | Doris trusts the declaration and does not validate existing or incoming data. A false declaration can cause an unsafe Shuffle elimination and incorrect query results. |
| Distribution key | The ordered columns in `DISTRIBUTED BY HASH(...)`. Column order is part of the distribution semantics. |
| Target distribution position | The zero-based position of a mapped column in the ordered Hash distribution key. The final proof must cover every position. |
| Natural distribution | The storage Bucket layout delivered by an OLAP Scan. It is no longer natural after an Exchange redistributes rows. |
| Natural mapping proof | The FE-only `NaturalDistributionMappingSpec` that records the physical table, selected index, partitions, Bucket positions, and currently visible determinants. It cannot create an Exchange. |
| Stable column unique ID | Persistent column identity used to distinguish an original column from a same-name replacement. Legacy tables without stable IDs use base schema version as a conservative substitute. |
| Table-local metadata | A complete JSON mapping snapshot is stored under a reserved internal key in the `TableProperty.properties` map owned by the `OlapTable` and follows the physical table object instead of being indexed by table name in global `ConstraintManager.constraintsMap`. |
| Rolling-upgrade gate | Before ADD or Restore, every registered FE must report the exact current `version-shortHash`. Query planning ignores mappings and falls back to regular planning when a version is different or unknown. |

## Feature Overview

For two Hash-distributed tables in the same stable Colocate Group, Doris normally requires join equalities to cover all distribution columns directly. If both tables are distributed by `tenant_id`, the direct proof is:

```sql
ON orders.tenant_id = users.tenant_id
```

When the business invariant is `user_id -> tenant_id`, a join on `user_id` also connects matching rows in corresponding Buckets:

```sql
ON orders.user_id = users.user_id
```

The feature lets the user declare that invariant explicitly:

```sql
ALTER TABLE orders
ADD CONSTRAINT orders_user_mapping
COLOCATE MAPPING tenant_by_user (user_id)
DETERMINES DISTRIBUTION KEY (tenant_id)
NOT ENFORCED;
```

When both tables declare compatible mappings and every proof condition succeeds, Nereids can select the existing `COLOCATE` Hash Join. Otherwise it retains the existing Shuffle or Broadcast alternatives.

The feature is disabled by default:

```sql
SET enable_colocate_mapping_constraint = true;
```

### Code Composition

The following figures use `git diff --numstat` for the current worktree against the `upstream/master` merge-base and include the latest persistence and Aggregate-barrier tests. Each file is assigned once by primary responsibility, so the percentages describe change composition rather than complexity or risk.

| Category | Files | Added/Deleted Lines | Share of All Changes | Share of Production Changes |
|---|---:|---:|---:|---:|
| Core feature logic | 21 | 1,279 / 95 | 25.3% | 72.1% |
| Metadata and lifecycle compatibility | 8 | 521 / 11 | 9.8% | 27.9% |
| Unit tests, regression cases, and results | 16 | 3,496 / 25 | 64.9% | N/A |
| Total | 45 | 5,296 / 131 | 100% | N/A |

Core logic is concentrated in:

- Syntax and commands: `DorisLexer.g4`, `DorisParser.g4`, `LogicalPlanBuilder.java`, `Constraint.java`, `AddConstraintCommand.java`, `DropConstraintCommand.java`, and `ShowConstraintsCommand.java`.
- Constraint model: `DistributionMappingConstraint.java` and the catalog `Constraint.java`.
- Scan and properties: `LogicalOlapScanToPhysicalOlapScan.java`, `DistributionMapping.java`, `DistributionSpecHash.java`, `NaturalDistributionMappingSpec.java`, and `PhysicalProperties.java`.
- Propagation and final join proof: `PhysicalHashAggregate.java`, `ChildOutputPropertyDeriver.java`, `RequestPropertyDeriver.java`, `ChildrenPropertiesRegulator.java`, `CostAndEnforcerJob.java`, and `JoinUtils.java`.
- Session control: `SessionVariable.java`.

Metadata and lifecycle compatibility is concentrated in:

- Table-local storage and access: `TableProperty.java` and `ConstraintManager.java`.
- Journal and replay: `ModifyTablePropertyOperationLog.java`, `EditLog.java`, and `Env.java`.
- DDL protection and restore: `SchemaChangeHandler.java` and `RestoreJob.java`.
- FE version visibility: `Frontend.java`.

Unlike the earlier patch-oriented design, the current implementation does not modify external catalogs, HMS events, `RefreshManager`, MTMV rename, recycle-bin, or Replace/Swap code paths. Most lifecycle correctness follows from one rule: the mapping belongs to the physical table object.

## 1. Purpose

### 1.1 Applicable Scenarios

- Internal OLAP tables are in the same stable Colocate Group.
- Their Hash distribution layouts are compatible.
- Queries commonly join on business keys that stably determine the physical distribution keys.
- The user can validate the declared invariant through data-production controls or independent audits.

### 1.2 Supported Scope

- Single-column and composite determinants.
- One mapping covering one or more ordered distribution-key positions.
- Multiple mappings jointly covering a composite distribution key.
- Direct distribution-key equalities combined with mapping-derived coverage.
- Conservative propagation through Project and supported ordinary non-DISTINCT Aggregate plans that preserve natural Bucket locality.
- Direct Slot aliases and non-truncating, hash-value-preserving character widening casts in Project output.
- Safe fallback to existing join distribution alternatives when ordinary proof conditions fail.

### 1.3 Explicit Non-Goals

- External catalogs, HMS tables, `RemoteOlapTable`, and temporary tables.
- Non-Hash-distributed tables.
- Runtime enforcement of the `NOT ENFORCED` data invariant.
- Expression determinants, multi-hop inference, or mapping-closure inference.
- Using a mapping to construct Exchange, Shuffle, or Bucket Shuffle.
- Propagation through Union, Intersect, Except, Repeat/Grouping Sets, or a path already redistributed by Exchange.
- Propagation across DISTINCT aggregate functions, MultiDistinct phases, or pure deduplication Aggregates. Such queries remain executable, but upper planning that depends on proof crossing this boundary falls back.
- Automatic mapping copies for CREATE TABLE LIKE or CTAS.

## 2. Overall Design

### 2.1 Design Principles

1. **Mappings belong to physical table objects**: a complete snapshot lives under a reserved internal key in the `TableProperty.properties` map owned by the `OlapTable`; renames and object lifecycle operations do not maintain a second name-based index.
2. **Proof, not execution capability**: a mapping proves existing Bucket locality and reuses the current Colocate Hash Join.
3. **Two-stage conservative validation**: each child must satisfy a non-enforceable mapping request, followed by a final cross-table mapping and colocate validation.
4. **Do not use what cannot be proven**: an ordinary proof miss, schema-incompatible mapping, or mixed FE version prevents construction of a mapping proof and makes the query fall back to regular planning; metadata mutation and Restore still fail strictly.
5. **Central lifecycle rules**: table-local ownership, schema identity binding, and a single version gate replace per-operation patches.
6. **Backward-readable and preservable persistence**: the complete snapshot uses the existing table-property map and journal envelope, so old FE replays and checkpoints the internal key as opaque data. It still cannot understand or use mappings, so the version gate prevents it from participating in feature use.

### 2.2 Metadata Model

`DistributionMappingConstraint` persists:

- The table-local constraint name and cross-table mapping ID.
- Ordered determinant and target distribution-column names.
- The base schema version at binding time.
- Stable unique IDs for determinant and target columns.
- Type signatures for determinant and target columns.

ADD binds the declaration to the current schema. Planning and Restore then verify:

- The table is still Hash distributed.
- Target columns remain an ordered subset of the current distribution key.
- Names, types, and stable unique IDs still match.
- If a referenced column has no stable unique ID, the base schema version is unchanged.

The last rule intentionally makes any schema-version change invalidate a mapping on legacy tables without stable IDs. This avoids accidentally binding to a dropped-and-recreated same-name column; the operator must DROP and recreate the mapping.

### 2.3 Persistence Model

- Persistent source: `TableProperty.properties["__distribution_mapping_constraints"]` stores a complete JSON snapshot in deterministic constraint-name order. The runtime mapping map is derived from this property and is not persisted separately.
- ADD/DROP journal: every operation rewrites the complete snapshot through the standard properties field of `OP_MODIFY_TABLE_PROPERTIES`; dropping the final mapping explicitly writes `[]`.
- Old-FE replay/checkpoint: old code merges the internal key as an opaque table property and serializes it again, preserving the latest ADD or DROP snapshot without understanding, showing, or using mappings.
- New-FE replay: the table is located by database and table ID, then the complete snapshot is applied under the table write lock and decoded into the derived mapping map.
- Binlog: FE versions that support the feature do not publish mapping snapshots as ordinary table-property binlogs.

### 2.4 Planning Proof Model

```text
Olap Scan
  -> validate FE versions and mapping/schema binding
  -> convert persisted constraints into DistributionMapping objects
  -> create NATURAL hash property + NaturalDistributionMappingSpec
  -> conservatively remap proof through Project/ordinary non-DISTINCT Aggregate
  -> drop proof at DISTINCT/MultiDistinct/pure-deduplication Aggregate
  -> generate a COLOCATE_MAPPING_REQUIRE join candidate
  -> validate both child properties and the final cross-table proof
  -> reuse the existing COLOCATE Hash Join
```

`NaturalDistributionMappingSpec` separately carries hidden physical Bucket facts. A Project or ordinary non-DISTINCT Aggregate may remove an original distribution-key Slot without pretending that an executable Hash distribution still exists. The proof is only valid for mapping-based Colocate eligibility and is never enforceable by Exchange.

## 3. Module Design

### 3.1 SQL and Command Layer

Syntax:

```sql
ALTER TABLE <table>
ADD CONSTRAINT <constraint_name>
COLOCATE MAPPING <mapping_id> (<determinants>)
DETERMINES DISTRIBUTION KEY (<distribution_columns>)
NOT ENFORCED;
```

The command layer performs privilege checks, column resolution, object-identity revalidation, and ADD/DROP/SHOW dispatch. Mappings are accepted only for internal, non-temporary OLAP tables.

### 3.2 ConstraintManager and Locking

`ConstraintManager` is the access API, but mappings are not inserted into its global `constraintsMap`. It:

- Validates and reads or writes the table-local map.
- Prevents a mapping and an existing PK/FK/UNIQUE constraint from sharing a name.
- Creates schema identity bindings.
- Applies the centralized version gate and planning-time compatibility validation.
- Merges global constraints and table-local mappings for SHOW.

ADD/DROP lock order is database read lock -> table write lock -> ConstraintManager lock. The in-memory mutation and journal submission occur while metadata is locked; `EditLogItem.await()` runs after all metadata locks are released.

### 3.3 Schema Change

For a determinant or target distribution column referenced by a mapping:

- DROP COLUMN is rejected.
- RENAME COLUMN is rejected.
- MODIFY COLUMN is rejected.
- Hash-to-Random distribution conversion is rejected.

A same-name column change limited to a Rollup does not alter the base-table mapping and is not incorrectly blocked.

If an old FE replays an unsupported schema operation and bypasses these front-door checks, a new FE still detects the incompatibility through schema identity validation, ignores the table's mappings, emits a rate-limited warning, and falls back to regular planning instead of consuming a stale proof.

### 3.4 Scan and Property Propagation

Scan constructs a mapping proof only when the session switch is enabled, mappings exist, and version/schema validation succeeds. A mapping enters the physical property only when every determinant Slot is available.

Project supports:

- A directly forwarded Slot.
- `Alias(Slot)`.
- `Alias(Cast(Slot))` only for a non-truncating character widening cast that preserves the value and Hash bytes.

General expressions, narrowing casts, or a missing determinant drop the affected mapping.

Aggregate propagation is allowed only when:

- It is not a DISTINCT aggregate function, MultiDistinct phase, or pure deduplication Aggregate.
- Its child still carries natural Bucket locality and no Exchange has cut it.
- It does not originate from Repeat or Grouping Sets.
- Every Group By expression is a direct Slot.
- Direct distribution keys and complete determinants in Group By cover every distribution-key position.
- The output retains determinants required by the parent Join.

DISTINCT, MultiDistinct, and pure deduplication Aggregates both stop mapping-property requests from reaching their children and clear mapping proofs from their outputs. This prevents a DISTINCT plan from forwarding a locality proof that is valid only for a different row grouping. The query remains supported; only the Mapping optimization is disabled.

### 3.5 Join Proof

The mapping candidate uses non-enforceable `COLOCATE_MAPPING_REQUIRE`. If a child does not already satisfy it, the optimizer cannot insert an Exchange to manufacture success and must discard the candidate.

Final validation requires:

- Equal distribution-key counts.
- The same stable Colocate Group, or the existing same-table/same-index/single-partition exception.
- Slot-to-Slot equality for every hash conjunct.
- Matching mapping ID, determinant arity, and target distribution positions.
- Join equalities connecting determinant columns in declaration order.
- Complete coverage of every distribution position by direct equality or mapping.

Failure of any condition prevents a mapping-based Colocate Join.

### 3.6 Cache and Execution

- SQL cache is disabled only when a Scan actually constructs a mapping proof.
- Queries that do not consume mappings keep existing cache behavior.
- No MTMV rename or rewrite-cache lifecycle patch is required because mappings have no global name-based reference.
- BE executes the existing Colocate Hash Join; there is no new runtime column, Thrift field, or BE state.

## 4. Critical Execution Flows

### 4.1 Query Flow

1. With the session switch disabled, mapping metadata does not affect planning.
2. A Scan with mappings checks every FE's exact `version-shortHash`; a mixed or unknown version ignores the table's mappings and falls back to regular planning.
3. The Scan validates the persisted schema binding; if any mapping is incompatible, it ignores all mappings on that table, emits a rate-limited warning, and falls back to regular planning.
4. Only after both checks pass does it create a natural-distribution proof containing the available determinants.
5. Project and eligible ordinary non-DISTINCT Aggregates conservatively propagate the proof; DISTINCT/MultiDistinct/pure-deduplication Aggregates, Exchange, and set-operation boundaries drop it.
6. Join adds a mapping candidate while preserving the original Shuffle/Broadcast candidates.
7. The candidate survives only if both children satisfy the request and final cross-table validation succeeds.
8. Execution uses the existing Colocate Hash Join.

### 4.2 ADD CONSTRAINT Flow

1. Resolve determinant and target columns and check ALTER privilege.
2. Revalidate that the analyzed table object is still the current database entry.
3. Under database read and table write locks, validate table state, type, columns, Hash layout, target order, and constraint-name collisions.
4. Require every registered FE to report the exact current build.
5. Bind names to schema version, stable IDs, and type signatures.
6. Update the complete, constraint-name-sorted snapshot in `TableProperty.properties` and submit the backward-readable journal.
7. Release metadata locks before waiting for journal durability.

### 4.3 DROP CONSTRAINT Flow

1. Resolve the current table and check ALTER privilege.
2. If the name identifies a table-local mapping, use the mapping path; otherwise use the existing PK/FK/UNIQUE path.
3. Remove the mapping, write the resulting complete snapshot, and submit the journal under the table and manager locks; the final removal writes `[]`.
4. Release locks and wait for persistence.

DROP intentionally has no version gate so mappings can be removed before downgrade or while FE versions are mixed.

### 4.4 Table and Database Rename

A mapping has no database-name or table-name reference and follows the same physical `OlapTable` object:

```text
rename name binding
    -> same OlapTable object
    -> same TableProperty
    -> same mapping metadata
```

No mapping index needs to be updated and no stale old-name entry can remain. Column rename is different because mappings bind column identity, so it is rejected.

### 4.5 Truncate, Drop, and Recover

- Truncate's metadata copy preserves `TableProperty`, so mappings survive; new data must still honor the declared invariant.
- Drop places the physical table object and its mappings in the recycle bin.
- Recover restores that same object and its mappings.
- A same-name CREATE after Drop creates a different object and does not inherit the old mapping.

### 4.6 Replace and Swap

Ownership follows the physical object rather than the name:

- `REPLACE ... swap=false`: the replacement object takes the target name and keeps the replacement object's mappings; the old object and its mappings are discarded.
- `REPLACE ... swap=true`: the two physical objects exchange names, and each mapping remains attached to its original object.

### 4.7 CREATE TABLE LIKE and CTAS

Both statements create new physical table objects and do not copy mappings. A `NOT ENFORCED` mapping is a business assertion about a data-production contract and cannot be inferred merely from schema similarity.

### 4.8 Backup and Restore

Backup deep-copies the `OlapTable`, including the mapping snapshot in `TableProperty.properties`.

Before changing target table state or creating replicas, Restore preflights every backed-up OLAP table that contains mappings:

1. All FE versions must be identical.
2. Every mapping must be compatible with the backed-up table schema.
3. A failure stops Restore before the target enters RESTORE state.

This rule is intentionally conservative. Even when restoring into an existing table and the selected path may restore only partitions without copying mappings, the presence of mappings in backup metadata still activates the gate. A single rule is preferred over path-specific lifecycle exceptions.

### 4.9 Rolling Upgrade

1. ADD is rejected while old/new FE versions coexist or a version is not yet known through heartbeat.
2. Existing mappings do not affect ordinary queries while the session switch is disabled.
3. If the switch is enabled and a query scans a mapped table, mixed versions ignore the mappings and fall back to regular planning instead of failing the query.
4. ADD and Restore become available, and queries automatically resume mapping planning, after every FE reports the same exact `version-shortHash`.

This trades only the mapping optimization benefit during the upgrade window while preserving ordinary query availability and keeping the compatibility surface small.

### 4.10 Downgrade

Recommended sequence:

1. Stop enabling `enable_colocate_mapping_constraint` for queries.
2. DROP every mapping while new FE is still available. DROP remains allowed with mixed versions.
3. Confirm cleanup with `SHOW CONSTRAINTS`.
4. Start the FE downgrade.

Old FE can read and ignore the new fields but cannot preserve or use the feature. Downgrading with mappings still present is unsupported.

### 4.11 External Catalogs and HMS Events

External tables do not support mappings. Refresh, catalog rename/drop, HMS alter/rename/drop events, plugin events, and remote-ID changes therefore do not participate in this lifecycle. This removes the need for asynchronous two-way reconciliation between external metadata and internal mapping state.

## 5. Behavioral Changes

### 5.1 Query Behavior

- No planning change while the feature is disabled.
- Eligible joins may change from Shuffle/Broadcast to Colocate when enabled.
- An ordinary proof miss falls back without an error.
- Mixed FE versions or schema-incompatible mapping metadata ignore mappings, emit a rate-limited warning, and fall back to regular planning.
- SQL cache is disabled only for queries whose Scans actually construct mapping proofs.
- A false `NOT ENFORCED` declaration can produce incorrect results; this is the primary user responsibility.

### 5.2 DDL and Lifecycle Behavior

- Referenced columns cannot be dropped, renamed, or modified.
- A table with mappings cannot convert from Hash to Random distribution.
- Table/database rename, Truncate, and Drop/Recover preserve mappings.
- Replace/Swap follows physical-object ownership.
- CREATE TABLE LIKE and CTAS do not copy mappings.
- External and temporary tables reject mapping ADD.
- Restore applies the conservative gate whenever backup metadata contains mappings.

### 5.3 Compatibility Behavior

- ADD and Restore require exact FE-version agreement; queries use mappings only with version agreement and otherwise fall back to regular planning.
- DROP remains available without version agreement.
- Old FE replays the internal table-property key as opaque data and preserves it in checkpoints, but cannot show or use mappings and does not enforce mapping-specific DDL safeguards.
- Legacy tables without stable column IDs require mapping recreation after any base schema-version change.

## 6. Operational Considerations

### 6.1 Data-Correctness Responsibility

Before rollout, independently validate:

- Equal determinants within a table always produce equal target distribution keys.
- Tables sharing a mapping ID implement exactly the same mapping semantics.
- Composite determinant order is identical.
- Target positions match each table's ordered Hash distribution key.

Doris does not validate these conditions during INSERT, UPDATE, load, or Compaction.

### 6.2 Rollout and Upgrade

- Upgrade every FE before creating or restoring mappings. The session switch does not need to be forcibly disabled during rolling upgrade because queries automatically fall back.
- Use `SHOW FRONTENDS` to confirm version convergence. Unknown versions do not block ordinary queries, but they block ADD, Restore, and mapping optimization.
- Do not depend on the mapping optimization benefit during a rolling FE upgrade.

### 6.3 Downgrade

- DROP mappings before downgrade. Even though old FE preserves the opaque snapshot, it cannot show or use mappings or enforce mapping-specific DDL safeguards, so disabling the session switch alone does not make the feature supported.
- Include a `SHOW CONSTRAINTS` cleanup check in the downgrade procedure.

### 6.4 Schema and Lifecycle Operations

- DROP a mapping before changing a referenced column, then recreate it so the binding targets the new schema.
- Recreate mappings on legacy tables without stable IDs after any schema-version change.
- After Replace/Swap, inspect mappings according to physical-object ownership rather than old table names.
- Ensure every FE has upgraded and completed heartbeat before Restore.

### 6.5 Performance and Troubleshooting

- The feature is off by default. When enabled, an eligible hash join receives one additional non-enforceable mapping candidate and the existing cost model still chooses the final plan.
- Successful use primarily saves network Shuffle; the BE execution path is unchanged.
- Use `SHOW CONSTRAINTS` for metadata and `EXPLAIN` for the selected `COLOCATE` strategy.
- Rate-limited version and schema warnings identify incompatible FE nodes, tables, or constraint names for upgrade completion, cleanup, or recreation.
- No dedicated metric is added; warnings, SHOW, and EXPLAIN cover the primary diagnostic paths.

## 7. Test and Verification Scope

Coverage includes:

- Syntax, ADD/DROP/SHOW, privileges, and constraint-name collisions.
- Single/composite determinants, multiple mappings, and mixed direct/mapping coverage.
- Project alias, VARCHAR widening cast, positive propagation through ordinary non-DISTINCT Aggregates, and conservative fallback for DISTINCT/MultiDistinct/pure-deduplication Aggregates.
- Conservative fallback for incomplete determinants, expressions, narrowing casts, Repeat, set operations, and post-Exchange plans.
- Colocate Group stability, selected index/partition checks, and non-enforceable mapping candidates.
- Schema identity, missing stable IDs, referenced-column DDL, and Hash-to-Random protection.
- Old/new-FE readability and preservation of the complete `TableProperty` snapshot in journal/image, replay, basic failover semantics, and binlog isolation.
- Truncate, Drop/Recover, CREATE TABLE LIKE, Backup/Restore, and query fallback for mixed FE versions or incompatible schemas.
- SQL-cache disabling only after a mapping proof is constructed.

The final task record contains the exact commands and results for FE checkstyle, relevant FE unit tests, the standard `./build.sh --fe` build, and the `query_p0/colocate/test_colocate_mapping_constraint` regression suite.
