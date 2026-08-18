// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.spi;

/**
 * Enumerates optional, connector-declared capability switches consumed directly by
 * static query-planning code (pushdown/DDL/view/statistics gating, etc.).
 *
 * <p>This is an escape-hatch layer for capability checks that don't warrant a dedicated
 * provider abstraction. Write operations and sink traits (parallel write, partition-local
 * sort, full-schema write order, static-partition materialization) are NOT declared here —
 * they live on the connector's {@link org.apache.doris.connector.spi.write.ConnectorWritePlanProvider}
 * instead, surfaced via {@link Connector#getWritePlanProvider()}.</p>
 *
 * <h2>Two resolution scopes</h2>
 *
 * <p>Most of these the engine resolves once per CATALOG, from {@link Connector#getCapabilities()}.
 * Five it resolves per TABLE, as the union of the catalog-wide set and that table's own
 * {@link ConnectorTableSchema#getTableCapabilities()} — which is what lets a heterogeneous connector
 * (hive: orc/parquet/text/json/view/hudi in one catalog) admit only the tables that qualify. <b>Every
 * constant below states its scope.</b> Putting a catalog-scoped capability in a table's set is silently
 * ignored, so the distinction is part of the contract, not an implementation detail.</p>
 *
 * <p>The split is not arbitrary. A capability can be table-scoped only if, at the moment the engine asks,
 * (a) there IS a table, and (b) reading that table's cached schema is affordable. Three of the
 * catalog-scoped ones fail (a) — the answer picks the table subclass before the table object exists,
 * or feeds a table-valued function, or gates a CREATE TABLE clause for a table that does not exist yet.
 * Two fail (b): they are consulted from inside table initialization, or in order to decide whether to
 * load metadata at all, so reading the schema cache there would invert the order and force a remote
 * round-trip per table. The remaining ones are catalog-scoped because two call sites must agree, or
 * because no consumer needs the refinement. Widening any of them to table scope is a reviewable change,
 * not a mechanical one.</p>
 */
public enum ConnectorCapability {
    /**
     * Indicates the connector exposes a point-in-time snapshot of a table (MVCC), so its tables can serve
     * time travel and back a materialized view's freshness tracking.
     *
     * <p><b>Scope: catalog-wide only.</b> The engine reads it to choose WHICH TABLE SUBCLASS to instantiate,
     * i.e. strictly before the table object exists; a table-scoped answer would have to come from that
     * table's schema, which cannot be reached without the table.</p>
     */
    SUPPORTS_MVCC_SNAPSHOT,
    /**
     * Indicates the connector exposes per-partition statistics (record count, on-disk size,
     * file count) via {@link ConnectorPartitionListingOps#listPartitions}.
     *
     * <p>{@code SHOW PARTITIONS} renders a rich multi-column result (Partition / PartitionKey /
     * RecordCount / FileSizeInBytes / FileCount) for connectors declaring this capability, instead
     * of the single partition-name column used by connectors that only implement
     * {@code listPartitionNames}.</p>
     *
     * <p><b>Scope: catalog-wide only.</b> Two call sites must return the SAME answer — one decides how many
     * columns each result row carries, the other how many column headers to declare — and the header path
     * has no resolved table in hand. A per-table answer risks a row width that disagrees with its header,
     * which is a visibly wrong result rather than a missed optimization.</p>
     */
    SUPPORTS_PARTITION_STATS,
    /**
     * Indicates the connector's tables support background per-column auto-analyze (NDV / min / max /
     * null-count collection) through the generic {@code ExternalAnalysisTask} FULL path.
     *
     * <p>The statistics auto-collector admits a plugin-driven table into the background auto-analyze
     * framework only when its connector declares this (replacing the legacy {@code instanceof
     * IcebergExternalTable} whitelist), and then forces {@code AnalysisMethod.FULL} — sample analyze is
     * unimplemented for external SQL-driven tables ({@code ExternalAnalysisTask.doSample} throws).
     * Row/passthrough connectors that cannot serve per-column statistics (e.g. JDBC, ES) must NOT
     * declare it so they stay excluded.</p>
     *
     * <p><b>Scope: catalog-wide OR per-table.</b> hive declares it per-table for its plain-hive data tables
     * only (legacy gated on the table being plain hive, so an embedded hudi table stays out).</p>
     */
    SUPPORTS_COLUMN_AUTO_ANALYZE,
    /**
     * Indicates the connector's file-scan tables support Top-N lazy materialization: the scan first
     * reads only the ordering/filter columns to locate the Top-N row ids, then materializes the
     * remaining columns for just those rows (via the synthesized {@code GLOBAL_ROWID_COL}).
     *
     * <p>The nereids Top-N lazy-materialize probe enables the {@code LazyMaterializeTopN} post-processor
     * for a plugin-driven table only when its connector declares this (replacing the legacy exact-class
     * {@code SUPPORT_RELATION_TYPES} membership of {@code IcebergExternalTable}). Row/passthrough
     * connectors (e.g. JDBC, ES) must NOT declare it.</p>
     *
     * <p><b>Scope: catalog-wide OR per-table.</b> hive declares it per-table because eligibility is
     * orc/parquet-only, which it cannot express for a catalog that also holds text/json tables.</p>
     */
    SUPPORTS_TOPN_LAZY_MATERIALIZE,
    /**
     * Indicates the connector's table/database properties are user-facing and safe to render in
     * {@code SHOW CREATE TABLE} / {@code SHOW CREATE DATABASE}.
     *
     * <p>The SHOW CREATE TABLE plugin-driven arm renders LOCATION + PROPERTIES (and, when the
     * connector pre-renders them under the {@code show.*} reserved keys, the PARTITION BY / ORDER BY
     * clauses) only for connectors declaring this (replacing the legacy paimon-only engine-name gate).
     * Row/passthrough connectors whose {@code getTableProperties()} returns connection properties
     * <b>including credentials</b> (e.g. JDBC, ES) must NOT declare it, or SHOW CREATE TABLE would leak
     * the connection password — the security control the legacy engine-name gate provided.</p>
     *
     * <p><b>Scope: catalog-wide only.</b> Nothing needs the refinement — property safety is a property of the
     * connector, not of one of its tables — and since this doubles as the credential-leak guard, moving it to
     * table scope would put a security decision behind a per-table value. Widening it needs its own review.</p>
     */
    SUPPORTS_SHOW_CREATE_DDL,
    /**
     * Indicates the connector exposes views as queryable objects distinct from tables.
     *
     * <p>When a connector declares this, a plugin-driven table resolves its {@code isView()} from the
     * connector ({@link ConnectorViewOps#viewExists}) instead of the {@code false} default, the catalog
     * merges the connector's {@link ConnectorViewOps#listViewNames} back into {@code SHOW TABLES} (iceberg
     * subtracts views from {@code listTableNames}), and the read/DML/SHOW CREATE arms treat the object as a
     * view. Connectors with no view concept (e.g. JDBC, ES) must NOT declare it so every table stays
     * {@code isView()==false} and no view round-trips are issued.</p>
     *
     * <p><b>Scope: catalog-wide only.</b> The engine asks this from INSIDE table initialization (resolving
     * {@code isView()} is part of initializing the table) and also while merely listing names. A table-scoped
     * answer would have to be read from that table's cached schema, so every table in every plugin catalog
     * would trigger a schema load just to learn it is not a view — an order inversion that turns a free
     * in-memory check into one remote round-trip per table.</p>
     */
    SUPPORTS_VIEW,
    /**
     * Indicates the connector's file-scan tables support nested-column pruning: a query that reads only some
     * sub-fields of a STRUCT/ARRAY/MAP column reads just those leaves from the data file instead of the whole
     * complex column (read-amplification avoidance).
     *
     * <p>The nereids nested-column-prune probe ({@code LogicalFileScan.supportPruneNestedColumn}) enables it
     * for a plugin-driven table only when its connector declares this (replacing the legacy exact-class
     * {@code IcebergExternalTable} arm). It is only correct when the connector also carries a stable per-field
     * id down its column tree (top-level via {@link ConnectorColumn#withUniqueId} + nested via
     * {@link ConnectorType#withChildrenFieldIds}), because the engine rewrites the nested access path from
     * field <em>names</em> to those ids ({@code SlotTypeReplacer}) and the BE field-id scan path matches
     * nested leaves by id — an un-translated (name / {@code -1}) leaf is skipped and returns NULL. Row/
     * passthrough connectors (e.g. JDBC, ES) and connectors that do not carry nested field ids must NOT
     * declare it.</p>
     *
     * <p><b>Scope: catalog-wide OR per-table.</b> hive declares it per-table because eligibility is
     * orc/parquet-only; blanket-declaring it for a mixed catalog would be a correctness bug, not just an
     * over-admission — a text/json table has no field ids, so pruned leaves would read back NULL.</p>
     */
    SUPPORTS_NESTED_COLUMN_PRUNE,
    /**
     * Indicates the connector's external metadata (schema / partitions / snapshot) can be pre-warmed
     * asynchronously by the planner before it takes the internal read lock, rather than loaded lazily
     * during binding.
     *
     * <p>{@code PluginDrivenExternalTable.supportsExternalMetadataPreload} returns true for a plugin-driven
     * table only when its connector declares this (replacing the legacy engine-name {@code "jdbc"} gate), so
     * {@code StatementContext.registerExternalTableForPreload} admits the table into the async pre-load pass
     * (itself opt-in via the {@code enable_preload_external_metadata} session variable, default off). It is a
     * pure planning/lock-latency optimization with no correctness effect: connectors whose metadata reads are
     * cheap or not yet validated for concurrent pre-warming (e.g. ES) simply do not declare it and fall back
     * to synchronous load at binding time.</p>
     *
     * <p><b>Scope: catalog-wide only.</b> Its sole consumer asks it in order to decide whether to load this
     * table's metadata at all, so a table-scoped answer — which lives in that table's cached schema — would
     * mean loading the metadata to find out whether to load the metadata.</p>
     */
    SUPPORTS_METADATA_PRELOAD,
    /**
     * Indicates the connector projects the querying user's per-connection delegated credential (OIDC/JWT/SAML)
     * onto the remote metadata source, so metadata reads are authorized as that user rather than a single shared
     * catalog identity (the Iceberg REST {@code iceberg.rest.session=user} model).
     *
     * <p>This capability gates two behaviors. (a) FE credential injection: {@code ConnectorSessionBuilder.from}
     * copies the user's delegated credential onto the {@link ConnectorSession} ONLY for connectors declaring
     * this, so a JDBC/ES/hive-iceberg session never carries an OIDC token it would never use (least-privilege).
     * (b) Shared-cache bypass: {@code ExternalCatalog.shouldBypassTableNameCache} / {@code ExternalDatabase}
     * skip the catalog+name-keyed (NOT user-keyed) FE metadata caches for a credential-bearing session, so one
     * user's REST-authorized/vended view is never served to another (cross-user leakage). Connectors that
     * authenticate with a single static catalog identity (every non-REST iceberg flavor, JDBC, ES, ...) must
     * NOT declare it. Declared by the iceberg connector only when configured {@code iceberg.rest.session=user}.</p>
     *
     * <p><b>Scope: catalog-wide only.</b> It is a property of how the catalog authenticates, and it is read
     * while BUILDING the session — before any table is named, let alone loaded.</p>
     */
    SUPPORTS_USER_SESSION,
    /**
     * Indicates the connector's file-scan tables support {@code ANALYZE ... WITH SAMPLE} (scale-factor estimation
     * from raw per-file byte sizes via {@link ConnectorStatisticsOps#listFileSizes}, with fe-core doing the
     * Doris-type slot-width math).
     *
     * <p>fe-core admits sampled analyze for a plugin-driven table only when it declares this. A heterogeneous
     * connector (hive) declares it PER-TABLE in getTableSchema for its plain-hive tables only (legacy
     * gated on {@code dlaType==HIVE}), so iceberg/hudi-on-HMS are excluded. Connectors whose {@code doSample} is
     * unimplemented (native iceberg/paimon, JDBC, ES) must NOT declare it so sampled analyze stays rejected at
     * build time.</p>
     *
     * <p><b>Scope: catalog-wide OR per-table.</b></p>
     */
    SUPPORTS_SAMPLE_ANALYZE,
    /**
     * Indicates the connector accepts a create-time write sort order — the {@code CREATE TABLE ... ORDER BY (...)}
     * clause.
     *
     * <p>fe-core admits the ORDER BY write-order clause for a plugin-driven CREATE TABLE only when the target
     * connector declares this (replacing the legacy engine-name {@code iceberg} gate); a create against any target
     * that does not declare it (paimon/hive/maxcompute, and every non-plugin internal-catalog engine) is rejected
     * up front. The declaring connector (iceberg) owns the sort-column validation (existence / sortable type /
     * duplicates) inside its own {@code createTable}. This is a DDL-clause gate and is distinct from the runtime
     * sink trait {@code ConnectorWritePlanProvider.requiresFullSchemaWriteOrder()}, which governs how rows are
     * ordered on the write path, not whether the CREATE TABLE DDL accepts the clause.</p>
     *
     * <p><b>Scope: catalog-wide only.</b> It gates a clause of the statement that CREATES the table, so the
     * table it would be refined against does not exist when the question is asked.</p>
     */
    SUPPORTS_SORT_ORDER,
    /**
     * Indicates the connector supports {@code ALTER TABLE} column schema-change DDL, including
     * dotted nested paths (e.g. {@code ADD/DROP/RENAME/MODIFY COLUMN s.b}, {@code arr.element},
     * {@code m.value}) and {@code MODIFY COLUMN ... COMMENT}.
     *
     * <p>The nereids {@code AlterTableCommand} column-operation validation admits the Iceberg-style
     * schema-change clause set (nested paths, the {@code MODIFY COLUMN COMMENT} op) for a plugin-driven
     * table only when its connector declares this (replacing the legacy exact-class {@code instanceof
     * IcebergExternalTable} gate). The actual mutation is routed through {@code PluginDrivenExternalCatalog}'s
     * {@code ColumnPath} column-DDL overrides into the connector's {@link ConnectorColumnEvolutionOps} column-evolution
     * ops. Connectors without column schema-change support (JDBC, ES, maxcompute today) must NOT
     * declare it so their tables reject nested paths at analysis and column DDL stays unsupported.</p>
     *
     * <p><b>Scope: catalog-wide OR per-table.</b> An iceberg-on-HMS table (whose catalog connector is hive)
     * inherits it through the per-table set the gateway reflects from its sibling, exactly like
     * {@link #SUPPORTS_NESTED_COLUMN_PRUNE}.</p>
     */
    SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE,
    /**
     * Indicates the connector accepts the relation-scoped {@code @options('k'='v', ...)} scan-param
     * clause, whose keys are the SOURCE's own scan-option vocabulary (e.g. paimon's
     * {@code scan.snapshot-id} / {@code scan.mode}).
     *
     * <p>{@code BindRelation} rejects {@code @options} up front for any table whose connector does not
     * declare this. That rejection is REQUIRED, not cosmetic: {@code @options} changes WHICH data a
     * relation reads, and a table type that silently ignored it would answer a historical query with
     * latest data. The declaring connector owns the whole option vocabulary — fe-core never inspects a
     * key — via {@code ConnectorMetadata.resolveTimeTravel(ConnectorTimeTravelSpec.Kind#OPTIONS)}, which
     * validates the keys and resolves them into an immutable pin.</p>
     *
     * <p><b>Scope: catalog-wide OR per-table.</b> Per-table so a connector may honor the clause on its
     * data tables while declining it on the subset of system tables whose readers cannot observe a
     * selected snapshot.</p>
     */
    SUPPORTS_SCAN_PARAM_OPTIONS
}
