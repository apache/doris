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

package org.apache.doris.connector.api;

/**
 * Enumerates the optional capabilities a connector may declare.
 * The planner and execution engine use these to decide which
 * pushdown and write paths are available.
 */
public enum ConnectorCapability {
    SUPPORTS_FILTER_PUSHDOWN,
    SUPPORTS_PROJECTION_PUSHDOWN,
    SUPPORTS_LIMIT_PUSHDOWN,
    SUPPORTS_PARTITION_PRUNING,
    SUPPORTS_INSERT,
    SUPPORTS_DELETE,
    SUPPORTS_UPDATE,
    SUPPORTS_MERGE,
    SUPPORTS_CREATE_TABLE,
    SUPPORTS_MVCC_SNAPSHOT,
    SUPPORTS_METASTORE_EVENTS,
    SUPPORTS_STATISTICS,
    SUPPORTS_VENDED_CREDENTIALS,
    SUPPORTS_ACID_TRANSACTIONS,
    SUPPORTS_TIME_TRAVEL,
    /**
     * Indicates the connector supports multiple concurrent writers (sink instances).
     *
     * <p>Connectors that do NOT declare this capability will use GATHER distribution
     * (single writer), which is the safe default for transactional sinks like JDBC
     * where each writer commits independently.</p>
     *
     * <p>File-based connectors (Hive, Iceberg, etc.) that can safely handle
     * parallel writers should declare this capability.</p>
     */
    SUPPORTS_PARALLEL_WRITE,
    /**
     * Indicates the connector requires dynamic-partition writes to be hash-distributed by
     * partition columns and locally sorted by them before reaching the sink.
     *
     * <p>Streaming partition writers (e.g. the MaxCompute Storage API) close the previous
     * partition writer as soon as a new partition value appears; un-grouped (unsorted)
     * multi-partition rows therefore cause "writer has been closed" errors. The planner uses
     * this capability to require a hash-by-partition distribution plus a mandatory local sort
     * on the partition columns for dynamic-partition writes.</p>
     *
     * <p>A connector declaring this is expected to also declare
     * {@link #SUPPORTS_PARALLEL_WRITE} (hash distribution is inherently parallel) and
     * {@link #SINK_REQUIRE_FULL_SCHEMA_ORDER}: the sink distribution locates partition columns by their
     * <b>full-schema</b> position in the child output, which only holds when the bind layer projects the
     * write to full-schema order (the projection gated by {@code SINK_REQUIRE_FULL_SCHEMA_ORDER}). A
     * connector declaring this without {@code SINK_REQUIRE_FULL_SCHEMA_ORDER} would shuffle/sort by the
     * wrong column whenever cols order diverges from the full schema.</p>
     */
    SINK_REQUIRE_PARTITION_LOCAL_SORT,
    /**
     * Indicates the connector's write path maps data columns <b>positionally</b> against the full
     * table schema (e.g. MaxCompute's columnar Storage API / JNI writer), rather than by column name.
     *
     * <p>For such connectors the sink's output rows must be projected to <b>full table schema order</b>
     * with any unmentioned columns filled (NULL / default) — exactly like the legacy MaxCompute bind
     * path — so that a reordered or partial explicit column list does not land values in the wrong
     * remote columns. Name-mapped connectors (e.g. JDBC, which builds an {@code INSERT INTO t (cols)}
     * statement) must NOT declare this capability: their data stays in user/cols order to match the
     * generated column list.</p>
     */
    SINK_REQUIRE_FULL_SCHEMA_ORDER,
    /**
     * Indicates the connector supports passthrough query via the {@code query()} TVF.
     *
     * <p>Connectors declaring this capability must implement
     * {@link ConnectorTableOps#getColumnsFromQuery} to provide column metadata
     * for arbitrary SQL queries passed through to the remote data source.</p>
     */
    SUPPORTS_PASSTHROUGH_QUERY,
    /**
     * Indicates the connector exposes per-partition statistics (record count, on-disk size,
     * file count) via {@link ConnectorTableOps#listPartitions}.
     *
     * <p>{@code SHOW PARTITIONS} renders a rich multi-column result (Partition / PartitionKey /
     * RecordCount / FileSizeInBytes / FileCount) for connectors declaring this capability, instead
     * of the single partition-name column used by connectors that only implement
     * {@code listPartitionNames}. This is distinct from {@link #SUPPORTS_STATISTICS}, which is
     * table-level statistics for the optimizer.</p>
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
     */
    SUPPORTS_SHOW_CREATE_DDL,
    /**
     * Indicates the connector exposes views as queryable objects distinct from tables.
     *
     * <p>When a connector declares this, a plugin-driven table resolves its {@code isView()} from the
     * connector ({@link ConnectorTableOps#viewExists}) instead of the {@code false} default, the catalog
     * merges the connector's {@link ConnectorTableOps#listViewNames} back into {@code SHOW TABLES} (iceberg
     * subtracts views from {@code listTableNames}), and the read/DML/SHOW CREATE arms treat the object as a
     * view. Connectors with no view concept (e.g. JDBC, ES) must NOT declare it so every table stays
     * {@code isView()==false} and no view round-trips are issued.</p>
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
     */
    SUPPORTS_NESTED_COLUMN_PRUNE
}
