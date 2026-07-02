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

package org.apache.doris.connector.api.write;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Plans the write (sink) for a connector table: produces the opaque
 * {@link org.apache.doris.thrift.TDataSink} that BE uses to write data.
 *
 * <p>This is the write-side analogue of
 * {@link org.apache.doris.connector.api.scan.ConnectorScanPlanProvider}. A
 * connector with write capability returns an implementation from
 * {@link org.apache.doris.connector.api.Connector#getWritePlanProvider()}; the
 * engine calls {@link #planWrite} when translating a physical table sink, then
 * dispatches the resulting Thrift data sink to BE unchanged.</p>
 */
public interface ConnectorWritePlanProvider {

    /**
     * Builds the data sink for the given bound write request.
     *
     * @param session the current session
     * @param handle  the bound write request (target table, columns, overwrite, context)
     * @return a {@link ConnectorSinkPlan} wrapping the Thrift data sink
     */
    ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle);

    /**
     * Appends connector-specific EXPLAIN detail for the write (e.g. the generated INSERT SQL,
     * sink dialect, target format). Write-side analogue of the scan provider's
     * {@code appendExplainInfo}: the engine emits the generic plugin-driven sink line, then calls
     * this so the connector can surface its own write details without the engine knowing the
     * sink dialect.
     *
     * <p>This runs when the plan's EXPLAIN string is generated, which is <i>before</i> the write
     * plan is bound (the sink's {@code planWrite} has not run yet for an EXPLAIN). The connector
     * therefore derives the detail from the {@code handle} and may consult the source for metadata
     * (e.g. remote column names). Default: no extra EXPLAIN info.</p>
     *
     * @param output  the EXPLAIN string being built
     * @param prefix  the current indentation prefix
     * @param session the current session (may be consulted for session-scoped write options)
     * @param handle  the write request (target table handle and write columns)
     */
    default void appendExplainInfo(StringBuilder output, String prefix,
            ConnectorSession session, ConnectorWriteHandle handle) {
        // Default: no extra EXPLAIN info
    }

    /**
     * Declares whether the target has a write-side sort order and, if so, its sort columns, in an
     * engine-neutral form. The engine calls this when translating the table sink: for a non-{@code null}
     * result it builds the Thrift {@code TSortInfo} from the columns (resolving each
     * {@link ConnectorWriteSortColumn#getColumnIndex()} against the bound sink output) and threads it
     * back to {@link #planWrite} via {@link ConnectorWriteHandle#getSortInfo()} so the connector can stamp
     * it onto its opaque sink.
     *
     * <p>The {@code null}-vs-list distinction mirrors a source's {@code isSorted()} gate: {@code null}
     * means the target has <b>no</b> write sort order (no {@code TSortInfo}); a non-{@code null} list
     * means it <b>has</b> one — even an empty list, which yields an empty {@code TSortInfo} (a target
     * sorted only by non-resolvable transforms still requests sorted-write semantics). Depends only on
     * the target table (not the bound write), so it takes the {@link ConnectorTableHandle}: at
     * translation time the full write handle is not yet formed. Default: {@code null} — jdbc / maxcompute
     * keep their byte-identical unsorted sink output.</p>
     *
     * @param session     the current session
     * @param tableHandle the target table handle
     * @return the ordered write-sort columns (possibly empty) if the target has a sort order, or
     *         {@code null} if it has none
     */
    default List<ConnectorWriteSortColumn> getWriteSortColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return null;
    }

    /**
     * Declares the target's write-time partitioning, in an engine-neutral form, so the engine can reproduce
     * the connector's write distribution (the iceberg merge-write {@code DistributionSpecMerge}) without
     * importing the connector's native partition-spec types. The engine resolves each
     * {@link ConnectorWritePartitionField#getSourceColumnName()} to a bound output expr id locally and builds
     * the distribution from the field tuple + {@link ConnectorWritePartitionSpec#getSpecId()}.
     *
     * <p>{@code null} (not an empty spec) means the target is unpartitioned, mirroring the legacy
     * {@code spec().isPartitioned()} gate — the engine then falls back to its non-partitioned merge
     * distribution. Depends only on the target table (not the bound write), so it takes the
     * {@link ConnectorTableHandle}. Default: {@code null} — jdbc / maxcompute / paimon keep their
     * byte-identical non-partitioned write distribution.</p>
     *
     * @param session     the current session
     * @param tableHandle the target table handle
     * @return the current spec id + ordered partition fields if the target is partitioned, or {@code null}
     *         if it is unpartitioned
     */
    default ConnectorWritePartitionSpec getWritePartitioning(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return null;
    }

    /**
     * Declares the connector's <b>synthetic write columns</b> for the target — request-scoped hidden
     * columns the engine injects into {@code PluginDrivenExternalTable.getFullSchema()} while a write/DML
     * over this table is in flight, in an engine-neutral form. The engine appends these (converted via
     * its {@code ConnectorColumnConverter}) to the table's full schema only when the request signals it
     * (show-hidden, or the synthetic-write-column ctx flag set for this table during row-level DML), so a
     * synthesized DELETE/UPDATE/MERGE plan can bind slots that reference them.
     *
     * <p>These are the per-row write metadata a connector needs for row-level DML — for iceberg the
     * {@code __DORIS_ICEBERG_ROWID_COL__} STRUCT (file_path / row_position / partition_spec_id /
     * partition_data), declared {@link ConnectorColumn#invisible() invisible} so it never surfaces in
     * SELECT/SHOW. Distinct from the connector's <i>always-present</i> hidden columns (e.g. iceberg v3
     * row-lineage), which are declared through the schema SPI and cached: those live in the schema cache,
     * whereas synthetic write columns are request-scoped and must not be cached. Depends only on the
     * target table (not the bound write), so it takes the {@link ConnectorTableHandle}. Default: empty —
     * a connector without synthetic write columns (jdbc / es / paimon / maxcompute) injects nothing,
     * keeping its byte-identical full schema.</p>
     *
     * @param session     the current session
     * @param tableHandle the target table handle
     * @return the synthetic write columns to inject, or an empty list if the target has none
     */
    default List<ConnectorColumn> getSyntheticWriteColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return Collections.emptyList();
    }

    /**
     * The write operations this provider can plan, in one place — the single source of truth for a
     * connector's write capability. Replaces the removed {@code ConnectorWriteOps} boolean methods and
     * the {@code SUPPORTS_INSERT} capability. Default: INSERT only (any write provider can at least
     * append). A connector overrides this to add OVERWRITE / DELETE / MERGE / REWRITE. Connector-level
     * (does not vary per table); per-table mode constraints stay in
     * {@link org.apache.doris.connector.api.ConnectorWriteOps#validateRowLevelDmlMode}.
     */
    default Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT);
    }

    /** Whether this connector can write into a named table branch ({@code INSERT INTO t@branch(name)}). Default: no. */
    default boolean supportsWriteBranch() {
        return false;
    }

    /**
     * Whether the connector supports multiple concurrent writers (parallel sink instances). Connectors that
     * do not declare this get GATHER (single-writer) distribution. Relocated from
     * {@code ConnectorCapability.SUPPORTS_PARALLEL_WRITE}. Default: no.
     */
    default boolean requiresParallelWrite() {
        return false;
    }

    /**
     * Whether the connector maps write data columns positionally against the full table schema (so the sink
     * must project rows to full-schema order with unmentioned columns filled). Relocated from
     * {@code ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER}. Default: no.
     */
    default boolean requiresFullSchemaWriteOrder() {
        return false;
    }

    /**
     * Whether dynamic-partition writes must be hash-distributed by partition columns and locally sorted by
     * them before the sink (e.g. MaxCompute Storage API). Relocated from
     * {@code ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT}. A connector declaring this must also
     * declare {@link #requiresParallelWrite()} and {@link #requiresFullSchemaWriteOrder()}. Default: no.
     */
    default boolean requiresPartitionLocalSort() {
        return false;
    }

    /**
     * Whether the connector's data files physically retain partition columns, so a static-partition write
     * must materialize the PARTITION-clause literal into the data column instead of NULL-filling it (e.g.
     * Iceberg). Relocated from {@code ConnectorCapability.SINK_MATERIALIZE_STATIC_PARTITION_VALUES}. Default: no.
     */
    default boolean requiresMaterializeStaticPartitionValues() {
        return false;
    }
}
