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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Operations for retrieving table-level statistics from a connector.
 */
public interface ConnectorStatisticsOps {

    /** Returns table statistics, or empty if not available. */
    default Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session,
            ConnectorTableHandle handle) {
        return Optional.empty();
    }

    /**
     * Returns table statistics AS OF {@code snapshot} &mdash; the row count at the pinned snapshot, for a
     * time-travel read (FOR VERSION/TIME AS OF, {@code @branch}/{@code @tag}).
     *
     * <p>The default ignores the snapshot and returns the latest statistics via
     * {@link #getTableStatistics(ConnectorSession, ConnectorTableHandle)}. WHY this exists: the table-level
     * row count feeds the CBO. Without a snapshot dimension the optimizer estimates a time-travel query's
     * cardinality from the LATEST snapshot while the scan reads the pinned one, skewing join reorder / build
     * side selection (estimate-only &mdash; results stay correct). A connector that can count at a snapshot
     * overrides this (mirrors the {@code getTableSchema}/{@code getColumnHandles} snapshot overloads). fe-core
     * only calls it for a genuine versioned read, so the shared latest-keyed row-count cache is untouched for
     * plain reads.</p>
     */
    default Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session,
            ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        return getTableStatistics(session, handle);
    }

    /**
     * Returns per-column statistics the connector can serve WITHOUT a table scan — the query-planner
     * column-statistics fast path, consulted on a stats-cache miss (fe-core's
     * {@code ColumnStatisticsCacheLoader}). Must be cheap (a metadata read, no scan). Returns empty when
     * unavailable, so a connector with no cheap column stats simply does not override it and fe-core falls
     * back to a full ANALYZE. fe-core derives the Doris {@code ColumnStatistic} (dataSize / avgSize) from the
     * returned raw facts — see {@link ConnectorColumnStatistics}.
     */
    default Optional<ConnectorColumnStatistics> getColumnStatistics(
            ConnectorSession session,
            ConnectorTableHandle handle,
            String columnName) {
        return Optional.empty();
    }

    /**
     * Estimates the table's on-disk data size in bytes by listing its data files, for connectors that can
     * cheaply enumerate them (e.g. hive). fe-core uses this to estimate a row count
     * ({@code dataSize / <row width>}) when neither an exact row count nor a metastore-recorded size (from
     * {@link #getTableStatistics}) is available — fe-core only calls it when the
     * {@code enable_get_row_count_from_file_list} global is set. A potentially expensive remote listing, so
     * connectors that cannot do it cheaply must NOT override it. Returns -1 when the size cannot be
     * estimated (not supported, unlistable, empty, or any error) — the default.
     */
    default long estimateDataSizeByListingFiles(ConnectorSession session, ConnectorTableHandle handle) {
        return -1;
    }

    /**
     * Returns the RAW byte length of every data file across ALL partitions of the table (not sampled, not summed),
     * for {@code ANALYZE ... WITH SAMPLE}: fe-core seed-shuffles and cumulates these sizes to a sample scale
     * factor, then does the Doris-type slot-width math itself. Unlike {@link #estimateDataSizeByListingFiles} it
     * neither partition-samples nor sums, because the sampler needs the individual file sizes. A potentially
     * expensive full remote listing, so connectors that cannot enumerate files cheaply must NOT override it
     * (default empty -> the sampler falls back to scale factor 1). Best-effort: an override must return empty on
     * any listing error rather than throw (statistics must not fail a query).
     */
    default List<Long> listFileSizes(ConnectorSession session, ConnectorTableHandle handle) {
        return Collections.emptyList();
    }
}
