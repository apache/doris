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

package org.apache.doris.connector.spi.scan;

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Everything the engine knows about one scan when it asks a connector to plan it:
 * {@link ConnectorScanPlanProvider#planScan}'s single parameter object.
 *
 * <p><b>Why a parameter object.</b> These fields arrived one at a time, each as another {@code planScan}
 * overload that delegated to the previous one — four in the end, of which only the shortest was abstract. A
 * connector that implemented that abstract method, the obvious thing to do when reading the interface, ended
 * up silently ignoring the row limit, the pruned partition set and the {@code COUNT(*)} signal: everything
 * still worked, just slower, with nothing to point at. There is now one method to implement and one object
 * carrying whatever the engine has; a signal added later becomes a field with a default, and no connector
 * silently loses it.</p>
 *
 * <p>Immutable. The engine builds one per scan; {@link #withRequiredPartitions} makes the batched variant
 * ({@link ConnectorScanPlanProvider#planScanForPartitionBatch}) without copying the rest by hand.</p>
 */
public final class ConnectorScanRequest {

    private final ConnectorTableHandle tableHandle;
    private final List<ConnectorColumnHandle> columns;
    private final Optional<ConnectorExpression> filter;
    private final long limit;
    private final List<String> requiredPartitions;
    private final boolean countPushdown;

    private ConnectorScanRequest(ConnectorTableHandle tableHandle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter, long limit, List<String> requiredPartitions,
            boolean countPushdown) {
        this.tableHandle = tableHandle;
        this.columns = columns;
        this.filter = filter;
        this.limit = limit;
        this.requiredPartitions = requiredPartitions;
        this.countPushdown = countPushdown;
    }

    /**
     * Starts a request for {@code tableHandle} reading {@code columns} — the two facts every scan has.
     * Everything else has a default that means "the engine is not asking for anything special".
     */
    public static Builder builder(ConnectorTableHandle tableHandle, List<ConnectorColumnHandle> columns) {
        return new Builder(tableHandle, columns);
    }

    /**
     * The table to scan. Already carries whatever earlier pushdown steps put on it
     * ({@code applyFilter} / {@code applyProjection}, an MVCC snapshot pin, a rewrite-group scope).
     */
    public ConnectorTableHandle getTableHandle() {
        return tableHandle;
    }

    /** The columns to read. */
    public List<ConnectorColumnHandle> getColumns() {
        return columns;
    }

    /** The filter remaining after pushdown, or empty when there is none to push. */
    public Optional<ConnectorExpression> getFilter() {
        return filter;
    }

    /** The maximum number of rows to return, or {@code -1} for no limit. */
    public long getLimit() {
        return limit;
    }

    /**
     * The partitions the engine's pruning left, as metastore-rendered spec strings
     * (e.g. {@code "pt=1/region=cn"}); EMPTY means "not pruned — scan every partition".
     *
     * <p>Never means "scan nothing": a predicate that prunes everything away is short-circuited by the engine
     * before the connector is asked, except for a predicate-driven connector
     * ({@link ConnectorScanPlanProvider#ignorePartitionPruneShortCircuit}), which is handed scan-all and
     * re-plans from the filter instead.</p>
     */
    public List<String> getRequiredPartitions() {
        return requiredPartitions;
    }

    /**
     * Whether a no-grouping {@code COUNT(*)} is being pushed into this scan, so BE is already in count mode.
     * A connector that can answer the count from metadata (a per-split precomputed row count) should emit it
     * instead of planning ranges that materialize rows; one that cannot ignores this and plans normally.
     */
    public boolean isCountPushdown() {
        return countPushdown;
    }

    /** This request with the partition set replaced — the batched scan's per-batch request. */
    public ConnectorScanRequest withRequiredPartitions(List<String> partitions) {
        return new ConnectorScanRequest(tableHandle, columns, filter, limit,
                normalizePartitions(partitions), countPushdown);
    }

    private static List<String> normalizePartitions(List<String> partitions) {
        return partitions == null ? Collections.emptyList() : partitions;
    }

    /** Builds a {@link ConnectorScanRequest}; every setter is optional. */
    public static final class Builder {

        private final ConnectorTableHandle tableHandle;
        private final List<ConnectorColumnHandle> columns;
        private Optional<ConnectorExpression> filter = Optional.empty();
        private long limit = -1;
        private List<String> requiredPartitions = Collections.emptyList();
        private boolean countPushdown;

        private Builder(ConnectorTableHandle tableHandle, List<ConnectorColumnHandle> columns) {
            this.tableHandle = Objects.requireNonNull(tableHandle, "tableHandle");
            this.columns = Objects.requireNonNull(columns, "columns");
        }

        public Builder filter(Optional<ConnectorExpression> filter) {
            this.filter = Objects.requireNonNull(filter, "filter");
            return this;
        }

        public Builder limit(long limit) {
            this.limit = limit;
            return this;
        }

        /** {@code null} is accepted and means the same as empty: not pruned, scan every partition. */
        public Builder requiredPartitions(List<String> requiredPartitions) {
            this.requiredPartitions = normalizePartitions(requiredPartitions);
            return this;
        }

        public Builder countPushdown(boolean countPushdown) {
            this.countPushdown = countPushdown;
            return this;
        }

        public ConnectorScanRequest build() {
            return new ConnectorScanRequest(tableHandle, columns, filter, limit,
                    requiredPartitions, countPushdown);
        }
    }
}
