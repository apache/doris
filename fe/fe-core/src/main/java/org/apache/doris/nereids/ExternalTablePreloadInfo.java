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

package org.apache.doris.nereids;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.datasource.ExternalTable;

import java.util.Map;
import java.util.Optional;

/** Tracks how a single external table is referenced before metadata preload happens. */
public class ExternalTablePreloadInfo {
    private final ExternalTable table;
    private boolean hasLatestOnlyRelation;
    private boolean hasNonLatestRelation;
    private boolean hasUnfilteredLatestRelation;
    /**
     * The scan-path partition view materialized by the pre-lock preload pass, or {@code null} when that pass
     * did not run (or did not warm one). An {@link Optional#empty()} value is meaningful and different from
     * {@code null}: the connector view is UNAVAILABLE (unrepresentable partition), which every scan-path
     * consumer must read as "scan every partition".
     */
    private Optional<Map<String, PartitionItem>> scanPartitionView;

    public ExternalTablePreloadInfo(ExternalTable table) {
        this.table = table;
    }

    public ExternalTable getTable() {
        return table;
    }

    public void markLatestRelation() {
        hasLatestOnlyRelation = true;
    }

    public void markNonLatestRelation() {
        hasNonLatestRelation = true;
    }

    public void markUnfilteredLatestRelation() {
        hasUnfilteredLatestRelation = true;
    }

    public boolean hasLatestOnlyRelation() {
        return hasLatestOnlyRelation;
    }

    public boolean hasNonLatestRelation() {
        return hasNonLatestRelation;
    }

    public boolean shouldPreloadLatestSnapshot() {
        // A historical alias has independent scan state and must not cancel the latest alias warmup.
        return hasLatestOnlyRelation;
    }

    /**
     * Whether at least one latest relation had no initial LogicalFilter. The pre-lock full-view warmup is
     * only useful for such a scan; a selectively filtered relation must let connector pruning avoid it.
     */
    public boolean shouldPreloadUnfilteredScanPartitionView() {
        return hasUnfilteredLatestRelation;
    }

    /** Whether the pre-lock preload pass materialized this table's scan partition view. */
    public boolean hasScanPartitionView() {
        return scanPartitionView != null;
    }

    /**
     * The scan-path partition view materialized before the internal table locks were taken, so a lock-sensitive
     * consumer reuses it instead of paying connector I/O under the lock. Only valid when
     * {@link #hasScanPartitionView()} is true.
     */
    public Optional<Map<String, PartitionItem>> getScanPartitionView() {
        return scanPartitionView;
    }

    public void setScanPartitionView(Optional<Map<String, PartitionItem>> scanPartitionView) {
        this.scanPartitionView = scanPartitionView;
    }

    /** Drops the recorded view so a later execution of a reused statement does not read a stale one. */
    public void clearScanPartitionView() {
        this.scanPartitionView = null;
    }
}
