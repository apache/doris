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

package org.apache.doris.statistics.hbo;

import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Describes plan statistics which are derived from history based optimizer.
 */
public class InputTableStatisticsInfo {
    private final Optional<List<PlanStatistics>> inputTableStatistics;

    public InputTableStatisticsInfo(Optional<List<PlanStatistics>> inputTableStatistics) {
        this.inputTableStatistics = Objects.requireNonNull(inputTableStatistics,
                "inputTableStatistics is null");
    }

    /**
     * Fold the input table statistics of another node which shares the fingerprint of this one: two
     * runtime nodes can carry one fingerprint (the two scans of a self join, for instance), and the
     * provider stores one entry per fingerprint.
     *
     * <p>The two nodes read the same table population, so an entry of the same scan (the same table, the
     * same index and the same partitions) is one population however many nodes read it: folding two of
     * them keeps the larger measurement instead of adding the two up. Summing them would describe a table
     * which holds the rows of both scans at once, and the drift comparison - which sums the rows of every
     * recorded scan of a table - would then judge the next query of a single scan stale by construction.
     * Two different scans are both kept, because they describe two populations.
     */
    public InputTableStatisticsInfo merge(InputTableStatisticsInfo other) {
        if (inputTableStatistics.isEmpty()) {
            return other;
        }
        if (other.inputTableStatistics.isEmpty()) {
            return this;
        }
        Map<String, PlanStatistics> merged = new LinkedHashMap<>();
        for (PlanStatistics statistics : inputTableStatistics.get()) {
            merged.merge(identityOf(statistics), statistics, InputTableStatisticsInfo::largerRows);
        }
        for (PlanStatistics statistics : other.inputTableStatistics.get()) {
            merged.merge(identityOf(statistics), statistics, InputTableStatisticsInfo::largerRows);
        }
        return new InputTableStatisticsInfo(Optional.of(new ArrayList<>(merged.values())));
    }

    /** The larger of two measurements of one population. */
    private static PlanStatistics largerRows(PlanStatistics first, PlanStatistics second) {
        return second.getInputRows() > first.getInputRows() ? second : first;
    }

    /**
     * The scan an entry describes: its table, its index and the partitions it selected. Two scans which
     * differ only by their predicates read one population, so they share the identity a plan sorts them
     * by ({@code S{<table>#pt:...#ix:...}}).
     */
    private static String identityOf(PlanStatistics statistics) {
        if (!(statistics instanceof ScanPlanStatistics)) {
            return "node#" + statistics.getNodeId();
        }
        ScanPlanStatistics scanStatistics = (ScanPlanStatistics) statistics;
        if (scanStatistics.getScan() == null || scanStatistics.getScan().getTable() == null) {
            return "node#" + statistics.getNodeId();
        }
        PhysicalOlapScan scan = scanStatistics.getScan();
        List<Long> partitions = new ArrayList<>(scan.getSelectedPartitionIds());
        Collections.sort(partitions);
        return scan.getTable().getId() + "#" + scan.getSelectedIndexId() + "#" + partitions;
    }

    /**
     * Whether this entry describes the rows at least one input table contributed.
     *
     * <p>The read side treats an entry without input table statistics as a wildcard which matches by
     * fingerprint alone (that is how {@code HBO SET LEARNED STATISTICS} injects one). A published entry of
     * a query whose profile recorded no input table for any of its nodes - an external table has no scan
     * statistics item, for instance - would look exactly like such a wildcard, so the publish path stores
     * an entry only when it carries this information.
     */
    public boolean hasInputTables() {
        return inputTableStatistics.isPresent() && !inputTableStatistics.get().isEmpty();
    }

    public Optional<List<PlanStatistics>> getInputTableStatistics() {
        return inputTableStatistics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InputTableStatisticsInfo that = (InputTableStatisticsInfo) o;
        return Objects.equals(inputTableStatistics, that.inputTableStatistics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputTableStatistics);
    }
}
