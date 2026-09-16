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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.ExternalTablePreloadInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.PartitionCompensator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Used to collect query partitions, only collect once
 * */
public class QueryPartitionCollector extends DefaultPlanVisitor<Void, CascadesContext> {

    public static final Logger LOG = LogManager.getLogger(QueryPartitionCollector.class);

    @Override
    public Void visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation, CascadesContext context) {
        TableIf table = catalogRelation.getTable();
        if (table.getDatabase() == null) {
            LOG.error("QueryPartitionCollector visitLogicalCatalogRelation database is null, table is "
                    + table.getName());
            return null;
        }
        StatementContext statementContext = context.getStatementContext();
        // Collect relationId to tableId mapping
        Multimap<Integer, Integer> relationIdToTableId = statementContext.getCommonTableIdToRelationIdMap();
        relationIdToTableId.put(statementContext.getTableId(catalogRelation.getTable()).asInt(),
                catalogRelation.getRelationId().asInt());
        // Collect table used partition mapping
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = statementContext
                .getTableUsedPartitionNameMap();
        Set<String> tablePartitions = new HashSet<>();
        if (catalogRelation instanceof LogicalOlapScan) {
            // Handle olap table
            LogicalOlapScan logicalOlapScan = (LogicalOlapScan) catalogRelation;
            for (Long partitionId : logicalOlapScan.getSelectedPartitionIds()) {
                tablePartitions.add(logicalOlapScan.getTable().getPartition(partitionId).getName());
            }
            tableUsedPartitionNameMap.put(table.getFullQualifiers(),
                    Pair.of(catalogRelation.getRelationId(), tablePartitions));
        } else if (catalogRelation instanceof LogicalFileScan
                && catalogRelation.getTable() != null
                && ((ExternalTable) catalogRelation.getTable()).supportInternalPartitionPruned()) {
            LogicalFileScan logicalFileScan = (LogicalFileScan) catalogRelation;
            SelectedPartitions selectedPartitions = logicalFileScan.getSelectedPartitions();
            if (selectedPartitions.isDeferredPartitionPruning()) {
                Set<String> deferredPartitions = materializeDeferredPartitions(
                        (PluginDrivenExternalTable) table, logicalFileScan, context);
                if (deferredPartitions == null) {
                    // The connector view is unavailable: keep the "query all partitions" marker.
                    tableUsedPartitionNameMap.put(table.getFullQualifiers(), PartitionCompensator.ALL_PARTITIONS);
                } else {
                    tablePartitions.addAll(deferredPartitions);
                    tableUsedPartitionNameMap.put(table.getFullQualifiers(),
                            Pair.of(catalogRelation.getRelationId(), tablePartitions));
                }
            } else if (selectedPartitions.isNotPruned()) {
                // NOT_PRUNED is the "pruning did not run" sentinel, NOT a concrete selection: its map is empty
                // because partitioning never got enumerated (an unrepresentable connector partition makes the
                // whole view unavailable, and PruneFileScanPartition then returns this sentinel), yet the scan
                // reads EVERY partition. Recording the empty map would tell the compensator the query reads no
                // partitions at all and reject an otherwise eligible MV rewrite.
                tableUsedPartitionNameMap.put(table.getFullQualifiers(), PartitionCompensator.ALL_PARTITIONS);
            } else {
                tablePartitions.addAll(selectedPartitions.selectedPartitions.keySet());
                tableUsedPartitionNameMap.put(table.getFullQualifiers(),
                        Pair.of(catalogRelation.getRelationId(), tablePartitions));
            }
        } else {
            // not support get partition scene, we consider query all partitions from table
            tableUsedPartitionNameMap.put(table.getFullQualifiers(), PartitionCompensator.ALL_PARTITIONS);
        }
        return null;
    }

    /**
     * Materializes the partition names a DEFERRED (not yet enumerated) file scan reads, for the MV partition
     * compensation decision only, or {@code null} when the connector view cannot be materialized.
     *
     * <p>{@code DEFERRED} is produced by {@link PluginDrivenExternalTable#initSelectedPartitions}, i.e. only a
     * table whose connector can prune partitions from a predicate - so the cast holds, and the connector is the
     * only authority for the partition names this scan reads.</p>
     *
     * <p>WHY enumeration and not {@link PartitionCompensator#ALL_PARTITIONS}: the marker means "this query reads
     * EVERY partition of the base table", which the compensator turns into "the materialized view already covers
     * everything, so no union compensation is needed". A deferred view is merely NOT ENUMERATED YET, so reporting
     * the marker suppresses exactly the compensation that re-reads the base partitions an MV does not cover - e.g.
     * a partition added to the base table after the last MV refresh, whose rows then silently disappear from a
     * rewritten query ({@code mv.external_table.part_partition_invalid}, {@code test_hive_rewrite_mtmv}). The
     * enumeration is the scan's unfiltered view - the same full view the scan itself has to materialize before
     * generating splits, served from the connector's partition view cache. When no partition predicate pruned the
     * scan (the only way {@code DEFERRED} survives {@code PruneFileScanPartition}, whose connector-declined path
     * materializes a local selection) it is exactly the query's own selection; on a plan collected before that
     * pruning it is a superset, which can only make the compensator union MORE base partitions, never fewer.</p>
     *
     * <p>WHY the reuse below: this visitor runs from {@code InitMaterializationContextHook.afterRewrite}, i.e.
     * while {@link StatementContext#lock()} is still held, so enumerating here would block metadata writers and
     * DDL on every internal table of the statement for a whole connector round-trip. The pre-lock preload pass
     * therefore materializes this exact view for a latest reference and records it; reuse it and take connector
     * I/O only when no preload ran - the preload switch is opt-in, and with it off this is no worse than the
     * pre-change eager materialization, which enumerated the same view at bind time under the same lock.</p>
     */
    private static Set<String> materializeDeferredPartitions(PluginDrivenExternalTable table, LogicalFileScan scan,
            CascadesContext context) {
        Optional<Map<String, PartitionItem>> preloaded = preloadedScanPartitionView(
                context.getStatementContext(), table, scan);
        Optional<Map<String, PartitionItem>> partitions = preloaded != null ? preloaded
                : table.getNameToPartitionItemsForScan(context.getStatementContext().getSnapshot(table,
                        scan.getTableSnapshot(), scan.getScanParams()));
        return partitions.map(Map::keySet).orElse(null);
    }

    /**
     * The scan partition view the pre-lock preload pass materialized for this scan, or {@code null} when the
     * scan must resolve it itself. An {@link Optional#empty()} return is a materialized-but-unavailable view.
     *
     * <p>Only a reference without a version selector is served: that is the only shape the preload pass warms,
     * and a selector-carrying reference must enumerate its own generation.</p>
     */
    static Optional<Map<String, PartitionItem>> preloadedScanPartitionView(StatementContext statementContext,
            PluginDrivenExternalTable table, LogicalFileScan scan) {
        if (scan.getTableSnapshot().isPresent() || scan.getScanParams().isPresent()) {
            return null;
        }
        return statementContext.getExternalTablePreloadInfo(table.getId())
                .filter(ExternalTablePreloadInfo::hasScanPartitionView)
                .map(ExternalTablePreloadInfo::getScanPartitionView)
                .orElse(null);
    }
}
