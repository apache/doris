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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.connector.converter.NereidsToConnectorExpressionConverter;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.ExternalTablePreloadInfo;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionPruneResult;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to prune partition of file scan. For different external tables, there is no unified partition prune method.
 * For example, Hive is using hive meta store api to get partitions. Iceberg is using Iceberg api to get FileScanTask,
 * which doesn't return a partition list.
 * So here we only support Hive table partition prune.
 * For other external table, simply pass the conjuncts to LogicalFileScan, so that different
 * external file ScanNode could do the partition filter by themselves.
 */
public class PruneFileScanPartition extends OneRewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(PruneFileScanPartition.class);

    @Override
    public Rule build() {
        return logicalFilter(logicalFileScan()).whenNot(p -> p.child().getSelectedPartitions().isPruned
                        || p.child().getSelectedPartitions().isNativePlanning())
                .thenApply(ctx -> {
                    LogicalFilter<LogicalFileScan> filter = ctx.root;
                    LogicalFileScan scan = filter.child();
                    ExternalTable tbl = scan.getTable();

                    SelectedPartitions selectedPartitions;
                    if (tbl.supportInternalPartitionPruned()) {
                        selectedPartitions = pruneExternalPartitions(tbl, filter, scan, ctx.cascadesContext);
                    } else {
                        // set isPruned so that it won't go pass the partition prune again
                        selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true);
                    }
                    if (selectedPartitions == scan.getSelectedPartitions()) {
                        return null;
                    }
                    LogicalFileScan rewrittenScan = scan.withSelectedPartitions(selectedPartitions);
                    return new LogicalFilter<>(filter.getConjuncts(), rewrittenScan);
                }).toRule(RuleType.FILE_SCAN_PARTITION_PRUNE);
    }

    /**
     * Builds binary-search ranges for a connector-declined fallback from the partition map already recorded for
     * this statement.
     *
     * <p>A DEFERRED partition view must not be enumerated a second time here: a remote mutation between the two
     * listings could make the range generation disagree with the partition map and either abort planning or
     * silently omit a matching partition. A MATERIALIZED connector snapshot is different: its ranges and its
     * version token are both derived from the SAME immutable MVCC pin the recorded map came from, so the
     * cross-query {@link org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager} can serve them and
     * skip a per-query O(N log N) rebuild of every partition on an unchanged, large table.</p>
     */
    static Optional<SortedPartitionRanges<String>> fallbackSortedPartitionRanges(
            ExternalTable externalTable, LogicalFileScan scan,
            SelectedPartitions selectedPartitions, Map<String, PartitionItem> partitionItems) {
        return selectedPartitions.sortedPartitionRanges
                .or(() -> materializedSortedPartitionRanges(externalTable, scan, selectedPartitions))
                .or(() -> Optional.ofNullable(SortedPartitionRanges.build(partitionItems)));
    }

    /**
     * The cross-query range cache lookup, only for a view whose ranges provably describe the same fact as the
     * recorded map (a MATERIALIZED connector snapshot). A DEFERRED view keeps its recorded map untrusted by the
     * cache: the map is the connector's single generation for this statement, while a cache entry could have
     * been built from another enumeration.
     */
    private static Optional<SortedPartitionRanges<String>> materializedSortedPartitionRanges(
            ExternalTable externalTable, LogicalFileScan scan, SelectedPartitions selectedPartitions) {
        if (selectedPartitions.state != SelectedPartitions.State.MATERIALIZED) {
            return Optional.empty();
        }
        return externalTable.getSortedPartitionRanges(scan);
    }

    private SelectedPartitions pruneExternalPartitions(ExternalTable externalTable,
            LogicalFilter<LogicalFileScan> filter, LogicalFileScan scan, CascadesContext ctx) {
        SelectedPartitions initialSelection = scan.getSelectedPartitions();
        Optional<ExternalTablePreloadInfo> preload = scan.getTableSnapshot().isPresent()
                || scan.getScanParams().isPresent() ? Optional.empty()
                : ctx.getStatementContext().getExternalTablePreloadInfo(externalTable.getId());
        boolean preloadedFilter = preload.isPresent()
                && preload.get().hasFilteredScanView(scan.getPreloadRelationId());
        if ((initialSelection.isDeferredPartitionPruning() || initialSelection.isNotPruned())
                && externalTable instanceof PluginDrivenExternalTable
                && ((PluginDrivenExternalTable) externalTable).supportsConnectorPartitionPruning()
                && !preloadedFilter && scan.getTableSnapshot().isEmpty() && scan.getScanParams().isEmpty()
                && !preload.map(ExternalTablePreloadInfo::hasScanPartitionView).orElse(false)
                && ctx.getStatementContext().hasAnyPlanReadLockTable()
                && ctx.getStatementContext().shouldSkipUnpreloadedScanPartitionPruning()) {
            return initialSelection;
        }
        Map<String, PartitionItem> selectedPartitionItems = Maps.newHashMap();
        Optional<MvccSnapshot> snapshot = ctx.getStatementContext().getSnapshot(
                externalTable, scan.getTableSnapshot(), scan.getScanParams());
        if (!externalTable.supportInternalPartitionPruned(snapshot)) {
            // Keep the enumerated count without comparing raw transform values or mixed-spec keys.
            // An unpruned selection lets the connector apply predicates using each file's own spec.
            return scan.getSelectedPartitions();
        }
        List<Column> partitionColumns = externalTable.getPartitionColumns(snapshot);
        if (CollectionUtils.isEmpty(partitionColumns)) {
            // non partitioned table, return NOT_PRUNED.
            // non partition table will be handled in HiveScanNode.
            return SelectedPartitions.NOT_PRUNED;
        }
        Map<String, Slot> scanOutput = scan.getOutput()
                .stream()
                .collect(Collectors.toMap(slot -> slot.getName().toLowerCase(), Function.identity()));
        List<Slot> partitionSlots = partitionColumns.stream()
                .map(column -> scanOutput.get(column.getName().toLowerCase()))
                .collect(Collectors.toList());

        // The pruner's input contract is positional AND injective: PartitionPruner zips each partition
        // slot with the partition key's value at the same index, and OneListPartitionEvaluator collects
        // (slot -> literal) into an ImmutableMap. A repeated slot therefore aborts planning with
        // "Multiple entries with same key", and a null slot (a declared partition column missing from the
        // scan output) would NPE. Either shape means the table's partition model is not expressible as a
        // Doris partition-column set — e.g. an iceberg spec with two partition FIELDS over one source
        // column. Decline to prune instead of failing the query: NOT_PRUNED leaves isPruned=false, so
        // PluginDrivenScanNode.resolveRequiredPartitions returns scan-all and the query reads every
        // partition — never fewer rows, only a lost optimization.
        if (partitionSlots.contains(null) || Sets.newHashSet(partitionSlots).size() != partitionSlots.size()) {
            LOG.warn("skip partition pruning for {}.{}: partition columns {} do not map to a distinct,"
                            + " fully-resolved scan slot set", externalTable.getDbName(),
                    externalTable.getName(), partitionSlots);
            return SelectedPartitions.NOT_PRUNED;
        }

        Map<String, PartitionItem> nameToPartitionItem = scan.getSelectedPartitions().selectedPartitions;
        boolean connectorFilteredPartitions = false;
        Optional<FilterApplicationResult<ConnectorTableHandle>> connectorFilterResult = Optional.empty();
        ConnectorExpression connectorPredicate = null;
        if (nameToPartitionItem.isEmpty()
                && scan.getSelectedPartitions().isDeferredPartitionPruning()
                && externalTable instanceof PluginDrivenExternalTable
                && ((PluginDrivenExternalTable) externalTable).supportsConnectorPartitionPruning()) {
            connectorPredicate = NereidsToConnectorExpressionConverter.convert(filter.getPredicate());
            Optional<PluginDrivenExternalTable.ConnectorFilteredPartitionView> connectorPartitions;
            if (preloadedFilter) {
                connectorPartitions = preload.get().getFilteredScanView(scan.getPreloadRelationId());
                connectorPredicate = preload.get().getCoveredScanPredicate(scan.getPreloadRelationId());
            } else if (preload.isPresent() && preload.get().hasScanPartitionView()
                    && ctx.getStatementContext().hasAnyPlanReadLockTable()) {
                // A pushed/inferred filter may first appear after collect. Prune the already-warmed full
                // generation rather than issuing a selective HMS RPC while internal metadata locks are held.
                connectorPartitions = Optional.empty();
            } else {
                connectorPartitions = connectorPredicate == null ? Optional.empty()
                        : ((PluginDrivenExternalTable) externalTable).applyPartitionFilterForScan(
                                snapshot, connectorPredicate);
            }
            if (connectorPartitions.isPresent()) {
                nameToPartitionItem = connectorPartitions.get().getItems();
                // Carry the handle the selection was materialized from into physical planning: applying
                // the same predicate a second time could observe a different remote generation and mix
                // this name set with another handle's partition metadata.
                connectorFilterResult = Optional.of(connectorPartitions.get().getFilterResult());
                connectorFilteredPartitions = true;
            }
        }
        if (!connectorFilteredPartitions && nameToPartitionItem.isEmpty()
                && (scan.getSelectedPartitions().isNotPruned()
                || scan.getSelectedPartitions().isDeferredPartitionPruning())) {
            // The plugin-driven scan path degrades instead of failing: a connector entry that cannot be
            // represented as a Doris partition item makes the whole view unavailable, and pruning is then
            // skipped (every partition is read) rather than pruning against a partial/empty set.
            //
            // The view is resolved ONCE per statement and table reference (resolveScanPartitionView): the MV
            // partition collector records the same view, and its compensation union is restricted to those
            // names, so a second enumeration here could hand the scan a generation the compensation never saw.
            Optional<Map<String, PartitionItem>> scanView =
                    externalTable instanceof PluginDrivenExternalTable
                            ? ctx.getStatementContext().resolveScanPartitionView(externalTable,
                                    scan.getTableSnapshot(), scan.getScanParams(),
                                    () -> ((PluginDrivenExternalTable) externalTable)
                                            .getNameToPartitionItemsForScan(snapshot))
                            : Optional.of(externalTable.getNameToPartitionItems(snapshot));
            if (!scanView.isPresent()) {
                return SelectedPartitions.NOT_PRUNED;
            }
            nameToPartitionItem = scanView.get();
        }
        final Map<String, PartitionItem> partitionItems = nameToPartitionItem;
        Optional<SortedPartitionRanges<String>> sortedPartitionRanges = Optional.empty();
        boolean enableBinarySearch = ctx.getConnectContext() == null
                || ctx.getConnectContext().getSessionVariable().enableBinarySearchFilteringPartitions;
        if (enableBinarySearch && !partitionItems.isEmpty()) {
            if (connectorFilteredPartitions
                    || partitionItems.size() < scan.getSelectedPartitions().totalPartitionNum) {
                // Build from the map whenever it is not the recorded full selection: a connector-filtered
                // view is this statement's single generation, and a pruned subset is not described by
                // ranges cached for the full universe.
                sortedPartitionRanges = Optional.ofNullable(SortedPartitionRanges.build(partitionItems));
            } else {
                sortedPartitionRanges = fallbackSortedPartitionRanges(externalTable, scan,
                        scan.getSelectedPartitions(), partitionItems);
            }
        }
        PartitionPruneResult<String> result = PartitionPruner.pruneWithResult(
                partitionSlots, filter.getPredicate(), partitionItems, ctx,
                PartitionTableType.EXTERNAL, sortedPartitionRanges);
        List<String> prunedPartitions = new ArrayList<>(result.partitions);

        for (String name : prunedPartitions) {
            PartitionItem item = partitionItems.get(name);
            // The index and selected map must describe the same pinned partition generation.
            // Plugin MVCC views publish them together and share the very same PartitionItem objects.
            Preconditions.checkState(item != null,
                    "pruned partition %s is missing in the selected partitions snapshot", name);
            selectedPartitionItems.put(name, item);
        }
        // A connector-filtered universe has no known total. Otherwise keep the recorded total when an
        // explicit PARTITION(...) selection made the map a subset, and fall back to the map size when this
        // rule materialized the view for the first time.
        long totalPartitionNum = connectorFilteredPartitions
                ? SelectedPartitions.UNKNOWN_TOTAL_PARTITION_NUM
                : Math.max(partitionItems.size(), scan.getSelectedPartitions().totalPartitionNum);
        boolean hasPartitionPredicate = connectorFilteredPartitions || result.hasPartitionPredicate
                || scan.getSelectedPartitions().hasPartitionPredicate;
        if (connectorFilterResult.isPresent()) {
            return SelectedPartitions.connectorFiltered(totalPartitionNum, selectedPartitionItems,
                    hasPartitionPredicate, connectorFilterResult.get(), connectorPredicate);
        }
        return new SelectedPartitions(totalPartitionNum, selectedPartitionItems, true, hasPartitionPredicate);
    }
}
