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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionExprFactory;
import org.apache.doris.mtmv.MTMVPartitionExprService;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Handle materialized view partition union compensate handler
 * */
public class PartitionCompensator {

    public static final Logger LOG = LogManager.getLogger(PartitionCompensator.class);
    // if the partition pair is null which means could not get partitions from table in QueryPartitionCollector,
    // we think the table scans query all-partitions default
    public static final Pair<RelationId, Set<String>> ALL_PARTITIONS = Pair.of(null, null);
    // It means all partitions are used when query
    public static final Collection<Pair<RelationId, Set<String>>> ALL_PARTITIONS_LIST =
            ImmutableList.of(ALL_PARTITIONS);

    /**
     * How many inputs of the query read a table the MV is partitioned on. A compensation narrows a table,
     * not an input, so a table read through more than one of them -- the aliases of a self-join -- is
     * narrowed for all of them at once. The query's own relations are what to count, not the plan the MV has
     * already rewritten: there the base scans are gone, replaced by the MV scan.
     */
    public static int pctInputsInQuery(List<CatalogRelation> queryRelations, MTMV mtmv) throws AnalysisException {
        Set<List<String>> pctTableQualifiers = Sets.newHashSet();
        for (MTMVRelatedTableIf pctTable : mtmv.getMvPartitionInfo().getPctTables()) {
            pctTableQualifiers.add(pctTable.getFullQualifiers());
        }
        int count = 0;
        for (CatalogRelation relation : queryRelations) {
            if (pctTableQualifiers.contains(relation.getTable().getFullQualifiers())) {
                count++;
            }
        }
        return count;
    }

    /**
     * Maybe only some partitions is invalid in materialized view, or base table maybe add, modify, delete partition
     * So we should calc the invalid partition used in query
     * @param queryUsedBaseTablePartitionMap partitions used by query related partition table
     * @param pctInputsInQuery how many relations of the query read a table the MV is partitioned on; a
     *         self-join reads one such table through two of them
     * @param rewrittenPlan tmp rewrittenPlan when mv rewrite
     * @param materializationContext the context of materialization,which hold materialized view meta and other info
     * @param cascadesContext the context of cascades
     * @return the key in pair is mvNeedRemovePartitionNameSet, the value in pair is baseTableNeedUnionPartitionNameSet
     */
    public static Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> calcInvalidPartitions(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap, int pctInputsInQuery, Plan rewrittenPlan,
            AsyncMaterializationContext materializationContext, CascadesContext cascadesContext)
            throws AnalysisException {
        return calcInvalidPartitions(queryUsedBaseTablePartitionMap, pctInputsInQuery, rewrittenPlan,
                materializationContext, cascadesContext, Sets.newHashSet());
    }

    /**
     * The same, recording in {@code uncoveredNarrowedBasePartitions} the uncovered base partitions the
     * compensation reads beside the MV partitions it keeps. Those MV partitions answer groups the
     * partitions also contribute to, so unioning the two answers does not give whole groups.
     */
    public static Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> calcInvalidPartitions(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap, int pctInputsInQuery, Plan rewrittenPlan,
            AsyncMaterializationContext materializationContext, CascadesContext cascadesContext,
            Set<String> uncoveredNarrowedBasePartitions)
            throws AnalysisException {
        return calcInvalidPartitions(queryUsedBaseTablePartitionMap, pctInputsInQuery, rewrittenPlan,
                materializationContext, cascadesContext, uncoveredNarrowedBasePartitions, Sets.newHashSet());
    }

    /**
     * The same, additionally recording in {@code columnsSharingRetainedBucket} the base columns whose
     * compensation reads a partition that falls in the same transform bucket as an MV partition that stays.
     * Grouping by the view's transform splits the branches apart only while each retained partition holds its
     * whole bucket; see {@link #addColumnsWhoseCompensationSharesRetainedBucket}.
     */
    public static Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> calcInvalidPartitions(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap, int pctInputsInQuery, Plan rewrittenPlan,
            AsyncMaterializationContext materializationContext, CascadesContext cascadesContext,
            Set<String> uncoveredNarrowedBasePartitions, Set<BaseColInfo> columnsSharingRetainedBucket)
            throws AnalysisException {
        // check partition is valid or not
        MTMV mtmv = materializationContext.getMtmv();
        PartitionInfo mvPartitionInfo = mtmv.getPartitionInfo();
        if (PartitionType.UNPARTITIONED.equals(mvPartitionInfo.getType())) {
            // if not partition, if rewrite success, it means mv is available
            return Pair.of(ImmutableMap.of(), ImmutableMap.of());
        }
        if (mtmv.getMvPartitionInfo().getPctTables().isEmpty() || queryUsedBaseTablePartitionMap.isEmpty()) {
            // if mv is not partitioned or query not query any partition, doesn't compensate
            return Pair.of(ImmutableMap.of(), ImmutableMap.of());
        }
        // get mv valid partitions
        Collection<Partition> mvValidPartitions = cascadesContext.getStatementContext()
                .getMvCanRewritePartitionsMap().get(new BaseTableInfo(mtmv));
        Set<String> rewrittenPlanUsePartitionNameSet = new HashSet<>();
        List<LogicalOlapScan> mvOlapScanList = rewrittenPlan.collectToList(node ->
                node instanceof LogicalOlapScan
                        && Objects.equals(((CatalogRelation) node).getTable().getName(), mtmv.getName()));
        for (LogicalOlapScan olapScan : mvOlapScanList) {
            olapScan.getSelectedPartitionIds().forEach(id ->
                    rewrittenPlanUsePartitionNameSet.add(olapScan.getTable().getPartition(id).getName()));
        }
        Map<MTMVRelatedTableIf, Map<String, Set<String>>> mtmvRelatedTableIfMapMap
                = materializationContext.calculatePartitionMappings(queryUsedBaseTablePartitionMap);
        boolean allCompensateIsNull = true;

        Map<BaseTableInfo, Set<String>> mvPartitionNeedRemoveNameMap = new HashMap<>();
        Map<BaseColInfo, Set<String>> baseTablePartitionNeedUnionNameMap = new HashMap<>();

        Map<BaseTableInfo, BaseColInfo> pctInfoMap = new HashMap<>();
        mtmv.getMvPartitionInfo().getPctInfos().forEach(
                colInfo -> pctInfoMap.put(colInfo.getTableInfo(), colInfo));

        for (Map.Entry<MTMVRelatedTableIf, Map<String, Set<String>>> partitionMapping
                : mtmvRelatedTableIfMapMap.entrySet()) {
            MTMVRelatedTableIf relatedTable = partitionMapping.getKey();
            Set<String> relatedTableUsedPartitionSet
                    = queryUsedBaseTablePartitionMap.get(relatedTable.getFullQualifiers());
            Pair<Pair<BaseTableInfo, Set<String>>, Pair<BaseColInfo, Set<String>>> needCompensatePartitions
                    = getNeedCompensatePartitions(mvValidPartitions, relatedTableUsedPartitionSet,
                    rewrittenPlanUsePartitionNameSet, pctInfoMap.get(new BaseTableInfo(relatedTable)),
                    partitionMapping.getValue(), materializationContext);
            allCompensateIsNull &= needCompensatePartitions == null;
            if (needCompensatePartitions == null) {
                continue;
            }
            Pair<BaseTableInfo, Set<String>> mvNeedRemovePartition = needCompensatePartitions.key();
            Pair<BaseColInfo, Set<String>> baseTableNeedUnionTable = needCompensatePartitions.value();
            if ((mvNeedRemovePartition.value().isEmpty() && baseTableNeedUnionTable.value().isEmpty())) {
                continue;
            }
            if (!mvNeedRemovePartition.value().isEmpty()) {
                mvPartitionNeedRemoveNameMap
                        .computeIfAbsent(mvNeedRemovePartition.key(), k -> new HashSet<>())
                        .addAll(mvNeedRemovePartition.value());
            }
            if (!baseTableNeedUnionTable.value().isEmpty()) {
                baseTablePartitionNeedUnionNameMap
                        .computeIfAbsent(baseTableNeedUnionTable.key(), k -> new HashSet<>())
                        .addAll(baseTableNeedUnionTable.value());
            }
        }
        if (allCompensateIsNull) {
            return null;
        }
        // A base partition the query reads that no MV partition of this plan is named from -- one the
        // partition_sync_limit window left out, say -- is one the MV does not answer for: a partition of the MV
        // reads a table through the partitions it does name, and the mapping has no entry for this one. Its
        // rows have to come from the base table, and two things follow from that, decided apart.
        //
        // The base branch of the union reads those partitions. A table the mapping mentions not at all is
        // uncovered in every partition the query uses, so it is read for the query's own partitions there
        // rather than skipped -- without this the plan would answer for that table out of the MV alone.
        //
        // And the MV partitions this plan uses have to be taken out when they hold rows of such a partition: a
        // partition the window has since left out can have been materialized in one of them by an earlier
        // refresh, and the union would then count its rows twice. The refresh snapshot names exactly the
        // partitions a refresh read into each MV partition, so that is what this is asked of. (It is not the
        // keys: a list partition's default one holds the keys no other partition of the table claims, so a
        // partition can be read into an MV partition whose keys do not cover it.)
        Map<MTMVRelatedTableIf, Set<String>> uncoveredByTable = Maps.newLinkedHashMap();
        boolean uncoveredRecorded = false;
        for (MTMVRelatedTableIf pctTable : mtmv.getMvPartitionInfo().getPctTables()) {
            Set<String> queryUsed = queryUsedBaseTablePartitionMap.get(pctTable.getFullQualifiers());
            if (CollectionUtils.isEmpty(queryUsed)) {
                continue;
            }
            // The mapping of a table the query's filtering left out entirely is empty, so a table it does not
            // mention at all is a table every query-used partition of which is uncovered.
            Map<String, Set<String>> tableMapping = mtmvRelatedTableIfMapMap.getOrDefault(pctTable, Maps.newHashMap());
            Set<String> named = Sets.newHashSet();
            for (Set<String> names : tableMapping.values()) {
                named.addAll(names);
            }
            Set<String> uncovered = Sets.difference(queryUsed, named);
            if (uncovered.isEmpty()) {
                continue;
            }
            uncoveredByTable.put(pctTable, uncovered);
            Set<String> recorded = Sets.newHashSet();
            MTMVRefreshSnapshot refreshSnapshot = mtmv.getRefreshSnapshot();
            if (refreshSnapshot != null) {
                for (String mvPartitionName : rewrittenPlanUsePartitionNameSet) {
                    recorded.addAll(refreshSnapshot.getPctSnapshots(mvPartitionName, new BaseTableInfo(pctTable)));
                }
            }
            if (!Sets.intersection(uncovered, recorded).isEmpty()) {
                uncoveredRecorded = true;
            }
        }
        // Narrowing a table's read keeps the MV partitions answering for the rest, and that only holds while
        // one input of the query reads the tables it narrows: the pass that turns the compensation into a plan
        // puts the filters it is given on one join, so two narrowed inputs read the pairs both filters reach
        // and lose the mixed ones -- an a_old x b_kept row, say, when A is narrowed for a partition the
        // mapping does not name and B for one the MV no longer holds, or when one table is narrowed while a
        // self-join reads it through two inputs, both of which the filter reaches. Nor can a table be left as
        // it is while the MV partitions that answered for it are taken out. So the MV partitions are taken
        // out, and every table the MV is partitioned on is read for the query's own partitions, whenever more
        // than one table is narrowed, a single table is narrowed while the query reads such a table through
        // more inputs than that, or a refresh read one of the uncovered partitions into an MV partition.
        int narrowedTables = 0;
        for (MTMVRelatedTableIf pctTable : mtmv.getMvPartitionInfo().getPctTables()) {
            BaseColInfo pctInfo = pctInfoMap.get(new BaseTableInfo(pctTable));
            Set<String> queryUsed = queryUsedBaseTablePartitionMap.get(pctTable.getFullQualifiers());
            Set<String> stale = pctInfo == null ? null : baseTablePartitionNeedUnionNameMap.get(pctInfo);
            boolean narrowedByStalePass = CollectionUtils.isNotEmpty(stale) && !stale.containsAll(queryUsed);
            if (narrowedByStalePass || uncoveredByTable.containsKey(pctTable)) {
                narrowedTables++;
            }
        }
        boolean takeMvPartitionsOut = uncoveredRecorded || narrowedTables > 1
                || (narrowedTables > 0 && pctInputsInQuery > narrowedTables);
        if (!takeMvPartitionsOut) {
            // The MV partitions stay, and the uncovered partitions are added to the base branch beside them.
            // Such an uncovered partition can answer a group an MV partition also answers when its keys
            // meet, in the MV's partition columns, the keys of the base partitions that MV partition is
            // recorded with: a merged LIST component {kept (2020,1), bridge (2038,2)} projected onto d
            // holds 2020 and 2038, and an uncovered old (2020,2) is read beside it, so the union would
            // answer d=2020 twice. A RANGE partition whose range no retained MV partition covers answers
            // its own groups and is not recorded here. Only the MV partitions that stay count: one the
            // staleness pass has already scheduled for removal is re-read through the base branch too, so
            // its keys are the compensation's own, not a retained group the uncovered partition would meet.
            Set<String> removedMvPartitions = Sets.newHashSet();
            mvPartitionNeedRemoveNameMap.values().forEach(removedMvPartitions::addAll);
            Set<String> retainedMvPartitions = Sets.difference(
                    rewrittenPlanUsePartitionNameSet, removedMvPartitions);
            for (Entry<MTMVRelatedTableIf, Set<String>> entry : uncoveredByTable.entrySet()) {
                BaseColInfo pctInfo = pctInfoMap.get(new BaseTableInfo(entry.getKey()));
                if (pctInfo == null) {
                    continue;
                }
                uncoveredNarrowedBasePartitions.addAll(overlappingUncoveredListPartitions(
                        entry.getKey(), pctInfo, entry.getValue(), retainedMvPartitions,
                        mtmvRelatedTableIfMapMap));
            }
            addColumnsWhoseCompensationSharesRetainedBucket(mtmv, pctInfoMap,
                    baseTablePartitionNeedUnionNameMap, uncoveredByTable, retainedMvPartitions,
                    columnsSharingRetainedBucket);
        }
        if (!uncoveredByTable.isEmpty()) {
            for (MTMVRelatedTableIf pctTable : mtmv.getMvPartitionInfo().getPctTables()) {
                Set<String> queryUsed = queryUsedBaseTablePartitionMap.get(pctTable.getFullQualifiers());
                if (CollectionUtils.isEmpty(queryUsed)) {
                    continue;
                }
                BaseColInfo pctInfo = pctInfoMap.get(new BaseTableInfo(pctTable));
                if (pctInfo == null) {
                    continue;
                }
                if (takeMvPartitionsOut) {
                    baseTablePartitionNeedUnionNameMap.put(pctInfo, new HashSet<>(queryUsed));
                    continue;
                }
                Set<String> uncovered = uncoveredByTable.get(pctTable);
                if (CollectionUtils.isEmpty(uncovered)) {
                    continue;
                }
                baseTablePartitionNeedUnionNameMap
                        .computeIfAbsent(pctInfo, k -> new HashSet<>()).addAll(uncovered);
            }
            if (takeMvPartitionsOut && !rewrittenPlanUsePartitionNameSet.isEmpty()) {
                mvPartitionNeedRemoveNameMap
                        .computeIfAbsent(new BaseTableInfo(mtmv), k -> new HashSet<>())
                        .addAll(rewrittenPlanUsePartitionNameSet);
            }
            allCompensateIsNull = false;
        }

        // merge all partition to delete or union
        Set<String> needRemovePartitionSet = new HashSet<>();
        mvPartitionNeedRemoveNameMap.values().forEach(needRemovePartitionSet::addAll);
        mvPartitionNeedRemoveNameMap.replaceAll((k, v) -> needRemovePartitionSet);

        // The partitions to union stay with the table they belong to: a partition name means that table's
        // keys, and two tables of a multi-table MV can use the same name for different keys -- the merge of the
        // cross-table LIST descriptors makes such MVs buildable. Unioning the names across the tables would
        // read the other table's partitions too, and with the union rewrite the MV's own branch already
        // supplies their rows, so the answer would count them twice.

        return Pair.of(mvPartitionNeedRemoveNameMap, baseTablePartitionNeedUnionNameMap);
    }

    /**
     * The uncovered partitions of this table whose keys meet, in the MV's partition columns, the keys of the
     * base partitions a retained MV partition is recorded with. Only a LIST partitioned table can have them:
     * a merged component's MV partition holds several keys of the MV's partition column, and an uncovered
     * partition can carry one of them. A RANGE base partition is either covered by a retained MV partition
     * -- then it is not uncovered -- or outside its range, where it answers groups of its own.
     */
    private static Set<String> overlappingUncoveredListPartitions(
            MTMVRelatedTableIf table, BaseColInfo pctInfo, Set<String> uncovered,
            Set<String> retainedMvPartitions,
            Map<MTMVRelatedTableIf, Map<String, Set<String>>> tableMappings) {
        if (!(table instanceof OlapTable)
                || !(((OlapTable) table).getPartitionInfo() instanceof ListPartitionInfo)) {
            return Sets.newHashSet();
        }
        OlapTable olapTable = (OlapTable) table;
        List<Column> partitionColumns = olapTable.getPartitionColumns();
        int keyPosition = -1;
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (partitionColumns.get(i).getName().equalsIgnoreCase(pctInfo.getColName())) {
                keyPosition = i;
                break;
            }
        }
        if (keyPosition < 0) {
            return Sets.newHashSet();
        }
        Set<LiteralExpr> retainedValues = Sets.newHashSet();
        Map<String, Set<String>> mvToBasePartitions = tableMappings.get(table);
        if (mvToBasePartitions != null) {
            for (String mvPartition : retainedMvPartitions) {
                for (String basePartition : mvToBasePartitions.getOrDefault(mvPartition, Sets.newHashSet())) {
                    addProjectedListKeys(olapTable, basePartition, keyPosition, retainedValues);
                }
            }
        }
        Set<String> res = Sets.newHashSet();
        for (String partition : uncovered) {
            Set<LiteralExpr> values = Sets.newHashSet();
            addProjectedListKeys(olapTable, partition, keyPosition, values);
            if (values.stream().anyMatch(retainedValues::contains)) {
                res.add(partition);
            }
        }
        return res;
    }

    private static void addProjectedListKeys(OlapTable table, String partitionName, int keyPosition,
            Set<LiteralExpr> out) {
        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            return;
        }
        PartitionItem item = table.getPartitionInfo().getItem(partition.getId());
        if (!(item instanceof ListPartitionItem)) {
            return;
        }
        for (PartitionKey key : ((ListPartitionItem) item).getItems()) {
            if (keyPosition < key.getKeys().size()) {
                out.add(key.getKeys().get(keyPosition));
            }
        }
    }

    /**
     * The base columns whose compensation reads a partition that falls in the same transform bucket as an MV
     * partition that stays.
     *
     * <p>The aggregate rule lets a grouping by the view's own transform keep the two branches apart because
     * the view's partitions are disjoint in that transform's value. That holds only while each retained
     * partition holds the whole bucket: {@code partition_sync_limit} drops base partitions before the roll-up,
     * so a retained year partition can hold the days the window kept while an expired day of the same year is
     * compensated -- grouping by {@code date_trunc(d, 'year')} then answers that year from both branches.
     * Likewise a retained partition can hold a recorded base partition that runs past its own upper bound: the
     * roll-up of a {@code DATETIME(3)} partition ending a fraction of a second after a bucket boundary takes
     * off one second and lands in the bucket before it, while the rows past the boundary are grouped into the
     * next one. A compensated partition does that by itself, with no retained partition's record to show it:
     * its roll-up names the bucket before the boundary, and the rows in the fraction reach a bucket a retained
     * partition can answer -- so its actual upper bound is compared against the one its roll-up names. Such a
     * column is recorded here, so the aggregate rule declines the transform coverage and the query is
     * answered from the base table instead.
     */
    private static void addColumnsWhoseCompensationSharesRetainedBucket(MTMV mtmv,
            Map<BaseTableInfo, BaseColInfo> pctInfoMap,
            Map<BaseColInfo, Set<String>> baseTablePartitionNeedUnionNameMap,
            Map<MTMVRelatedTableIf, Set<String>> uncoveredByTable,
            Set<String> retainedMvPartitions,
            Set<BaseColInfo> columnsSharingRetainedBucket) throws AnalysisException {
        MTMVPartitionInfo mvPartitionInfo = mtmv.getMvPartitionInfo();
        if (mvPartitionInfo.getPartitionType() != MTMVPartitionType.EXPR || retainedMvPartitions.isEmpty()) {
            return;
        }
        MTMVPartitionExprService exprService;
        try {
            exprService = MTMVPartitionExprFactory.getExprService(mvPartitionInfo.getExpr());
        } catch (AnalysisException e) {
            // The view was created from an expression this factory reads, so it cannot happen.
            throw new IllegalStateException("Cannot read the partition expression of " + mtmv.getName(), e);
        }
        Map<String, PartitionItem> mvItems = mtmv.getAndCopyPartitionItems();
        Map<PartitionKeyDesc, String> retainedDescs = Maps.newHashMap();
        for (String mvPartitionName : retainedMvPartitions) {
            PartitionItem mvItem = mvItems.get(mvPartitionName);
            if (mvItem != null) {
                retainedDescs.put(mvItem.toPartitionKeyDesc(), mvPartitionName);
            }
        }
        MTMVRefreshSnapshot refreshSnapshot = mtmv.getRefreshSnapshot();
        for (MTMVRelatedTableIf pctTable : mvPartitionInfo.getPctTables()) {
            BaseColInfo pctInfo = pctInfoMap.get(new BaseTableInfo(pctTable));
            if (pctInfo == null) {
                continue;
            }
            Set<String> compensated = Sets.newHashSet(
                    baseTablePartitionNeedUnionNameMap.getOrDefault(pctInfo, Sets.newHashSet()));
            compensated.addAll(uncoveredByTable.getOrDefault(pctTable, Sets.newHashSet()));
            if (compensated.isEmpty()) {
                continue;
            }
            Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotFromContext(pctTable);
            // The partitions are read once and shared by both checks below: each takes the table map, and
            // the map is every partition of the table, not just the ones either check looks up.
            Map<String, PartitionItem> baseItems = pctTable.getAndCopyPartitionItems(snapshot);
            boolean sharesRetainedBucket;
            try {
                sharesRetainedBucket = compensatedSharesRetainedBucket(pctTable, pctInfo.getColName(),
                        baseItems, snapshot, exprService, mvPartitionInfo, compensated, retainedDescs)
                        || (refreshSnapshot != null && retainedPartitionRunsPastItsUpperBound(
                                pctTable, pctInfo.getColName(), snapshot, baseItems, refreshSnapshot,
                                retainedMvPartitions, mvItems));
            } catch (AnalysisException e) {
                // Which bucket one of them falls in cannot be told, so the transform is not trusted to keep
                // the branches apart.
                sharesRetainedBucket = true;
            }
            if (sharesRetainedBucket) {
                columnsSharingRetainedBucket.add(pctInfo);
            }
        }
    }

    private static boolean compensatedSharesRetainedBucket(MTMVRelatedTableIf pctTable, String colName,
            Map<String, PartitionItem> items, Optional<MvccSnapshot> snapshot,
            MTMVPartitionExprService exprService, MTMVPartitionInfo mvPartitionInfo, Set<String> compensated,
            Map<PartitionKeyDesc, String> retainedDescs) throws AnalysisException {
        int colPos = mvPartitionInfo.getPctColPos(pctTable);
        Column partitionColumn = partitionColumnOf(pctTable, colName, snapshot);
        for (String partitionName : compensated) {
            PartitionItem item = items.get(partitionName);
            if (item == null) {
                continue;
            }
            PartitionKeyDesc actualDesc = item.toPartitionKeyDesc(colPos);
            PartitionKeyDesc rollUpDesc = exprService.generateRollUpPartitionKeyDesc(
                    actualDesc, mvPartitionInfo, pctTable, snapshot);
            if (retainedDescs.containsKey(rollUpDesc)) {
                return true;
            }
            if (partitionColumn != null && overrunsRolledBucket(actualDesc, rollUpDesc, partitionColumn)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a partition's range reaches past the bucket its roll-up descriptor names, so that the rows in
     * between are grouped into a bucket the descriptor does not name.
     */
    private static boolean overrunsRolledBucket(PartitionKeyDesc actualDesc, PartitionKeyDesc rollUpDesc,
            Column partitionColumn) throws AnalysisException {
        if (actualDesc.getPartitionType() != PartitionKeyDesc.PartitionKeyValueType.FIXED
                || rollUpDesc.getPartitionType() != PartitionKeyDesc.PartitionKeyValueType.FIXED) {
            return false;
        }
        List<Column> baseColumns = ImmutableList.of(partitionColumn);
        PartitionKey actualUpper = PartitionKey.createPartitionKey(actualDesc.getUpperValues(), baseColumns);
        PartitionKey rolledUpper = PartitionKey.createPartitionKey(rollUpDesc.getUpperValues(), baseColumns);
        return actualUpper.compareTo(rolledUpper) > 0;
    }

    private static Column partitionColumnOf(MTMVRelatedTableIf pctTable, String colName,
            Optional<MvccSnapshot> snapshot) throws AnalysisException {
        for (Column column : pctTable.getPartitionColumns(snapshot)) {
            if (column.getName().equalsIgnoreCase(colName)) {
                return column;
            }
        }
        return null;
    }

    private static boolean retainedPartitionRunsPastItsUpperBound(MTMVRelatedTableIf pctTable, String colName,
            Optional<MvccSnapshot> snapshot, Map<String, PartitionItem> baseItems,
            MTMVRefreshSnapshot refreshSnapshot, Set<String> retainedMvPartitions,
            Map<String, PartitionItem> mvItems) throws AnalysisException {
        Column partitionColumn = partitionColumnOf(pctTable, colName, snapshot);
        if (partitionColumn == null) {
            return false;
        }
        List<Column> baseColumns = ImmutableList.of(partitionColumn);
        BaseTableInfo pctTableInfo = new BaseTableInfo(pctTable);
        for (String mvPartitionName : retainedMvPartitions) {
            PartitionItem mvItem = mvItems.get(mvPartitionName);
            if (mvItem == null) {
                continue;
            }
            PartitionKey mvUpper = PartitionKey.createPartitionKey(
                    mvItem.toPartitionKeyDesc().getUpperValues(), baseColumns);
            for (String basePartitionName : refreshSnapshot.getPctSnapshots(mvPartitionName, pctTableInfo)) {
                PartitionItem baseItem = baseItems.get(basePartitionName);
                if (baseItem == null) {
                    continue;
                }
                PartitionKey baseUpper = PartitionKey.createPartitionKey(
                        baseItem.toPartitionKeyDesc().getUpperValues(), baseColumns);
                if (baseUpper.compareTo(mvUpper) > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Pair<Pair<BaseTableInfo, Set<String>>, Pair<BaseColInfo, Set<String>>> getNeedCompensatePartitions(
            Collection<Partition> mvValidPartitions,
            Set<String> queryUsedBaseTablePartitionNameSet,
            Set<String> rewrittenPlanUsePartitionNameSet,
            BaseColInfo relatedPartitionTable,
            Map<String, Set<String>> partitionMapping,
            MaterializationContext materializationContext
    ) {
        // compensated result
        Set<String> baseTableNeedUnionPartitionNameSet = new HashSet<>();
        // the middle result when compensate
        Set<String> mvValidPartitionNameSet = new HashSet<>();
        Set<String> mvValidBaseTablePartitionNameSet = new HashSet<>();
        Set<String> mvValidHasDataRelatedBaseTableNameSet = new HashSet<>();
        MTMV mtmv = ((AsyncMaterializationContext) materializationContext).getMtmv();
        for (Partition mvValidPartition : mvValidPartitions) {
            mvValidPartitionNameSet.add(mvValidPartition.getName());
            Set<String> relatedBaseTablePartitions = partitionMapping.get(mvValidPartition.getName());
            if (relatedBaseTablePartitions != null) {
                mvValidBaseTablePartitionNameSet.addAll(relatedBaseTablePartitions);
                if (!mtmv.selectNonEmptyPartitionIds(ImmutableList.of(mvValidPartition.getId()),
                        Optional.empty()).isEmpty()) {
                    mvValidHasDataRelatedBaseTableNameSet.addAll(relatedBaseTablePartitions);
                }
            }
        }
        if (Sets.intersection(mvValidHasDataRelatedBaseTableNameSet, queryUsedBaseTablePartitionNameSet).isEmpty()) {
            // if mv couldn't offer any partition for query, query rewrite should bail out
            return null;
        }
        // Check when mv partition relates base table partition data change or delete partition,
        // the mv partition would be invalid.
        // Partitions rewritten plan used but not in mv valid partition name set,
        // need to be removed in mv and union base table
        Set<String> mvNeedRemovePartitionNameSet = new HashSet<>();
        Sets.difference(rewrittenPlanUsePartitionNameSet, mvValidPartitionNameSet)
                .copyInto(mvNeedRemovePartitionNameSet);
        for (String partitionName : mvNeedRemovePartitionNameSet) {
            Set<String> baseTablePartitions = partitionMapping.get(partitionName);
            if (baseTablePartitions == null) {
                // Base table partition maybe deleted, need not union
                continue;
            }
            if (!Sets.intersection(baseTablePartitions, queryUsedBaseTablePartitionNameSet).isEmpty()) {
                // An MV partition is the atomic unit removed from the rewritten plan. If any base
                // partition in its roll-up bucket is used by the query, compensate the whole bucket.
                baseTableNeedUnionPartitionNameSet.addAll(baseTablePartitions);
            }
        }
        // If related base table creates partitions or mv is created with ttl, need base table union
        Sets.difference(queryUsedBaseTablePartitionNameSet, mvValidBaseTablePartitionNameSet)
                .copyInto(baseTableNeedUnionPartitionNameSet);
        // Construct result map
        Pair<BaseTableInfo, Set<String>> mvPartitionNeedRemoveNameMap = Pair.of(
                new BaseTableInfo(mtmv), ImmutableSet.of());
        if (!mvNeedRemovePartitionNameSet.isEmpty()) {
            mvPartitionNeedRemoveNameMap = Pair.of(new BaseTableInfo(mtmv), mvNeedRemovePartitionNameSet);
        }
        Pair<BaseColInfo, Set<String>> baseTablePartitionNeedUnionNameMap = Pair.of(
                relatedPartitionTable, ImmutableSet.of());
        if (!baseTableNeedUnionPartitionNameSet.isEmpty()) {
            baseTablePartitionNeedUnionNameMap = Pair.of(relatedPartitionTable, baseTableNeedUnionPartitionNameSet);
        }
        return Pair.of(mvPartitionNeedRemoveNameMap, baseTablePartitionNeedUnionNameMap);
    }

    /**
     * Check if need union compensate or not
     * If query base table all partitions with ALL_PARTITIONS or ALL_PARTITIONS_LIST, should not do union compensate
     * because it means query all partitions from base table and prune partition failed
     */
    public static boolean needUnionRewrite(MaterializationContext materializationContext,
                                           StatementContext statementContext) throws AnalysisException {
        if (!(materializationContext instanceof AsyncMaterializationContext)) {
            return false;
        }
        MTMV mtmv = ((AsyncMaterializationContext) materializationContext).getMtmv();
        PartitionType type = mtmv.getPartitionInfo().getType();
        MTMVPartitionInfo mvPartitionInfo = mtmv.getMvPartitionInfo();
        List<BaseColInfo> pctInfos = mvPartitionInfo.getPctInfos();
        Set<MTMVRelatedTableIf> pctTables = mvPartitionInfo.getPctTables();
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap =
                statementContext.getTableUsedPartitionNameMap();
        for (MTMVRelatedTableIf pctTable : pctTables) {
            if (pctTable instanceof ExternalTable && !((ExternalTable) pctTable).supportInternalPartitionPruned()) {
                // if pct table is external table and not support internal partition pruned,
                // we consider query all partitions from pct table, this would cause loop union compensate,
                // so we skip union compensate in this case
                return false;
            }
            Collection<Pair<RelationId, Set<String>>> tableUsedPartitions
                    = tableUsedPartitionNameMap.get(pctTable.getFullQualifiers());
            if (ALL_PARTITIONS_LIST.equals(tableUsedPartitions)
                    || tableUsedPartitions.stream().anyMatch(ALL_PARTITIONS::equals)) {
                // If query base table all partitions with ALL_PARTITIONS or ALL_PARTITIONS_LIST,
                // should not do union compensate, because it means query all partitions from base table
                // and prune partition failed
                return false;
            }
        }
        return !PartitionType.UNPARTITIONED.equals(type) && !pctInfos.isEmpty();
    }

    /**
     * Get query used partitions
     * this is calculated from tableUsedPartitionNameMap and tables in statementContext
     *
     * @param currentUsedRelationIdSet if union compensate occurs, the new query used partitions is changed,
     *         so need to get used partitions by relation id set
     */
    public static Map<List<String>, Set<String>> getQueryUsedPartitions(StatementContext statementContext,
            BitSet currentUsedRelationIdSet) {
        // get table used partitions
        // if table is not in statementContext().getTables() which means the table is partition prune as empty relation
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = statementContext
                .getTableUsedPartitionNameMap();
        // if value is empty, means query no partitions
        // if value is null, means query all partitions
        // if value is not empty, means query some partitions
        Map<List<String>, Set<String>> queryUsedRelatedTablePartitionsMap = new HashMap<>();
        tableLoop:
        for (List<String> queryUsedTable : tableUsedPartitionNameMap.keySet()) {
            Set<String> usedPartitionSet = new HashSet<>();
            Collection<Pair<RelationId, Set<String>>> tableUsedPartitions =
                    tableUsedPartitionNameMap.get(queryUsedTable);
            if (ALL_PARTITIONS_LIST.equals(tableUsedPartitions)) {
                // It means all partitions are used when query
                queryUsedRelatedTablePartitionsMap.put(queryUsedTable, null);
                continue;
            }
            for (Pair<RelationId, Set<String>> tableUsedPartitionPair : tableUsedPartitions) {
                if (ALL_PARTITIONS.equals(tableUsedPartitionPair)) {
                    // It means all partitions are used when query
                    queryUsedRelatedTablePartitionsMap.put(queryUsedTable, null);
                    continue tableLoop;
                }
                // If currentUsedRelationIdSet is not empty, need check relation id to get concrete used partitions
                BitSet usedPartitionRelation = new BitSet();
                usedPartitionRelation.set(tableUsedPartitionPair.key().asInt());
                if (!currentUsedRelationIdSet.isEmpty()
                        && !currentUsedRelationIdSet.intersects(usedPartitionRelation)) {
                    continue;
                }
                usedPartitionSet.addAll(tableUsedPartitionPair.value());
            }
            queryUsedRelatedTablePartitionsMap.put(queryUsedTable, usedPartitionSet);
        }
        return queryUsedRelatedTablePartitionsMap;
    }
}
