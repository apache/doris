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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
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
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * Maybe only some partitions is invalid in materialized view, or base table maybe add, modify, delete partition
     * So we should calc the invalid partition used in query
     * @param queryUsedBaseTablePartitionMap partitions used by query related partition table
     * @param rewrittenPlan tmp rewrittenPlan when mv rewrite
     * @param materializationContext the context of materialization,which hold materialized view meta and other info
     * @param cascadesContext the context of cascades
     * @return the key in pair is mvNeedRemovePartitionNameSet, the value in pair is baseTableNeedUnionPartitionNameSet
     */
    public static Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> calcInvalidPartitions(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap, Plan rewrittenPlan,
            AsyncMaterializationContext materializationContext, CascadesContext cascadesContext)
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
                = materializationContext.calculatePartitionMappings();
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
            // merge all partition to delete or union
            Set<String> needRemovePartitionSet = new HashSet<>();
            mvPartitionNeedRemoveNameMap.values().forEach(needRemovePartitionSet::addAll);
            mvPartitionNeedRemoveNameMap.replaceAll((k, v) -> needRemovePartitionSet);

            // consider multi base table partition name not same, how to handle it?
            Set<String> needUnionPartitionSet = new HashSet<>();
            baseTablePartitionNeedUnionNameMap.values().forEach(needUnionPartitionSet::addAll);
            baseTablePartitionNeedUnionNameMap.replaceAll((k, v) -> needUnionPartitionSet);
        }
        if (allCompensateIsNull) {
            return null;
        }
        return Pair.of(mvPartitionNeedRemoveNameMap, baseTablePartitionNeedUnionNameMap);
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
                if (!mtmv.selectNonEmptyPartitionIds(ImmutableList.of(mvValidPartition.getId())).isEmpty()) {
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
            baseTableNeedUnionPartitionNameSet.addAll(baseTablePartitions);
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

    public static boolean needUnionRewrite(
            Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> invalidPartitions,
            CascadesContext cascadesContext) {
        return invalidPartitions != null
                && (!invalidPartitions.key().values().isEmpty() || !invalidPartitions.value().values().isEmpty());
    }

    /**
     * Check if need union compensate or not
     */
    public static boolean needUnionRewrite(MaterializationContext materializationContext) {
        if (!(materializationContext instanceof AsyncMaterializationContext)) {
            return false;
        }
        MTMV mtmv = ((AsyncMaterializationContext) materializationContext).getMtmv();
        PartitionType type = mtmv.getPartitionInfo().getType();
        List<BaseColInfo> pctInfos = mtmv.getMvPartitionInfo().getPctInfos();
        return !PartitionType.UNPARTITIONED.equals(type) && !pctInfos.isEmpty();
    }

    /**
     * Get query used partitions
     * this is calculated from tableUsedPartitionNameMap and tables in statementContext
     *
     * @param customRelationIdSet if union compensate occurs, the new query used partitions is changed,
     *         so need to get used partitions by relation id set
     */
    public static Map<List<String>, Set<String>> getQueryUsedPartitions(StatementContext statementContext,
            BitSet customRelationIdSet) {
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
                usedPartitionSet.addAll(tableUsedPartitionPair.value());
            }
            queryUsedRelatedTablePartitionsMap.put(queryUsedTable, usedPartitionSet);
        }
        return queryUsedRelatedTablePartitionsMap;
    }
}
