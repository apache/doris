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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    public static final Pair<RelationId, Set<String>> ALL_PARTITIONS = Pair.of(null, null);

    /**
     * Get table used partitions by the table full qualifiers
     * */
    public static Set<String> getQueryTableUsedPartition(
            List<String> targetTableFullQualifiers,
            StructInfo queryStructInfo,
            CascadesContext cascadesContext) {
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap
                = cascadesContext.getStatementContext().getTableUsedPartitionNameMap();
        Collection<Pair<RelationId, Set<String>>> tableUsedPartitions =
                tableUsedPartitionNameMap.get(targetTableFullQualifiers);
        Set<RelationId> queryUsedRelationSet = queryStructInfo.getRelationIdStructInfoNodeMap().keySet();
        Set<String> queryUsedPartitionSet = new HashSet<>();
        for (Pair<RelationId, Set<String>> relationPartition : tableUsedPartitions) {
            if (queryUsedRelationSet.contains(relationPartition.key())) {
                queryUsedPartitionSet.addAll(relationPartition.value());
            }
        }
        return queryUsedPartitionSet;
    }

    /**
     * Maybe only some partitions is invalid in materialized view, or base table maybe add, modify, delete partition
     * So we should calc the invalid partition used in query
     * @param queryUsedBaseTablePartitionNameSet partitions used by query related partition table
     * @param rewrittenPlan tmp rewrittenPlan when mv rewrite
     * @param materializationContext the context of materialization,which hold materialized view meta and other info
     * @param cascadesContext the context of cascades
     * @return the key in pair is mvNeedRemovePartitionNameSet, the value in pair is baseTableNeedUnionPartitionNameSet
     */
    public static Pair<Map<BaseTableInfo, Set<String>>, Map<BaseTableInfo, Set<String>>> calcInvalidPartitions(
            Set<String> queryUsedBaseTablePartitionNameSet, Plan rewrittenPlan,
            AsyncMaterializationContext materializationContext, CascadesContext cascadesContext)
            throws AnalysisException {
        Set<String> mvNeedRemovePartitionNameSet = new HashSet<>();
        Set<String> baseTableNeedUnionPartitionNameSet = new HashSet<>();
        // check partition is valid or not
        MTMV mtmv = materializationContext.getMtmv();
        PartitionInfo mvPartitionInfo = mtmv.getPartitionInfo();
        if (PartitionType.UNPARTITIONED.equals(mvPartitionInfo.getType())) {
            // if not partition, if rewrite success, it means mv is available
            return Pair.of(ImmutableMap.of(), ImmutableMap.of());
        }
        MTMVPartitionInfo mvCustomPartitionInfo = mtmv.getMvPartitionInfo();
        BaseTableInfo relatedPartitionTable = mvCustomPartitionInfo.getRelatedTableInfo();
        if (relatedPartitionTable == null || queryUsedBaseTablePartitionNameSet.isEmpty()) {
            // if mv is not partitioned or query not query any partition, doesn't compensate
            return Pair.of(ImmutableMap.of(), ImmutableMap.of());
        }
        Collection<Partition> mvValidPartitions = cascadesContext.getStatementContext()
                .getMvCanRewritePartitionsMap().get(new BaseTableInfo(mtmv));
        Set<String> mvValidPartitionNameSet = new HashSet<>();
        Set<String> mvValidBaseTablePartitionNameSet = new HashSet<>();
        Set<String> mvValidHasDataRelatedBaseTableNameSet = new HashSet<>();
        Pair<Map<String, Set<String>>, Map<String, String>> partitionMapping = mtmv.calculateDoublyPartitionMappings();
        for (Partition mvValidPartition : mvValidPartitions) {
            mvValidPartitionNameSet.add(mvValidPartition.getName());
            Set<String> relatedBaseTablePartitions = partitionMapping.key().get(mvValidPartition.getName());
            if (relatedBaseTablePartitions != null) {
                mvValidBaseTablePartitionNameSet.addAll(relatedBaseTablePartitions);
            }
            if (!mtmv.selectNonEmptyPartitionIds(ImmutableList.of(mvValidPartition.getId())).isEmpty()) {
                if (relatedBaseTablePartitions != null) {
                    mvValidHasDataRelatedBaseTableNameSet.addAll(relatedBaseTablePartitions);
                }
            }
        }
        if (Sets.intersection(mvValidHasDataRelatedBaseTableNameSet, queryUsedBaseTablePartitionNameSet).isEmpty()) {
            // if mv can not offer any partition for query, query rewrite bail out
            return null;
        }
        // Check when mv partition relates base table partition data change or delete partition
        Set<String> rewrittenPlanUsePartitionNameSet = new HashSet<>();
        List<Object> mvOlapScanList = rewrittenPlan.collectToList(node ->
                node instanceof LogicalOlapScan
                        && Objects.equals(((CatalogRelation) node).getTable().getName(), mtmv.getName()));
        for (Object olapScanObj : mvOlapScanList) {
            LogicalOlapScan olapScan = (LogicalOlapScan) olapScanObj;
            olapScan.getSelectedPartitionIds().forEach(id ->
                    rewrittenPlanUsePartitionNameSet.add(olapScan.getTable().getPartition(id).getName()));
        }
        // If rewritten plan use but not in mv valid partition name set, need remove in mv and base table union
        Sets.difference(rewrittenPlanUsePartitionNameSet, mvValidPartitionNameSet)
                .copyInto(mvNeedRemovePartitionNameSet);
        for (String partitionName : mvNeedRemovePartitionNameSet) {
            baseTableNeedUnionPartitionNameSet.addAll(partitionMapping.key().get(partitionName));
        }
        // If related base table create partitions or mv is created with ttl, need base table union
        Sets.difference(queryUsedBaseTablePartitionNameSet, mvValidBaseTablePartitionNameSet)
                .copyInto(baseTableNeedUnionPartitionNameSet);
        // Construct result map
        Map<BaseTableInfo, Set<String>> mvPartitionNeedRemoveNameMap = new HashMap<>();
        if (!mvNeedRemovePartitionNameSet.isEmpty()) {
            mvPartitionNeedRemoveNameMap.put(new BaseTableInfo(mtmv), mvNeedRemovePartitionNameSet);
        }
        Map<BaseTableInfo, Set<String>> baseTablePartitionNeedUnionNameMap = new HashMap<>();
        if (!baseTableNeedUnionPartitionNameSet.isEmpty()) {
            baseTablePartitionNeedUnionNameMap.put(relatedPartitionTable, baseTableNeedUnionPartitionNameSet);
        }
        return Pair.of(mvPartitionNeedRemoveNameMap, baseTablePartitionNeedUnionNameMap);
    }

    public static boolean needUnionRewrite(
            Pair<Map<BaseTableInfo, Set<String>>, Map<BaseTableInfo, Set<String>>> invalidPartitions,
            CascadesContext cascadesContext) {
        return invalidPartitions != null
                && (!invalidPartitions.key().isEmpty() || !invalidPartitions.value().isEmpty());
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
        BaseTableInfo relatedTableInfo = mtmv.getMvPartitionInfo().getRelatedTableInfo();
        return !PartitionType.UNPARTITIONED.equals(type) && relatedTableInfo != null;
    }

    public static boolean isAllPartition(Pair<RelationId, Set<String>> usedPartition) {
        return ALL_PARTITIONS.equals(usedPartition);
    }

    /**
     * Get query used partitions
     * this is calculated from tableUsedPartitionNameMap and tables in statementContext
     * */
    public static Map<List<String>, Set<String>> getQueryUsedPartitions(ConnectContext ctx) {
        // get table used partitions
        // if table is not in statementContext().getTables() which means the table is partition prune as empty relation
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = ctx.getStatementContext()
                .getTableUsedPartitionNameMap();
        // if value is empty, means query no partitions
        // if value is null, means query all partitions
        // if value is not empty, means query some partitions
        Map<List<String>, Set<String>> queryUsedRelatedTablePartitionsMap = new HashMap<>();
        for (Map.Entry<List<String>, TableIf> tableIfEntry : ctx.getStatementContext().getTables().entrySet()) {
            Set<String> usedPartitionSet = new HashSet<>();
            if (!tableUsedPartitionNameMap.get(tableIfEntry.getKey()).isEmpty()) {
                for (Pair<RelationId, Set<String>> partitionPair
                        : tableUsedPartitionNameMap.get(tableIfEntry.getKey())) {
                    if (PartitionCompensator.isAllPartition(partitionPair)) {
                        queryUsedRelatedTablePartitionsMap.put(tableIfEntry.getKey(), null);
                        break;
                    }
                    usedPartitionSet.addAll(partitionPair.value());
                }
            }
            queryUsedRelatedTablePartitionsMap.put(tableIfEntry.getKey(), usedPartitionSet);
        }
        return queryUsedRelatedTablePartitionsMap;
    }
}
