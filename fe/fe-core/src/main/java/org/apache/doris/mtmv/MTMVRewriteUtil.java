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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class MTMVRewriteUtil {
    private static final Logger LOG = LogManager.getLogger(MTMVRewriteUtil.class);

    /**
     * Determine which partition of mtmv can be rewritten
     *
     * @param mtmv
     * @param ctx
     * @return
     */
    public static Collection<Partition> getMTMVCanRewritePartitions(MTMV mtmv, ConnectContext ctx,
            long currentTimeMills, boolean forceConsistent,
            Map<List<String>, Set<String>> queryUsedPartitions) {
        List<Partition> res = Lists.newArrayList();
        Collection<Partition> allPartitions = mtmv.getPartitions();
        MTMVRelation mtmvRelation = mtmv.getRelation();
        if (mtmvRelation == null) {
            return res;
        }
        // check mv is normal
        if (!mtmv.canBeCandidate()) {
            return res;
        }
        Set<String> mtmvNeedComparePartitions = null;
        MTMVRefreshContext refreshContext = null;
        // check gracePeriod
        long gracePeriodMills = mtmv.getGracePeriod();
        for (Partition partition : allPartitions) {
            if (gracePeriodMills > 0 && currentTimeMills <= (partition.getVisibleVersionTime()
                    + gracePeriodMills) && !forceConsistent) {
                res.add(partition);
                continue;
            }
            if (refreshContext == null) {
                try {
                    refreshContext = MTMVRefreshContext.buildContext(mtmv);
                } catch (AnalysisException e) {
                    LOG.warn("buildContext failed", e);
                    // After failure, one should quickly return to avoid repeated failures
                    return res;
                }
            }
            if (mtmvNeedComparePartitions == null) {
                try {
                    mtmvNeedComparePartitions = getMtmvPartitionsByRelatedPartitions(mtmv, refreshContext,
                            queryUsedPartitions);
                } catch (AnalysisException e) {
                    LOG.warn(e);
                    return res;
                }
            }
            // if the partition which query not used, should not compare partition version
            if (!mtmvNeedComparePartitions.contains(partition.getName())) {
                continue;
            }
            try {
                if (MTMVPartitionUtil.isMTMVPartitionSync(refreshContext, partition.getName(),
                        mtmvRelation.getBaseTablesOneLevelAndFromView(),
                        forceConsistent ? ImmutableSet.of() : mtmv.getQueryRewriteConsistencyRelaxedTables())) {
                    res.add(partition);
                }
            } catch (AnalysisException e) {
                // ignore it
                LOG.warn("check isMTMVPartitionSync failed", e);
            }
        }
        return res;
    }

    /**
     * Get mtmv partitions by related table partitions, if relatedPartitions is null, return all mtmv partitions
     * if mtmv is self-manage partition, return all mtmv partitions,
     * if mtmv is nested mv, return all mtmv partitions,
     * else return mtmv partitions by relatedPartitions
     */
    private static Set<String> getMtmvPartitionsByRelatedPartitions(MTMV mtmv, MTMVRefreshContext refreshContext,
            Map<List<String>, Set<String>> queryUsedPartitions) throws AnalysisException {
        if (mtmv.getMvPartitionInfo().getPartitionType().equals(MTMVPartitionType.SELF_MANAGE)) {
            return mtmv.getPartitionNames();
        }
        // if relatedPartitions is null, which means QueryPartitionCollector visitLogicalCatalogRelation can not
        // get query used partitions, should get all mtmv partitions
        if (queryUsedPartitions == null) {
            return mtmv.getPartitionNames();
        }
        // if nested mv, should return directly
        Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
        Set<List<String>> pctTableQualifiers = pctTables.stream().map(MTMVRelatedTableIf::getFullQualifiers).collect(
                Collectors.toSet());
        if (Sets.intersection(pctTableQualifiers, queryUsedPartitions.keySet()).isEmpty()) {
            return mtmv.getPartitionNames();
        }
        Set<String> res = Sets.newHashSet();

        Map<Pair<MTMVRelatedTableIf, String>, String> relatedToMv = getPctToMv(
                refreshContext.getPartitionMappings());
        for (Entry<List<String>, Set<String>> entry : queryUsedPartitions.entrySet()) {
            TableIf tableIf = MTMVUtil.getTable(entry.getKey());
            if (!pctTables.contains(tableIf)) {
                continue;
            }
            if (entry.getValue() == null) {
                return mtmv.getPartitionNames();
            }
            Set<String> pctPartitions = entry.getValue();
            for (String pctPartition : pctPartitions) {
                String mvPartition = relatedToMv.get(Pair.of(tableIf, pctPartition));
                if (mvPartition != null) {
                    res.add(mvPartition);
                }
            }
        }
        return res;
    }

    @VisibleForTesting
    public static Map<Pair<MTMVRelatedTableIf, String>, String> getPctToMv(
            Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings) {
        Map<Pair<MTMVRelatedTableIf, String>, String> res = Maps.newHashMap();
        for (Entry<String, Map<MTMVRelatedTableIf, Set<String>>> entry : partitionMappings.entrySet()) {
            String mvPartitionName = entry.getKey();
            for (Entry<MTMVRelatedTableIf, Set<String>> entry2 : entry.getValue().entrySet()) {
                MTMVRelatedTableIf pctTable = entry2.getKey();
                for (String pctPartitionName : entry2.getValue()) {
                    res.put(Pair.of(pctTable, pctPartitionName), mvPartitionName);
                }
            }
        }
        return res;
    }
}
