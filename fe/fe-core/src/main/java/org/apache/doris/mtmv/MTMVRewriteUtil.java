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
import org.apache.doris.mtmv.MTMVRefreshContext.PreparedPartitionSnapshots;
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
        // A base table whose list partitions have a default one cannot be represented completely by this MV: a
        // key only that partition holds has no MV partition to be read into, so the MV does not hold its rows
        // and a query that may read them cannot be answered from it (measured: `LIST(k)` with p1=(1) and
        // p_default, a committed k=2 row, an MV partitioned by k -- the MV holds key 1 alone, and a query over
        // the base table was rewritten to it and lost the k=2 row). Which keys those are is not in the
        // metadata, so this is the whole MV rather than the partitions that read that table.
        try {
            for (MTMVRelatedTableIf pctTable : mtmv.getMvPartitionInfo().getPctTables()) {
                if (MTMVPartitionUtil.hasDefaultListPartition(pctTable)) {
                    return res;
                }
            }
        } catch (AnalysisException e) {
            // Cannot tell which keys the MV does not hold, so it is not offered as an answer.
            LOG.warn("can not read the partition info of {}, no partition is offered", mtmv.getName(), e);
            return res;
        }
        Set<String> mtmvNeedComparePartitions = null;
        MTMVRefreshContext refreshContext = null;
        PreparedPartitionSnapshots partitionSnapshots = null;
        // check gracePeriod
        long gracePeriodMills = mtmv.getGracePeriod();
        for (Partition partition : allPartitions) {
            // Which MV partitions the query's base partitions are mapped from is a fact about the query, not
            // about this MV partition, and a base partition the query reads that no MV partition is mapped
            // from is one whose rows the MV does not hold. So it is answered before the grace period below,
            // which reports a partition usable without comparing it: taken afterwards, a partition inside its
            // grace period would answer the query from the MV alone and drop that partition's rows. With the
            // union rewrite on the answer is the MV plus the base table, so an uncovered partition costs the
            // rewrite for nothing: it is passed over there and the compensation reads it.
            if (refreshContext == null) {
                try {
                    refreshContext = MTMVRefreshContext.buildContext(mtmv,
                            queryUsedPartitions != null ? queryUsedPartitions : Maps.newHashMap());
                } catch (AnalysisException e) {
                    LOG.warn("buildContext failed", e);
                    // After failure, one should quickly return to avoid repeated failures
                    return res;
                }
            }
            if (mtmvNeedComparePartitions == null) {
                try {
                    mtmvNeedComparePartitions = getMtmvPartitionsByRelatedPartitions(mtmv, refreshContext,
                            queryUsedPartitions,
                            ctx.getSessionVariable().isEnableMaterializedViewUnionRewrite());
                } catch (AnalysisException e) {
                    LOG.warn(e);
                    return res;
                }
            }
            if (mtmvNeedComparePartitions.isEmpty()) {
                // A base partition the query reads is mapped from no MV partition of this one -- a partition
                // the mapping left out, an expired one for instance -- so no MV partition holds its rows.
                // None of them may answer this query, whatever the state of the partitions in grace below:
                // the answer is the base table.
                return Lists.newArrayList();
            }
            // A partition that alignment has just added is as fresh as it is empty: a base ADD PARTITION can
            // widen the keys an MV partition covers, which drops the populated one and adds an empty
            // replacement, and its creation time would let the grace period admit it before any refresh has
            // read it -- a query rewritten to it then reads no rows at all, including the ones the base
            // partitions it is to hold carry. Grace answers for a partition a refresh has written, which is
            // what its version says: the version of a written partition, even one a refresh left empty, is not
            // the one it started at, while a replacement keeps that one and its creation time. Neither the
            // refresh snapshot nor `hasData` can be asked -- the snapshot is keyed by the partition's name, and
            // distinct keys can generate the same name, while on cloud `hasData` answers true for a session
            // that turned empty-partition pruning off, whatever the partition's version is.
            if (gracePeriodMills > 0 && currentTimeMills <= (partition.getVisibleVersionTime()
                    + gracePeriodMills) && !forceConsistent
                    && partition.getVisibleVersion() > Partition.PARTITION_INIT_VERSION) {
                res.add(partition);
                continue;
            }
            // if the partition which query not used, should not compare partition version
            if (!mtmvNeedComparePartitions.contains(partition.getName())) {
                continue;
            }
            if (partitionSnapshots == null) {
                Set<String> partitionsToPreload = Sets.newHashSet();
                for (Partition candidate : allPartitions) {
                    boolean withinGracePeriod = gracePeriodMills > 0
                            && currentTimeMills <= candidate.getVisibleVersionTime() + gracePeriodMills
                            && !forceConsistent;
                    if (!withinGracePeriod && mtmvNeedComparePartitions.contains(candidate.getName())) {
                        partitionsToPreload.add(candidate.getName());
                    }
                }
                try {
                    partitionSnapshots = refreshContext.prepareComparablePartitionSnapshots(partitionsToPreload);
                } catch (AnalysisException e) {
                    LOG.warn("preload partition snapshots failed", e);
                    return res;
                }
            }
            try {
                if (MTMVPartitionUtil.isMTMVPartitionSync(refreshContext, partitionSnapshots, partition.getName(),
                        mtmvRelation.getBaseTablesOneLevelAndFromView(),
                        forceConsistent ? ImmutableSet.of() : mtmv.getQueryRewriteConsistencyRelaxedTables())) {
                    res.add(partition);
                }
            } catch (AnalysisException e) {
                // ignore it
                LOG.warn("check isMTMVPartitionSync failed", e);
            }
        }
        // The union rewrite takes the partitions that are not valid out of the MV plan and compensates them
        // from the base table, so a partially valid answer is a complete one. Without it the MV alone answers
        // the query, and it has to be the whole of what the query reads: a partition that is not valid would
        // otherwise be read from the MV as it is, stale rows and all. Read as a subset rather than as a size,
        // since a partition within its grace period is answered usable without having been compared and is not
        // necessarily one the query reads.
        if (mtmvNeedComparePartitions != null
                && !ctx.getSessionVariable().isEnableMaterializedViewUnionRewrite()) {
            Set<String> usable = Sets.newHashSet();
            for (Partition partition : res) {
                usable.add(partition.getName());
            }
            if (!usable.containsAll(mtmvNeedComparePartitions)) {
                return Lists.newArrayList();
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
            Map<List<String>, Set<String>> queryUsedPartitions, boolean unionRewrite) throws AnalysisException {
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

        Map<Pair<MTMVRelatedTableIf, String>, Set<String>> relatedToMv = getPctToMv(
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
                Set<String> mvPartitions = relatedToMv.get(Pair.of(tableIf, pctPartition));
                if (mvPartitions == null) {
                    // A partition the query reads that no MV partition is mapped from -- one the partition
                    // mapping left out, an expired base partition for instance -- is one the MV's rows say
                    // nothing about. With the union rewrite on it is taken out of the MV plan and read from
                    // the base table, so the MV partitions the other base partitions map to still answer the
                    // query and this one is passed over. Without it the MV alone would answer, so no partition
                    // is answered for at all: the MV is not a candidate for this query and the query is
                    // answered from the base table.
                    if (unionRewrite) {
                        continue;
                    }
                    return Sets.newHashSet();
                }
                res.addAll(mvPartitions);
            }
        }
        return res;
    }

    /**
     * The MV partitions each base partition is read by, for the tables a partition mapping describes.
     *
     * <p>A set rather than one name, because one base partition can be read by more than one MV partition: a
     * list partitioned table's default partition holds the rows no other partition of it claims, so it is
     * read by every MV partition whose key range their own key falls in, and a map that answered with one of
     * them would let a rewrite stand on the MV partition that happens to be left while the rows are in
     * another one.
     */
    @VisibleForTesting
    public static Map<Pair<MTMVRelatedTableIf, String>, Set<String>> getPctToMv(
            Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings) {
        Map<Pair<MTMVRelatedTableIf, String>, Set<String>> res = Maps.newHashMap();
        for (Entry<String, Map<MTMVRelatedTableIf, Set<String>>> entry : partitionMappings.entrySet()) {
            String mvPartitionName = entry.getKey();
            for (Entry<MTMVRelatedTableIf, Set<String>> entry2 : entry.getValue().entrySet()) {
                MTMVRelatedTableIf pctTable = entry2.getKey();
                for (String pctPartitionName : entry2.getValue()) {
                    res.computeIfAbsent(Pair.of(pctTable, pctPartitionName), k -> Sets.newHashSet())
                            .add(mvPartitionName);
                }
            }
        }
        return res;
    }
}
