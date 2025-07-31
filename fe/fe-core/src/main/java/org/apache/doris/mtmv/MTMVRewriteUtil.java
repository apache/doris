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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.qe.ConnectContext;

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
            Set<String> relatedPartitions) {
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
        // if relatedPartitions is empty but not null, which means query no partitions
        if (relatedPartitions != null && relatedPartitions.size() == 0) {
            return res;
        }
        Set<String> mtmvNeedComparePartitions = null;
        MTMVRefreshContext refreshContext = null;
        // check gracePeriod
        long gracePeriodMills = mtmv.getGracePeriod();
        if (refreshContext == null) {
            try {
                refreshContext = MTMVRefreshContext.buildContext(mtmv);
            } catch (AnalysisException e) {
                LOG.warn("buildContext failed", e);
                // After failure, one should quickly return to avoid repeated failures
                return res;
            }
        }
        if (mtmv.getIncrementalRefresh()) {
            try {
                MTMVSnapshotIf tableSnapshotInfo = MaterializedViewUtils.getIncrementalMVTableSnapshotInfo(mtmv);
                MTMVSnapshotIf mvSnapshotInfo = MaterializedViewUtils.getIncrementalMVSnapshotInfo(mtmv);
                long tableSnapshotTimestamp = 0L;
                long mvSnapshotTimestamp = 0L;
                if (tableSnapshotInfo instanceof MTMVSnapshotIdSnapshot
                        && mvSnapshotInfo instanceof MTMVSnapshotIdSnapshot) {
                    tableSnapshotTimestamp = ((MTMVSnapshotIdSnapshot) tableSnapshotInfo).getTimestamp();
                    mvSnapshotTimestamp = ((MTMVSnapshotIdSnapshot) mvSnapshotInfo).getTimestamp();
                }
                if (mvSnapshotTimestamp > 0 && (tableSnapshotTimestamp == mvSnapshotTimestamp
                        || (gracePeriodMills > 0 && tableSnapshotTimestamp > mvSnapshotTimestamp
                        && tableSnapshotTimestamp <= (mvSnapshotTimestamp + gracePeriodMills) && !forceConsistent))) {
                    for (Partition partition : allPartitions) {
                        if (mtmvNeedComparePartitions == null) {
                            mtmvNeedComparePartitions = getMtmvPartitionsByRelatedPartitions(mtmv, refreshContext,
                                relatedPartitions);
                        }
                        // if the partition which query not used, should not compare partition version
                        if (!mtmvNeedComparePartitions.contains(partition.getName())) {
                            continue;
                        }
                        res.add(partition);
                    }
                }
            } catch (AnalysisException e) {
                // ignore it
                LOG.warn("check isMTMVPartitionSync failed", e);
            }
            return res;
        }
        for (Partition partition : allPartitions) {
            if (gracePeriodMills > 0 && currentTimeMills <= (partition.getVisibleVersionTime()
                    + gracePeriodMills) && !forceConsistent) {
                res.add(partition);
                continue;
            }
            if (mtmvNeedComparePartitions == null) {
                mtmvNeedComparePartitions = getMtmvPartitionsByRelatedPartitions(mtmv, refreshContext,
                        relatedPartitions);
            }
            // if the partition which query not used, should not compare partition version
            if (!mtmvNeedComparePartitions.contains(partition.getName())) {
                continue;
            }
            try {
                if (MTMVPartitionUtil.isMTMVPartitionSync(refreshContext, partition.getName(),
                        mtmvRelation.getBaseTablesOneLevel(),
                        Sets.newHashSet())) {
                    res.add(partition);
                }
            } catch (AnalysisException e) {
                // ignore it
                LOG.warn("check isMTMVPartitionSync failed", e);
            }
        }
        return res;
    }

    private static Set<String> getMtmvPartitionsByRelatedPartitions(MTMV mtmv, MTMVRefreshContext refreshContext,
            Set<String> relatedPartitions) {
        // if relatedPartitions is null, which means QueryPartitionCollector visitLogicalCatalogRelation can not
        // get query used partitions, should get all mtmv partitions
        if (relatedPartitions == null) {
            return mtmv.getPartitionNames();
        }
        Set<String> res = Sets.newHashSet();
        Map<String, String> relatedToMv = getRelatedToMv(refreshContext.getPartitionMappings());
        for (String relatedPartition : relatedPartitions) {
            res.add(relatedToMv.get(relatedPartition));
        }
        return res;
    }

    private static Map<String, String> getRelatedToMv(Map<String, Set<String>> mvToRelated) {
        Map<String, String> res = Maps.newHashMap();
        for (Entry<String, Set<String>> entry : mvToRelated.entrySet()) {
            for (String relatedPartition : entry.getValue()) {
                res.put(relatedPartition, entry.getKey());
            }
        }
        return res;
    }
}
