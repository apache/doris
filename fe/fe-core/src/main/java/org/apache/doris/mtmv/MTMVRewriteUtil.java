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
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
            long currentTimeMills, boolean forceConsistent) {
        List<Partition> res = Lists.newArrayList();
        Collection<Partition> allPartitions = mtmv.getPartitions();
        MTMVRelation mtmvRelation = mtmv.getRelation();
        if (mtmvRelation == null) {
            return res;
        }
        // check mv is normal
        if (mtmv.getStatus().getState() != MTMVState.NORMAL
                || mtmv.getStatus().getRefreshState() == MTMVRefreshState.INIT) {
            return res;
        }
        Map<String, Set<String>> partitionMappings = null;
        // check gracePeriod
        long gracePeriodMills = mtmv.getGracePeriod();
        for (Partition partition : allPartitions) {
            if (gracePeriodMills > 0 && currentTimeMills <= (partition.getVisibleVersionTime()
                    + gracePeriodMills) && !forceConsistent) {
                res.add(partition);
                continue;
            }
            try {
                if (partitionMappings == null) {
                    partitionMappings = mtmv.calculatePartitionMappings();
                }
                if (MTMVPartitionUtil.isMTMVPartitionSync(mtmv, partition.getName(),
                        partitionMappings.get(partition.getName()), mtmvRelation.getBaseTables(),
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
}
