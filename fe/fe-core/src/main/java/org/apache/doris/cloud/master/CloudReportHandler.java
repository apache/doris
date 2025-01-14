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

package org.apache.doris.cloud.master;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.DropReplicaTask;
import org.apache.doris.thrift.TTablet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CloudReportHandler extends ReportHandler {
    private static final Logger LOG = LogManager.getLogger(CloudReportHandler.class);

    @Override
    public void tabletReport(long backendId, Map<Long, TTablet> backendTablets,
                             Map<Long, Long> backendPartitionsVersion, long backendReportVersion, long numTablets) {
        long start = System.currentTimeMillis();
        LOG.info("backend[{}] have {} tablet(s), {} need deal tablet(s). report version: {}",
                backendId, numTablets, backendTablets.size(), backendReportVersion);
        // current be useful
        Set<Long> tabletIdsInFe = ((CloudEnv) Env.getCurrentEnv()).getCloudTabletRebalancer()
                .getSnapshotTabletsInPrimaryAndSecondaryByBeId(backendId);

        Set<Long> tabletIdsInBe = backendTablets.keySet();
        // handle (be - meta)
        Set<Long> tabletIdsNeedDrop = diffTablets(tabletIdsInFe, tabletIdsInBe);
        // drop agent task
        deleteFromBackend(backendId, tabletIdsNeedDrop);

        Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
        LOG.info("finished to handle task report from backend {}-{}, "
                + "diff task num: {}, cost: {} ms.",
                backendId, be != null ? be.getHost() : "",
                tabletIdsNeedDrop.size(),
                (System.currentTimeMillis() - start));
    }

    // tabletIdsInFe, tablet is used in Primary or Secondary
    // tabletIdsInBe, tablet report exceed time, need to check
    // returns tabletIds need to drop
    private Set<Long> diffTablets(Set<Long> tabletIdsInFe, Set<Long> tabletIdsInBe) {
        // tabletsInBe - tabletsInFe
        Set<Long> result = new HashSet<>(tabletIdsInBe);
        result.removeAll(tabletIdsInFe);
        return result;
    }

    private static void deleteFromBackend(long backendId, Set<Long> tabletIdsWillDrop) {
        int deleteFromBackendCounter = 0;
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Long tabletId : tabletIdsWillDrop) {
            DropReplicaTask task = new DropReplicaTask(backendId, tabletId, -1, -1, false);
            batchTask.addTask(task);
            LOG.info("delete tablet[{}] from backend[{}]", tabletId, backendId);
            ++deleteFromBackendCounter;
        }

        if (batchTask.getTaskNum() != 0) {
            AgentTaskExecutor.submit(batchTask);
        }

        LOG.info("delete {} tablet(s) from backend[{}]", deleteFromBackendCounter, backendId);
    }
}
