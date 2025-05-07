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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class BindWgToComputeGroupThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(BindWgToComputeGroupThread.class);

    public BindWgToComputeGroupThread() {
        super("BindWgToComputeGroupThread");
    }

    private void waitCatalogReady() throws InterruptedException {
        boolean isReady = false;
        while (!isReady) {
            FrontendNodeType feType = Env.getCurrentEnv().getFeType();
            isReady = feType.equals(FrontendNodeType.INIT) || feType.equals(FrontendNodeType.UNKNOWN);
            if (isReady) {
                LOG.info("[init_wg]FE is ready");
                break;
            } else {
                LOG.info("[init_wg]FE is not ready, just wait.");
                Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            }
        }
        Thread.currentThread().join(Config.resource_not_ready_sleep_seconds * 1000L);
    }

    public void run() {
        if (!FeConstants.bindWgToComputeGroup) {
            return;
        }
        try {
            waitCatalogReady();
            Env.getCurrentEnv().getWorkloadGroupMgr().tryCreateNormalWorkloadGroup();
            createNewComputeGroup();
        } catch (Throwable t) {
            LOG.info("[init_wg]Error happens when drop old workload group, ", t);
        }
    }

    private void createNewComputeGroup() throws InterruptedException {
        WorkloadGroupMgr wgMgr = Env.getCurrentEnv().getWorkloadGroupMgr();
        LOG.info("[init_wg] print current cg before, id map:{}, name map : {}",
                wgMgr.getIdToWorkloadGroup(),
                wgMgr.getNameToWorkloadGroup());
        List<WorkloadGroup> oldWgList = wgMgr.getOldWorkloadGroup();
        if (oldWgList.isEmpty()) {
            LOG.info("[init_wg]There is no old workload group, just return.");
            return;
        }

        ComputeGroupMgr cgMgr = Env.getCurrentEnv().getComputeGroupMgr();
        Set<String> idSet = cgMgr.getAllComputeGroupIds();
        while (idSet.size() == 0) {
            LOG.info("[init_wg]Not get any backends, sleep");
            Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            idSet = cgMgr.getAllComputeGroupIds();
        }
        LOG.info("[init_wg]Get cgs from backend, {}", String.join(",", idSet));
        for (WorkloadGroup wg : oldWgList) {
            wgMgr.bindWorkloadGroupToComputeGroup(idSet, wg);
        }
        LOG.info(
                "[init_wg]Finish bing workload group to compute group, wg size: {}, cg size: {}, "
                        + "id map:{}, name map :{}",
                oldWgList.size(), idSet,
                wgMgr.getIdToWorkloadGroup(),
                wgMgr.getNameToWorkloadGroup());
    }
}
