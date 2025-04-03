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
import org.apache.doris.resource.computegroup.ComputeGroup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class BindWgToComputeGroupThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(BindWgToComputeGroupThread.class);

    public BindWgToComputeGroupThread() {
        super("BindWgToComputeGroupThread");
    }

    public void run() {
        if (!FeConstants.bindWgToComputeGroup) {
            return;
        }
        try {
            boolean isReady = false;
            while (!isReady) {
                FrontendNodeType feType = Env.getCurrentEnv().getFeType();
                isReady = feType.equals(FrontendNodeType.INIT) || feType.equals(FrontendNodeType.UNKNOWN);
                if (isReady) {
                    LOG.info("[bing_wg_to_cg]FE is ready");
                    break;
                } else {
                    LOG.info("[bing_wg_to_cg]FE is not ready, just wait.");
                    Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
                }
            }
            Thread.currentThread()
                    .join(Config.resource_not_ready_sleep_seconds * 1000L);

            List<WorkloadGroup> oldWgList = Env.getCurrentEnv().getWorkloadGroupMgr().getOldWorkloadGroup();
            if (oldWgList.isEmpty()) {
                LOG.info("[bing_wg_to_cg]There is no old workload group, just return.");
                return;
            }

            ComputeGroup allComputeGroup = Env.getCurrentEnv().getComputeGroupMgr().getAllBackendComputeGroup();
            Set<String> cgIdents = allComputeGroup.getIdentifiers();
            while (cgIdents.size() == 0) {
                LOG.info("[bing_wg_to_cg]Not get any backends, sleep");
                Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
                cgIdents = allComputeGroup.getIdentifiers();
            }
            LOG.info("[bing_wg_to_cg]Get cgs from backend, {}", String.join(",", cgIdents));
            for (WorkloadGroup wg : oldWgList) {
                Env.getCurrentEnv().getWorkloadGroupMgr()
                        .bindWorkloadGroupToComputeGroup(allComputeGroup.getIdentifiers(), wg);
            }
            LOG.info("[bing_wg_to_cg]Finish bing workload group to compute group, wg size: {}, cg size: {}",
                    oldWgList.size(), allComputeGroup.getIdentifiers());
        } catch (Throwable t) {
            LOG.info("[bing_wg_to_cg]Error happens when drop old workload group, ", t);
        }
    }
}
