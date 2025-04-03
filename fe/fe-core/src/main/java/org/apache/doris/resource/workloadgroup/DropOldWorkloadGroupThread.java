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
import org.apache.doris.common.FeConstants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class DropOldWorkloadGroupThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(DropOldWorkloadGroupThread.class);

    public DropOldWorkloadGroupThread() {
        super("DropOldWorkloadGroupThread");
    }

    public void run() {
        if (!FeConstants.dropOldWorkloadGroup) {
            return;
        }
        try {
            List<WorkloadGroup> oldWgList = Env.getCurrentEnv().getWorkloadGroupMgr().getOldWorkloadGroup();
            if (oldWgList.isEmpty()) {
                LOG.info("There is no old workload group, just return.");
                return;
            }
            Set<String> cgSet = Env.getCurrentEnv().getComputeGroupMgr().getComputeGroupIdentifiers();
            for (WorkloadGroup wg : oldWgList) {
                Env.getCurrentEnv().getWorkloadGroupMgr().dropOldWorkloadGroup(cgSet, wg);
            }
        } catch (Throwable t) {
            LOG.info("error happens when drop old workload group, ", t);
        }
    }
}
