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

public class CreateInternalWorkloadGroupThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(CreateInternalWorkloadGroupThread.class);

    public CreateInternalWorkloadGroupThread() {
        super("CreateInternalWorkloadGroupThread");
    }

    public void run() {
        if (!FeConstants.shouldCreateInternalWorkloadGroup) {
            return;
        }
        try {
            Env env = Env.getCurrentEnv();
            while (!env.isReady()) {
                Thread.sleep(5000);
            }
            if (!env.getWorkloadGroupMgr()
                    .isWorkloadGroupExists(WorkloadGroupMgr.INTERNAL_GROUP_NAME)) {
                env.getWorkloadGroupMgr().createInternalWorkloadGroup();
                LOG.info("create internal workload group succ");
            } else {
                LOG.info("internal workload group already exists.");
            }
        } catch (Throwable t) {
            LOG.warn("create internal workload group failed. ", t);
        }
    }

}
