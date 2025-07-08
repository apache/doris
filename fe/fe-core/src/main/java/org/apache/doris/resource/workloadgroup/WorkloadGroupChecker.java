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
import org.apache.doris.common.util.MasterDaemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkloadGroupChecker extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(WorkloadGroupChecker.class);

    public WorkloadGroupChecker() {
        super("workloadgroup-checker-thread",
                FeConstants.disableWGCheckerForUT ? 3600000 : Config.workload_group_check_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            WorkloadGroupMgr wgMgr = Env.getCurrentEnv().getWorkloadGroupMgr();
            // 1 create new wg for old wg in compute group
            // note this convert logic can be removed in next release
            wgMgr.convertOldWgToNewWg();

            // 2 check workload group
            wgMgr.checkWorkloadGroup();
        } catch (Throwable t) {
            LOG.info("[init_wg]Error happens when check workload group, ", t);
        }
    }

}
