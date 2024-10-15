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

package org.apache.doris.cloud.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CloudCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);

    public CloudCoordinator(ConnectContext context, Analyzer analyzer,
                            Planner planner, StatsErrorEstimator statsErrorEstimator) {
        super(context, analyzer, planner, statsErrorEstimator);
    }

    public CloudCoordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable, List<PlanFragment> fragments,
                       List<ScanNode> scanNodes, String timezone, boolean loadZeroTolerance,
                    boolean enbaleProfile) {
        super(jobId, queryId, descTable, fragments, scanNodes, timezone, loadZeroTolerance, enbaleProfile);
    }

    @Override
    protected void prepare() throws UserException {
        String cluster = null;
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            if (!Strings.isNullOrEmpty(context.getSessionVariable().getCloudCluster())) {
                cluster = context.getSessionVariable().getCloudCluster();
                try {
                    ((CloudEnv) Env.getCurrentEnv()).checkCloudClusterPriv(cluster);
                } catch (Exception e) {
                    LOG.warn("get cluster by session context exception", e);
                    throw new UserException("get cluster by session context exception", e);
                }
                LOG.debug("get cluster by session context cluster: {}", cluster);
            } else {
                cluster = context.getCloudCluster();
                LOG.debug("get cluster by context {}", cluster);
            }
        } else {
            LOG.warn("connect context is null in coordinator prepare");
            // may cant throw exception? maybe cant get context in some scenarios
            return;
        }

        if (Strings.isNullOrEmpty(cluster)) {
            LOG.warn("invalid clusterName: {}", cluster);
            throw new UserException("empty clusterName, please check cloud cluster privilege");
        }

        this.idToBackend = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudIdToBackend(cluster);

        super.prepare();

        if (idToBackend == null || idToBackend.isEmpty()) {
            LOG.warn("no available backends, idToBackend {}", idToBackend);
            String clusterName = ConnectContext.get() != null
                    ? ConnectContext.get().getCloudCluster() : "ctx empty cant get clusterName";
            throw new UserException("no available backends, the cluster maybe not be set or been dropped clusterName = "
                + clusterName);
        }
    }
}
