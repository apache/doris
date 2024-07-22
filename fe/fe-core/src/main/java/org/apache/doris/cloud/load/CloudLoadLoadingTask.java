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

package org.apache.doris.cloud.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.loadv2.LoadLoadingTask;
import org.apache.doris.load.loadv2.LoadTaskCallback;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CloudLoadLoadingTask extends LoadLoadingTask {
    private static final Logger LOG = LogManager.getLogger(CloudLoadLoadingTask.class);

    private String cloudClusterId;

    public CloudLoadLoadingTask(Database db, OlapTable table,
            BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
            long jobDeadlineMs, long execMemLimit, boolean strictMode, boolean isPartialUpdate,
            long txnId, LoadTaskCallback callback, String timezone,
            long timeoutS, int loadParallelism, int sendBatchParallelism,
            boolean loadZeroTolerance, Profile jobProfile, boolean singleTabletLoadPerSink,
            boolean useNewLoadScanNode, Priority priority, boolean enableMemTableOnSinkNode, int batchSize,
            String clusterId) {
        super(db, table, brokerDesc, fileGroups, jobDeadlineMs, execMemLimit, strictMode, isPartialUpdate,
                txnId, callback, timezone, timeoutS, loadParallelism, sendBatchParallelism, loadZeroTolerance,
                jobProfile, singleTabletLoadPerSink, useNewLoadScanNode, priority, enableMemTableOnSinkNode, batchSize);
        this.cloudClusterId = clusterId;
    }

    private AutoCloseConnectContext buildConnectContext() throws UserException {
        String clusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getClusterNameByClusterId(this.cloudClusterId);
        if (Strings.isNullOrEmpty(clusterName)) {
            LOG.warn("cluster name is empty, cluster id is {}", this.cloudClusterId);
            throw new UserException("cluster name is empty, cluster id is: " + this.cloudClusterId);
        }

        if (ConnectContext.get() == null) {
            ConnectContext connectContext = new ConnectContext();
            connectContext.setCloudCluster(clusterName);
            return new AutoCloseConnectContext(connectContext);
        } else {
            ConnectContext.get().setCloudCluster(clusterName);
            return null;
        }
    }

    @Override
    protected void executeOnce() throws Exception {
        try (AutoCloseConnectContext r = buildConnectContext()) {
            super.executeOnce();
        }
    }
}
