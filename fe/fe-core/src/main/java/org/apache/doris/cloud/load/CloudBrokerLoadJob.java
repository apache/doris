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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.loadv2.BrokerPendingTaskAttachment;
import org.apache.doris.load.loadv2.LoadLoadingTask;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

public class CloudBrokerLoadJob extends BrokerLoadJob {
    private static final Logger LOG = LogManager.getLogger(CloudBrokerLoadJob.class);

    protected static final String CLOUD_CLUSTER_ID = "clusterId";
    protected String cloudClusterId;

    public CloudBrokerLoadJob() {
    }

    public CloudBrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt,
            UserIdentity userInfo) throws MetaNotFoundException {
        super(dbId, label, brokerDesc, originStmt, userInfo);
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            String clusterName = context.getCloudCluster();
            if (Strings.isNullOrEmpty(clusterName)) {
                LOG.warn("cluster name is empty");
                throw new MetaNotFoundException("cluster name is empty");
            }

            this.cloudClusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                    .getCloudClusterIdByName(clusterName);
            if (!Strings.isNullOrEmpty(context.getSessionVariable().getCloudCluster())) {
                clusterName = context.getSessionVariable().getCloudCluster();
                this.cloudClusterId =
                    ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIdByName(clusterName);
            }
            if (Strings.isNullOrEmpty(this.cloudClusterId)) {
                LOG.warn("cluster id is empty, cluster name {}", clusterName);
                throw new MetaNotFoundException("cluster id is empty, cluster name: " + clusterName);
            }
            sessionVariables.put(CLOUD_CLUSTER_ID, this.cloudClusterId);
        }
    }

    private AutoCloseConnectContext buildConnectContext() throws UserException {
        cloudClusterId = sessionVariables.get(CLOUD_CLUSTER_ID);
        String clusterName =  ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getClusterNameByClusterId(cloudClusterId);
        if (Strings.isNullOrEmpty(clusterName)) {
            LOG.warn("cluster name is empty, cluster id is {}", cloudClusterId);
            throw new UserException("cluster name is empty, cluster id is: " + cloudClusterId);
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

    private LoadLoadingTask createTask(Database db, OlapTable table, List<BrokerFileGroup> brokerFileGroups,
            boolean isEnableMemtableOnSinkNode, FileGroupAggKey aggKey, BrokerPendingTaskAttachment attachment)
            throws UserException {
        LoadLoadingTask task = new LoadLoadingTask(db, table, brokerDesc,
                brokerFileGroups, getDeadlineMs(), getExecMemLimit(),
                isStrictMode(), isPartialUpdate(), transactionId, this, getTimeZone(), getTimeout(),
                getLoadParallelism(), getSendBatchParallelism(),
                getMaxFilterRatio() <= 0, enableProfile ? jobProfile : null, isSingleTabletLoadPerSink(),
                useNewLoadScanNode(), getPriority(), isEnableMemtableOnSinkNode);
        UUID uuid = UUID.randomUUID();
        TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());

        try (AutoCloseConnectContext r = buildConnectContext()) {
            task.init(loadId, attachment.getFileStatusByTable(aggKey),
                    attachment.getFileNumByTable(aggKey), getUserInfo());
        } catch (UserException e) {
            throw e;
        }
        return task;
    }
}
