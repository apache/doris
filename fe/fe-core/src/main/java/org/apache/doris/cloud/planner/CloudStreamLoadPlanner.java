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

package org.apache.doris.cloud.planner;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudStreamLoadPlanner extends StreamLoadPlanner {
    private static final Logger LOG = LogManager.getLogger(CloudStreamLoadPlanner.class);

    private String cloudClusterName;

    public CloudStreamLoadPlanner(Database db, OlapTable destTable, LoadTaskInfo taskInfo, String cloudClusterName) {
        super(db, destTable, taskInfo);
        this.cloudClusterName = cloudClusterName;
    }

    private AutoCloseConnectContext buildConnectContext() throws UserException {
        if (ConnectContext.get() == null) {
            ConnectContext ctx = new ConnectContext();
            ctx.setCloudCluster(cloudClusterName);
            return new AutoCloseConnectContext(ctx);
        } else {
            ConnectContext.get().setCloudCluster(cloudClusterName);
            return null;
        }
    }

    @Override
    public TPipelineFragmentParams plan(TUniqueId loadId, int fragmentInstanceIdIndex) throws UserException {
        try (AutoCloseConnectContext r = buildConnectContext()) {
            return super.plan(loadId, fragmentInstanceIdIndex);
        } catch (UserException e) {
            throw e;
        }
    }
}
