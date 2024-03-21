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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CloudGroupCommitPlanner extends GroupCommitPlanner {
    private static final Logger LOG = LogManager.getLogger(CloudGroupCommitPlanner.class);

    public CloudGroupCommitPlanner(Database db, OlapTable table, List<String> targetColumnNames, TUniqueId queryId,
            String groupCommit)
            throws UserException, TException {
        super(db, table, targetColumnNames, queryId, groupCommit);
    }

    @Override
    protected void selectBackends(ConnectContext ctx) throws DdlException {
        backend = ctx.getInsertGroupCommit(this.table.getId());
        if (backend != null && backend.isAlive() && !backend.isDecommissioned()
                && backend.getCloudClusterName().equals(ctx.getCloudCluster())) {
            return;
        }

        String cluster = ctx.getCloudCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_CLUSTER_ERROR);
        }

        // select be
        List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudIdToBackend(cluster)
                .values().stream().collect(Collectors.toList());
        Collections.shuffle(backends);
        for (Backend backend : backends) {
            if (backend.isActive() && !backend.isDecommissioned()) {
                this.backend = backend;
                ctx.setInsertGroupCommit(this.table.getId(), backend);
                LOG.debug("choose new be {}", backend.getId());
                return;
            }
        }

        List<String> backendsInfo = backends.stream()
                .map(be -> "{ beId=" + be.getId() + ", alive=" + be.isAlive() + ", active=" + be.isActive()
                        + ", decommission=" + be.isDecommissioned() + " }")
                .collect(Collectors.toList());
        throw new DdlException("No suitable backend for cloud cluster=" + cluster + ", backends = " + backendsInfo);
    }

}

