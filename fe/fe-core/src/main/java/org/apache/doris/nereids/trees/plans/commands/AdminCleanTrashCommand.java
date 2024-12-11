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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.CleanTrashTask;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * admin clean trash
 */
public class AdminCleanTrashCommand extends Command implements NoForward {
    private static final Logger LOG = LogManager.getLogger(AdminCleanTrashCommand.class);
    private List<String> backendsQuery;

    public AdminCleanTrashCommand() {
        super(PlanType.CLEAN_TRASH_COMMAND);
    }

    public AdminCleanTrashCommand(List<String> backendsQuery) {
        super(PlanType.CLEAN_TRASH_COMMAND);
        this.backendsQuery = backendsQuery;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        handleCleanTrash(backendsQuery);
    }

    private void handleCleanTrash(List<String> backendsQuery) throws AnalysisException {
        List<Backend> backends = getNeedCleanedBackends(backendsQuery);
        cleanTrash(backends);
    }

    private List<Backend> getNeedCleanedBackends(List<String> backendsQuery) throws AnalysisException {
        List<Backend> needCleanedBackends = Lists.newArrayList();
        ImmutableMap<Long, Backend> backendsInfo = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        if (backendsQuery == null) {
            needCleanedBackends.addAll(backendsInfo.values());
        } else {
            Map<String, Long> backendsID = new HashMap<>();
            for (Backend backend : backendsInfo.values()) {
                backendsID.put(
                        NetUtils.getHostPortInAccessibleFormat(backend.getHost(), backend.getHeartbeatPort()),
                        backend.getId());
            }
            for (String backendQuery : backendsQuery) {
                if (backendsID.containsKey(backendQuery)) {
                    needCleanedBackends.add(backendsInfo.get(backendsID.get(backendQuery)));
                    backendsID.remove(backendQuery);
                }
            }
        }
        return needCleanedBackends;
    }

    private void cleanTrash(List<Backend> backends) {
        if (backends.isEmpty()) {
            LOG.info("The matching be is empty, no be to clean trash.");
            return;
        }
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Backend backend : backends) {
            CleanTrashTask cleanTrashTask = new CleanTrashTask(backend.getId());
            batchTask.addTask(cleanTrashTask);
            LOG.info("clean trash in be {}, beId {}", backend.getHost(), backend.getId());
        }
        AgentTaskExecutor.submit(batchTask);
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("AdminCleanTrashCommand not supported in cloud mode");
        throw new DdlException("Unsupported operation");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCleanTrashCommand(this, context);
    }
}
