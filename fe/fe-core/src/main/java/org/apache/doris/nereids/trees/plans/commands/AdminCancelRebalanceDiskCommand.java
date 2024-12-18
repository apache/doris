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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * admin cancel rebalance disk
 */
public class AdminCancelRebalanceDiskCommand extends Command implements NoForward {
    private static final Logger LOG = LogManager.getLogger(AdminCancelRebalanceDiskCommand.class);
    private List<String> backends;

    public AdminCancelRebalanceDiskCommand() {
        super(PlanType.ADMIN_CANCEL_REBALANCE_DISK_COMMAND);
    }

    public AdminCancelRebalanceDiskCommand(List<String> backends) {
        super(PlanType.ADMIN_CANCEL_REBALANCE_DISK_COMMAND);
        this.backends = backends;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        handleCancelRebalanceDisk();
    }

    private void handleCancelRebalanceDisk() throws AnalysisException {
        List<Backend> rebalanceDiskBackends = getNeedRebalanceDiskBackends(backends);
        if (rebalanceDiskBackends.isEmpty()) {
            LOG.info("The matching be is empty, no be to cancel rebalance disk.");
            return;
        }
        Env.getCurrentEnv().getTabletScheduler().cancelRebalanceDisk(rebalanceDiskBackends);
    }

    private List<Backend> getNeedRebalanceDiskBackends(List<String> backends) throws AnalysisException {
        ImmutableMap<Long, Backend> backendsInfo = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        List<Backend> needRebalanceDiskBackends = Lists.newArrayList();
        if (backends == null) {
            needRebalanceDiskBackends.addAll(backendsInfo.values());
        } else {
            Map<String, Long> backendsID = new HashMap<>();
            for (Backend backend : backendsInfo.values()) {
                backendsID.put(
                        NetUtils.getHostPortInAccessibleFormat(backend.getHost(), backend.getHeartbeatPort()),
                        backend.getId());
            }
            for (String be : backends) {
                if (backendsID.containsKey(be)) {
                    needRebalanceDiskBackends.add(backendsInfo.get(backendsID.get(be)));
                    backendsID.remove(be);
                }
            }
        }
        return needRebalanceDiskBackends;
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("AdminCancelRebalanceDiskCommand not supported in cloud mode");
        throw new DdlException("Unsupported operation");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCancelRebalanceDiskCommand(this, context);
    }
}
