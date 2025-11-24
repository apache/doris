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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * CancelDecommissionBackendCommand
 */
public class CancelDecommissionBackendCommand extends CancelCommand {
    private static final Logger LOG = LogManager.getLogger(CancelDecommissionBackendCommand.class);
    private final List<String> params;
    private final List<SystemInfoService.HostInfo> hostInfos;
    private final List<String> ids;

    public CancelDecommissionBackendCommand(List<String> params) {
        super(PlanType.CANCEL_DECOMMISSION_BACKEND_COMMAND);
        Objects.requireNonNull(params, "params is null!");
        this.params = params;
        this.hostInfos = Lists.newArrayList();
        this.ids = Lists.newArrayList();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        ctx.getEnv().getSystemHandler().cancel(this);
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.OPERATOR.getPrivs().toString());
        }
        for (String param : params) {
            if (!param.contains(":")) {
                ids.add(param);
            } else {
                SystemInfoService.HostInfo hostInfo = SystemInfoService.getHostAndPort(param);
                this.hostInfos.add(hostInfo);
            }

        }
        Preconditions.checkState(!this.hostInfos.isEmpty() || !this.ids.isEmpty(),
                "hostInfos or ids can not be empty");
    }

    public List<SystemInfoService.HostInfo> getHostInfos() {
        return hostInfos;
    }

    public List<String> getIds() {
        return ids;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCancelDecommissionCommand(this, context);
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CANCEL DECOMMISSION BACKEND ");
        if (!ids.isEmpty()) {
            for (int i = 0; i < hostInfos.size(); i++) {
                sb.append("\"").append(hostInfos.get(i)).append("\"");
                if (i != hostInfos.size() - 1) {
                    sb.append(", ");
                }
            }
        } else {
            for (int i = 0; i < params.size(); i++) {
                sb.append("\"").append(params.get(i)).append("\"");
                if (i != params.size() - 1) {
                    sb.append(", ");
                }
            }
        }
        return sb.toString();
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("CancelDecommissionBackendCommand not supported in cloud mode");
        throw new DdlException("Unsupported operation");
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CANCEL;
    }
}
