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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.NodeType;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * admin set frontend config ("key" = "value");
 */
public class AdminSetFrontendConfigCommand extends Command implements ForwardWithSync {
    private boolean applyToAll;
    private NodeType type;
    private Map<String, String> configs;
    private OriginStatement originStmt;

    private RedirectStatus redirectStatus = RedirectStatus.NO_FORWARD;

    /**
     * AdminSetFrontendConfigCommand
     */
    public AdminSetFrontendConfigCommand(NodeType type, Map<String, String> configs, boolean applyToAll) {
        super(PlanType.ADMIN_SET_FRONTEND_CONFIG_COMMAND);
        this.type = type;
        this.configs = configs;
        if (this.configs == null) {
            this.configs = Maps.newHashMap();
        }
        this.applyToAll = applyToAll;

        if (!this.applyToAll) {
            // we have to analyze configs here to determine whether to forward it to master
            for (String key : this.configs.keySet()) {
                if (ConfigBase.checkIsMasterOnly(key)) {
                    redirectStatus = RedirectStatus.FORWARD_NO_SYNC;
                    break;
                }
            }
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        originStmt = ctx.getStatementContext().getOriginStatement();
        Env.getCurrentEnv().setConfig(this);
    }

    /**
     * validate
     */
    public void validate() throws UserException {
        if (configs.size() != 1) {
            throw new AnalysisException("config parameter size is not equal to 1");
        }
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (type != NodeType.FRONTEND) {
            throw new AnalysisException("Only support setting Frontend configs now");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetFrontendConfigCommand(this, context);
    }

    public boolean isApplyToAll() {
        return applyToAll;
    }

    public NodeType getNodeTypeType() {
        return type;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    public RedirectStatus getRedirectStatus() {
        return redirectStatus;
    }

    /**
     * getLocalSetStmt
     */
    public OriginStatement getLocalSetStmt() {
        Object[] keyArr = configs.keySet().toArray();
        String sql = String.format("ADMIN SET FRONTEND CONFIG (\"%s\" = \"%s\");",
                keyArr[0].toString(), configs.get(keyArr[0].toString()));

        return new OriginStatement(sql, originStmt.idx);
    }
}
