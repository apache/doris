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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;

/**
 * DROP AUTHENTICATION INTEGRATION command entry.
 */
public class DropAuthenticationIntegrationCommand extends DropCommand {
    private final boolean ifExists;
    private final String integrationName;

    public DropAuthenticationIntegrationCommand(boolean ifExists, String integrationName) {
        super(PlanType.DROP_AUTHENTICATION_INTEGRATION_COMMAND);
        this.ifExists = ifExists;
        this.integrationName = Objects.requireNonNull(integrationName, "integrationName can not be null");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropAuthenticationIntegrationCommand(this, context);
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        Env.getCurrentEnv().getAuthenticationIntegrationMgr()
                .dropAuthenticationIntegration(integrationName, ifExists);
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getIntegrationName() {
        return integrationName;
    }
}
