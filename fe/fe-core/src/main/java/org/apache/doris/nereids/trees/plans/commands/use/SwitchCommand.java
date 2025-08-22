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

package org.apache.doris.nereids.trees.plans.commands.use;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.NoForward;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * switch command.
 */
public class SwitchCommand extends Command implements NoForward {
    private static final Logger LOG = LogManager.getLogger(SwitchCommand.class);
    private final String catalogName;

    public SwitchCommand(String catalogName) {
        super(PlanType.SWITCH_COMMAND);
        this.catalogName = catalogName;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // validate catalog access
        validate(ctx);
        handleSwitchStmt(ctx);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitSwitchCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SWITCH;
    }

    @VisibleForTesting
    public String getCatalogName() {
        return catalogName;
    }

    public String toSql() {
        return "SWITCH `" + catalogName + "`";
    }

    private void validate(ConnectContext context) throws AnalysisException {
        Util.checkCatalogAllRules(catalogName);

        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                ConnectContext.get(), catalogName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_CATALOG_ACCESS_DENIED, context.getQualifiedUser(), catalogName);
        }
    }

    /**
     * Process switch catalog.
     */
    private void handleSwitchStmt(ConnectContext context) {
        try {
            context.getEnv().changeCatalog(context, catalogName);
        } catch (DdlException e) {
            LOG.warn("handle switch command failed! ", e);
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        }
        context.getState().setOk();
    }
}
