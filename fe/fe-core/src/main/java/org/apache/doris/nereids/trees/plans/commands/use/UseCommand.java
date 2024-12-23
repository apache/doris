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
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.NoForward;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Representation of a use db statement.
 */
public class UseCommand extends Command implements NoForward {
    private static final Logger LOG = LogManager.getLogger(UseCommand.class);
    private String catalogName;
    private String databaseName;

    public UseCommand(String databaseName) {
        super(PlanType.USE_COMMAND);
        this.databaseName = databaseName;
    }

    public UseCommand(String catalogName, String databaseName) {
        super(PlanType.USE_COMMAND);
        this.catalogName = catalogName;
        this.databaseName = databaseName;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        handleUseStmt(ctx);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUseCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.USE;
    }

    private void validate(ConnectContext context) throws AnalysisException {
        if (Strings.isNullOrEmpty(databaseName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
        String currentCatalogName = catalogName == null ? ConnectContext.get().getDefaultCatalog() : catalogName;

        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), currentCatalogName, databaseName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, context.getQualifiedUser(),
                    databaseName);
        }
    }

    /**
     * Process use statement.
     */
    private void handleUseStmt(ConnectContext context) {
        try {
            if (catalogName != null) {
                context.getEnv().changeCatalog(context, catalogName);
            }
            context.getEnv().changeDb(context, databaseName);
        } catch (DdlException e) {
            LOG.warn("The handling of the use command failed.", e);
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    /**
     * Generate sql string.
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("USE ");
        if (catalogName != null) {
            sb.append("`").append(catalogName).append("`.");
        }
        sb.append("`").append(databaseName).append("`");
        return sb.toString();
    }
}
