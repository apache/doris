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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;
import java.util.Objects;

/**
 * CancelBuildIndexCommand
 */
public class CancelBuildIndexCommand extends CancelCommand {
    private final TableNameInfo tableNameInfo;
    private final List<Long> alterJobIdList;

    /**
     * CancelBuildIndexCommand
     */
    public CancelBuildIndexCommand(TableNameInfo tableNameInfo,
                                   List<Long> alterJobIdList) {
        super(PlanType.CANCEL_BUILD_INDEX_COMMAND);
        Objects.requireNonNull(tableNameInfo, "tableNameInfo is null");
        Objects.requireNonNull(alterJobIdList, "alterJobIdList is null");
        this.tableNameInfo = tableNameInfo;
        this.alterJobIdList = alterJobIdList;
    }

    public String getDbName() {
        return tableNameInfo.getDb();
    }

    public String getTableName() {
        return tableNameInfo.getTbl();
    }

    public List<Long> getAlterJobIdList() {
        return alterJobIdList;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().cancelBuildIndex(this);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        tableNameInfo.analyze(ctx);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableNameInfo.getCtl(), this.getClass().getSimpleName());

        if (FeConstants.runningUnitTest) {
            return;
        }
        // check access
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "CANCEL ALTER TABLE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableNameInfo.getDb() + ": " + tableNameInfo.getTbl());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCancelBuildIndexCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CANCEL;
    }
}
