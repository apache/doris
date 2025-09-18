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
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

/**
 * DropViewCommand
 */
public class DropViewCommand extends Command implements ForwardWithSync {
    private boolean ifExists;
    private final TableNameInfo tableName;

    public DropViewCommand(boolean ifExists, TableNameInfo tableName) {
        super(PlanType.DROP_VIEW_COMMAND);
        this.ifExists = ifExists;
        this.tableName = tableName;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Env.getCurrentEnv().dropView(tableName.getCtl(), tableName.getDb(), tableName.getTbl(), ifExists);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            tableName.setDb(ctx.getDatabase());
        }
        tableName.analyze(ctx);
        InternalDatabaseUtil.checkDatabase(tableName.getDb(), ctx);
        // check access
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropViewCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public TableNameInfo getTableName() {
        return tableName;
    }
}
