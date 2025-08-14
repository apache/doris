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
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;

import java.util.Objects;

/**
 * drop sync materialized view
 */
public class DropMaterializedViewCommand extends Command implements ForwardWithSync {
    private final TableNameInfo tableName;
    private final boolean ifExists;
    private final String mvName;

    public DropMaterializedViewCommand(TableNameInfo tableName, boolean ifExists, String mvName) {
        super(PlanType.DROP_MATERIALIZED_VIEW_COMMAND);
        this.tableName = Objects.requireNonNull(tableName, "require tableName object");
        this.ifExists = ifExists;
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Env.getCurrentEnv().dropMaterializedView(this);
    }

    /**
     * validate
     */
    @VisibleForTesting
    public void validate(ConnectContext ctx) throws AnalysisException {
        tableName.analyze(ctx);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());
        // check access
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(),
                        tableName.getTbl(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.ALTER.getPrivs().toString(), tableName.getTbl());
        }
    }

    public TableNameInfo getTableName() {
        return tableName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getMvName() {
        return mvName;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropMaterializedViewCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
