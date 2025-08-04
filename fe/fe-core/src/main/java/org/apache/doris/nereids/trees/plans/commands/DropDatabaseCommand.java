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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlCompatibleDatabase;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DropDatabaseInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

/**
 * drop database command
 */
public class DropDatabaseCommand extends Command implements ForwardWithSync {
    private final DropDatabaseInfo dropDatabaseInfo;

    public DropDatabaseCommand(DropDatabaseInfo dropDatabaseInfo) {
        super(PlanType.DROP_DATABASE_COMMAND);
        this.dropDatabaseInfo = dropDatabaseInfo;
    }

    private void analyze(ConnectContext ctx) throws UserException {
        String catalogName = dropDatabaseInfo.getCatalogName();
        String databaseName = dropDatabaseInfo.getDatabaseName();
        if (Strings.isNullOrEmpty(databaseName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, databaseName);
        }

        InternalDatabaseUtil.checkDatabase(databaseName, ctx);

        DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(databaseName);
        if (db instanceof MysqlCompatibleDatabase) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    ctx.getQualifiedUser(), databaseName);
        }

        String effectiveCatalog = StringUtils.isEmpty(catalogName) ? ctx.getCurrentCatalog().getName() : catalogName;

        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ctx, effectiveCatalog, databaseName, PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    ctx.getQualifiedUser(), databaseName);
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        analyze(ctx);
        Env.getCurrentEnv().dropDb(dropDatabaseInfo.getCatalogName(), dropDatabaseInfo.getDatabaseName(),
                    dropDatabaseInfo.isIfExists(), dropDatabaseInfo.isForce());
    }

    public DropDatabaseInfo getDropDatabaseInfo() {
        return dropDatabaseInfo;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropDatabaseCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
