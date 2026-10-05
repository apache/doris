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

package org.apache.doris.nereids.trees.plans.commands.alter;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.AlterCommand;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * Represents the command for ALTER DATABASE db_name RENAME new_db_name.
 */
public class AlterDatabaseRenameCommand extends AlterCommand {
    private final String dbName;
    private final String newDbName;

    public AlterDatabaseRenameCommand(String dbName, String newDbName) {
        super(PlanType.ALTER_DATABASE_RENAME_COMMAND);
        this.dbName = dbName;
        this.newDbName = newDbName;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        InternalDatabaseUtil.checkDatabase(dbName, ConnectContext.get());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                        PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, ctx.getQualifiedUser(), dbName);
        }

        FeNameFormat.checkDbName(newDbName);
        Env.getCurrentInternalCatalog().renameDatabase(dbName, newDbName);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterDatabaseRenameCommand(this, context);
    }

    public String toSql() {
        return "ALTER DATABASE " + dbName + " RENAME " + newDbName;
    }
}
