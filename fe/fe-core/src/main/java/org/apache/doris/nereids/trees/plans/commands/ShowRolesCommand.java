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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * show roles command
 */
public class ShowRolesCommand extends ShowCommand {
    public static final Logger LOG = LogManager.getLogger(ShowRolesCommand.class);
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("Name", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Comment", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Users", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("GlobalPrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("CatalogPrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("DatabasePrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("TablePrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("ResourcePrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("CloudClusterPrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("CloudStagePrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("StorageVaultPrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("WorkloadGroupPrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("ComputeGroupPrivs", ScalarType.createVarchar(300)));

        META_DATA = builder.build();
    }

    /**
     * constructor
     */

    public ShowRolesCommand() {
        super(PlanType.SHOW_ROLE_COMMAND);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }

        List<List<String>> infos = Env.getCurrentEnv().getAuth().getRoleInfo();
        return new ShowResultSet(META_DATA, infos);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowRolesCommand(this, context);
    }
}
