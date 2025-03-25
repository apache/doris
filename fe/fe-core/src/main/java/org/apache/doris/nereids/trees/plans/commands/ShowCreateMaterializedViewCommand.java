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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the command for SHOW CREATE MATERIALIZED VIEW.
 */
public class ShowCreateMaterializedViewCommand extends ShowCommand {
    private static final ShowResultSetMetaData MATERIALIZED_VIEW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ViewName", ScalarType.createVarchar(30)))
                    .addColumn(new Column("CreateStmt", ScalarType.createVarchar(500)))
                    .build();

    private final String mvName;
    private final TableNameInfo tblNameInfo;

    public ShowCreateMaterializedViewCommand(String mvName, TableNameInfo tableNameInfo) {
        super(PlanType.SHOW_CREATE_MATERIALIZED_VIEW_COMMAND);
        this.tblNameInfo = tableNameInfo;
        this.mvName = mvName;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        tblNameInfo.analyze(ctx);

        // disallow external catalog
        Util.prohibitExternalCatalog(tblNameInfo.getCtl(), this.getClass().getSimpleName());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tblNameInfo.getCtl(), tblNameInfo.getDb(), tblNameInfo.getTbl(),
                        PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE MATERIALIZED",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tblNameInfo.toSql());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateMaterializedViewCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        List<List<String>> resultRowSet = new ArrayList<>();
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(tblNameInfo.getDb());
        Table table = db.getTableOrAnalysisException(tblNameInfo.getTbl());
        if (table instanceof OlapTable) {
            OlapTable baseTable = ((OlapTable) table);
            Long indexIdByName = baseTable.getIndexIdByName(mvName);
            if (indexIdByName != null) {
                MaterializedIndexMeta meta = baseTable.getIndexMetaByIndexId(indexIdByName);
                if (meta != null && meta.getDefineStmt() != null) {
                    String originStmt = meta.getDefineStmt().originStmt;
                    List<String> data = new ArrayList<>();
                    data.add(tblNameInfo.getTbl());
                    data.add(mvName);
                    data.add(originStmt);
                    resultRowSet.add(data);
                }
            }
        }
        return new ShowResultSet(MATERIALIZED_VIEW_META_DATA, resultRowSet);
    }
}

