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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnIdFlushDaemon;
import org.apache.doris.catalog.ColumnIdFlushDaemon.FlushStatus;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Represents the command for SHOW CONVERT LIGHT SCHEMA CHANGE PROCESS .
 */
public class ShowConvertLSCCommand extends ShowCommand {
    private static final int COLUMN_LENGTH = 30;
    private final String dbName;

    public ShowConvertLSCCommand(String dbName) {
        super(PlanType.SHOW_CONVERT_LSC_COMMAND);
        this.dbName = dbName;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // Check if the user has ADMIN or OPERATOR privileges
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(),
                PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
        }
        return handleShowConvertLSC();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowConvertLscCommand(this, context);
    }

    /**
     * get show result set metadata.
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        Column databaseColumn = new Column("database", ScalarType.createVarcharType(COLUMN_LENGTH));
        Column tableNameColumn = new Column("table", ScalarType.createVarcharType(COLUMN_LENGTH));
        Column statusColumn = new Column("status", ScalarType.createVarcharType(COLUMN_LENGTH));
        builder.addColumn(databaseColumn);
        builder.addColumn(tableNameColumn);
        builder.addColumn(statusColumn);
        return builder.build();
    }

    private ShowResultSet handleShowConvertLSC() {
        ColumnIdFlushDaemon columnIdFlusher = Env.getCurrentEnv().getColumnIdFlusher();
        columnIdFlusher.readLock();
        List<List<String>> rows;
        try {
            Map<String, Map<String, FlushStatus>> resultCollector =
                    columnIdFlusher.getResultCollector();
            rows = new ArrayList<>();
            if (dbName != null) {
                Map<String, ColumnIdFlushDaemon.FlushStatus> tblNameToStatus = resultCollector.get(dbName);
                if (tblNameToStatus != null) {
                    tblNameToStatus.forEach((tblName, status) -> {
                        List<String> row = Arrays.asList(dbName, tblName, status.getMsg());
                        rows.add(row);
                    });
                }
            } else {
                resultCollector.forEach((dbName, tblNameToStatus) ->
                        tblNameToStatus.forEach((tblName, status) -> {
                            List<String> row = Arrays.asList(dbName, tblName, status.getMsg());
                            rows.add(row);
                        }));
            }
        } finally {
            columnIdFlusher.readUnlock();
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
