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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * show table id command
 */
public class ShowTableIdCommand extends ShowCommand {
    private final long tableId;

    /**
     * constructor
     */
    public ShowTableIdCommand(long tableId) {
        super(PlanType.SHOW_TABLE_ID_COMMAND);
        this.tableId = tableId;
    }

    /**
     * get meta for show tableId
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("DbName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("TableName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("DbId", ScalarType.createVarchar(30)));
        return builder.build();
    }

    private ShowResultSet handleShowTableId(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rows = Lists.newArrayList();
        Env env = ctx.getEnv();
        List<Long> dbIds = env.getInternalCatalog().getDbIds();
        for (long dbId : dbIds) {
            Database database = env.getInternalCatalog().getDbNullable(dbId);
            if (database == null) {
                continue;
            }
            TableIf table = database.getTableNullable(tableId);
            if (table != null) {
                List<String> row = new ArrayList<>();
                row.add(database.getFullName());
                row.add(table.getName());
                row.add(String.valueOf(database.getId()));
                rows.add(row);
                break;
            }
        }
        ShowResultSet resultSet = new ShowResultSet(getMetaData(), rows);
        return resultSet;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check access first
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW TABLE");
        }
        return handleShowTableId(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableIdCommand(this, context);
    }
}
