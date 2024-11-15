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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InsertResult;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * show last insert command
 */
public class ShowLastInsertCommand extends ShowCommand {
    public static final Logger LOG = LogManager.getLogger(ShowLastInsertCommand.class);

    /**
     * constructor
     */
    public ShowLastInsertCommand() {
        super(PlanType.SHOW_LAST_INSERT_COMMAND);
    }

    /**
     * get meta for show last insert
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("TransactionId", ScalarType.createVarchar(128)));
        builder.addColumn(new Column("Label", ScalarType.createVarchar(128)));
        builder.addColumn(new Column("Database", ScalarType.createVarchar(128)));
        builder.addColumn(new Column("Table", ScalarType.createVarchar(128)));
        builder.addColumn(new Column("TransactionStatus", ScalarType.createVarchar(64)));
        builder.addColumn(new Column("LoadedRows", ScalarType.createVarchar(128)));
        builder.addColumn(new Column("FilteredRows", ScalarType.createVarchar(128)));
        return builder.build();
    }

    private ShowResultSet handleShowLastInsert(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> resultRowSet = Lists.newArrayList();
        if (ConnectContext.get() != null) {
            InsertResult insertResult = ConnectContext.get().getInsertResult();
            if (insertResult != null) {
                resultRowSet.add(insertResult.toRow());
            }
        }
        ShowResultSet resultSet = new ShowResultSet(getMetaData(), resultRowSet);
        return resultSet;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowLastInsert(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLastInsertCommand(this, context);
    }
}
