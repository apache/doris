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
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * show triggers command
 */
public class ShowTriggersCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Trigger", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Event", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Statement", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Timing", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Created", ScalarType.createVarchar(80)))
                    .addColumn(new Column("sql_mode", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Definer", ScalarType.createVarchar(80)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(80)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Database Collation", ScalarType.createVarchar(80)))
                    .build();

    public ShowTriggersCommand() {
        super(PlanType.SHOW_TRIGGERS_COMMAND);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTriggersCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rowSet = Lists.newArrayList();
        return new ShowResultSet(META_DATA, rowSet);
    }
}
