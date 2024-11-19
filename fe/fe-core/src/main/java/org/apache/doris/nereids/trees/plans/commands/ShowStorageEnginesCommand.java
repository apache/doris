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
 * show storage engines command
 */
public class ShowStorageEnginesCommand extends ShowCommand {

    /**
     * constructor
     */
    public ShowStorageEnginesCommand() {
        super(PlanType.SHOW_STORAGE_ENGINES_COMMAND);
    }

    /**
     * get meta for show tableId
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Engine", ScalarType.createVarchar(64)));
        builder.addColumn(new Column("Support", ScalarType.createVarchar(8)));
        builder.addColumn(new Column("Comment", ScalarType.createVarchar(80)));
        builder.addColumn(new Column("Transactions", ScalarType.createVarchar(3)));
        builder.addColumn(new Column("XA", ScalarType.createVarchar(3)));
        builder.addColumn(new Column("Savepoints", ScalarType.createVarchar(3)));
        return builder.build();
    }

    private ShowResultSet handleShowEngines(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rowSet = Lists.newArrayList();
        rowSet.add(Lists.newArrayList("Olap engine", "YES", "Default storage engine of palo", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("MySQL", "YES", "MySQL server which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ELASTICSEARCH", "YES", "ELASTICSEARCH cluster which data is in it",
                "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("HIVE", "YES", "HIVE database which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ICEBERG", "YES", "ICEBERG data lake which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ODBC", "YES", "ODBC driver which data we can connect", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("HUDI", "YES", "HUDI data lake which data is in it", "NO", "NO", "NO"));

        // Only success
        ShowResultSet resultSet = new ShowResultSet(getMetaData(), rowSet);
        return resultSet;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowEngines(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowStorageEnginesCommand(this, context);
    }
}
