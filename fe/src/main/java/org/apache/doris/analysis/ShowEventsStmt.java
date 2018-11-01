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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnType;
import org.apache.doris.qe.ShowResultSetMetaData;

// Show Events statement
public class ShowEventsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Db", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Name", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Definer", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Time", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Type", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Execute at", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Interval value", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Interval field", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Status", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Ends", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Status", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Originator", ColumnType.createVarchar(30)))
                    .addColumn(new Column("character_set_client", ColumnType.createVarchar(30)))
                    .addColumn(new Column("collation_connection", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Database Collation", ColumnType.createVarchar(30)))
                    .build();

    @Override
    public void analyze(Analyzer analyzer) {
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
