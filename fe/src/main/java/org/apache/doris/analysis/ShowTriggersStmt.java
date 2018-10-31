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

public class ShowTriggersStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Trigger", ColumnType.createVarchar(64)))
                    .addColumn(new Column("Event", ColumnType.createVarchar(10)))
                    .addColumn(new Column("Table", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Statement", ColumnType.createVarchar(64)))
                    .addColumn(new Column("Timing", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Created", ColumnType.createVarchar(80)))
                    .addColumn(new Column("sql_mode", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Definer", ColumnType.createVarchar(80)))
                    .addColumn(new Column("character_set_client", ColumnType.createVarchar(80)))
                    .addColumn(new Column("collation_connection", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Database Collation", ColumnType.createVarchar(80)))
                    .build();

    @Override
    public void analyze(Analyzer analyzer) {
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
