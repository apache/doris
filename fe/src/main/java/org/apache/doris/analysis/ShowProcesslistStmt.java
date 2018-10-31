// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.qe.ShowResultSetMetaData;

// SHOW PROCESSLIST statement.
// Used to show connection belong to this user.
public class ShowProcesslistStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ColumnType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("User", ColumnType.createVarchar(16)))
                    .addColumn(new Column("Host", ColumnType.createVarchar(16)))
                    .addColumn(new Column("Cluster", ColumnType.createVarchar(16)))
                    .addColumn(new Column("Db", ColumnType.createVarchar(16)))
                    .addColumn(new Column("Command", ColumnType.createVarchar(16)))
                    .addColumn(new Column("Time", ColumnType.createType(PrimitiveType.INT)))
                    .addColumn(new Column("State", ColumnType.createVarchar(64)))
                    .addColumn(new Column("Info", ColumnType.createVarchar(16)))
                    .build();

    @Override
    public void analyze(Analyzer analyzer) {
    }

    @Override
    public String toSql() {
        return "SHOW PROCESSLIST";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
