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
import org.apache.doris.qe.ShowResultSetMetaData;

// SHOW KEYS FROM TABLE
public class ShowKeysStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Table", ColumnType.createVarchar(64)))
                    .addColumn(new Column("Non_unique", ColumnType.createVarchar(10)))
                    .addColumn(new Column("Key_name", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Seq_in_index", ColumnType.createVarchar(64)))
                    .addColumn(new Column("Column_name", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Collation", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Cardinality", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Sub_part", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Packed", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Null", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Index_type", ColumnType.createVarchar(80)))
                    .addColumn(new Column("Comment", ColumnType.createVarchar(80)))
                    .build();

    @Override
    public void analyze(Analyzer analyzer) {
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
