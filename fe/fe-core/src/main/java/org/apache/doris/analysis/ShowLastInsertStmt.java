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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.qe.ShowResultSetMetaData;

// SHOW LAST INSERT
public class ShowLastInsertStmt extends ShowStmt implements NotFallbackInParser {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TransactionId", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Label", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Database", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(128)))
                    .addColumn(new Column("TransactionStatus", ScalarType.createVarchar(64)))
                    .addColumn(new Column("LoadedRows", ScalarType.createVarchar(128)))
                    .addColumn(new Column("FilteredRows", ScalarType.createVarchar(128)))
                    .build();

    @Override
    public void analyze(Analyzer analyzer) {
        // No need to check priv here. Bacause `show last insert` can only view
        // the insert result of current session.
        // So if user does not have priv to insert, than there is no result to show.
    }

    @Override
    public String toSql() {
        return "SHOW INSERT RESULT";
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
