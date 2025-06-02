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

import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

// SHOW PROCESSLIST statement.
// Used to show connection belong to this user.
public class ShowProcesslistStmt extends ShowStmt implements NotFallbackInParser {
    private static final ShowResultSetMetaData META_DATA;

    static {
        Table tbl = SchemaTable.TABLE_MAP.get("processlist");
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        tbl.getBaseSchema().stream().forEach(column -> builder.addColumn(column));
        META_DATA = builder.build();
    }

    private boolean isFull;
    private boolean isShowAllFe;

    public ShowProcesslistStmt(boolean isFull) {
        this.isFull = isFull;
    }

    public boolean isFull() {
        return isFull;
    }

    @Override
    public void analyze(Analyzer analyzer) {
        this.isShowAllFe = ConnectContext.get().getSessionVariable().getShowAllFeConnection();
    }

    public boolean isShowAllFe() {
        return isShowAllFe;
    }

    @Override
    public String toSql() {
        return "SHOW " + (isFull ? "FULL" : "") + "PROCESSLIST";
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
