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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.base.Strings;

/**
 * Statement for remove a table in catalog recycle bin.
 */
public class RemoveTableStmt extends DdlStmt {
    private TableName tableName;
    private long tableId = -1;

    public RemoveTableStmt(TableName tableName, long tableId) {
        this.tableName = tableName;
        this.tableId = tableId;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            tableName.setDb(analyzer.getDefaultDb());
        }
        tableName.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REMOVE TABLE ");
        if (!Strings.isNullOrEmpty(getDbName())) {
            sb.append(getDbName()).append(".");
        }
        sb.append(getTableName());
        if (this.tableId != -1) {
            sb.append(" ");
            sb.append(this.tableId);
        }
        return sb.toString();
    }
}
