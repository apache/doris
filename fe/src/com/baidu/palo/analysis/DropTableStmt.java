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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;

import com.google.common.base.Strings;

// DROP TABLE
public class DropTableStmt extends DdlStmt {
    private boolean ifExists;
    private final TableName tableName;
    private final boolean isView;

    public DropTableStmt(boolean ifExists, TableName tableName) {
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.isView = false;
    }

    public DropTableStmt(boolean ifExists, TableName tableName, boolean isView) {
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.isView = isView;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public boolean isView() {
        return isView;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            tableName.setDb(analyzer.getDefaultDb());
        }
        tableName.analyze(analyzer);
        // check access
        if (!analyzer.getCatalog().getUserMgr()
                .checkAccess(analyzer.getUser(), tableName.getDb(), AccessPrivilege.READ_WRITE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    analyzer.getUser(), tableName.getDb());
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP TABLE ").append(tableName.toSql());
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
