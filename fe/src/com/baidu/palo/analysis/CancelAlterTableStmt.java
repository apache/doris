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

import com.baidu.palo.analysis.ShowAlterStmt.AlterType;
import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.common.AnalysisException;

/*
 * CANCEL ALTER COLUMN|ROLLUP FROM db_name.table_name
 */
public class CancelAlterTableStmt extends CancelStmt {

    private AlterType alterType;

    private TableName dbTableName;

    public AlterType getAlterType() {
        return alterType;
    }

    public String getDbName() {
        return dbTableName.getDb();
    }

    public String getTableName() {
        return dbTableName.getTbl();
    }

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName) {
        this.alterType = alterType;
        this.dbTableName = dbTableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        dbTableName.analyze(analyzer);

        // check access
        if (!analyzer.getCatalog().getUserMgr()
                .checkAccess(analyzer.getUser(), dbTableName.getDb(), AccessPrivilege.READ_WRITE)) {
            throw new AnalysisException("No privilege to access database[" + dbTableName.getDb() + "]");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CANCEL ALTER " + this.alterType);
        stringBuilder.append(" FROM " + dbTableName.toSql());
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

}
