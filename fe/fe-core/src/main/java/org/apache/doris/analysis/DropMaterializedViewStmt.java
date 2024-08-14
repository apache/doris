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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

/**
 * DROP MATERIALIZED VIEW [ IF EXISTS ] <mv_name> ON [db_name].<table_name>
 *
 * Parameters
 * IF EXISTS: Do not throw an error if the materialized view does not exist. A notice is issued in this case.
 * mv_name: The name of the materialized view to remove.
 * db_name: The name of db to which materialized view belongs.
 * table_name: The name of table to which materialized view belongs.
 */
public class DropMaterializedViewStmt extends DdlStmt {

    private String mvName;
    private TableName tableName;
    private boolean ifExists;

    public DropMaterializedViewStmt(boolean ifExists, String mvName, TableName tableName) {
        this.mvName = mvName;
        this.tableName = tableName;
        this.ifExists = ifExists;
    }

    public String getMvName() {
        return mvName;
    }

    public TableName getTableName() {
        return tableName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (Strings.isNullOrEmpty(mvName)) {
            throw new AnalysisException("The materialized name could not be empty or null.");
        }
        tableName.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        // check access
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(),
                        tableName.getTbl(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.ALTER.getPrivs().toString(), tableName.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP MATERIALIZED VIEW ");
        if (ifExists) {
            stringBuilder.append("IF EXISTS ");
        }
        stringBuilder.append("`").append(mvName).append("` ");
        stringBuilder.append("ON ").append(tableName.toSql());
        return stringBuilder.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
