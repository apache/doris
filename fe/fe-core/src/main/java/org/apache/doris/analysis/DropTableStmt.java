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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

// DROP TABLE
public class DropTableStmt extends DdlStmt {
    private boolean ifExists;
    private final TableName tableName;
    private final boolean isView;
    private boolean forceDrop;
    private boolean isMaterializedView;

    public DropTableStmt(boolean ifExists, TableName tableName, boolean forceDrop) {
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.isView = false;
        this.forceDrop = forceDrop;
    }

    public DropTableStmt(boolean ifExists, TableName tableName, boolean isView, boolean forceDrop) {
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.isView = isView;
        this.forceDrop = forceDrop;
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

    public String getCatalogName() {
        return tableName.getCtl();
    }

    public boolean isView() {
        return isView;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    public void setMaterializedView(boolean value) {
        isMaterializedView = value;
    }

    public boolean isMaterializedView() {
        return isMaterializedView;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            tableName.setDb(analyzer.getDefaultDb());
        }
        tableName.analyze(analyzer);
        InternalDatabaseUtil.checkDatabase(tableName.getDb(), ConnectContext.get());
        // check access
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(),
                        tableName.getTbl(), PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
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
