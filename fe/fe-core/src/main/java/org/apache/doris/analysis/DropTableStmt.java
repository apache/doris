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
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.List;
import java.util.stream.Collectors;

// DROP TABLE
public class DropTableStmt extends DdlStmt {
    private boolean ifExists;
    private List<TableName> tableNames;
    private final boolean isView;
    private boolean forceDrop;
    private boolean isMaterializedView;

    public DropTableStmt(boolean ifExists, List<TableName> tableNames, boolean forceDrop) {
        this.ifExists = ifExists;
        this.tableNames = tableNames;
        this.isView = false;
        this.forceDrop = forceDrop;
    }

    public DropTableStmt(boolean ifExists, List<TableName> tableNames, boolean isView, boolean forceDrop) {
        this.ifExists = ifExists;
        this.tableNames = tableNames;
        this.isView = isView;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public List<TableName> getTableNames() {
        return tableNames;
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
        for (TableName tableName : tableNames) {
            if (Strings.isNullOrEmpty(tableName.getDb())) {
                tableName.setDb(analyzer.getDefaultDb());
            }
            tableName.analyze(analyzer);
            // disallow external catalog
            Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

            // check access
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), tableName.getDb(),
                    tableName.getTbl(), PrivPredicate.DROP)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP TABLE ")
                .append(tableNames.stream().map(tableName -> tableName.toSql()).collect(Collectors.joining(", ")));
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
