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

import org.apache.doris.analysis.ShowAlterStmt.AlterType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

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

    private List<Long> alterJobIdList;

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName) {
        this(alterType, dbTableName, null);
    }

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName, List<Long> alterJobIdList) {
        this.alterType = alterType;
        this.dbTableName = dbTableName;
        this.alterJobIdList = alterJobIdList;
    }

    public List<Long> getAlterJobIdList() {
        return alterJobIdList;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        dbTableName.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(dbTableName.getCtl(), this.getClass().getSimpleName());

        // check access
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbTableName.getCtl(), dbTableName.getDb(),
                        dbTableName.getTbl(),
                        PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "CANCEL ALTER TABLE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbTableName.getDb() + ": " + dbTableName.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CANCEL ALTER " + this.alterType);
        stringBuilder.append(" FROM " + dbTableName.toSql());
        if (!CollectionUtils.isEmpty(alterJobIdList)) {
            stringBuilder.append(" (").append(String.join(",", alterJobIdList.stream()
                    .map(String::valueOf).collect(Collectors.toList())));
            stringBuilder.append(")");
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CANCEL;
    }

}
