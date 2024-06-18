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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * SHOW CREATE MATERIALIZED VIEW mv_name ON table_name.
 **/
@AllArgsConstructor
@Getter
public class ShowCreateMaterializedViewStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(255)))
                    .addColumn(new Column("ViewName", ScalarType.createVarchar(255)))
                    .addColumn(new Column("CreateStmt", ScalarType.createVarchar(65535)))
                    .build();

    private String mvName;

    private TableName tableName;

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE MATERIALIZED",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName.toSql());
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SHOW CREATE MATERIALIZED VIEW ");
        stringBuilder.append("`").append(mvName).append("` ");
        stringBuilder.append("ON ").append(tableName.toSql());
        return stringBuilder.toString();
    }
}
