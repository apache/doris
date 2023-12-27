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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

// Show create database statement
//  Syntax:
//      SHOW CREATE DATABASE db
public class ShowCreateDbStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Database", ScalarType.createVarchar(30)))
                    .build();

    private String db;

    public ShowCreateDbStmt(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(db)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, db);
        }

        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), db,
                PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
                                Privilege.ALTER_PRIV,
                                Privilege.CREATE_PRIV,
                                Privilege.DROP_PRIV),
                        Operator.OR))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                                                ConnectContext.get().getQualifiedUser(), db);
        }
    }

    @Override
    public String toSql() {
        return "SHOW CREATE DATABASE `" + db + "`";
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
