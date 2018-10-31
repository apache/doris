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

import com.baidu.palo.analysis.CompoundPredicate.Operator;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.UserException;
import com.baidu.palo.mysql.privilege.PaloPrivilege;
import com.baidu.palo.mysql.privilege.PrivBitSet;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

// Show create database statement
//  Syntax:
//      SHOW CREATE DATABASE db
public class ShowCreateDbStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Create Database", ColumnType.createVarchar(30)))
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
        db = ClusterNamespace.getFullName(getClusterName(), db);

        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), db,
                                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV,
                                                                                              PaloPrivilege.ALTER_PRIV,
                                                                                              PaloPrivilege.CREATE_PRIV,
                                                                                              PaloPrivilege.DROP_PRIV),
                                                                                Operator.OR))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED,
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
