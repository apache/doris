// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.analysis.CompoundPredicate.Operator;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.UserException;
import com.baidu.palo.mysql.privilege.PaloPrivilege;
import com.baidu.palo.mysql.privilege.PrivBitSet;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;

import com.google.common.base.Strings;

public class RecoverPartitionStmt extends DdlStmt {
    private TableName dbTblName;
    private String partitionName;

    public RecoverPartitionStmt(TableName dbTblName, String partitionName) {
        this.dbTblName = dbTblName;
        this.partitionName = partitionName;
    }

    public String getDbName() {
        return dbTblName.getDb();
    }

    public String getTableName() {
        return dbTblName.getTbl();
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        dbTblName.analyze(analyzer);
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbTblName.getDb(),
                                                                dbTblName.getTbl(),
                                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                               PaloPrivilege.CREATE_PRIV,
                                                                                               PaloPrivilege.ADMIN_PRIV),
                                                                                 Operator.OR))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "RECOVERY",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                dbTblName.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("RECOVER PARTITION ").append(partitionName).append(" FROM ");
        if (!Strings.isNullOrEmpty(getDbName())) {
            sb.append(getDbName()).append(".");
        }
        sb.append(getTableName());
        return sb.toString();
    }
}
