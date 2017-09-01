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

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.google.common.base.Strings;

import java.util.List;

// GRANT STMT
// grant privilege to some user, this is an administrator operation.
//
// GRANT privilege [, privilege] ON db_name TO user
public class GrantStmt extends DdlStmt {
    private String user;
    private String db;
    private List<AccessPrivilege> privileges;

    public GrantStmt(String user, String db, List<AccessPrivilege> privileges) {
        this.user = user;
        this.db = db;
        this.privileges = privileges;
    }

    public String getUser() {
        return user;
    }

    public String getDb() {
        return db;
    }

    public AccessPrivilege getPrivilege() {
        return AccessPrivilege.merge(privileges);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(user)) {
            throw new AnalysisException("No user in grant statement.");
        }
        if (Strings.isNullOrEmpty(db)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
        db = ClusterNamespace.getFullName(getClusterName(), db);
        user = ClusterNamespace.getFullName(getClusterName(), user);
        
        if (privileges == null || privileges.isEmpty()) {
            throw new AnalysisException("No privileges in grant statement.");
        }

        if (!analyzer.getCatalog().getUserMgr().checkUserAccess(analyzer.getUser(), user)) {
            throw new AnalysisException("No privilege to grant.");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT ");
        int idx = 0;
        for (AccessPrivilege privilege : privileges) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append(privilege);
            idx++;
        }
        sb.append(" ON ").append(db).append(" TO '").append(user).append("'");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
