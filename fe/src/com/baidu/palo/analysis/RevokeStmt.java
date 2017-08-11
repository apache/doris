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

import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;

import com.google.common.base.Strings;

// REVOKE STMT
// revoke privilege from some user, this is an administrator operation.
//
// REVOKE ALL ON db_name FROM user
public class RevokeStmt extends DdlStmt {
    private String user;
    private String db;

    public RevokeStmt(String user, String db) {
        this.user = user;
        this.db = db;
    }

    public String getUser() {
        return user;
    }

    public String getDb() {
        return db;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(user)) {
            throw new AnalysisException("No user in grant statement.");
        }
        user = ClusterNamespace.getUserFullName(analyzer.getClusterName(), user);
        if (Strings.isNullOrEmpty(db)) {
            throw new AnalysisException("No database in grant statement.");
        }
        db = ClusterNamespace.getDbFullName(analyzer.getClusterName(), db);
        if (!analyzer.getCatalog().getUserMgr().checkUserAccess(analyzer.getUser(), user)) {
            throw new AnalysisException("No privilege to grant.");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REVOKE ALL ON ").append(db).append(" FROM '").append(user).append("'");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
