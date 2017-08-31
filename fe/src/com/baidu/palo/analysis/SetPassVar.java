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
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.mysql.MysqlPassword;

import com.google.common.base.Strings;

public class SetPassVar extends SetVar {
    private String user;
    private String passwdParam;
    private byte[] passwdBytes;

    // The password in parameter is a hashed password.
    public SetPassVar(String user, String passwd) {
        this.user = user;
        this.passwdParam = passwd;
    }

    public String getUser() {
        return user;
    }

    public byte[] getPassword() {
        return passwdBytes;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(analyzer.getClusterName())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER);
        }
        if (Strings.isNullOrEmpty(user)) {
            user = analyzer.getUser();
        } else {
            user = ClusterNamespace.getFullName(analyzer.getClusterName(), user);
        }
        // Check password
        passwdBytes = MysqlPassword.checkPassword(passwdParam);
        // Check user
        if (!analyzer.getCatalog().getUserMgr().checkUserAccess(analyzer.getUser(), user)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_PASSWORD_NOT_ALLOWED);
        }
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        return "SET PASSWORD FOR '" + user + "' = '" + new String(passwdBytes) + "'";
    }
}
