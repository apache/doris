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
import com.baidu.palo.common.FeNameFormat;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.mysql.MysqlPassword;

import com.google.common.base.Strings;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

// this is memory struct from CREATE USER statement
// CREATE USER user_name [IDENTIFIED BY [PASSWORD] 'password']
//
// CREATE USER user_name
//      this clause create user without password
//      eg. CREATE USER 'jeffrey'
//
// CREATE USER user_name IDENTIFIED BY 'password'
//      this clause create user with password in plaintext mode
//      eg. CREATE USER 'jeffrey' IDENTIFIED BY 'mypass'
//
// CREATE USER user_name IDENTIFIED BY PASSWORD 'password'
//      this clause create user with password in hashed mode.
//      eg. CREATE USER 'jeffrey' IDENTIFIED BY PASSWORD '*90E462C37378CED12064BB3388827D2BA3A9B689'
public class CreateUserStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateUserStmt.class);

    private String user;
    private String password;
    private byte[] scramblePassword;
    private boolean isPlain;
    private boolean isSuperuser;

    public CreateUserStmt() {
    }

    public CreateUserStmt(UserDesc userDesc) {
        user = userDesc.getUser();
        password = userDesc.getPassword();
        isPlain = userDesc.isPlain();
    }

    public CreateUserStmt(UserDesc userDesc, boolean isSuperuser) {
        user = userDesc.getUser();
        password = userDesc.getPassword();
        isPlain = userDesc.isPlain();
        this.isSuperuser = isSuperuser;
    }

    public boolean isSuperuser() {
        return isSuperuser;
    }

    public byte[] getPassword() {
        return scramblePassword;
    }

    public String getUser() {
        return user;
    }

    private void checkUser() throws AnalysisException {
        if (Strings.isNullOrEmpty(user)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CANNOT_USER, "CREATE USER", user);
        }

        FeNameFormat.checkUserName(user);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        checkUser();
        // convert plain password to hashed password
        if (!Strings.isNullOrEmpty(password)) {
            // TODO(zhaochun): convert password
            if (isPlain) {
                // convert plain password to scramble
                scramblePassword = MysqlPassword.makeScrambledPassword(password);
            } else {
                scramblePassword = MysqlPassword.checkPassword(password);
            }
        } else {
            scramblePassword = new byte[0];
        }
        user = ClusterNamespace.getUserFullName(getClusterName(), user);
        // check authenticate
        if (isSuperuser) {
            // Only root can create superuser
            if (!analyzer.getCatalog().getUserMgr().isAdmin(analyzer.getUser())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_PERMISSION_TO_CREATE_USER, analyzer.getUser());
            }
        } else {
            if (!analyzer.getCatalog().getUserMgr().isSuperuser(analyzer.getUser())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_PERMISSION_TO_CREATE_USER, analyzer.getUser());
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE USER '").append(user).append("'");
        if (!Strings.isNullOrEmpty(password)) {
            if (isPlain) {
                stringBuilder.append(" IDENTIFIED BY '").append(password).append("'");
            } else {
                stringBuilder.append(" IDENTIFIED BY PASSWORD '").append(password).append("'");
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
