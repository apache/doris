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

import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.Pair;
import com.baidu.palo.ha.FrontendNodeType;
import com.baidu.palo.system.SystemInfoService;

import org.apache.commons.lang.NotImplementedException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;

public class FrontendClause extends AlterClause {
    protected String hostPort;
    protected String host;
    protected int port;
    protected FrontendNodeType role;

    protected FrontendClause(String hostPort, FrontendNodeType role) {
        this.hostPort = hostPort;
        this.role = role;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!analyzer.getCatalog().getUserMgr().isAdmin(analyzer.getUser())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    "ADD/DROP OBSERVER/REPLICA");
        }

        Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort);
        this.host = pair.first;
        this.port = pair.second;
        Preconditions.checkState(!Strings.isNullOrEmpty(host));
    }

    @Override
    public String toSql() {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }

}
