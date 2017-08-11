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
import com.baidu.palo.load.LoadErrorHub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// FORMAT:
//   ALTER SYSTEM SET GLOBAL LOAD_ERROR_URL= "mysql://user:password@host:port[/database[/table]]"

public class AlterLoadErrorUrlClause extends AlterClause {
    private static final Logger LOG = LogManager.getLogger(AlterLoadErrorUrlClause.class);

    private static final String DEFAULT_TABLE = "load_errors";
    private String url;
    private LoadErrorHub.Param param;

    protected AlterLoadErrorUrlClause(String url) {
        this.url = url;
    }

    public LoadErrorHub.Param getParam() {
        return param;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        // only root can do it
        if (!analyzer.getCatalog().getUserMgr().isAdmin(analyzer.getUser())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    "SET LOAD_ERROR_URL");
        }

        this.param = LoadErrorHub.analyzeUrl(url);
    }


    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SYSTEM SET LOAD_ERROR_URL = \"");
        sb.append(url);
        sb.append("\"");
        return sb.toString();
    }

}
