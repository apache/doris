// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.proc.ProcNodeInterface;
import com.baidu.palo.common.proc.ProcResult;
import com.baidu.palo.common.proc.ProcService;
import com.baidu.palo.qe.ShowResultSetMetaData;

// SHOW PROC statement. Used to show proc information, only admin can use.
public class ShowProcStmt extends ShowStmt {
    private String path;
    private ProcNodeInterface node;

    public ShowProcStmt(String path) {
        this.path = path;
    }

    public ProcNodeInterface getNode() {
        return node;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        if (!analyzer.getCatalog().getUserMgr().isAdmin(analyzer.getUser())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW PROC");
        }
        node = ProcService.getInstance().open(path);
        if (node == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PROC_PATH, path);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW PROC '").append(path).append("'");
        return sb.toString();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ProcResult result = null;
        try {
            result = node.fetchResult();
        } catch (AnalysisException e) {
            return builder.build();
        }

        for (String col : result.getColumnNames()) {
            builder.addColumn(new Column(col, ColumnType.createVarchar(30)));
        }
        return builder.build();
    }
}
