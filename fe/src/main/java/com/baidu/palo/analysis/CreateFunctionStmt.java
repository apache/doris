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
import com.baidu.palo.common.UserException;

import java.util.List;
import java.util.Map;

/**
 * Created by zhaochun on 14-7-30.
 */
public class CreateFunctionStmt extends StatementBase {
    private final FunctionName                            functionName;
    private final List<com.baidu.palo.catalog.ColumnType> argumentType;
    private final com.baidu.palo.catalog.ColumnType       returnType;
    private final String                                  soFilePath;
    private final Map<String, String>                     properties;
    private final boolean                                 isAggregate;

    public CreateFunctionStmt(FunctionName functionName,
      List<com.baidu.palo.catalog.ColumnType> argumentType,
      com.baidu.palo.catalog.ColumnType returnType, String soFilePath,
      Map<String, String> properties, boolean isAggregate) {
        this.functionName = functionName;
        this.argumentType = argumentType;
        this.returnType = returnType;
        this.soFilePath = soFilePath;
        this.properties = properties;
        this.isAggregate = isAggregate;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE ");
        if (isAggregate) {
            stringBuilder.append("AGGREGATE ");
        }
        stringBuilder.append("FUNCTION ");
        stringBuilder.append(functionName.toString());
        stringBuilder.append("(");
        int i = 0;
        for (com.baidu.palo.catalog.ColumnType type : argumentType) {
            if (i != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(type.toString());
            i++;
        }
        stringBuilder.append(") RETURNS ");
        stringBuilder.append(returnType.toString());
        stringBuilder.append(" SONAME ");
        stringBuilder.append(soFilePath);
        if (properties.size() > 0) {
            stringBuilder.append(" PROPERTIES (");
            i = 0;
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (i != 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append(entry.getKey());
                stringBuilder.append("=");
                stringBuilder.append(entry.getValue());
                i++;
            }
            stringBuilder.append(")");

        }
        return stringBuilder.toString();
    }
    
    @Override 
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
