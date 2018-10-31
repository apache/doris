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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;

// add some columns to one index.
public class AddColumnsClause extends AlterClause {
    private List<Column> columns;
    private String rollupName;

    private Map<String, String> properties;

    public List<Column> getColumns() {
        return columns;
    }

    public String getRollupName() {
        return rollupName;
    }

    public AddColumnsClause(List<Column> columns, String rollupName, Map<String, String> properties) {
        this.columns = columns;
        this.rollupName = rollupName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (columns == null || columns.isEmpty()) {
            throw new AnalysisException("Columns is empty in add columns clause.");
        }
        for (Column colDef : columns) {
            colDef.analyze(true);

            if (false == colDef.isAllowNull() && colDef.getDefaultValue() == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DEFAULT_FOR_FIELD, colDef.getName());
            }
        }

        // Make sure return null if rollup name is empty.
        rollupName = Strings.emptyToNull(rollupName);
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD COLUMN (");
        int idx = 0;
        for (Column column : columns) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append(column.toSql());
            idx++;
        }
        sb.append(")");
        if (rollupName != null) {
            sb.append(" IN `").append(rollupName).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
