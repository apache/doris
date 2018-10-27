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

import com.baidu.palo.catalog.Column;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;

import com.google.common.base.Strings;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// clause which is used to add one column to
public class AddColumnClause extends AlterClause {
    private static final Logger LOG = LogManager.getLogger(AddColumnClause.class);
    private Column col;
    // Column position
    private ColumnPosition colPos;
    // if rollupName is null, add to column to base index.
    private String rollupName;

    private Map<String, String> properties;

    public Column getCol() {
        return col;
    }

    public ColumnPosition getColPos() {
        return colPos;
    }

    public String getRollupName() {
        return rollupName;
    }

    public AddColumnClause(Column col, ColumnPosition colPos, String rollupName,
                           Map<String, String> properties) {
        this.col = col;
        this.colPos = colPos;
        this.rollupName = rollupName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (col == null) {
            throw new AnalysisException("No column definition in add columnn clause.");
        }
        col.analyze(true);
        if (colPos != null) {
            colPos.analyze();
        }

        if (false == col.isAllowNull() && col.getDefaultValue() == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DEFAULT_FOR_FIELD, col.getName());
        }

        if (col.getAggregationType() != null && colPos != null && colPos.isFirst()) {
            throw new AnalysisException("Cannot add value column[" + col.getName() + "] at first");
        }

        if (Strings.isNullOrEmpty(rollupName)) {
            rollupName = null;
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD COLUMN ").append(col.toSql());
        if (colPos != null) {
            sb.append(" ").append(colPos.toSql());
        }
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
