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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;

import com.google.common.base.Strings;

import java.util.Map;

// rename column
public class ColumnRenameClause extends AlterTableClause {
    private String colName;
    private String newColName;

    public ColumnRenameClause(String colName, String newColName) {
        super(AlterOpType.RENAME);
        this.colName = colName;
        this.newColName = newColName;
        this.needTableStable = false;
    }

    public String getColName() {
        return colName;
    }

    public String getNewColName() {
        return newColName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(colName)) {
            throw new AnalysisException("Column name is not set");
        }

        if (Strings.isNullOrEmpty(newColName)) {
            throw new AnalysisException("New column name is not set");
        }

        FeNameFormat.checkColumnName(newColName);
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return true;
    }

    @Override
    public String toSql() {
        return "RENAME COLUMN " + colName + " " + newColName;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
