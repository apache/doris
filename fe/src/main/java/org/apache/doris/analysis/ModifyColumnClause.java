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

import com.google.common.base.Strings;

import java.util.Map;

// modify one column
public class ModifyColumnClause extends AlterClause {
    private Column col;
    private ColumnPosition colPos;
    // which rollup is to be modify, if rollup is null, modify base table.
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

    public ModifyColumnClause(Column col, ColumnPosition colPos, String rollup,
                              Map<String, String> properties) {
        this.col = col;
        this.colPos = colPos;
        this.rollupName = rollup;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (col == null) {
            throw new AnalysisException("No column definition in modify column clause.");
        }
        col.analyze(true);
        if (colPos != null) {
            colPos.analyze();
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
        sb.append("MODIFY COLUMN ").append(col.toSql());
        if (colPos != null) {
            sb.append(" ").append(colPos);
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
