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

import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;

// reorder column
public class ReorderColumnsClause extends AlterTableClause {
    private List<String> columnsByPos;
    private String rollupName;

    private Map<String, String> properties;

    public List<String> getColumnsByPos() {
        return columnsByPos;
    }

    public String getRollupName() {
        return rollupName;
    }

    public ReorderColumnsClause(List<String> cols, String rollup, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.columnsByPos = cols;
        this.rollupName = rollup;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (columnsByPos == null || columnsByPos.isEmpty()) {
            throw new AnalysisException("No column in reorder columns clause.");
        }
        for (String col : columnsByPos) {
            if (Strings.isNullOrEmpty(col)) {
                throw new AnalysisException("Empty column in reorder columns.");
            }
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
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ORDER BY ");
        int idx = 0;
        for (String col : columnsByPos) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(col).append('`');
            idx++;
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
