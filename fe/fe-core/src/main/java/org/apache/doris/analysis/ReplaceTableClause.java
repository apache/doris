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
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.base.Strings;

import java.util.Map;

// clause which is used to replace table
// eg:
// ALTER TABLE tbl REPLACE WITH TABLE tbl2;
public class ReplaceTableClause extends AlterTableClause {
    private String tblName;
    private Map<String, String> properties;

    // parsed from properties.
    // if false, after replace, there will be only one table exist with.
    // if true, the new table and the old table will be exchanged.
    // default is true.
    private boolean swapTable;

    public ReplaceTableClause(String tblName, Map<String, String> properties) {
        super(AlterOpType.REPLACE_TABLE);
        this.tblName = tblName;
        this.properties = properties;
    }

    public String getTblName() {
        return tblName;
    }

    public boolean isSwapTable() {
        return swapTable;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(tblName)) {
            throw new AnalysisException("No table specified");
        }

        this.swapTable = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_SWAP_TABLE, true);

        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
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
        return true;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE WITH TABLE ").append(tblName);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
