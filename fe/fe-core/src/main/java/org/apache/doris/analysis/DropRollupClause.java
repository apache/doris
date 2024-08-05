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

import java.util.Map;

// Delete one rollup from table
public class DropRollupClause extends AlterTableClause {
    private final String rollupName;
    private Map<String, String> properties;

    public DropRollupClause(String rollupName, Map<String, String> properties) {
        super(AlterOpType.DROP_ROLLUP);
        this.rollupName = rollupName;
        this.properties = properties;
        this.needTableStable = false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(rollupName)) {
            throw new AnalysisException("No rollup in delete rollup.");
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public boolean allowOpMTMV() {
        return true;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP ROLLUP ");
        stringBuilder.append("`").append(rollupName).append("`");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String getRollupName() {
        return rollupName;
    }
}
