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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ShowResultSetMetaData;

// SHOW CREATE ROUTINE LOAD statement.
public class ShowCreateRoutineLoadStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Routine Load Id", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Routine Load Name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Routine Load", ScalarType.createVarchar(30)))
                    .build();

    private final LabelName labelName;

    private final boolean includeHistory;

    public ShowCreateRoutineLoadStmt(LabelName labelName, boolean includeHistory) {
        this.labelName = labelName;
        this.includeHistory = includeHistory;
    }

    public String getDb() {
        return labelName.getDbName();
    }

    public String getLabel() {
        return labelName.getLabelName();
    }

    public boolean isIncludeHistory() {
        return includeHistory;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        labelName.analyze(analyzer);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
