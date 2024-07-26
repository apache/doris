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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

/*
  Stop routine load job by name

  syntax:
      STOP ROUTINE LOAD [database.]name
 */
public class StopRoutineLoadStmt extends DdlStmt {

    private final LabelName labelName;

    public StopRoutineLoadStmt(LabelName labelName) {
        this.labelName = labelName;
    }

    public String getName() {
        return labelName.getLabelName();
    }

    public String getDbFullName() {
        return labelName.getDbName();
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        labelName.analyze(analyzer);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.STOP;
    }
}
