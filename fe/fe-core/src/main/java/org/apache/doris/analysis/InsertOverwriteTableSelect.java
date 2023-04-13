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

import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;

import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertOverwriteTableSelect extends DdlStmt {
    @Getter
    private final CreateTableLikeStmt createTableLikeStmt;

    @Getter
    private final InsertStmt insertStmt;
    @Getter
    private final AlterTableStmt alterTableStmt;
    @Getter
    private QueryStmt queryStmt;

    public InsertOverwriteTableSelect(CreateTableLikeStmt createTableLikeStmt, QueryStmt queryStmt) {
        this.createTableLikeStmt = createTableLikeStmt;
        this.insertStmt = new InsertStmt(createTableLikeStmt.getDbTbl(), queryStmt.clone());
        this.queryStmt = queryStmt;

        List<AlterClause> ops = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        properties.put("swap", "false");
        ops.add(new ReplaceTableClause(createTableLikeStmt.getDbTbl().getTbl(), properties));
        this.alterTableStmt = new AlterTableStmt(createTableLikeStmt.getExistedTable(), ops);
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // first: we analyze queryStmt before create table.
        // To avoid duplicate registrations of table/colRefs,
        // create a new root analyzer and clone the query statement for this initial pass.
        Analyzer dummyRootAnalyzer = new Analyzer(analyzer.getEnv(), analyzer.getContext());
        QueryStmt tmpStmt = queryStmt.clone();
        tmpStmt.analyze(dummyRootAnalyzer);
        this.queryStmt = tmpStmt;
        ArrayList<Expr> resultExprs = getQueryStmt().getResultExprs();
        List<String> columnNames = createTableLikeStmt.getColLabels();
        if (columnNames != null && columnNames.size() != resultExprs.size()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COL_NUMBER_NOT_MATCH);
        }
    }
}
