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

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a CREATE TABLE AS SELECT (CTAS) statement
 *  Syntax:
 *      CREATE TABLE table_name [( column_name_list )]
 *          opt_engine opt_partition opt_properties KW_AS query_stmt
 */
public class CreateTableAsSelectStmt extends DdlStmt {
    private final CreateTableStmt createTableStmt;
    private final List<String> columnNames;
    private QueryStmt queryStmt;
    
    public CreateTableAsSelectStmt(CreateTableStmt createTableStmt,
                                   List<String> columnNames, QueryStmt queryStmt) {
        this.createTableStmt = createTableStmt;
        this.columnNames = columnNames;
        this.queryStmt = queryStmt;
        // Insert is not currently supported
    }
    
    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // first: we analyze queryStmt before create table.
        // To avoid duplicate registrations of table/colRefs,
        // create a new root analyzer and clone the query statement for this initial pass.
        Analyzer dummyRootAnalyzer = new Analyzer(analyzer.getCatalog(), analyzer.getContext());
        QueryStmt tmpStmt = queryStmt.clone();
        tmpStmt.analyze(dummyRootAnalyzer);
        this.queryStmt = tmpStmt;
        ArrayList<Expr> resultExprs = getQueryStmt().getResultExprs();
        // TODO: support decimal
        for (Expr expr : resultExprs) {
            if (expr.getType().isDecimalV2()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNSUPPORTED_TYPE_IN_CTAS, expr.getType());
            }
        }
        if (columnNames != null && columnNames.size() != resultExprs.size()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COL_NUMBER_NOT_MATCH);
        }
    }
    
    public CreateTableStmt getCreateTableStmt() {
        return createTableStmt;
    }
    
    public List<String> getColumnNames() {
        return columnNames;
    }
    
    public QueryStmt getQueryStmt() {
        return queryStmt;
    }
}
