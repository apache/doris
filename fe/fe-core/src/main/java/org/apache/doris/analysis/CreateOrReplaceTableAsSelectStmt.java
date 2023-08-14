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

import org.apache.doris.common.UserException;

import java.util.List;

/**
 * Represents a CREATE OR REPLACE TABLE AS SELECT (CTAS) statement.
 * Syntax:
 * CREATE OR REPLACE TABLE table_name [( column_name_list )]
 * opt_engine opt_partition opt_properties KW_AS query_stmt
 */
public class CreateOrReplaceTableAsSelectStmt extends CreateTableAsSelectStmt {

    protected CreateOrReplaceTableAsSelectStmt(CreateTableStmt createTableStmt, List<String> columnNames,
                                               QueryStmt queryStmt) {
        super(createTableStmt, columnNames, queryStmt);
    }

    public CreateTableAsSelectStmt convertToCreateTableAsSelectStmt() {
        return new CreateTableAsSelectStmt(this.getCreateTableStmt(), this.getColumnNames(), this.getQueryStmt());
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        TableName dbTbl = this.getCreateTableStmt().getDbTbl();
        dbTbl.analyze(analyzer);
    }
}
