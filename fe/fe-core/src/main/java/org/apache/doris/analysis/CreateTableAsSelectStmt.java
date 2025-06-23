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

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Represents a CREATE TABLE AS SELECT (CTAS) statement.
 * Syntax:
 * CREATE TABLE table_name [( column_name_list )]
 * opt_engine opt_partition opt_properties KW_AS query_stmt
 */
@Deprecated
public class CreateTableAsSelectStmt extends DdlStmt implements NotFallbackInParser {

    @Getter
    private final CreateTableStmt createTableStmt;

    @Getter
    private final List<String> columnNames;

    @Getter
    private QueryStmt queryStmt;

    @Getter
    private final InsertStmt insertStmt;

    /**
     * If the table has already exists, set this flag to true.
     */
    @Setter
    @Getter
    private boolean tableHasExists = false;

    protected CreateTableAsSelectStmt(CreateTableStmt createTableStmt,
                                      List<String> columnNames, QueryStmt queryStmt) {
        this.createTableStmt = createTableStmt;
        this.columnNames = columnNames;
        this.queryStmt = queryStmt;
        this.insertStmt = new NativeInsertStmt(createTableStmt.getDbTbl(), null, null,
                queryStmt, null, columnNames, true);
    }

    /**
     * Cannot analyze insertStmt because the table has not been created yet.
     */
    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // no use
    }

    @Override
    public void reset() {
        super.reset();
        queryStmt.reset();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
