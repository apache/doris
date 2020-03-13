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

package org.apache.doris.common.util;

import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.AnalysisException;

import java.util.List;

// Utils about SQL parser
public class SqlParserUtils {

    // parse origin statement and get the first one.
    // Doris supports "multi-statement" protocol of MySQL, so when receiving the origin statement like:
    //      "select k1 from tbl;"
    // The parser will return a list with 2 elements: [SelectStmt, EmptyStmt]
    // In this case, we only need the first Stmt.
    public static StatementBase getFirstStmt(SqlParser parser) throws Exception {
        List<StatementBase> stmts = (List<StatementBase>) parser.parse().value;
        return stmts.get(0);
    }

    public static StatementBase getStmt(SqlParser parser, int idx) throws Exception {
        List<StatementBase> stmts = (List<StatementBase>) parser.parse().value;
        if (idx >= stmts.size()) {
            throw new AnalysisException("Invalid statement index: " + idx + ". size: " + stmts.size());
        }
        return stmts.get(idx);
    }

    // get all parsed statements as a list
    public static List<StatementBase> getMultiStmts(SqlParser parser) throws Exception {
        return (List<StatementBase>) parser.parse().value;
    }
}
