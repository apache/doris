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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.EmptyStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.List;

// Utils about SQL parser
public class SqlParserUtils {

    private static final Logger LOG = LogManager.getLogger(SqlParserUtils.class);

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
        List<StatementBase> stmts = (List<StatementBase>) parser.parse().value;
        /*
         * When user execute query by some client library such as python MysqlDb, if user execute like:
         * 
         *      "select * from tbl1;"  (with a comma at the end of statement)
         *      
         * The sql parser will produce 2 statements: SelectStmt and EmptyStmt.
         * Here we discard the second EmptyStmt to make it act like one single statement.
         * This is for some compatibility. Because in python MysqlDb, if the first SelectStmt results in
         * some warnings, it will try to execute a 'SHOW WARNINGS' statement right after the SelectStmt,
         * but before the execution of EmptyStmt. So there will be an exception:
         * 
         *      (2014, "Commands out of sync; you can't run this command now")
         * 
         * I though it is a flaw of python MysqlDb.
         * However, in order to maintain the consistency of user use, here we remove all EmptyStmt
         * at the end to prevent errors.(Leave at least one statement)
         * 
         * But if user execute statements like:
         * 
         *      "select * from tbl1;;select 2"
         *      
         * If first "select * from tbl1" has warnings, python MysqlDb will still throw exception.
         */
        while (stmts.size() > 1 && stmts.get(stmts.size() - 1) instanceof EmptyStmt) {
            stmts.remove(stmts.size() - 1);
        }
        return stmts;
    }

    public static StatementBase parseAndAnalyzeStmt(String originStmt, ConnectContext ctx) throws UserException {
        LOG.info("begin to parse stmt: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase;
        try {
            statementBase = SqlParserUtils.getFirstStmt(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            LOG.error("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        } catch (Exception e) {
            String errorMsg = String.format("get exception when parse stmt. Origin stmt is %s . Error msg is %s.",
                    originStmt, e.getMessage());
            throw new AnalysisException(errorMsg);
        }
        statementBase.analyze(analyzer);
        return statementBase;
    }
}
