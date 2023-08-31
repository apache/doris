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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.CaseInsensitiveStream;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.parser.ParseErrorListener;
import org.apache.doris.nereids.parser.PostProcessor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

/**
 * This tool is used to use nereids to parse SQL statements.
 */
public class ParseSqlUtils {

    /**
     * parse sql to LogicalPlanAdapter
     * @param sql one sql
     * @return LogicalPlanAdapter
     */
    public static LogicalPlanAdapter parseSingleStringSql(String sql) {
        DorisLexer lexer = new DorisLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        DorisParser parser = new DorisParser(tokenStream);

        parser.addParseListener(new PostProcessor());
        parser.removeErrorListeners();
        parser.addErrorListener(new ParseErrorListener());

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parser.singleStatement();
        } catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.seek(0); // rewind input stream
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parser.singleStatement();
        }
        // use nereids to parse 'select...into outfile' sql
        // and get LogicalPlanAdapter
        StatementContext statementContext = new StatementContext();
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null) {
            connectContext.setStatementContext(statementContext);
            statementContext.setConnectContext(connectContext);
        }

        LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
        LogicalPlan statements = (LogicalPlan) logicalPlanBuilder.visit(tree);
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(statements, statementContext);
        logicalPlanAdapter.setOrigStmt(new OriginStatement(sql, 0));
        return logicalPlanAdapter;
    }
}
