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

package org.apache.doris.nereids.parser;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisSqlSeparatorLexer;
import org.apache.doris.nereids.DorisSqlSeparatorParser;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Sql parser, convert sql DSL to logical plan.
 */
public class NereidsParser {
    private static final ParseErrorListener PARSE_ERROR_LISTENER = new ParseErrorListener();
    private static final PostProcessor POST_PROCESSOR = new PostProcessor();

    /**
     * In MySQL protocol, client could send multi-statement in a single packet.
     * see <a href="https://dev.mysql.com/doc/internals/en/com-set-option.html">docs</a> for more information.
     */
    public List<StatementBase> parseSQL(String originStr) {
        List<Pair<LogicalPlan, StatementContext>> logicalPlans = parseMultiple(originStr);
        List<StatementBase> statementBases = Lists.newArrayList();
        for (Pair<LogicalPlan, StatementContext> parsedPlanToContext : logicalPlans) {
            // TODO: this is a trick to support explain. Since we do not support any other command in a short time.
            //     It is acceptable. In the future, we need to refactor this.
            StatementContext statementContext = parsedPlanToContext.second;
            if (parsedPlanToContext.first instanceof ExplainCommand) {
                ExplainCommand explainCommand = (ExplainCommand) parsedPlanToContext.first;
                LogicalPlan innerPlan = explainCommand.getLogicalPlan();
                LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(innerPlan, statementContext);
                ExplainLevel explainLevel = explainCommand.getLevel();
                ExplainOptions explainOptions = new ExplainOptions(explainLevel);
                logicalPlanAdapter.setIsExplain(explainOptions);
                statementBases.add(logicalPlanAdapter);
            } else {
                statementBases.add(new LogicalPlanAdapter(parsedPlanToContext.first, statementContext));
            }
        }
        return statementBases;
    }

    /**
     * parse sql DSL string.
     *
     * @param sql sql string
     * @return logical plan
     */
    public LogicalPlan parseSingle(String sql) {
        return parse(sql, DorisParser::singleStatement);
    }

    public List<Pair<LogicalPlan, StatementContext>> parseMultiple(String sql) {
        return parse(sql, DorisParser::multiStatements);
    }

    public Expression parseExpression(String expression) {
        return parse(expression, DorisParser::expression);
    }

    /**
     * split sql to multi singleStmts.
     *
     * @param sql sql string
     * @return singleStmt list
     */
    public List<String> parseMultiStmts(String sql) {
        DorisSqlSeparatorLexer lexer = new DorisSqlSeparatorLexer(CharStreams.fromString(sql));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        org.apache.doris.nereids.DorisSqlSeparatorParser parser = new DorisSqlSeparatorParser(tokenStream);
        ParserRuleContext tree;

        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parser.statements();
        } catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.seek(0);
            parser.reset();

            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parser.statement();
        }

        DorisSqlSeparatorParser.StatementsContext stmt = (DorisSqlSeparatorParser.StatementsContext) tree;
        List<String> singleStmtList = Lists.newArrayList();
        for (DorisSqlSeparatorParser.StatementContext statementContext : stmt.statement()) {
            if (!isEmptySql(statementContext)) {
                singleStmtList.add(statementContext.getText());
            }
        }

        return Collections.unmodifiableList(singleStmtList);
    }

    private boolean isEmptySql(DorisSqlSeparatorParser.StatementContext statementContext) {
        if (statementContext.children == null) {
            return true;
        }
        for (ParseTree child : statementContext.children) {
            if (!(child instanceof DorisSqlSeparatorParser.CommentContext)
                    && !(child instanceof DorisSqlSeparatorParser.WsContext)) {
                return false;
            }
        }
        return true;
    }

    private <T> T parse(String sql, Function<DorisParser, ParserRuleContext> parseFunction) {
        ParserRuleContext tree = toAst(sql, parseFunction);
        LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
        return (T) logicalPlanBuilder.visit(tree);
    }

    private ParserRuleContext toAst(String sql, Function<DorisParser, ParserRuleContext> parseFunction) {
        DorisLexer lexer = new DorisLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        DorisParser parser = new DorisParser(tokenStream);

        parser.addParseListener(POST_PROCESSOR);
        parser.removeErrorListeners();
        parser.addErrorListener(PARSE_ERROR_LISTENER);

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parseFunction.apply(parser);
        } catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.seek(0); // rewind input stream
            parser.reset();

            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parseFunction.apply(parser);
        }
        return tree;
    }
}
