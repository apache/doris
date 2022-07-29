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
import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Sql parser, convert sql DSL to logical plan.
 */
public class NereidsParser {
    private static final ParseErrorListener PARSE_ERROR_LISTENER = new ParseErrorListener();
    private static final PostProcessor POST_PROCESSOR = new PostProcessor();

    /**
     * In MySQL protocol, client could send multi-statement in.
     * a single packet.
     * https://dev.mysql.com/doc/internals/en/com-set-option.html
     */
    public List<StatementBase> parseSQL(String originStr) {
        List<LogicalPlan> logicalPlans = parseMultiple(originStr);
        List<StatementBase> statementBases = new ArrayList<>();
        for (LogicalPlan logicalPlan : logicalPlans) {
            // TODO: this is a trick to support explain. Since we do not support any other command in a short time.
            //     It is acceptable. In the future, we need to refactor this.
            if (logicalPlan instanceof ExplainCommand) {
                ExplainCommand explainCommand = (ExplainCommand) logicalPlan;
                LogicalPlan innerPlan = explainCommand.getLogicalPlan();
                LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(innerPlan);
                logicalPlanAdapter.setIsExplain(new ExplainOptions(
                        explainCommand.getLevel() == ExplainLevel.VERBOSE,
                        explainCommand.getLevel() == ExplainLevel.GRAPH));
                statementBases.add(logicalPlanAdapter);
            } else {
                statementBases.add(new LogicalPlanAdapter(logicalPlan));
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
        return (LogicalPlan) parse(sql, DorisParser::singleStatement);
    }

    public List<LogicalPlan> parseMultiple(String sql) {
        return (List<LogicalPlan>) parse(sql, DorisParser::multiStatements);
    }

    public Expression parseExpression(String expression) {
        return (Expression) parse(expression, DorisParser::expression);
    }

    private Object parse(String sql, Function<DorisParser, ParserRuleContext> parseFunction) {
        ParserRuleContext tree = toAst(sql, parseFunction);
        LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
        return logicalPlanBuilder.visit(tree);
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
