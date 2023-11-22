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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.UnsupportedDialectException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.trino.LogicalPlanTrinoBuilder;
import org.apache.doris.nereids.parser.trino.TrinoParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Sql parser, convert sql DSL to logical plan.
 */
public class NereidsParser {
    public static final Logger LOG = LogManager.getLogger(NereidsParser.class);
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
            statementBases.add(new LogicalPlanAdapter(parsedPlanToContext.first, parsedPlanToContext.second));
        }
        return statementBases;
    }

    /**
     * ParseSQL with dialect.
     */
    public List<StatementBase> parseSQL(String sql, SessionVariable sessionVariable) {
        if (ParseDialect.TRINO_395.getDialect().getDialectName()
                .equalsIgnoreCase(sessionVariable.getSqlDialect())) {
            return parseSQLWithDialect(sql, sessionVariable);
        } else {
            return parseSQL(sql);
        }
    }

    private List<StatementBase> parseSQLWithDialect(String sql, SessionVariable sessionVariable) {
        final List<StatementBase> logicalPlans = new ArrayList<>();
        try {
            io.trino.sql.parser.StatementSplitter splitter = new io.trino.sql.parser.StatementSplitter(sql);
            ParserContext parserContext = new ParserContext(ParseDialect.TRINO_395);
            StatementContext statementContext = new StatementContext();
            for (io.trino.sql.parser.StatementSplitter.Statement statement : splitter.getCompleteStatements()) {
                Object parsedPlan = parseSingleWithDialect(statement.statement(), parserContext);
                logicalPlans.add(parsedPlan == null
                        ? null : new LogicalPlanAdapter((LogicalPlan) parsedPlan, statementContext));
            }
        } catch (io.trino.sql.parser.ParsingException | UnsupportedDialectException e) {
            LOG.debug("Failed to parse logical plan from trino, sql is :{}", sql, e);
            return parseSQL(sql);
        }
        if (logicalPlans.isEmpty() || logicalPlans.stream().anyMatch(Objects::isNull)) {
            return parseSQL(sql);
        }
        return logicalPlans;
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

    public DataType parseDataType(String dataType) {
        return parse(dataType, DorisParser::dataType);
    }

    public Map<String, String> parseProperties(String properties) {
        return parse(properties, DorisParser::propertyItemList);
    }

    private <T> T parse(String sql, Function<DorisParser, ParserRuleContext> parseFunction) {
        ParserRuleContext tree = toAst(sql, parseFunction);
        LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder();
        return (T) logicalPlanBuilder.visit(tree);
    }

    /**
     * Parse dialect sql.
     *
     * @param sql sql string
     * @param parserContext parse context
     * @return logical plan
     */
    public <T> T parseSingleWithDialect(String sql, ParserContext parserContext) {
        if (ParseDialect.TRINO_395.equals(parserContext.getParserDialect())) {
            io.trino.sql.tree.Statement statement = TrinoParser.parse(sql);
            return (T) new LogicalPlanTrinoBuilder().visit(statement, parserContext);
        } else {
            LOG.debug("Failed to parse logical plan, the dialect name is {}, version is {}",
                    parserContext.getParserDialect().getDialect().getDialectName(),
                    parserContext.getParserDialect().getVersion());
            throw new UnsupportedDialectException(parserContext.getParserDialect());
        }
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
