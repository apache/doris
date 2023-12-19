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
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.spark.SparkSql3LogicalPlanBuilder;
import org.apache.doris.nereids.parser.trino.TrinoParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;

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
        return parseSQL(originStr, (LogicalPlanBuilder) null);
    }

    /**
     * ParseSQL with dialect.
     */
    public List<StatementBase> parseSQL(String sql, SessionVariable sessionVariable) {
        @Nullable ParseDialect.Dialect sqlDialect = ParseDialect.Dialect.getByName(sessionVariable.getSqlDialect());
        return parseSQLWithDialect(sql, sqlDialect, sessionVariable);
    }

    private List<StatementBase> parseSQL(String originStr, @Nullable LogicalPlanBuilder logicalPlanBuilder) {
        List<Pair<LogicalPlan, StatementContext>> logicalPlans = parseMultiple(originStr, logicalPlanBuilder);
        List<StatementBase> statementBases = Lists.newArrayList();
        for (Pair<LogicalPlan, StatementContext> parsedPlanToContext : logicalPlans) {
            statementBases.add(new LogicalPlanAdapter(parsedPlanToContext.first, parsedPlanToContext.second));
        }
        return statementBases;
    }

    private List<StatementBase> parseSQLWithDialect(String sql,
                                                    @Nullable ParseDialect.Dialect sqlDialect,
                                                    SessionVariable sessionVariable) {
        if (!Strings.isNullOrEmpty(Config.sql_convertor_service)) {
            // if sql convertor service is enabled, no need to parse sql again by specific dialect.
            return parseSQL(sql);
        }
        switch (sqlDialect) {
            case TRINO:
                final List<StatementBase> logicalPlans = TrinoParser.parse(sql, sessionVariable);
                if (CollectionUtils.isEmpty(logicalPlans)) {
                    return parseSQL(sql);
                }
                return logicalPlans;

            case SPARK_SQL:
                return parseSQL(sql, new SparkSql3LogicalPlanBuilder());

            default:
                return parseSQL(sql);
        }
    }

    /**
     * parse sql DSL string.
     *
     * @param sql sql string
     * @return logical plan
     */
    public LogicalPlan parseSingle(String sql) {
        return parseSingle(sql, null);
    }

    /**
     * parse sql DSL string.
     *
     * @param sql sql string
     * @return logical plan
     */
    public LogicalPlan parseSingle(String sql, @Nullable LogicalPlanBuilder logicalPlanBuilder) {
        return parse(sql, logicalPlanBuilder, DorisParser::singleStatement);
    }

    public List<Pair<LogicalPlan, StatementContext>> parseMultiple(String sql) {
        return parseMultiple(sql, null);
    }

    public List<Pair<LogicalPlan, StatementContext>> parseMultiple(String sql,
                                                                   @Nullable LogicalPlanBuilder logicalPlanBuilder) {
        return parse(sql, logicalPlanBuilder, DorisParser::multiStatements);
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
        return parse(sql, null, parseFunction);
    }

    private <T> T parse(String sql, @Nullable LogicalPlanBuilder logicalPlanBuilder,
                        Function<DorisParser, ParserRuleContext> parseFunction) {
        ParserRuleContext tree = toAst(sql, parseFunction);
        LogicalPlanBuilder realLogicalPlanBuilder = logicalPlanBuilder == null
                    ? new LogicalPlanBuilder() : logicalPlanBuilder;
        return (T) realLogicalPlanBuilder.visit(tree);
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
