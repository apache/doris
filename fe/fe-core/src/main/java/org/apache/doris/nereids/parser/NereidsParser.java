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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.plsql.PLSqlLogicalPlanBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.plugin.DialectConverterPlugin;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Sql parser, convert sql DSL to logical plan.
 */
public class NereidsParser {
    public static final Logger LOG = LogManager.getLogger(NereidsParser.class);
    private static final ParseErrorListener PARSE_ERROR_LISTENER = new ParseErrorListener();
    private static final PostProcessor POST_PROCESSOR = new PostProcessor();

    private static final BitSet EXPLAIN_TOKENS = new BitSet();

    static {
        EXPLAIN_TOKENS.set(DorisLexer.EXPLAIN);
        EXPLAIN_TOKENS.set(DorisLexer.PARSED);
        EXPLAIN_TOKENS.set(DorisLexer.ANALYZED);
        EXPLAIN_TOKENS.set(DorisLexer.LOGICAL);
        EXPLAIN_TOKENS.set(DorisLexer.REWRITTEN);
        EXPLAIN_TOKENS.set(DorisLexer.PHYSICAL);
        EXPLAIN_TOKENS.set(DorisLexer.OPTIMIZED);
        EXPLAIN_TOKENS.set(DorisLexer.PLAN);
        EXPLAIN_TOKENS.set(DorisLexer.PROCESS);

    }

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
        return parseSQLWithDialect(sql, sessionVariable);
    }

    /**
     * ParseSQL with logicalPlanBuilder.
     */
    public List<StatementBase> parseSQL(String originStr, @Nullable LogicalPlanBuilder logicalPlanBuilder) {
        List<Pair<LogicalPlan, StatementContext>> logicalPlans = parseMultiple(originStr, logicalPlanBuilder);
        List<StatementBase> statementBases = Lists.newArrayList();
        for (Pair<LogicalPlan, StatementContext> parsedPlanToContext : logicalPlans) {
            statementBases.add(new LogicalPlanAdapter(parsedPlanToContext.first, parsedPlanToContext.second));
        }
        return statementBases;
    }

    /**
     * scan to token
     * for example: select id from tbl return Tokens: ['select', 'id', 'from', 'tbl']
     */
    public static TokenSource scan(String sql) {
        return new DorisLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
    }

    /**
     * tryParseExplainPlan
     * @param sql sql
     * @return key: ExplainOptions, value: explain body
     */
    public static Optional<Pair<ExplainOptions, String>> tryParseExplainPlan(String sql) {
        try {
            TokenSource tokenSource = scan(sql);
            if (expect(tokenSource, DorisLexer.EXPLAIN) == null) {
                return Optional.empty();
            }

            Token token = readUntilNonComment(tokenSource);
            if (token == null) {
                return Optional.empty();
            }

            int tokenType = token.getType();
            ExplainLevel explainLevel = ExplainLevel.ALL_PLAN;
            if (tokenType == DorisLexer.PARSED) {
                explainLevel = ExplainLevel.PARSED_PLAN;
                token = readUntilNonComment(tokenSource);
            } else if (tokenType == DorisLexer.ANALYZED) {
                explainLevel = ExplainLevel.ANALYZED_PLAN;
                token = readUntilNonComment(tokenSource);
            } else if (tokenType == DorisLexer.LOGICAL || tokenType == DorisLexer.REWRITTEN) {
                explainLevel = ExplainLevel.REWRITTEN_PLAN;
                token = readUntilNonComment(tokenSource);
            } else if (tokenType == DorisLexer.PHYSICAL || tokenType == DorisLexer.OPTIMIZED) {
                explainLevel = ExplainLevel.OPTIMIZED_PLAN;
                token = readUntilNonComment(tokenSource);
            }

            if (token == null) {
                return Optional.empty();
            }
            tokenType = token.getType();
            if (tokenType != DorisLexer.PLAN) {
                return Optional.empty();
            }

            token = readUntilNonComment(tokenSource);
            Token explainPlanBody;
            boolean showPlanProcess = false;
            if (token.getType() == DorisLexer.PROCESS) {
                showPlanProcess = true;
                explainPlanBody = readUntilNonComment(tokenSource);
            } else {
                explainPlanBody = token;
            }

            if (explainPlanBody == null) {
                return Optional.empty();
            }
            ExplainOptions explainOptions = new ExplainOptions(explainLevel, showPlanProcess);
            return Optional.of(Pair.of(explainOptions, sql.substring(explainPlanBody.getStartIndex())));
        } catch (Throwable t) {
            return Optional.empty();
        }
    }

    private static Token expect(TokenSource tokenSource, int tokenType) {
        Token nextToken = readUntilNonComment(tokenSource);
        if (nextToken == null) {
            return null;
        }
        return nextToken.getType() == tokenType ? nextToken : null;
    }

    private static Token readUntilNonComment(TokenSource tokenSource) {
        Token token = tokenSource.nextToken();
        while (token != null) {
            int tokenType = token.getType();
            if (tokenType == DorisLexer.BRACKETED_COMMENT
                    || tokenType == DorisLexer.SIMPLE_COMMENT
                    || tokenType == DorisLexer.WS) {
                token = tokenSource.nextToken();
                continue;
            }
            break;
        }
        return token;
    }

    private List<StatementBase> parseSQLWithDialect(String sql,
                                                    SessionVariable sessionVariable) {
        @Nullable Dialect sqlDialect = Dialect.getByName(sessionVariable.getSqlDialect());
        if (sqlDialect == null) {
            return parseSQL(sql);
        }

        PluginMgr pluginMgr = Env.getCurrentEnv().getPluginMgr();
        List<DialectConverterPlugin> plugins = pluginMgr.getActiveDialectPluginList(sqlDialect);
        for (DialectConverterPlugin plugin : plugins) {
            try {
                List<StatementBase> statementBases = plugin.parseSqlWithDialect(sql, sessionVariable);
                if (CollectionUtils.isNotEmpty(statementBases)) {
                    return statementBases;
                }
            } catch (Throwable throwable) {
                LOG.warn("Parse sql with dialect {} failed, plugin: {}, sql: {}.",
                            sqlDialect, plugin.getClass().getSimpleName(), sql, throwable);
            }
        }

        if (ConnectContext.get().isRunProcedure()) {
            return parseSQL(sql, new PLSqlLogicalPlanBuilder());
        }
        // fallback if any exception occurs before
        return parseSQL(sql);
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

    public LogicalPlan parseForCreateView(String sql) {
        ParserRuleContext tree = toAst(sql, DorisParser::singleStatement);
        LogicalPlanBuilder realLogicalPlanBuilder = new LogicalPlanBuilderForCreateView();
        return (LogicalPlan) realLogicalPlanBuilder.visit(tree);
    }

    public Optional<String> parseForSyncMv(String sql) {
        ParserRuleContext tree = toAst(sql, DorisParser::singleStatement);
        LogicalPlanBuilderForSyncMv logicalPlanBuilderForSyncMv = new LogicalPlanBuilderForSyncMv();
        logicalPlanBuilderForSyncMv.visit(tree);
        return logicalPlanBuilderForSyncMv.getQuerySql();
    }

    /** toAst */
    public static ParserRuleContext toAst(String sql, Function<DorisParser, ParserRuleContext> parseFunction) {
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
