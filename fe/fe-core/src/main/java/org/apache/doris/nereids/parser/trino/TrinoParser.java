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

package org.apache.doris.nereids.parser.trino;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.UnsupportedDialectException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.ParseDialect;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Trino Parser, depends on 395 trino-parser, and 4.9.3 antlr-runtime
 */
public class TrinoParser {

    public static final Logger LOG = LogManager.getLogger(TrinoParser.class);

    private static final io.trino.sql.parser.ParsingOptions PARSING_OPTIONS = new io.trino.sql.parser.ParsingOptions(
                io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    /**
     * Parse with trino syntax, return null if parse failed
     */
    public static @Nullable List<StatementBase> parse(String sql, SessionVariable sessionVariable) {
        final List<StatementBase> logicalPlans = new ArrayList<>();
        try {
            io.trino.sql.parser.StatementSplitter splitter = new io.trino.sql.parser.StatementSplitter(
                        addDelimiterIfNeeded(sql));
            ParserContext parserContext = new ParserContext(ParseDialect.TRINO_395);
            StatementContext statementContext = new StatementContext();
            for (io.trino.sql.parser.StatementSplitter.Statement statement : splitter.getCompleteStatements()) {
                Object parsedPlan = parseSingle(statement.statement(), parserContext);
                logicalPlans.add(parsedPlan == null
                        ? null : new LogicalPlanAdapter((LogicalPlan) parsedPlan, statementContext));
            }
            if (logicalPlans.isEmpty() || logicalPlans.stream().anyMatch(Objects::isNull)) {
                return null;
            }
            return logicalPlans;
        } catch (io.trino.sql.parser.ParsingException | UnsupportedDialectException e) {
            LOG.debug("Failed to parse logical plan from trino, sql is :{}", sql, e);
            return null;
        }
    }

    private static io.trino.sql.tree.Statement parse(String sql) {
        io.trino.sql.parser.SqlParser sqlParser = new io.trino.sql.parser.SqlParser();
        return sqlParser.createStatement(sql, PARSING_OPTIONS);
    }

    /**
     * Parse trino dialect sql.
     *
     * @param sql sql string
     * @param parserContext parse context
     * @return logical plan
     */
    public static <T> T parseSingle(String sql, ParserContext parserContext) {
        Preconditions.checkArgument(parserContext.getParserDialect() == ParseDialect.TRINO_395);
        io.trino.sql.tree.Statement statement = TrinoParser.parse(sql);
        return (T) new TrinoLogicalPlanBuilder().visit(statement, parserContext);
    }

    /**
     * {@link io.trino.sql.parser.StatementSplitter} use ";" as the delimiter if not set
     * So add ";" if sql does not end with ";",
     * otherwise {@link io.trino.sql.parser.StatementSplitter#getCompleteStatements()} will return empty list
     */
    private static String addDelimiterIfNeeded(String sql) {
        if (!sql.trim().endsWith(";")) {
            return sql + ";";
        }
        return sql;
    }
}
