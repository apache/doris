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

package org.apache.doris.plugin.dialect.spark;

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.parser.ParserUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import org.apache.commons.lang3.StringUtils;

/**
 * Extends from {@link org.apache.doris.nereids.parser.LogicalPlanBuilder},
 * just focus on the difference between these query syntax.
 */
public class SparkSql3LogicalPlanBuilder extends LogicalPlanBuilder {
    // use a default alias name if not exists, keep the same name with spark-sql
    public static final String DEFAULT_TABLE_ALIAS = "__auto_generated_subquery_name";

    private final ParserContext parserContext;

    public SparkSql3LogicalPlanBuilder() {
        this.parserContext = new ParserContext(Dialect.SPARK_SQL);
    }

    @Override
    public LogicalPlan visitAliasedQuery(DorisParser.AliasedQueryContext ctx) {
        LogicalPlan plan = withTableAlias(visitQuery(ctx.query()), ctx.tableAlias());
        for (DorisParser.LateralViewContext lateralViewContext : ctx.lateralView()) {
            plan = withGenerate(plan, lateralViewContext);
        }
        return plan;
    }

    @Override
    public Expression visitFunctionCallExpression(DorisParser.FunctionCallExpressionContext ctx) {
        Expression expression = super.visitFunctionCallExpression(ctx);
        if (!(expression instanceof UnboundFunction)) {
            return expression;
        }
        UnboundFunction sourceFunction = (UnboundFunction) expression;
        Function transformedFunction = SparkSql3FnCallTransformers.getSingleton().transform(
                sourceFunction.getName(),
                sourceFunction.getArguments(),
                this.parserContext
        );
        if (transformedFunction == null) {
            return expression;
        }
        return transformedFunction;
    }

    private LogicalPlan withTableAlias(LogicalPlan plan, DorisParser.TableAliasContext ctx) {
        if (ctx.strictIdentifier() == null) {
            return plan;
        }
        return ParserUtils.withOrigin(ctx.strictIdentifier(), () -> {
            String alias = StringUtils.isEmpty(ctx.strictIdentifier().getText())
                        ? DEFAULT_TABLE_ALIAS : ctx.strictIdentifier().getText();
            if (null != ctx.identifierList()) {
                throw new ParseException("Do not implemented", ctx);
            }
            return new LogicalSubQueryAlias<>(alias, plan);
        });
    }
}
