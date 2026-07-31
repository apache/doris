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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Default;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Rewrite DEFAULT(column) expression to the actual default value of the column.
 * This rule transforms:
 * 1. DEFAULT(column) with default value -> the actual default value expression
 * 2. DEFAULT(column) with no default value but nullable -> NULL
 * 3. DEFAULT(column) with no default value and not nullable -> error
 */
public class RewriteDefaultExpression implements ExpressionPatternRuleFactory {

    public static final RewriteDefaultExpression INSTANCE = new RewriteDefaultExpression();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Default.class)
                        .thenApply(RewriteDefaultExpression::rewrite)
                        .toRule(ExpressionRuleType.REWRITE_DEFAULT_EXPRESSION)
        );
    }

    private static Expression rewrite(ExpressionMatchingContext<Default> context) {
        Default defaultExpr = context.expr;
        Expression child = defaultExpr.child();

        if (!(child instanceof SlotReference)) {
            throw new AnalysisException("DEFAULT requires a column reference, but got: " + child.toSql());
        }

        SlotReference slotRef = (SlotReference) child;
        Optional<Column> columnOpt = slotRef.getOriginalColumn();
        if (!columnOpt.isPresent()) {
            throw new AnalysisException("Cannot find column information for DEFAULT("
                    + slotRef.getName() + ")");
        }

        Column column = columnOpt.get();
        DataType targetType = DataType.fromCatalogType(column.getType());
        if (column.isGeneratedColumn()) {
            throw new AnalysisException("DEFAULT cannot be used on generated column '"
                    + column.getName() + "'");
        }

        String defaultValueSql = column.getDefaultValueSql();
        if (defaultValueSql == null) {
            if (column.isAllowNull()) {
                return new NullLiteral(targetType);
            } else {
                throw new AnalysisException("Column '" + column.getName()
                        + "' has no default value and does not allow NULL or column is auto-increment");
            }
        }

        Expression defaultValueExpr = new NereidsParser().parseExpression(defaultValueSql);
        if (defaultValueExpr instanceof UnboundFunction) {
            CascadesContext cascadesContext = context.cascadesContext;
            LogicalPlan plan = (LogicalPlan) context.rewriteContext.plan.orElse(null);
            defaultValueExpr = ExpressionAnalyzer.analyzeFunction(plan, cascadesContext, defaultValueExpr);
        }

        return TypeCoercionUtils.castIfNotSameType(defaultValueExpr, targetType);
    }
}
