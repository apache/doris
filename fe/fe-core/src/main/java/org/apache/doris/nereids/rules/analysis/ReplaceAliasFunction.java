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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.CompareMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * replace alias function to builtin function expression
 */
public class ReplaceAliasFunction extends DefaultExpressionRewriter<CascadesContext> implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalProject().thenApply(ctx -> {
                    LogicalProject<?> project = ctx.root;
                    List<NamedExpression> exprs = project.getProjects().stream()
                            .map(ne -> ((NamedExpression) ne.accept(this, ctx.cascadesContext)))
                            .collect(Collectors.toList());
                    return project.withProjects(exprs);
                }).toRule(RuleType.REPLACE_ALIAS_FUNCTION)
        );
    }

    /**
     * In the rule, we change alias function to builtin function.
     * Firstly, when we find an unbound function and check whether it's an alias function.
     * Secondly, we ensure it is an alias function, it's a original planner style expression, we get its sql-style,
     * but the NereidsParser cannot parser an expression, so we parse a one-row relation sql like:
     *      select {the alias function}
     * Thirdly, handle the unbound function's children recursively by the two steps above.
     */

    @Override
    public Expression visitUnboundFunction(UnboundFunction function, CascadesContext context) {
        Database db = getDb(context);
        AliasFunction aliasFunction = getAliasFunction(function, db);
        Expr originalFunction = aliasFunction.getOriginFunction();
        if (!(originalFunction instanceof FunctionCallExpr)) {
            throw new AnalysisException(String.format("unsupported type of originalFunction in aliasFunction: %s",
                    originalFunction.getType()));
        }
        Expression expr = translateToNereidsFunction(((FunctionCallExpr) originalFunction));
        System.out.println(expr);
        return function;
    }

    private AliasFunction getAliasFunction(UnboundFunction function, Database database) {
        List<Type> types = function.getArgumentsTypes().stream()
                .map(AbstractDataType::toCatalogDataType).collect(Collectors.toList());
        Function desc = new Function(new FunctionName(database.getFullName(), function.getName()),
                types, Type.INVALID, false);
        AliasFunction udf;
        try {
            udf = ((AliasFunction) database.getFunction(desc, CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
        return udf;
    }

    private Database getDb(CascadesContext context) {
        Env env = context.getConnectContext().getEnv();
        String dbName = context.getConnectContext().getDatabase();
        return env.getInternalCatalog().getDbNullable(dbName);
    }

    private Expression translateToNereidsFunction(FunctionCallExpr function) {
        String functionSql = function.toSql();
        LogicalPlan containerPlan = new NereidsParser().parseSingle(String.format("select %s", functionSql));
        Preconditions.checkArgument(containerPlan instanceof UnboundOneRowRelation);
        return ((UnboundOneRowRelation) containerPlan).getProjects().get(0);
    }
}