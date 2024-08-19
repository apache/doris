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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * LogicalPlanBuilderForSyncMv
 */
public class LogicalPlanBuilderForSyncMv extends LogicalPlanBuilder {
    private Optional<String> querySql;

    public LogicalPlanBuilderForSyncMv(Map<Integer, ParserRuleContext> selectHintMap) {
        super(selectHintMap);
    }

    @Override
    public Expression visitFunctionCallExpression(DorisParser.FunctionCallExpressionContext ctx) {
        Expression expression = super.visitFunctionCallExpression(ctx);
        if (expression instanceof UnboundFunction) {
            return ((UnboundFunction) expression)
                    .withIndexInSqlString(Optional.of(new UnboundFunction.FunctionIndexInSql(
                            ctx.functionIdentifier().functionNameIdentifier().start.getStartIndex(),
                            ctx.functionIdentifier().functionNameIdentifier().stop.getStopIndex(),
                            ctx.stop.getStopIndex())));
        } else {
            return expression;
        }
    }

    @Override
    public LogicalPlan visitQuery(DorisParser.QueryContext ctx) {
        LogicalPlan logicalPlan = super.visitQuery(ctx);
        PlanUtils.OutermostPlanFinderContext outermostPlanFinderContext =
                new PlanUtils.OutermostPlanFinderContext();
        logicalPlan.accept(PlanUtils.OutermostPlanFinder.INSTANCE, outermostPlanFinderContext);

        // find outermost logicalAggregate to rewrite agg_state related function
        Plan outermostAgg = outermostPlanFinderContext.outermostPlan;
        while (!(outermostAgg instanceof LogicalAggregate)) {
            if (!outermostAgg.children().isEmpty()) {
                outermostAgg = outermostAgg.child(0);
            } else {
                break;
            }
        }
        String originSql = getOriginSql(ctx);
        if (outermostAgg instanceof LogicalAggregate) {
            List<NamedExpression> outputs = ((LogicalAggregate) outermostAgg).getOutputs();
            TreeMap<Pair<Integer, Integer>, String> indexInSqlToString =
                    new TreeMap<>(new Pair.PairComparator<>());
            AggStateFunctionFinder aggStateFunctionFinder =
                    new AggStateFunctionFinder(ctx.start.getStartIndex());
            for (Expression expr : outputs) {
                aggStateFunctionFinder.find(expr, indexInSqlToString);
            }
            querySql = Optional.of(rewriteSql(originSql, indexInSqlToString));
        } else {
            querySql = Optional.of(originSql);
        }
        return logicalPlan;
    }

    @Override
    public CreateMTMVCommand visitCreateMTMV(DorisParser.CreateMTMVContext ctx) {
        visitQuery(ctx.query());
        return null;
    }

    public Optional<String> getQuerySql() {
        return querySql;
    }

    private static class AggStateFunctionFinder
            extends DefaultExpressionRewriter<TreeMap<Pair<Integer, Integer>, String>> {
        private int sqlBeginIndex;

        private FunctionRegistry functionRegistry;

        public AggStateFunctionFinder(int sqlBeginIndex) {
            this.sqlBeginIndex = sqlBeginIndex;
            this.functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
        }

        public Expression find(Expression expression,
                TreeMap<Pair<Integer, Integer>, String> indexInSqlToNewString) {
            return expression.accept(this, indexInSqlToNewString);
        }

        @Override
        public Expression visitUnboundFunction(UnboundFunction unboundFunction,
                TreeMap<Pair<Integer, Integer>, String> indexInSqlToNewString) {
            if (unboundFunction.getFunctionIndexInSql().isPresent()) {
                // try bind agg function
                List<Object> arguments = unboundFunction.isDistinct()
                        ? ImmutableList.builder().add(unboundFunction.isDistinct())
                                .addAll(unboundFunction.getArguments()).build()
                        : (List) unboundFunction.getArguments();

                String functionName = unboundFunction.getName();
                FunctionBuilder builder = functionRegistry
                        .findFunctionBuilder(unboundFunction.getDbName(), functionName, arguments);
                if (builder instanceof BuiltinFunctionBuilder) {
                    BoundFunction boundFunction =
                            (BoundFunction) builder.build(functionName, arguments).first;
                    if (boundFunction instanceof AggregateFunction) {
                        // rewrite to agg_state
                        UnboundFunction.FunctionIndexInSql functionIndexInSql = unboundFunction
                                .getFunctionIndexInSql().get().indexInQueryPart(sqlBeginIndex);
                        functionName = boundFunction.getName();
                        switch (functionName) {
                            case "min":
                            case "max":
                            case "sum":
                            case "count":
                            case "bitmap_union":
                            case "hll_union": {
                                // no need rewrite
                                break;
                            }
                            default: {
                                indexInSqlToNewString.put(
                                        Pair.of(functionIndexInSql.functionNameBegin,
                                                functionIndexInSql.functionNameEnd),
                                        String.format("%s%s(%s%s", functionName,
                                                AggCombinerFunctionBuilder.UNION_SUFFIX,
                                                functionName,
                                                AggCombinerFunctionBuilder.STATE_SUFFIX));
                                indexInSqlToNewString
                                        .put(Pair.of(functionIndexInSql.functionExpressionEnd,
                                                functionIndexInSql.functionExpressionEnd), "))");
                                break;
                            }
                        }
                    }
                }
            }
            return unboundFunction;
        }
    }

    private static String rewriteSql(String querySql,
            Map<Pair<Integer, Integer>, String> indexStringSqlMap) {
        StringBuilder builder = new StringBuilder();
        int beg = 0;
        for (Map.Entry<Pair<Integer, Integer>, String> entry : indexStringSqlMap.entrySet()) {
            Pair<Integer, Integer> index = entry.getKey();
            builder.append(querySql, beg, index.first);
            builder.append(entry.getValue());
            beg = index.second + 1;
        }
        builder.append(querySql, beg, querySql.length());
        return builder.toString();
    }
}
