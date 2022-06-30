// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * BindFunction.
 */
public class BindFunction implements AnalysisRuleFactory {
    @Override
    public List<Rule<Plan>> buildRules() {
        return ImmutableList.of(
            RuleType.BINDING_PROJECT_FUNCTION.build(
                logicalProject().then(project -> {
                    List<NamedExpression> boundExpr = bind(project.operator.getProjects());
                    LogicalProject op = new LogicalProject(boundExpr);
                    return plan(op, project.child());
                })
            ),
            RuleType.BINDING_AGGREGATE_FUNCTION.build(
                logicalAggregate().then(agg -> {
                    List<Expression> groupBy = bind(agg.operator.getGroupByExprList());
                    List<NamedExpression> output = bind(agg.operator.getOutputExpressionList());
                    LogicalAggregate op = new LogicalAggregate(groupBy, output);
                    return plan(op, agg.child());
                })
            )
        );
    }

    private <E extends Expression> List<E> bind(List<E> exprList) {
        return exprList.stream()
            .map(expr -> FunctionBinder.INSTANCE.bind(expr))
            .collect(Collectors.toList());
    }

    private static class FunctionBinder extends DefaultExpressionRewriter<Void> {
        public static final FunctionBinder INSTANCE = new FunctionBinder();

        public <E extends Expression> E bind(E expression) {
            return (E) expression.accept(this, null);
        }

        @Override
        public Expression visitUnboundFunction(UnboundFunction unboundFunction, Void context) {
            String name = unboundFunction.getName();
            // TODO: lookup function in the function registry
            if (!name.equalsIgnoreCase("sum")) {
                return unboundFunction;
            }

            List<Expression> arguments = unboundFunction.getArguments();
            if (arguments.size() != 1) {
                return unboundFunction;
            }
            return new Sum(unboundFunction.getArguments().get(0));
        }
    }
}
