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

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.Substring;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * BindFunction.
 */
public class BindFunction implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.BINDING_PROJECT_FUNCTION.build(
                logicalProject().then(project -> {
                    List<NamedExpression> boundExpr = bind(project.getProjects());
                    return new LogicalProject<>(boundExpr, project.child());
                })
            ),
            RuleType.BINDING_AGGREGATE_FUNCTION.build(
                logicalAggregate().then(agg -> {
                    List<Expression> groupBy = bind(agg.getGroupByExpressions());
                    List<NamedExpression> output = bind(agg.getOutputExpressions());
                    return agg.withGroupByAndOutput(groupBy, output);
                })
            )
        );
    }

    private <E extends Expression> List<E> bind(List<E> exprList) {
        return exprList.stream()
            .map(FunctionBinder.INSTANCE::bind)
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
            if (name.equalsIgnoreCase("sum")) {
                List<Expression> arguments = unboundFunction.getArguments();
                if (arguments.size() != 1) {
                    return unboundFunction;
                }
                return new Sum(unboundFunction.getArguments().get(0));
            } else if (name.equalsIgnoreCase("substr") || name.equalsIgnoreCase("substring")) {

                List<Expression> arguments = unboundFunction.getArguments();
                if (arguments.size() == 2) {
                    return new Substring(unboundFunction.getArguments().get(0),
                            unboundFunction.getArguments().get(1));
                } else if (arguments.size() == 3) {
                    return new Substring(unboundFunction.getArguments().get(0), unboundFunction.getArguments().get(1),
                            unboundFunction.getArguments().get(2));
                }
                return unboundFunction;
            }
            return unboundFunction;
        }
    }
}
