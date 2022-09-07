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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.Avg;
import org.apache.doris.nereids.trees.expressions.functions.Count;
import org.apache.doris.nereids.trees.expressions.functions.Max;
import org.apache.doris.nereids.trees.expressions.functions.Min;
import org.apache.doris.nereids.trees.expressions.functions.Substring;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.expressions.functions.WeekOfYear;
import org.apache.doris.nereids.trees.expressions.functions.Year;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.IntegerType;

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
            ),
            RuleType.BINDING_FILTER_FUNCTION.build(
               logicalFilter().then(filter -> {
                   List<Expression> predicates = bind(filter.getExpressions());
                   return new LogicalFilter<>(predicates.get(0), filter.child());
               })
            ),
            RuleType.BINDING_HAVING_FUNCTION.build(
                logicalHaving(logicalAggregate()).then(filter -> {
                    List<Expression> predicates = bind(filter.getExpressions());
                    return new LogicalHaving<>(predicates.get(0), filter.child());
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
            } else if (name.equalsIgnoreCase("count")) {
                List<Expression> arguments = unboundFunction.getArguments();
                if (arguments.size() > 1 || (arguments.size() == 0 && !unboundFunction.isStar())) {
                    return unboundFunction;
                }
                if (unboundFunction.isStar() || arguments.stream().allMatch(Expression::isConstant)) {
                    return new Count();
                }
                return new Count(unboundFunction.getArguments().get(0));
            } else if (name.equalsIgnoreCase("max")) {
                List<Expression> arguments = unboundFunction.getArguments();
                if (arguments.size() != 1) {
                    return unboundFunction;
                }
                return new Max(unboundFunction.getArguments().get(0));
            } else if (name.equalsIgnoreCase("min")) {
                List<Expression> arguments = unboundFunction.getArguments();
                if (arguments.size() != 1) {
                    return unboundFunction;
                }
                return new Min(unboundFunction.getArguments().get(0));
            } else if (name.equalsIgnoreCase("avg")) {
                List<Expression> arguments = unboundFunction.getArguments();
                if (arguments.size() != 1) {
                    return unboundFunction;
                }
                return new Avg(unboundFunction.getArguments().get(0));
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
            } else if (name.equalsIgnoreCase("year")) {
                List<Expression> arguments = unboundFunction.getArguments();
                if (arguments.size() != 1) {
                    return unboundFunction;
                }
                return new Year(unboundFunction.getArguments().get(0));
            } else if (name.equalsIgnoreCase("WeekOfYear")) {
                List<Expression> arguments = unboundFunction.getArguments();
                if (arguments.size() != 1) {
                    return unboundFunction;
                }
                return new WeekOfYear(unboundFunction.getArguments().get(0));
            }
            return unboundFunction;
        }

        @Override
        public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, Void context) {
            String funcOpName;
            if (arithmetic.getFuncName() == null) {
                funcOpName = String.format("%sS_%s", arithmetic.getTimeUnit(),
                        (arithmetic.getOp() == Operator.ADD) ? "ADD" : "SUB");
            } else {
                funcOpName = arithmetic.getFuncName();
            }

            Expression left = arithmetic.left();
            Expression right = arithmetic.right();

            if (!left.getDataType().isDateType()) {
                try {
                    left = left.castTo(DateTimeType.INSTANCE);
                } catch (Exception e) {
                    // ignore
                }
                if (!left.getDataType().isDateType() && !arithmetic.getTimeUnit().isDateTimeUnit()) {
                    left = arithmetic.left().castTo(DateType.INSTANCE);
                }
            }
            if (!right.getDataType().isIntType()) {
                right = right.castTo(IntegerType.INSTANCE);
            }
            return arithmetic.withFuncName(funcOpName).withChildren(ImmutableList.of(left, right));
        }
    }
}
