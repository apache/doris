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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.NondeterministicFunctionCollector.FunctionCollectContext;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Collect the nondeterministic expr in plan, these expressions will be put into context
 */
public class NondeterministicFunctionCollector
        extends DefaultPlanVisitor<Void, FunctionCollectContext> {

    public static final NondeterministicFunctionCollector INSTANCE = new NondeterministicFunctionCollector();

    @Override
    public Void visit(Plan plan, FunctionCollectContext collectContext) {
        List<? extends Expression> expressions = plan.getExpressions();
        if (expressions.isEmpty()) {
            return super.visit(plan, collectContext);
        }
        for (Expression expression : expressions) {
            Set<Expression> nondeterministicFunctions =
                    expression.collect(expr -> {
                        boolean containsNondeterministic = !((ExpressionTrait) expr).isDeterministic();
                        if (!collectContext.getCollectExpressionTypes().isEmpty()) {
                            containsNondeterministic &= collectContext.getCollectExpressionTypes().stream()
                                    .anyMatch(type -> type.isAssignableFrom(expr.getClass()));
                        }
                        return containsNondeterministic;
                    });
            collectContext.addAllExpressions(nondeterministicFunctions);
        }
        return super.visit(plan, collectContext);
    }

    /**
     * Function collect context, collected expressions would be added to collectedExpressions,
     * the expression in whiteFunctionSet would not be collected
     */
    public static class FunctionCollectContext {
        private final List<Expression> collectedExpressions = new ArrayList<>();
        /**
         * Collected expression target type, if empty, will collect all nondeterministic expression
         * Such as both current_date() and (current_date() < 2023-01-01) would be collected
         * if collectExpressionTypes is (FunctionTrait.class), only current_date() would be collected
         */
        private final Set<Class<? extends ExpressionTrait>> collectExpressionTypes;

        private FunctionCollectContext(Set<Class<? extends ExpressionTrait>> collectExpressionTypes) {
            this.collectExpressionTypes = collectExpressionTypes;
        }

        public static FunctionCollectContext of(Set<Class<? extends ExpressionTrait>> targetExpressionTypes) {
            return new FunctionCollectContext(targetExpressionTypes);
        }

        public static FunctionCollectContext of() {
            return new FunctionCollectContext(ImmutableSet.of());
        }

        public void addExpression(Expression expression) {
            if (expression != null) {
                this.collectedExpressions.add(expression);
            }
        }

        public void addAllExpressions(Collection<? extends Expression> expressions) {
            if (expressions != null) {
                this.collectedExpressions.addAll(expressions);
            }
        }

        public List<Expression> getCollectedExpressions() {
            return collectedExpressions;
        }

        public Set<Class<? extends ExpressionTrait>> getCollectExpressionTypes() {
            return collectExpressionTypes;
        }
    }
}
