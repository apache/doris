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

import org.apache.doris.nereids.trees.expressions.Any;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Nondeterministic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.NondeterministicFunctionCollector.FunctionCollectContext;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Collect the nondeterministic expr in plan, these expressions will be put into context
 */
public class NondeterministicFunctionCollector
        extends DefaultPlanVisitor<Void, FunctionCollectContext> {

    public static final NondeterministicFunctionCollector INSTANCE = new NondeterministicFunctionCollector();
    public static final ImmutableSet<Expression> WHITE_FUNCTION_LIST = ImmutableSet.of(
            new Now(), new Now(Any.INSTANCE), new CurrentDate(), new UnixTimestamp(Any.INSTANCE, Any.INSTANCE),
            new UnixTimestamp(Any.INSTANCE));

    @Override
    public Void visit(Plan plan, FunctionCollectContext collectContext) {
        List<? extends Expression> expressions = plan.getExpressions();
        if (expressions.isEmpty()) {
            return super.visit(plan, collectContext);
        }
        for (Expression shuttledExpression : expressions) {
            Set<Nondeterministic> nondeterministicFunctions = shuttledExpression.collect(
                    Nondeterministic.class::isInstance);
            for (Nondeterministic function : nondeterministicFunctions) {
                if (WHITE_FUNCTION_LIST.stream()
                        .anyMatch(whiteFunction -> Any.equals(whiteFunction, (Expression) function))) {
                    continue;
                }
                collectContext.addExpression((Expression) function);
            }
        }
        return super.visit(plan, collectContext);
    }

    public static class FunctionCollectContext {
        private final List<Expression> collectedExpressions = new LinkedList<>();
        private final Set<Expression> whiteFunctionSet = new HashSet<>();

        private FunctionCollectContext(Set<Expression> whiteFunctionSet) {
            this.whiteFunctionSet.addAll(whiteFunctionSet);
        }

        public static FunctionCollectContext of(Set<Expression> whiteListFunctionSet) {
            if (whiteListFunctionSet != null) {
                return new FunctionCollectContext(whiteListFunctionSet);
            }
            return new FunctionCollectContext(ImmutableSet.of());
        }

        public Set<Expression> getWhiteFunctionSet() {
            return whiteFunctionSet;
        }

        public void addExpression(Expression expression) {
            if (expression != null) {
                this.collectedExpressions.add(expression);
            }
        }

        public List<Expression> getCollectedExpressions() {
            return collectedExpressions;
        }
    }
}
