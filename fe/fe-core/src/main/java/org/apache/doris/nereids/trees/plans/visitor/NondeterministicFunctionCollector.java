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
import org.apache.doris.nereids.trees.expressions.functions.FunctionTrait;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;
import java.util.Set;

/**
 * Collect the nondeterministic expr in plan, these expressions will be put into context
 */
public class NondeterministicFunctionCollector
        extends DefaultPlanVisitor<Void, List<Expression>> {

    public static final NondeterministicFunctionCollector INSTANCE = new NondeterministicFunctionCollector();

    @Override
    public Void visit(Plan plan, List<Expression> collectedExpressions) {
        List<? extends Expression> expressions = plan.getExpressions();
        if (expressions.isEmpty()) {
            return super.visit(plan, collectedExpressions);
        }
        for (Expression expression : expressions) {
            Set<Expression> nondeterministicFunctions =
                    expression.collect(expr -> !((ExpressionTrait) expr).isDeterministic()
                            && expr instanceof FunctionTrait);
            collectedExpressions.addAll(nondeterministicFunctions);
        }
        return super.visit(plan, collectedExpressions);
    }
}
