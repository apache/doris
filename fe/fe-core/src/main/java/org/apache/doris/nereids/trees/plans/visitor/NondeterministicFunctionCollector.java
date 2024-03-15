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

import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Nondeterministic;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;

/**
 * Collect the nondeterministic expr in plan, these expressions will be put into context
 */
public class NondeterministicFunctionCollector
        extends DefaultPlanVisitor<Void, List<TreeNode<Expression>>> {

    public static final NondeterministicFunctionCollector INSTANCE
            = new NondeterministicFunctionCollector();

    @Override
    public Void visit(Plan plan, List<TreeNode<Expression>> collectedExpressions) {
        List<? extends Expression> expressions = plan.getExpressions();
        if (expressions.isEmpty()) {
            return super.visit(plan, collectedExpressions);
        }
        expressions.forEach(expression -> {
            collectedExpressions.addAll(expression.collect(Nondeterministic.class::isInstance));
        });
        return super.visit(plan, collectedExpressions);
    }
}
