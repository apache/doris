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

import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Nondeterministic;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;

import java.util.List;

/**
 * This is the facade and factory for common plan visitor.
 */
public class PlanVisitors {
    /**
     * Collect the nondeterministic expr in plan, these expressions will be put into context
     */
    public static class NondeterministicCollector extends DefaultPlanVisitor<Void, List<Expression>> {

        public static final NondeterministicCollector INSTANCE = new NondeterministicCollector();
        @Override
        public Void visit(Plan plan, List<Expression> context) {
            List<? extends Expression> expressions = plan.getExpressions();
            if (expressions == null) {
                return super.visit(plan, context);
            }
            expressions.forEach(expression -> NondeterministicExprCollector.INSTANCE.visit(expression, context));
            return super.visit(plan, context);
        }
        private static final class NondeterministicExprCollector
                extends DefaultExpressionVisitor<Void, List<Expression>> {
            private static final NondeterministicExprCollector INSTANCE = new NondeterministicExprCollector();

            @Override
            public Void visit(Expression expr, List<Expression> context) {
                if (expr == null) {
                    return null;
                }
                if (expr instanceof Nondeterministic) {
                    context.add(expr);
                }
                return super.visit(expr, context);
            }
        }
    }

    /**
     * Collect the table in plan
     * Note: will not get table if table is eliminated by EmptyRelation in rewrite.
     */
    public static class TableCollector extends DefaultPlanVisitor<Void, List<Table>> {

        public static final TableCollector INSTANCE = new TableCollector();
        @Override
        public Void visit(Plan plan, List<Table> context) {
            if (plan instanceof OlapScan) {
                context.add(((OlapScan)plan).getTable());
            }
            return super.visit(plan, context);
        }
    }
}
