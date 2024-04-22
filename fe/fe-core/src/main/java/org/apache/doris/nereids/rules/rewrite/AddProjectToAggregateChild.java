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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;

/**
 * Rewrite aggregate function with length function to a seperated projection, which can accelerate when using pipeline
 * engine. Pipeline engine can not accelerate length function in aggregate function but can accelerate in projection.
 * <p>
 * agg(length()) ==> agg(alias)->project(length())
 */
public class AddProjectToAggregateChild extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().when(AddProjectToAggregateChild::containsLengthString).then(agg -> {
            List<NamedExpression> outputExpressions = agg.getOutputExpressions();
            Builder<NamedExpression> newOutputs
                    = ImmutableList.builderWithExpectedSize(outputExpressions.size());
            Builder<NamedExpression> projectOutputs = ImmutableList.builder();
            for (NamedExpression outputExpression : outputExpressions) {
                projectOutputs.add(outputExpression);
                NamedExpression newOutput = (NamedExpression) outputExpression.rewriteUp(expr -> {
                    if (expr instanceof Length) {
                        NamedExpression alias = new Alias(expr, ((BoundFunction) expr).getName());
                        projectOutputs.add(alias);
                        return alias.toSlot();
                    }
                    return expr;
                });
                newOutputs.add(newOutput);
            }
            LogicalProject project = new LogicalProject<>(projectOutputs.build(), agg.child());
            return agg.withAggOutputChild(newOutputs.build(), project);
        }).toRule(RuleType.ADD_AGG_PROJECT);
    }

    private static boolean containsLengthString(LogicalAggregate<Plan> agg) {
        for (NamedExpression ne : agg.getOutputExpressions()) {
            boolean needRewrite = ne.anyMatch(expr -> {
                if (expr instanceof AggregateFunction && expr.containsType(Length.class)
                        && !expr.containsType(Alias.class)) {
                    return true;
                }
                return false;
            });
            if (needRewrite) {
                return true;
            }
        }
        return false;
    }
}
