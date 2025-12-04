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
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * count(distinct A) over(...)
 * =>
 * multi_distinct_count(A) over(...)
 */

public class DistinctWindowExpression extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalWindow()
                .then(this::rewrite)
                .toRule(RuleType.DISTINCT_WINDOW_EXPRESSION);
    }

    private Plan rewrite(LogicalWindow<Plan> window) {
        List<NamedExpression> newWindowExpressions = Lists.newArrayList();
        boolean windExprChanged = false;
        for (NamedExpression expr : window.getWindowExpressions()) {
            boolean converted = false;
            if (expr instanceof Alias) {
                Alias alias = (Alias) expr;
                if (alias.child() instanceof WindowExpression) {
                    WindowExpression windowExpression = (WindowExpression) expr.child(0);
                    if (windowExpression.getFunction() instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) windowExpression.getFunction();
                        Optional<AggregateFunction> multiDistinct = convertToMultiDistinctFunction(aggregateFunction);
                        if (multiDistinct.isPresent()) {
                            converted = true;
                            windExprChanged = true;
                            Alias newAlias = (Alias) alias.withChildren(
                                    windowExpression.withFunction(multiDistinct.get()));
                            newWindowExpressions.add(newAlias);
                        }
                    }
                }
            }
            if (!converted) {
                newWindowExpressions.add(expr);
            }
        }
        if (windExprChanged) {
            return window.withExpressionsAndChild(newWindowExpressions, window.child());
        }
        return null;
    }

    private Optional<AggregateFunction> convertToMultiDistinctFunction(AggregateFunction func) {
        if (func.isDistinct()) {
            if (func instanceof Count) {
                return Optional.of(new MultiDistinctCount(false, func.child(0)));
            } else if (func instanceof Sum) {
                return Optional.of(new MultiDistinctSum(false, ((Sum) func).child()));
            } else if (func instanceof GroupConcat) {
                return Optional.of(new MultiDistinctGroupConcat(false, func.children()));
            }
        }
        return Optional.empty();
    }
}
