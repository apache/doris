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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Common interface for logical/physical Aggregate.
 */
public interface Aggregate<CHILD_TYPE extends Plan> extends UnaryPlan<CHILD_TYPE>, OutputPrunable {

    List<Expression> getGroupByExpressions();

    List<NamedExpression> getOutputExpressions();

    Aggregate<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput);

    @Override
    Aggregate<Plan> withChildren(List<Plan> children);

    @Override
    default Aggregate<CHILD_TYPE> pruneOutputs(List<NamedExpression> prunedOutputs) {
        return withAggOutput(prunedOutputs);
    }

    default Set<AggregateFunction> getAggregateFunctions() {
        return ExpressionUtils.collect(getOutputExpressions(), AggregateFunction.class::isInstance);
    }

    default Set<Expression> getDistinctArguments() {
        return getAggregateFunctions().stream()
                .filter(AggregateFunction::isDistinct)
                .flatMap(aggregateExpression -> aggregateExpression.getArguments().stream())
                .collect(ImmutableSet.toImmutableSet());
    }
}
