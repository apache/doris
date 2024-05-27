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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * We need this rule to cast all filter and join conjunct's return type to boolean after rewrite.
 */
public class AdjustConjunctsReturnType extends DefaultPlanRewriter<Void> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, null);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        filter = (LogicalFilter<? extends Plan>) super.visit(filter, context);
        Set<Expression> conjuncts = filter.getConjuncts().stream()
                .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                .collect(ImmutableSet.toImmutableSet());
        return filter.withConjuncts(conjuncts);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        join = (LogicalJoin<? extends Plan, ? extends Plan>) super.visit(join, context);
        List<Expression> hashConjuncts = join.getHashJoinConjuncts().stream()
                .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                .collect(Collectors.toList());
        List<Expression> otherConjuncts = join.getOtherJoinConjuncts().stream()
                .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                .collect(Collectors.toList());
        List<Expression> markConjuncts = join.getMarkJoinConjuncts().stream()
                .map(expr -> TypeCoercionUtils.castIfNotSameType(expr, BooleanType.INSTANCE))
                .collect(Collectors.toList());
        return join.withJoinConjuncts(hashConjuncts, otherConjuncts, markConjuncts, join.getJoinReorderContext());
    }
}
