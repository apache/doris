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

package org.apache.doris.nereids.trees.expressions.visitor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.doris.nereids.trees.expressions.*;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.*;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanTypeUtils;
import org.apache.doris.nereids.util.PlanUtils;

import java.util.*;
import java.util.stream.Collectors;
public class SubqueryContainmentChecker {
    // todo: change it to visitor model
    public static final SubqueryContainmentChecker INSTANCE = new SubqueryContainmentChecker();
    private final List<LogicalPlan> selfPlans = Lists.newArrayList();
    private final List<LogicalPlan> otherPlans = Lists.newArrayList();
    private final List<Expression> exceptConditions = Lists.newArrayList();
    private LogicalPlan selfTopLogicalPlan = null;
    private LogicalPlan otherTopLogicalPlan = null;
    private boolean isForSpj = false;
    private Map<Expression, Expression> otherToSelfSlotMap = Maps.newHashMap();

    public boolean check(SubqueryExpr self, SubqueryExpr other, boolean isForSpj) {
        init(self, other, isForSpj);
        if (!(selfTopLogicalPlan instanceof LogicalProject) ||
            !(otherTopLogicalPlan instanceof LogicalProject)) {
            return false;
        } else if (isForSpj) {
            return checkPlanType() &&
                checkFromList() &&
                checkJoinType() &&
                checkConditions();
        } else {
            return checkPlanType() &&
                checkFromList() &&
                checkJoinType() &&
                checkConditions() &&
                checkGroupBy() &&
                checkHaving() &&
                checkWindowFunctions() &&
                checkSelectItems() &&
                checkDistinct() &&
                checkLimit();
        }
    }
    public List<Expression> extractExceptExpressions(SubqueryExpr query, SubqueryExpr other, boolean isForSpj) {
        List<Expression> exceptExpressions = new ArrayList<>();
        boolean contains = check(query, other, isForSpj);
        if (contains) {
            // since has executed checkConditions
            exceptExpressions.addAll(exceptConditions);
        }
        return exceptExpressions;
    }
    private void init(SubqueryExpr self, SubqueryExpr other, boolean isForSpj) {
        this.selfPlans.addAll(self.getQueryPlan().collect(LogicalPlan.class::isInstance));
        this.otherPlans.addAll(other.getQueryPlan().collect(LogicalPlan.class::isInstance));
        this.selfTopLogicalPlan = self.getQueryPlan();
        this.otherTopLogicalPlan = other.getQueryPlan();
        this.isForSpj = isForSpj;
    }
    private boolean checkPlanType() {
        return this.isForSpj ? PlanTypeUtils.isSpj(selfPlans) && PlanTypeUtils.isSpj(otherPlans) :
            PlanTypeUtils.isSupportedPlan(selfPlans) && PlanTypeUtils.isSupportedPlan(otherPlans);
    }
    private boolean checkFromList() {
        // TODO: only basic table, exclude subquery
        List<LogicalRelation> selfTables = selfPlans.stream().filter(LogicalRelation.class::isInstance)
            .map(LogicalRelation.class::cast)
            .collect(Collectors.toList());
        List<LogicalRelation> otherTables = otherPlans.stream().filter(LogicalRelation.class::isInstance)
            .map(LogicalRelation.class::cast)
            .collect(Collectors.toList());

        List<Long> selfIds = selfTables.stream().map(node -> node.getTable().getId()).collect(Collectors.toList());
        List<Long> otherIds = otherTables.stream().map(node -> node.getTable().getId()).collect(Collectors.toList());
        if (Sets.newHashSet(selfIds).size() != selfIds.size() ||
            Sets.newHashSet(otherIds).size() != otherIds.size()) {
            return false;
        } else if (selfIds.size() != otherIds.size()) {
            return false;
        } else {
            otherToSelfSlotMap = PlanUtils.createSlotMapping(otherTables, selfTables);
            return selfIds.stream().allMatch(e -> otherIds.contains(e));
        }
    }

    private boolean checkJoinType() {
        // TODO: support other join type
        return selfPlans.stream()
                .filter(LogicalJoin.class::isInstance)
                .map(p -> (LogicalJoin<Plan, Plan>) p)
                .allMatch(e -> (e.getJoinType() == JoinType.INNER_JOIN && e.getOnClauseCondition().isPresent())) &&
                otherPlans.stream()
                .filter(LogicalJoin.class::isInstance)
                .map(p -> (LogicalJoin<Plan, Plan>) p)
                .allMatch(e -> (e.getJoinType() == JoinType.INNER_JOIN && e.getOnClauseCondition().isPresent()));
    }

    private boolean excludeSpecialFilter(LogicalFilter filter) {
        // exclude __DORIS_DELETE_SIGN__ == 0/1 predicate
        return !(filter.getConjuncts().size() == 1 &&
            filter.getConjuncts().iterator().next() instanceof EqualTo &&
            ((EqualTo) filter.getConjuncts().iterator().next()).child(0).getDataType().isTinyIntType() &&
            ((TinyIntLiteral)((EqualTo) filter.getConjuncts().iterator().next()).child(0)).getValue() == 0 &&
            ((EqualTo) filter.getConjuncts().iterator().next()).child(1).isSlot() &&
            ((SlotReference)((EqualTo) filter.getConjuncts().iterator().next()).child(1)).getName().equals("__DORIS_DELETE_SIGN__"));
    }
    private boolean checkConditions() {
        // TODO: check not contain subquery
        // TODO: check not contain OR
        List<LogicalFilter<Plan>> selfFilters = selfPlans.stream()
            .filter(LogicalFilter.class::isInstance)
            .filter(f -> excludeSpecialFilter((LogicalFilter) f))
            .map(p -> (LogicalFilter<Plan>) p).collect(Collectors.toList());
        List<LogicalFilter<Plan>> otherFilters = otherPlans.stream()
            .filter(LogicalFilter.class::isInstance)
            .filter(f -> excludeSpecialFilter((LogicalFilter) f))
            .map(p -> (LogicalFilter<Plan>) p).collect(Collectors.toList());

        if (selfFilters.size() != 1 || otherFilters.size() != 1) {
            return false;
        } else {
            Set<Expression> selfConjunctSet = Sets.newHashSet(selfFilters.get(0).getConjuncts());
            Set<Expression> otherConjunctSet = otherFilters.get(0).getConjuncts().stream()
                .map(e -> ExpressionUtils.replace(e, otherToSelfSlotMap))
                .collect(Collectors.toSet());
            Iterator<Expression> selfIterator = selfConjunctSet.iterator();
            while (selfIterator.hasNext()) {
                Expression selfExpr = selfIterator.next();
                Iterator<Expression> otherIterator = otherConjunctSet.iterator();
                while (otherIterator.hasNext()) {
                    Expression outerExpr = otherIterator.next();
                    if (ExpressionIdenticalChecker.INSTANCE.check(selfExpr, outerExpr)) {
                        selfIterator.remove();
                        otherIterator.remove();
                    }
                }
            }
            exceptConditions.addAll(otherConjunctSet);
            return selfConjunctSet.isEmpty();
        }
    }
    private boolean checkSelectItems() {
        return selfTopLogicalPlan instanceof LogicalProject &&
            otherTopLogicalPlan instanceof LogicalProject &&
            selfTopLogicalPlan.equals(otherTopLogicalPlan);
    }
    private boolean checkGroupBy() {
        return false;
    }
    private boolean checkHaving() {
        return false;
    }
    private boolean checkWindowFunctions() {
        return false;
    }
    private boolean checkDistinct() {
        return false;
    }
    private boolean checkLimit() {
        return false;
    }
}
