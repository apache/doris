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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.SubqueryContainmentChecker;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Combine subQueries having containment relationship.
 * Only support exists-notexists currently, the rest types will
 * be supported in the future.
 * Will put this rule into cost based transformation framework.
 */
public class SubqueryCombine extends DefaultPlanRewriter<JobContext> implements CustomRewriter {

    /**
     * Types of subquery combinations.
     */
    public enum CombinePattern {
        EXISTS_NOTEXISTS_COMBINE,
        EXISTS_EXISTS_COMBINE,
        NOTEXISTS_NOTEXISTS_COMBINE,
        UNKNOWN
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        boolean preCheck = doPreCheck((LogicalPlan) plan);
        return preCheck ? plan.accept(this, jobContext) : plan;
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, JobContext context) {
        CombinePattern pattern = getCombinePattern(filter);
        LogicalFilter newFilter = filter;
        switch (pattern) {
            case EXISTS_NOTEXISTS_COMBINE:
                Set<Expression> newConjuncts = new HashSet<>();
                Set<Expression> otherConjuncts = new HashSet<>();
                SubqueryExpr existsExpr = null;
                SubqueryExpr notExistsExpr = null;
                SubqueryContainmentChecker checker = new SubqueryContainmentChecker();
                for (Expression expression : filter.getConjuncts()) {
                    if (isExists(expression)) {
                        existsExpr = (SubqueryExpr) expression;
                    } else if (isNotExists(expression)) {
                        notExistsExpr = (SubqueryExpr) expression;
                    } else {
                        otherConjuncts.add(expression);
                    }
                }
                boolean isValidForCombine = checkValidForSuqueryCombine(existsExpr, notExistsExpr, pattern, checker);
                if (isValidForCombine) {
                    Exists newExists = doSubqueryCombine(existsExpr, checker);
                    newConjuncts.addAll(otherConjuncts);
                    newConjuncts.add(newExists);
                    newFilter = new LogicalFilter<>(newConjuncts, filter.child());
                }
                break;
            case EXISTS_EXISTS_COMBINE:
            case NOTEXISTS_NOTEXISTS_COMBINE:
                // TODO: support exists-exists and exists-notexists pattern
                break;
            case UNKNOWN:
            default:
                break;
        }
        return newFilter;
    }

    private boolean doPreCheck(LogicalPlan plan) {
        // ALL pre-checking can be put here
        return checkAllTablesUnderUKModel(plan);
    }

    private CombinePattern getCombinePattern(LogicalFilter filter) {
        if (checkMatchExistNotExists(filter.getConjuncts())) {
            return CombinePattern.EXISTS_NOTEXISTS_COMBINE;
        } else if (checkMatchExistsExists(filter.getConjuncts())) {
            return CombinePattern.EXISTS_EXISTS_COMBINE;
        } else if (checkMatchNotExistsNotExists(filter.getConjuncts())) {
            return CombinePattern.NOTEXISTS_NOTEXISTS_COMBINE;
        } else {
            return CombinePattern.UNKNOWN;
        }
    }

    /**
     * Check all plan accessed tables are all in unique key model.
     */
    private boolean checkAllTablesUnderUKModel(LogicalPlan plan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(plan.collect(LogicalPlan.class::isInstance));
        List<LogicalRelation> tables = plans.stream().filter(LogicalRelation.class::isInstance)
                .map(LogicalRelation.class::cast)
                .collect(Collectors.toList());
        return tables.stream().filter(LogicalOlapScan.class::isInstance)
                .allMatch(f -> ((LogicalOlapScan) f).getTable().getKeysType() == KeysType.UNIQUE_KEYS
                && ((LogicalOlapScan) f).getTable().getKeysNum() != 0);
    }

    private boolean checkValidForSuqueryCombine(SubqueryExpr existsExpr, SubqueryExpr notExistsExpr,
                                                CombinePattern pattern, SubqueryContainmentChecker checker) {
        if (pattern == CombinePattern.EXISTS_NOTEXISTS_COMBINE) {
            // spj checking
            boolean existIsSpj = existsExpr.isSpj();
            boolean notExistIsSpj = notExistsExpr.isSpj();
            if (!existIsSpj || !notExistIsSpj) {
                return false;
            }
            Set<Expression> existsConjuncts = existsExpr.getSpjPredicate();
            Set<Expression> notExistsConjuncts = notExistsExpr.getSpjPredicate();
            if (existsConjuncts.isEmpty() || notExistsConjuncts.isEmpty()) {
                return false;
            }
            // check correlated filter to make sure semi-inner transformation goes inner-gby pattern
            Map<Boolean, List<Expression>> existsSplit = Utils.splitCorrelatedConjuncts(
                    existsConjuncts, existsExpr.getCorrelateExpressions());
            Map<Boolean, List<Expression>> notExistsSplit = Utils.splitCorrelatedConjuncts(
                    notExistsConjuncts, notExistsExpr.getCorrelateExpressions());
            if (!existsExpr.getCorrelateSlots().equals(notExistsExpr.getCorrelateSlots())) {
                return false;
            }
            List<Expression> existsCorrelatedPredicate = existsSplit.get(true);
            List<Expression> notExistsCorrelatedPredicate = notExistsSplit.get(true);
            boolean existsConNonEqual = checkContainNonEqualCondition(existsCorrelatedPredicate);
            boolean notExistsConNonEqual = checkContainNonEqualCondition(notExistsCorrelatedPredicate);
            if (!existsConNonEqual || !notExistsConNonEqual) {
                return false;
            } else {
                return checker.check(existsExpr, notExistsExpr, true);
            }
        } else if (pattern == CombinePattern.EXISTS_EXISTS_COMBINE) {
            return false;
        } else if (pattern == CombinePattern.NOTEXISTS_NOTEXISTS_COMBINE) {
            return false;
        } else {
            return false;
        }
    }

    private boolean checkContainNonEqualCondition(List<Expression> predicateList) {
        boolean containNonEqualCondition = false;
        for (Expression expr : predicateList) {
            if (!(expr instanceof EqualTo)) {
                containNonEqualCondition = true;
                break;
            }
        }
        return containNonEqualCondition;
    }

    private Expression getExceptConditions(SubqueryContainmentChecker checker) {
        // column has been replaced based on the column-mapping
        List<Expression> exceptConditions = checker.getExceptConditions();
        return ExpressionUtils.and(exceptConditions);
    }

    private Expression buildCaseWhenExpr(Expression filter) {
        List<WhenClause> whenClauses = new ArrayList<>();
        WhenClause whenClause = new WhenClause(filter, new TinyIntLiteral((byte) 1));
        Expression defaultOperand = new TinyIntLiteral((byte) 0);
        whenClauses.add(whenClause);
        return new CaseWhen(whenClauses, defaultOperand);
    }

    private Exists doSubqueryCombine(SubqueryExpr existsExpr, SubqueryContainmentChecker checker) {
        Expression exceptFilters = getExceptConditions(checker);
        // build having (sum(case when except_filters then 1 else 0 end) = 0)
        Expression caseWhen = buildCaseWhenExpr(exceptFilters);
        Expression sumEqualsZero = new EqualTo(new Sum(caseWhen), new BigIntLiteral(0));
        LogicalPlan oldPlan = existsExpr.getQueryPlan();
        LogicalHaving havingPlan = new LogicalHaving(ImmutableSet.of(sumEqualsZero), oldPlan);
        return new Exists(havingPlan, existsExpr.getCorrelateSlots(), existsExpr.getTypeCoercionExpr(),
                          ExpressionUtils.optionalAnd(sumEqualsZero), false);
    }

    private boolean isExists(Expression expr) {
        return expr instanceof Exists && !((Exists) expr).isNot();
    }

    private boolean isNotExists(Expression expr) {
        return expr instanceof Exists && ((Exists) expr).isNot();
    }

    private boolean checkMatchExistNotExists(Set<Expression> conjuncts) {
        Set<Exists> existsSet = new HashSet<>();
        Set<Exists> notExistsSet = new HashSet<>();
        for (Expression expr : conjuncts) {
            if (isExists(expr)) {
                existsSet.add((Exists) expr);
            } else if (isNotExists(expr)) {
                notExistsSet.add((Exists) expr);
            }
        }
        return existsSet.size() == 1 && notExistsSet.size() == 1;
    }

    private boolean checkMatchExistsExists(Set<Expression> conjuncts) {
        return false;
    }

    private boolean checkMatchNotExistsNotExists(Set<Expression> conjuncts) {
        return false;
    }
}


