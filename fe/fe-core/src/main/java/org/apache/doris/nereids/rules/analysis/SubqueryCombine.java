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

import com.google.common.collect.Lists;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.*;
import org.apache.doris.nereids.trees.expressions.literal.*;
import org.apache.doris.nereids.trees.expressions.visitor.SubqueryContainmentChecker;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.*;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.util.Utils;

import java.util.*;
import java.util.stream.Collectors;

public class SubqueryCombine extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    public enum CombinePattern {
        EXISTS_NOTEXISTS_COMBINE,
        EXISTS_EXISTS_COMBINE,
        NOTEXISTS_NOTEXISTS_COMBINE,
        UNKNOWN
    };

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
                for (Expression expression : filter.getConjuncts()) {
                    if (isExists(expression)) {
                        existsExpr = (SubqueryExpr) expression;
                    } else if (isNotExists(expression)) {
                        notExistsExpr = (SubqueryExpr) expression;
                    } else {
                        otherConjuncts.add(expression);
                    }
                }
                boolean isValidForCombine = checkValidForSuqueryCombine(existsExpr, notExistsExpr, pattern);
                if (isValidForCombine) {
                    Exists newExists = doSubqueryCombine(existsExpr, notExistsExpr);
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
        return checkAllTablesUnderUKModel(plan) &&
               checkPlanPattern(plan);
    }
    private CombinePattern getCombinePattern(LogicalFilter filter) {
        if (verifyMatchExistNotExists(filter.getConjuncts())) {
            return CombinePattern.EXISTS_NOTEXISTS_COMBINE;
        } else if (verifyMatchExistsExists(filter.getConjuncts())) {
            return CombinePattern.EXISTS_EXISTS_COMBINE;
        } else if (verifyMatchNotExistsNotExists(filter.getConjuncts())) {
            return CombinePattern.NOTEXISTS_NOTEXISTS_COMBINE;
        } else {
            return CombinePattern.UNKNOWN;
        }
    }
    private boolean checkAllTablesUnderUKModel(LogicalPlan plan) {
        List<LogicalPlan> plans = Lists.newArrayList();
        plans.addAll(plan.collect(LogicalPlan.class::isInstance));
        List<LogicalRelation> selfTables = plans.stream().filter(LogicalRelation.class::isInstance)
            .map(LogicalRelation.class::cast)
            .collect(Collectors.toList());
        return selfTables.stream().filter(LogicalOlapScan.class::isInstance)
            .allMatch(f -> ((LogicalOlapScan) f).getTable().getKeysType() == KeysType.UNIQUE_KEYS &&
                ((LogicalOlapScan) f).getTable().getKeysNum() != 0);
    }
    private boolean checkPlanPattern(LogicalPlan plan) {
        return true;
    }
    private boolean checkValidForSuqueryCombine(SubqueryExpr existsExpr, SubqueryExpr notExistsExpr, CombinePattern pattern) {
        // spj checking
        boolean existIsSpj = existsExpr.isSpj();
        boolean notExistIsSpj = notExistsExpr.isSpj();
        if (existIsSpj || notExistIsSpj) {
            return false;
        }
        Set<Expression> existsConjuncts = existsExpr.getSpjPredicate();
        Set<Expression> notExistsConjuncts = notExistsExpr.getSpjPredicate();

        // check correlated filter checking to make sure semi-inner transformation is inner-gby pattern
        Map<Boolean, List<Expression>> existsSplit = Utils.splitCorrelatedConjuncts(
            existsConjuncts, existsExpr.getCorrelateExpressions());
        Map<Boolean, List<Expression>> notExistsSplit = Utils.splitCorrelatedConjuncts(
            notExistsConjuncts, notExistsExpr.getCorrelateExpressions());
        if (!existsExpr.getCorrelateSlots().equals(notExistsExpr.getCorrelateSlots())) {
            return false;
        }
        List<Expression> existsCorrelatedPredicate = existsSplit.get(true);
        List<Expression> notExistsCorrelatedPredicate = notExistsSplit.get(true);
        boolean existsConNonEqual = checkContainNonEqual(existsCorrelatedPredicate);
        boolean notExistsConNonEqual = checkContainNonEqual(notExistsCorrelatedPredicate);
        if (!existsConNonEqual || !notExistsConNonEqual) {
            return false;
        } else {
            if (pattern == CombinePattern.EXISTS_NOTEXISTS_COMBINE) {
                SubqueryContainmentChecker checker = new SubqueryContainmentChecker();
                return checker.check(existsExpr, notExistsExpr, true);
            } else if (pattern == CombinePattern.EXISTS_EXISTS_COMBINE) {
                return false;
            } else if (pattern == CombinePattern.NOTEXISTS_NOTEXISTS_COMBINE) {
                return false;
            } else {
                return false;
            }
        }
    }
    private boolean checkContainNonEqual(List<Expression> predicateList) {
        boolean containNonEqual = false;
        for (Expression expr : predicateList) {
            if (!(expr instanceof EqualTo)) {
                containNonEqual = true;
                break;
            }
        }
        return containNonEqual;
    }

    private Expression exceptExpressions(SubqueryExpr existsExpr, SubqueryExpr notExistsExpr) {
        // replace column based on the column-mapping
        SubqueryContainmentChecker checker = new SubqueryContainmentChecker();
        List<Expression> exceptExprs = checker.extractExceptExpressions(existsExpr, notExistsExpr, true);
        // TODO: check the having filters are uncorrelated predicates
        return ExpressionUtils.and(exceptExprs);
    }
    private Expression buildCaseWhenExpr(Expression filter) {
        List<WhenClause> whenClauses = new ArrayList<>();
        WhenClause whenClause = new WhenClause(filter, new TinyIntLiteral((byte) 1));
        Expression defaultOperand = new TinyIntLiteral((byte) 0);
        whenClauses.add(whenClause);
        return new CaseWhen(whenClauses, defaultOperand);
    }
    private Exists doSubqueryCombine(SubqueryExpr existsExpr, SubqueryExpr notExistsExpr) {
        Expression exceptFilters = exceptExpressions(existsExpr, notExistsExpr);
        // 2. build having (sum(case when except_filters then 1 else 0 end) = 0)
        Expression caseWhen = buildCaseWhenExpr(exceptFilters);
        Expression sumEqualsZero = new EqualTo(new Sum(caseWhen), new BigIntLiteral(0));
        Set<Expression> aggSumFilters = new HashSet<>();
        aggSumFilters.add(sumEqualsZero);
        LogicalPlan oldPlan = existsExpr.getQueryPlan();
        LogicalHaving havingPlan = new LogicalHaving(aggSumFilters, oldPlan);
        return new Exists(havingPlan, existsExpr.getCorrelateSlots(), existsExpr.getTypeCoercionExpr(),
                          ExpressionUtils.optionalAnd(sumEqualsZero), false);
    }

    private boolean isExists(Expression expr) {
        return expr instanceof Exists && !((Exists)expr).isNot();
    }
    private boolean isNotExists(Expression expr) {
        return expr instanceof Exists && ((Exists)expr).isNot();
    }
    private boolean verifyMatchExistNotExists(Set<Expression> conjuncts) {
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

    private boolean verifyMatchExistsExists(Set<Expression> conjuncts) {
        return false;
    }

    private boolean verifyMatchNotExistsNotExists(Set<Expression> conjuncts) {
        return false;
    }
}


