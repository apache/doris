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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.pattern.GroupExpressionMatching.GroupExpressionIterator;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.pattern.PatternMatcher;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;

import java.util.function.Consumer;

/**
 * Utility to apply rules to plan and check output plan matches the expected pattern.
 */
public class PlanChecker {
    private ConnectContext connectContext;
    private CascadesContext cascadesContext;

    private Plan parsedPlan;

    public PlanChecker(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public PlanChecker(CascadesContext cascadesContext) {
        this.connectContext = cascadesContext.getConnectContext();
        this.cascadesContext = cascadesContext;
    }

    public PlanChecker checkParse(String sql, Consumer<PlanParseChecker> consumer) {
        PlanParseChecker checker = new PlanParseChecker(sql);
        consumer.accept(checker);
        parsedPlan = checker.parsedSupplier.get();
        return this;
    }

    public PlanChecker analyze() {
        MemoTestUtils.createCascadesContext(connectContext, parsedPlan);
        return this;
    }

    public PlanChecker analyze(String sql) {
        this.cascadesContext = MemoTestUtils.createCascadesContext(connectContext, sql);
        this.cascadesContext.newAnalyzer().analyze();
        return this;
    }

    public PlanChecker analyze(Plan plan) {
        this.cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
        this.cascadesContext.newAnalyzer().analyze();
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    public PlanChecker applyTopDown(RuleFactory rule) {
        cascadesContext.topDownRewrite(rule);
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    /**
     * apply a top down rewrite rule if you not care the ruleId
     * @param patternMatcher the rule dsl, such as: logicalOlapScan().then(olapScan -> olapScan)
     * @return this checker, for call chaining of follow-up check
     */
    public PlanChecker applyTopDown(PatternMatcher patternMatcher) {
        cascadesContext.topDownRewrite(new OneRewriteRuleFactory() {
            @Override
            public Rule build() {
                return patternMatcher.toRule(RuleType.TEST_REWRITE);
            }
        });
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    public PlanChecker applyBottomUp(RuleFactory rule) {
        cascadesContext.bottomUpRewrite(rule);
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    /**
     * apply a bottom up rewrite rule if you not care the ruleId
     * @param patternMatcher the rule dsl, such as: logicalOlapScan().then(olapScan -> olapScan)
     * @return this checker, for call chaining of follow-up check
     */
    public PlanChecker applyBottomUp(PatternMatcher patternMatcher) {
        cascadesContext.bottomUpRewrite(new OneRewriteRuleFactory() {
            @Override
            public Rule build() {
                return patternMatcher.toRule(RuleType.TEST_REWRITE);
            }
        });
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    public PlanChecker transform(PatternMatcher patternMatcher) {
        return transform(cascadesContext.getMemo().getRoot(), patternMatcher);
    }

    public PlanChecker transform(Group group, PatternMatcher patternMatcher) {
        // copy groupExpressions can prevent ConcurrentModificationException
        for (GroupExpression logicalExpression : Lists.newArrayList(group.getLogicalExpressions())) {
            transform(logicalExpression, patternMatcher);
        }

        for (GroupExpression physicalExpression : Lists.newArrayList(group.getPhysicalExpressions())) {
            transform(physicalExpression, patternMatcher);
        }
        return this;
    }

    public PlanChecker transform(GroupExpression groupExpression, PatternMatcher patternMatcher) {
        GroupExpressionMatching matchResult = new GroupExpressionMatching(patternMatcher.pattern, groupExpression);
        GroupExpressionIterator iterator = matchResult.iterator();

        while (iterator.hasNext()) {
            Plan before = iterator.next();
            Plan after = patternMatcher.matchedAction.apply(
                    new MatchingContext(before, patternMatcher.pattern, cascadesContext));
            if (before != after) {
                cascadesContext.getMemo().copyIn(after, before.getGroupExpression().get().getOwnerGroup(), false);
            }
        }

        for (Group childGroup : groupExpression.children()) {
            transform(childGroup, patternMatcher);
        }
        return this;
    }

    public PlanChecker matchesFromRoot(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        assertMatches(memo, () -> new GroupExpressionMatching(patternDesc.pattern,
                memo.getRoot().getLogicalExpression()).iterator().hasNext());
        return this;
    }

    public PlanChecker matches(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        assertMatches(memo, () -> GroupMatchingUtils.topDownFindMatching(memo.getRoot(), patternDesc.pattern));
        return this;
    }

    private PlanChecker assertMatches(Memo memo, Supplier<Boolean> asserter) {
        Assertions.assertTrue(asserter.get(),
                () -> "pattern not match, plan :\n"
                        + memo.getRoot().getLogicalExpression().getPlan().treeString()
                        + "\n"
        );
        return this;
    }

    public PlanChecker checkCascadesContext(Consumer<CascadesContext> contextChecker) {
        contextChecker.accept(cascadesContext);
        return this;
    }

    public PlanChecker checkGroupNum(int expectGroupNum) {
        Assertions.assertEquals(expectGroupNum, cascadesContext.getMemo().getGroups().size());
        return this;
    }

    public PlanChecker checkGroupExpressionNum(int expectGroupExpressionNum) {
        Assertions.assertEquals(expectGroupExpressionNum, cascadesContext.getMemo().getGroupExpressions().size());
        return this;
    }

    public PlanChecker checkFirstRootLogicalPlan(Plan expectPlan) {
        Assertions.assertEquals(expectPlan, cascadesContext.getMemo().getRoot().getLogicalExpression().getPlan());
        return this;
    }

    public PlanChecker checkMemo(Consumer<Memo> memoChecker) {
        memoChecker.accept(cascadesContext.getMemo());
        return this;
    }

    public PlanChecker setMaxInvokeTimesPerRule(int maxInvokeTime) {
        JobContext originJobContext = cascadesContext.getCurrentJobContext();
        cascadesContext.setCurrentJobContext(
                new JobContext(cascadesContext,
                        originJobContext.getRequiredProperties(), originJobContext.getCostUpperBound()) {
                    @Override
                    public void onInvokeRule(RuleType ruleType) {
                        // add invoke times
                        super.onInvokeRule(ruleType);

                        Integer invokeTimes = ruleInvokeTimes.get(ruleType);
                        if (invokeTimes > maxInvokeTime) {
                            throw new IllegalStateException(ruleType + " invoke too many times: " + maxInvokeTime);
                        }
                    }
                }
        );
        return this;
    }

    public static PlanChecker from(ConnectContext connectContext) {
        return new PlanChecker(connectContext);
    }

    public static PlanChecker from(ConnectContext connectContext, Plan initPlan) {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, initPlan);
        return new PlanChecker(cascadesContext);
    }

    public static PlanChecker from(CascadesContext cascadesContext) {
        return new PlanChecker(cascadesContext);
    }

    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }
}
