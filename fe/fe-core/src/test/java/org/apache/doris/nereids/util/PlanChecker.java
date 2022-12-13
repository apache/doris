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
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.batch.NereidsRewriteJobExecutor;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.joinorder.JoinOrderJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.pattern.PatternMatcher;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Utility to apply rules to plan and check output plan matches the expected pattern.
 */
public class PlanChecker {
    private final ConnectContext connectContext;
    private CascadesContext cascadesContext;

    private Plan parsedPlan;

    private PhysicalPlan physicalPlan;

    public PlanChecker(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public PlanChecker(CascadesContext cascadesContext) {
        this.connectContext = cascadesContext.getConnectContext();
        this.cascadesContext = cascadesContext;
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

    public PlanChecker applyTopDown(RuleFactory ruleFactory) {
        return applyTopDown(ruleFactory.buildRules());
    }

    public PlanChecker applyTopDown(List<Rule> rule) {
        cascadesContext.topDownRewrite(rule);
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    /**
     * apply a top down rewrite rule if you not care the ruleId
     *
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
     *
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

    public PlanChecker rewrite() {
        new NereidsRewriteJobExecutor(cascadesContext).execute();
        return this;
    }

    public PlanChecker implement() {
        Plan plan = transformToPhysicalPlan(cascadesContext.getMemo().getRoot());
        Assertions.assertTrue(plan instanceof PhysicalPlan);
        physicalPlan = ((PhysicalPlan) plan);
        return this;
    }

    public PhysicalPlan getPhysicalPlan() {
        return physicalPlan;
    }

    private Plan transformToPhysicalPlan(Group group) {
        PhysicalPlan current = null;
        loop:
        for (Rule rule : RuleSet.IMPLEMENTATION_RULES) {
            GroupExpressionMatching matching = new GroupExpressionMatching(rule.getPattern(),
                    group.getLogicalExpression());
            for (Plan plan : matching) {
                Plan after = rule.transform(plan, cascadesContext).get(0);
                if (after instanceof PhysicalPlan) {
                    current = (PhysicalPlan) after;
                    break loop;
                }
            }
        }
        Assertions.assertNotNull(current);
        return current.withChildren(group.getLogicalExpression().children()
                .stream().map(this::transformToPhysicalPlan).collect(Collectors.toList()));
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

        for (Plan before : matchResult) {
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

    // Exploration Rule.
    public PlanChecker applyExploration(Rule rule) {
        return applyExploration(cascadesContext.getMemo().getRoot(), rule);
    }

    private PlanChecker applyExploration(Group group, Rule rule) {
        // copy groupExpressions can prevent ConcurrentModificationException
        for (GroupExpression logicalExpression : Lists.newArrayList(group.getLogicalExpressions())) {
            applyExploration(logicalExpression, rule);
        }

        for (GroupExpression physicalExpression : Lists.newArrayList(group.getPhysicalExpressions())) {
            applyExploration(physicalExpression, rule);
        }
        return this;
    }

    private void applyExploration(GroupExpression groupExpression, Rule rule) {
        GroupExpressionMatching matchResult = new GroupExpressionMatching(rule.getPattern(), groupExpression);

        for (Plan before : matchResult) {
            Plan after = rule.transform(before, cascadesContext).get(0);
            if (before != after) {
                cascadesContext.getMemo().copyIn(after, before.getGroupExpression().get().getOwnerGroup(), false);
            }
        }

        for (Group childGroup : groupExpression.children()) {
            applyExploration(childGroup, rule);
        }
    }

    public PlanChecker deriveStats() {
        cascadesContext.pushJob(
                new DeriveStatsJob(cascadesContext.getMemo().getRoot().getLogicalExpression(),
                        cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        return this;
    }

    public PlanChecker orderJoin() {
        cascadesContext.pushJob(
                new JoinOrderJob(cascadesContext.getMemo().getRoot(), cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
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

    public PlanChecker matchesExploration(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        Supplier<Boolean> asserter = () -> new GroupExpressionMatching(patternDesc.pattern,
                memo.getRoot().getLogicalExpressions().get(1)).iterator().hasNext();
        Assertions.assertTrue(asserter.get(),
                () -> "pattern not match, plan :\n"
                        + memo.getRoot().getLogicalExpressions().get(1).getPlan().treeString()
                        + "\n");
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

    public PlanChecker checkPlannerResult(String sql, Consumer<NereidsPlanner> consumer) {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        NereidsPlanner nereidsPlanner = new NereidsPlanner(
                new StatementContext(connectContext, new OriginStatement(sql, 0)));
        nereidsPlanner.plan(LogicalPlanAdapter.of(parsed));
        consumer.accept(nereidsPlanner);
        return this;
    }

    /**
     * Check nereids planner could plan the input SQL without any exception.
     */
    public PlanChecker checkPlannerResult(String sql) {
        return checkPlannerResult(sql, planner -> {
        });
    }

    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    public Plan getPlan() {
        return cascadesContext.getMemo().copyOut();
    }

    public PlanChecker printlnTree() {
        System.out.println(cascadesContext.getMemo().copyOut().treeString());
        System.out.println("-----------------------------");
        return this;
    }

    public PlanChecker printlnAllTree() {
        System.out.println("--------------------------------");
        for (Plan plan : cascadesContext.getMemo().copyOutAll()) {
            System.out.println(plan.treeString());
            System.out.println("--------------------------------");
        }
        return this;
    }

    public int plansNumber() {
        return cascadesContext.getMemo().copyOutAll().size();
    }

    public PlanChecker printlnExploration() {
        System.out.println(
                cascadesContext.getMemo().copyOut(cascadesContext.getMemo().getRoot().logicalExpressionsAt(1), false)
                        .treeString());
        return this;
    }

    public PlanChecker printlnOrigin() {
        System.out.println(
                cascadesContext.getMemo().copyOut(cascadesContext.getMemo().getRoot().logicalExpressionsAt(0), false)
                        .treeString());
        return this;
    }

}
