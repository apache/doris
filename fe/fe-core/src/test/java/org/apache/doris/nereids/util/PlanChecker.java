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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlanProcess;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.jobs.joinorder.JoinOrderJob;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteTopDownJob;
import org.apache.doris.nereids.jobs.rewrite.RootPlanTreeRewriteJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.pattern.PatternMatcher;
import org.apache.doris.nereids.processor.post.Validator;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindRelation.CustomTableResolver;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Utility to apply rules to plan and check output plan matches the expected pattern.
 */
public class PlanChecker {
    private final ConnectContext connectContext;
    private CascadesContext cascadesContext;

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
        checker.parsedSupplier.get();
        return this;
    }

    public PlanChecker parse(String sql) {
        this.cascadesContext = MemoTestUtils.createCascadesContext(connectContext, sql);
        this.cascadesContext.toMemo();
        return this;
    }

    public PlanChecker analyze() {
        this.cascadesContext.newAnalyzer().analyze();
        this.cascadesContext.toMemo();
        InitMaterializationContextHook.INSTANCE.initMaterializationContext(this.cascadesContext);
        return this;
    }

    public PlanChecker analyze(Plan plan) {
        this.cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
        Set<String> originDisableRules = connectContext.getSessionVariable().getDisableNereidsRuleNames();
        Set<String> disableRuleWithAuth = Sets.newHashSet(originDisableRules);
        disableRuleWithAuth.add(RuleType.RELATION_AUTHENTICATION.name());
        connectContext.getSessionVariable().setDisableNereidsRules(String.join(",", disableRuleWithAuth));
        this.cascadesContext.newAnalyzer().analyze();
        connectContext.getSessionVariable().setDisableNereidsRules(String.join(",", originDisableRules));
        this.cascadesContext.toMemo();
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    public PlanChecker analyze(String sql) {
        this.cascadesContext = MemoTestUtils.createCascadesContext(connectContext, sql);
        this.cascadesContext.newAnalyzer().analyze();
        this.cascadesContext.toMemo();
        return this;
    }

    public PlanChecker customAnalyzer(Optional<CustomTableResolver> customTableResolver) {
        this.cascadesContext.newAnalyzer(customTableResolver).analyze();
        this.cascadesContext.toMemo();
        return this;
    }

    public PlanChecker customRewrite(CustomRewriter customRewriter) {
        Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                        ImmutableList.of(Rewriter.custom(RuleType.TEST_REWRITE, () -> customRewriter)))
                .execute();
        cascadesContext.toMemo();
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    public PlanChecker disableNereidsRules(String rules) {
        connectContext.getSessionVariable().setDisableNereidsRules(rules);
        return this;
    }

    public PlanChecker printPlanProcess(String sql) {
        List<PlanProcess> planProcesses = explainPlanProcess(sql);
        for (PlanProcess row : planProcesses) {
            System.out.println("RULE: " + row.ruleName + "\nBEFORE:\n"
                    + row.beforeShape + "\nafter:\n" + row.afterShape);
        }
        return this;
    }

    public List<PlanProcess> explainPlanProcess(String sql) {
        NereidsParser parser = new NereidsParser();
        LogicalPlan command = parser.parseSingle(sql);
        NereidsPlanner planner = new NereidsPlanner(
                new StatementContext(connectContext, new OriginStatement(sql, 0)));
        planner.planWithLock(command, PhysicalProperties.ANY, ExplainLevel.ALL_PLAN, true);
        this.cascadesContext = planner.getCascadesContext();
        return cascadesContext.getPlanProcesses();
    }

    public PlanChecker applyTopDown(RuleFactory ruleFactory) {
        return applyTopDown(ruleFactory.buildRules());
    }

    public PlanChecker applyTopDown(List<Rule> rule) {
        Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                        ImmutableList.of(new RootPlanTreeRewriteJob(rule, PlanTreeRewriteTopDownJob::new, true)))
                .execute();
        cascadesContext.toMemo();
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    /**
     * apply a top down rewrite rule if you not care the ruleId
     *
     * @param patternMatcher the rule dsl, such as: logicalOlapScan().then(olapScan -> olapScan)
     * @return this checker, for call chaining of follow-up check
     */
    public PlanChecker applyTopDownInMemo(PatternMatcher patternMatcher) {
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
        Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                        ImmutableList.of(Rewriter.bottomUp(rule)))
                .execute();
        cascadesContext.toMemo();
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    public PlanChecker applyBottomUp(List<Rule> rule) {
        Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                        ImmutableList.of(new RootPlanTreeRewriteJob(rule, PlanTreeRewriteBottomUpJob::new, true)))
                .execute();
        cascadesContext.toMemo();
        MemoValidator.validate(cascadesContext.getMemo());
        return this;
    }

    /**
     * apply a bottom up rewrite rule if you not care the ruleId
     *
     * @param patternMatcher the rule dsl, such as: logicalOlapScan().then(olapScan -> olapScan)
     * @return this checker, for call chaining of follow-up check
     */
    public PlanChecker applyBottomUpInMemo(PatternMatcher patternMatcher) {
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
        Rewriter.getWholeTreeRewriter(cascadesContext).execute();
        cascadesContext.toMemo();
        return this;
    }

    public PlanChecker optimize() {
        cascadesContext.setJobContext(PhysicalProperties.GATHER);
        double now = System.currentTimeMillis();
        new Optimizer(cascadesContext).execute();
        System.out.println("cascades:" + (System.currentTimeMillis() - now));
        return this;
    }

    public PlanChecker dpHypOptimize() {
        double now = System.currentTimeMillis();
        cascadesContext.getStatementContext().setDpHyp(true);
        cascadesContext.getConnectContext().getSessionVariable().enableDPHypOptimizer = true;
        Group root = cascadesContext.getMemo().getRoot();
        cascadesContext.pushJob(new JoinOrderJob(root, cascadesContext.getCurrentJobContext()));
        cascadesContext.pushJob(new DeriveStatsJob(root.getLogicalExpression(),
                cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        optimize();
        System.out.println("DPhyp:" + (System.currentTimeMillis() - now));
        return this;
    }

    public PlanChecker implement() {
        Plan plan = transformToPhysicalPlan(cascadesContext.getMemo().getRoot());
        Assertions.assertTrue(plan instanceof PhysicalPlan);
        if (plan instanceof PhysicalQuickSort && !((PhysicalQuickSort) plan).getSortPhase().isLocal()) {
            PhysicalQuickSort<? extends Plan> sort = (PhysicalQuickSort) plan;
            plan = sort.withChildren(new PhysicalDistribute<>(
                    DistributionSpecGather.INSTANCE,
                    plan.child(0)));
        }
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
        return flatGroupPlan(current);
    }

    private Plan flatGroupPlan(Plan plan) {
        if (plan instanceof GroupPlan) {
            return transformToPhysicalPlan(((GroupPlan) plan).getGroup());
        }
        List<Plan> newChildren = new ArrayList<>();
        for (Plan child : plan.children()) {
            newChildren.add(flatGroupPlan(child));
        }
        return plan.withChildren(newChildren);

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

    public PlanChecker applyExploration(List<Rule> rules) {
        rules.forEach(rule -> applyExploration(cascadesContext.getMemo().getRoot(), rule));
        return this;
    }

    private PlanChecker applyExploration(Group group, Rule rule) {
        // copy children expression, because group may be changed after apply rule.
        List<GroupExpression> logicalExpressions = Lists.newArrayList(group.getLogicalExpressions());
        // due to mergeGroup, the children Group of groupExpression may be replaced, so we need to use lambda to
        // get the child to make we can get child at the time we use child.
        // If we use for child: groupExpression.children(), it means that we take it in advance. It may cause NPE,
        // work flow: get children() to get left, right -> copyIn left() -> mergeGroup -> right is merged -> NPE
        for (int i = 0; i < logicalExpressions.size(); i++) {
            final int childIdx = i;
            applyExploration(() -> logicalExpressions.get(childIdx), rule);
        }

        List<GroupExpression> physicalExpressions = Lists.newArrayList(group.getPhysicalExpressions());
        for (int i = 0; i < physicalExpressions.size(); i++) {
            final int childIdx = i;
            applyExploration(() -> physicalExpressions.get(childIdx), rule);
        }
        return this;
    }

    private void applyExploration(Supplier<GroupExpression> groupExpression, Rule rule) {
        GroupExpressionMatching matchResult = new GroupExpressionMatching(rule.getPattern(), groupExpression.get());

        List<Group> childrenGroup = new ArrayList<>(groupExpression.get().children());
        for (Plan before : matchResult) {
            Plan after = rule.transform(before, cascadesContext).get(0);
            if (before != after) {
                cascadesContext.getMemo().copyIn(after, before.getGroupExpression().get().getOwnerGroup(), false);
            }
        }

        for (Group childGroup : childrenGroup) {
            applyExploration(childGroup, rule);
        }
    }

    public PlanChecker applyImplementation(Rule rule) {
        return applyImplementation(cascadesContext.getMemo().getRoot(), rule);
    }

    private PlanChecker applyImplementation(Group group, Rule rule) {
        // copy groupExpressions can prevent ConcurrentModificationException
        for (GroupExpression logicalExpression : Lists.newArrayList(group.getLogicalExpressions())) {
            applyImplementation(logicalExpression, rule);
        }

        for (GroupExpression physicalExpression : Lists.newArrayList(group.getPhysicalExpressions())) {
            applyImplementation(physicalExpression, rule);
        }
        return this;
    }

    private PlanChecker applyImplementation(GroupExpression groupExpression, Rule rule) {
        GroupExpressionMatching matchResult = new GroupExpressionMatching(rule.getPattern(), groupExpression);

        for (Plan before : matchResult) {
            List<Plan> afters = rule.transform(before, cascadesContext);
            for (Plan after : afters) {
                if (before != after) {
                    cascadesContext.getMemo().copyIn(after, before.getGroupExpression().get().getOwnerGroup(), false);
                }
            }
        }

        for (Group childGroup : groupExpression.children()) {
            for (GroupExpression logicalExpression : childGroup.getLogicalExpressions()) {
                applyImplementation(logicalExpression, rule);
            }

            for (GroupExpression physicalExpression : childGroup.getPhysicalExpressions()) {
                applyImplementation(physicalExpression, rule);
            }
        }
        return this;
    }

    public PlanChecker deriveStats() {
        cascadesContext.pushJob(
                new DeriveStatsJob(cascadesContext.getMemo().getRoot().getLogicalExpression(),
                        cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        return this;
    }

    public PlanChecker matchesFromRoot(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        assertMatches(memo, () -> new GroupExpressionMatching(patternDesc.pattern,
                memo.getRoot().getLogicalExpression()).iterator().hasNext());
        return this;
    }

    public PlanChecker notMatchesFromRoot(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        assertMatches(memo, () -> !(new GroupExpressionMatching(patternDesc.pattern,
                memo.getRoot().getLogicalExpression()).iterator().hasNext()));
        return this;
    }

    public PlanChecker matches(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        checkSlotFromChildren(memo);
        assertMatches(memo, () -> MatchingUtils.topDownFindMatching(memo.getRoot(), patternDesc.pattern));
        return this;
    }

    public PlanChecker nonMatch(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        checkSlotFromChildren(memo);
        assertMatches(memo, () -> !MatchingUtils.topDownFindMatching(memo.getRoot(), patternDesc.pattern));
        return this;
    }

    // TODO: remove it.
    public PlanChecker matchesNotCheck(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        assertMatches(memo, () -> MatchingUtils.topDownFindMatching(memo.getRoot(), patternDesc.pattern));
        return this;
    }

    public PlanChecker matchesExploration(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        checkSlotFromChildren(memo);
        Supplier<Boolean> asserter = () -> new GroupExpressionMatching(patternDesc.pattern,
                memo.getRoot().getLogicalExpressions().get(1)).iterator().hasNext();
        Assertions.assertTrue(asserter.get(),
                () -> "pattern not match, plan :\n"
                        + memo.getRoot().getLogicalExpressions().get(1).getPlan().treeString()
                        + "\n");
        return this;
    }

    private void checkSlotFromChildren(Memo memo) {
        Validator validator = new Validator();
        memo.getGroupExpressions().forEach((key, value) -> validator.visit(value.getPlan(), null));
    }

    private PlanChecker assertMatches(Memo memo, Supplier<Boolean> asserter) {
        Assertions.assertTrue(asserter.get(),
                () -> "pattern not match, plan :\n"
                        + memo.copyOut().treeString()
                        + "\n"
        );
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

    public PlanChecker checkExplain(String sql, Consumer<NereidsPlanner> consumer) {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        NereidsPlanner nereidsPlanner = new NereidsPlanner(
                new StatementContext(connectContext, new OriginStatement(sql, 0)));
        LogicalPlanAdapter adapter = LogicalPlanAdapter.of(parsed);
        adapter.setIsExplain(new ExplainOptions(ExplainLevel.ALL_PLAN, false));
        nereidsPlanner.planWithLock(adapter);
        consumer.accept(nereidsPlanner);
        return this;
    }

    public PlanChecker checkPlannerResult(String sql, Consumer<NereidsPlanner> consumer) {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        NereidsPlanner nereidsPlanner = new NereidsPlanner(
                new StatementContext(connectContext, new OriginStatement(sql, 0)));
        nereidsPlanner.planWithLock(LogicalPlanAdapter.of(parsed));
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

    public List<Plan> getAllPlan() {
        return cascadesContext.getMemo().copyOutAll();
    }

    private PhysicalPlan chooseBestPlan(Group rootGroup, PhysicalProperties physicalProperties) {
        GroupExpression groupExpression = rootGroup.getLowestCostPlan(physicalProperties).orElseThrow(
                () -> new AnalysisException("lowestCostPlans with physicalProperties("
                        + physicalProperties + ") doesn't exist in root group")).second;
        List<PhysicalProperties> inputPropertiesList = groupExpression.getInputPropertiesList(physicalProperties);

        List<Plan> planChildren = Lists.newArrayList();
        for (int i = 0; i < groupExpression.arity(); i++) {
            planChildren.add(chooseBestPlan(groupExpression.child(i), inputPropertiesList.get(i)));
        }

        Plan plan = groupExpression.getPlan().withChildren(planChildren);
        if (!(plan instanceof PhysicalPlan)) {
            throw new AnalysisException("Result plan must be PhysicalPlan");
        }

        PhysicalPlan physicalPlan = ((PhysicalPlan) plan).withPhysicalPropertiesAndStats(
                groupExpression.getOutputProperties(physicalProperties),
                groupExpression.getOwnerGroup().getStatistics());
        return physicalPlan;
    }

    public PhysicalPlan getBestPlanTree() {
        return chooseBestPlan(cascadesContext.getMemo().getRoot(), PhysicalProperties.GATHER);
    }

    public PhysicalPlan getBestPlanTree(PhysicalProperties properties) {
        return chooseBestPlan(cascadesContext.getMemo().getRoot(), properties);
    }

    public PlanChecker printlnBestPlanTree() {
        System.out.println(chooseBestPlan(cascadesContext.getMemo().getRoot(), PhysicalProperties.ANY).treeString());
        System.out.println("-----------------------------");
        return this;
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

    public String getExplainFragments() {
        PhysicalPlan plan = getBestPlanTree();
        PhysicalPlanTranslator t = new PhysicalPlanTranslator();
        PlanTranslatorContext ctx = new PlanTranslatorContext();
        plan.accept(t, ctx);

        StringBuilder str = new StringBuilder();
        List<PlanFragment> fragments = ctx.getPlanFragments();
        for (int i = 0; i < fragments.size(); ++i) {
            PlanFragment fragment = fragments.get(i);
            if (i > 0) {
                // a blank line between plan fragments
                str.append("\n");
            }
            str.append("PLAN FRAGMENT " + i + "\n");
            str.append(fragment.getExplainString(org.apache.doris.thrift.TExplainLevel.NORMAL));
        }
        return str.toString();
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

    public static boolean isPlanEqualWithoutID(Plan plan1, Plan plan2) {
        if (plan1.arity() != plan2.arity()
                || !plan1.getOutput().equals(plan2.getOutput()) || plan1.getClass() != plan2.getClass()) {
            System.out.println(plan1);
            System.out.println(plan2);
            return false;
        }
        for (int i = 0; i < plan1.arity(); i++) {
            if (!isPlanEqualWithoutID(plan1.child(i), plan2.child(i))) {
                return false;
            }
        }
        return true;
    }
}
