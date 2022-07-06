// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids;

import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.jobs.rewrite.RewriteBottomUpJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.analysis.BindFunction;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.BindSlotReference;
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.ssb.SSBUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSSBTest extends TestWithFeService {

    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        SSBUtils.createTables(this);
    }

    /**
     * TODO: check bound plan and expression details.
     */
    @Test
    public void q1_1() {
        checkAnalyze(SSBUtils.Q1_1);
    }

    @Test
    public void q1_2() {
        checkAnalyze(SSBUtils.Q1_2);
    }

    @Test
    public void q1_3() {
        checkAnalyze(SSBUtils.Q1_3);
    }

    @Test
    public void q2_1() {
        checkAnalyze(SSBUtils.Q2_1);
    }

    @Test
    public void q2_2() {
        checkAnalyze(SSBUtils.Q2_2);
    }

    @Test
    public void q2_3() {
        checkAnalyze(SSBUtils.Q2_3);
    }

    @Test
    public void q3_1() {
        checkAnalyze(SSBUtils.Q3_1);
    }

    @Test
    public void q3_2() {
        checkAnalyze(SSBUtils.Q3_2);
    }

    @Test
    public void q3_3() {
        checkAnalyze(SSBUtils.Q3_3);
    }

    @Test
    public void q3_4() {
        checkAnalyze(SSBUtils.Q3_4);
    }

    @Test
    public void q4_1() {
        checkAnalyze(SSBUtils.Q4_1);
    }

    @Test
    public void q4_2() {
        checkAnalyze(SSBUtils.Q4_2);
    }

    @Test
    public void q4_3() {
        checkAnalyze(SSBUtils.Q4_3);
    }

    private void checkAnalyze(String sql) {
        LogicalPlan analyzed = analyze(sql);
        System.out.println(analyzed.treeString());
        Assertions.assertTrue(checkBound(analyzed));
    }

    private LogicalPlan analyze(String sql) {
        try {
            LogicalPlan parsed = parser.parseSingle(sql);
            return analyze(parsed, connectContext);
        } catch (Throwable t) {
            throw new IllegalStateException("Analyze failed", t);
        }
    }

    private LogicalPlan analyze(LogicalPlan inputPlan, ConnectContext connectContext) {
        Memo memo = new Memo();
        memo.initialize(inputPlan);
        OptimizerContext optimizerContext = new OptimizerContext(memo);
        PlannerContext plannerContext = new PlannerContext(optimizerContext, connectContext, new PhysicalProperties());

        executeRewriteBottomUpJob(plannerContext, new BindFunction());
        executeRewriteBottomUpJob(plannerContext, new BindRelation());
        executeRewriteBottomUpJob(plannerContext, new BindSlotReference());
        executeRewriteBottomUpJob(plannerContext, new ProjectToGlobalAggregate());
        return (LogicalPlan) memo.copyOut();
    }

    private void executeRewriteBottomUpJob(PlannerContext plannerContext, RuleFactory<Plan> ruleFactory) {
        OptimizerContext optimizerContext = plannerContext.getOptimizerContext();
        Group rootGroup = optimizerContext.getMemo().getRoot();
        RewriteBottomUpJob job = new RewriteBottomUpJob(rootGroup, plannerContext, ImmutableList.of(ruleFactory));
        optimizerContext.pushJob(job);
        optimizerContext.getJobScheduler().executeJobPool(plannerContext);
    }

    private boolean checkBound(LogicalPlan root) {
        if (!checkPlanBound(root))  {
            return false;
        }
        return true;
    }

    /**
     * PlanNode and its expressions are all bound.
     */
    private boolean checkPlanBound(LogicalPlan plan) {
        if (plan instanceof Unbound) {
            return false;
        }

        List<Plan> children = plan.children();
        for (Plan child : children) {
            if (!checkPlanBound((LogicalPlan) child)) {
                return false;
            }
        }

        List<Expression> expressions = plan.getOperator().getExpressions();
        return expressions.stream().allMatch(this::checkExpressionBound);
    }

    private boolean checkExpressionBound(Expression expr) {
        if (expr instanceof Unbound) {
            return false;
        }

        List<Expression> children = expr.children();
        for (Expression child : children) {
            if (!checkExpressionBound(child)) {
                return false;
            }
        }
        return true;
    }
}
