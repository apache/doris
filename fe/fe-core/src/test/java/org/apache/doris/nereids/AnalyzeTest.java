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
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.BindSlotReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeTest extends TestWithFeService {

    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.t1 (\n"
                + "a int not null,\n"
                + "b varchar(128),\n"
                + "c int)\n"
                + "distributed by hash(b) buckets 10\n"
                + "properties('replication_num' = '1');");
        createTable("create table test.t2 (\n"
                + "d int not null, \n"
                + "e varchar(128), \n"
                + "f int)\n"
                + "distributed by hash(e) buckets 10\n"
                + "properties('replication_num' = '1');");
    }

    /**
     * TODO: check bound plan and expression details.
     */
    @Test
    public void test() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String sql = "select a, b, e from t1 join t2 on t1.a=t2.d";
        LogicalPlan analyzed = analyze(sql);
        Assertions.assertTrue(checkBound(analyzed));
    }

    private LogicalPlan analyze(String sql) throws Exception {
        LogicalPlan parsed = parser.parseSingle(sql);
        return analyze(parsed, connectContext);
    }

    private LogicalPlan analyze(LogicalPlan inputPlan, ConnectContext connectContext) {
        Memo memo = new Memo();
        memo.initialize(inputPlan);
        OptimizerContext optimizerContext = new OptimizerContext(memo);
        PlannerContext plannerContext = new PlannerContext(optimizerContext, connectContext, new PhysicalProperties());
        optimizerContext.pushJob(
            new RewriteBottomUpJob(memo.getRoot(), new BindSlotReference().buildRules(), plannerContext)
        );
        optimizerContext.pushJob(
            new RewriteBottomUpJob(memo.getRoot(), new BindRelation().buildRules(), plannerContext)
        );
        plannerContext.getOptimizerContext().getJobScheduler().executeJobPool(plannerContext);
        return (LogicalPlan) memo.copyOut();
    }

    private boolean checkBound(LogicalPlan root) {
        if (!checkPlanNodeBound(root))  {
            return false;
        }

        List<Plan> children = root.children();
        for (Plan child : children) {
            if (!checkBound((LogicalPlan) child)) {
                return false;
            }
        }
        return true;
    }

    /**
     * PlanNode and its expressions are all bound.
     */
    private boolean checkPlanNodeBound(LogicalPlan plan) {
        if (plan instanceof Unbound) {
            return false;
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
