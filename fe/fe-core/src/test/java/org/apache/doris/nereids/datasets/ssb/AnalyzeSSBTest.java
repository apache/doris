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

package org.apache.doris.nereids.datasets.ssb;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.OriginStatement;

import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSSBTest extends SSBTestBase {
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

    @Test
    public void testHint() throws Exception {
//        String plan = getSQLPlanOrErrorMsg(
//                "select * from lineorder join customer on lo_custkey=c_custkey");
//        System.out.println("origin plan:\n" + plan);
//
//        String shuffleJoin = getSQLPlanOrErrorMsg(
//                "select * from lineorder join [shuffle] customer on lo_custkey=c_custkey");
//        System.out.println("plan with shuffle join hint:\n" + shuffleJoin);


//        // shuffle customer
//        String shuffleJoin1 = getSQLPlanOrErrorMsg(
//                "select * from lineorder join /*+ shuffle */  customer on lo_custkey=c_custkey");
//        System.out.println("plan with shuffle join hint:\n" + shuffleJoin1);

//        // shuffle lineorder
//        String shuffleJoin2 = getSQLPlanOrErrorMsg(
//                "select * from  lineorder /*+ shuffle */ join  customer on lo_custkey=c_custkey");
//        System.out.println("plan with shuffle join hint:\n" + shuffleJoin2);


        // shuffle lineorder
//        String shuffleJoin3 = getSQLPlanOrErrorMsg(
//                "select * from  lineorder [shuffle] join  customer on lo_custkey=c_custkey");
//        System.out.println("plan with shuffle join hint:\n" + shuffleJoin3);

        String sql =  "select * from lineorder join [shuffle] customer on lo_custkey=c_custkey";
        // multi hints
        String explainString = getSQLPlanOrErrorMsg(sql);
        System.out.println("plan with shuffle join hint:\n" + explainString);

        OriginStatement originStatement = new OriginStatement(sql, 0);
        StatementContext statementContext = new StatementContext(connectContext, originStatement);
        NereidsPlanner planner = new NereidsPlanner(statementContext);

        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        LogicalPlanAdapter adaptor = new LogicalPlanAdapter(parsed);
        planner.plan(adaptor, connectContext.getSessionVariable().toThrift());

        ExplainOptions explainOptions = new ExplainOptions(true, true);

        org.apache.doris.thrift.TExplainLevel
                explainLevel = explainOptions.isVerbose()
                ? org.apache.doris.thrift.TExplainLevel.VERBOSE : org.apache.doris.thrift.TExplainLevel.NORMAL;
        System.out.println("nereids fragments:\n");
        List<PlanFragment> fragments = planner.getFragments();
        StringBuilder str = new StringBuilder();

        for (int i = 0; i < fragments.size(); ++i) {
            PlanFragment fragment = fragments.get(i);
            if (i > 0) {
                // a blank line between plan fragments
                str.append("\n");
            }
            str.append("PLAN FRAGMENT " + i + "\n");
            str.append(fragment.getExplainString(explainLevel));
        }

        System.out.println(str);

        String nereidsExplain = planner.getExplainString(new ExplainOptions(true, true));
        System.out.println("nereids explain result\n" + nereidsExplain);

    }

}
