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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.analyzer.NereidsAnalyzer;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.jobs.batch.DisassembleRulesJob;
import org.apache.doris.nereids.jobs.batch.FinalizeAnalyzeJob;
import org.apache.doris.nereids.jobs.batch.JoinReorderRulesJob;
import org.apache.doris.nereids.jobs.batch.OptimizeRulesJob;
import org.apache.doris.nereids.jobs.batch.PredicatePushDownRulesJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JoinSameTableTest extends TestWithFeService {
    private final List<String> testSql = Lists.newArrayList(
            "SELECT * FROM T1 JOIN T1 T2 ON T1.ID = T2.ID",
            "SELECT * FROM T1"
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Test
    public void testJoinSameTable() {
        LogicalPlan plan = new NereidsParser().parseSingle(testSql.get(0));
        System.out.println(plan.treeString());
        PlannerContext ctx = new NereidsAnalyzer(connectContext)
                .analyzeWithPlannerContext(plan);

        System.out.println("\n***** analyzed *****\n\n");
        System.out.println(ctx.getMemo().copyOut().treeString());

        new FinalizeAnalyzeJob(ctx).execute();
        System.out.println("\n***** eliminated *****\n\n");
        System.out.println(ctx.getMemo().copyOut().treeString());

        new JoinReorderRulesJob(ctx).execute();
        System.out.println("\n***** reordered *****\n\n");
        System.out.println(ctx.getMemo().copyOut().treeString());

        new PredicatePushDownRulesJob(ctx).execute();
        System.out.println("\n***** pushed *****\n\n");
        System.out.println(ctx.getMemo().copyOut().treeString());

        new DisassembleRulesJob(ctx).execute();
        System.out.println("\n***** disassembled *****\n\n");
        System.out.println(ctx.getMemo().copyOut().treeString());

        new OptimizeRulesJob(ctx).execute();
        System.out.println("\n***** optimized *****\n\n");
        PhysicalPlan plan1 = ctx.getMemo().getRoot().extractPlan();

        PlanFragment plan2 = new PhysicalPlanTranslator().translatePlan(plan1, new PlanTranslatorContext());
        System.out.println("\n***** translated *****\n\n");
        System.out.println(plan2);
    }
}
