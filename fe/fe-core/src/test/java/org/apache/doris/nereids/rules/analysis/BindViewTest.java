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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.NereidsAnalyzer;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BindViewTest extends TestWithFeService {
    private final List<String> testSql = Lists.newArrayList(
            "SELECT * FROM T1 JOIN (SELECT * FROM V1) T ON T1.ID1 = T.ID1",
            "SELECT * FROM T2 JOIN (SELECT * FROM V2) T ON T1.ID2 = T.ID2",
            "SELECT Y.ID1 FROM (SELECT * FROM V3) Y",
            "SELECT * FROM (SELECT * FROM V1 JOIN V2 ON V1.ID1 = V2.ID2) X JOIN (SELECT * FROM V1 JOIN V3 ON V1.ID1 = V3.ID2) Y ON X.ID1 = Y.ID3"
    );

    private final int currentTestCaseId = 3;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    ID1 bigint,\n"
                        + "    SCORE1 bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(ID1)\n"
                        + "DISTRIBUTED BY HASH(ID1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    ID2 bigint,\n"
                        + "    SCORE2 bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(ID2)\n"
                        + "DISTRIBUTED BY HASH(ID2) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T3 (\n"
                        + "    ID3 bigint,\n"
                        + "    SCORE3 bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(ID3)\n"
                        + "DISTRIBUTED BY HASH(ID3) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        createView("CREATE VIEW V1 AS SELECT * FROM T1");
        createView("CREATE VIEW V2 AS SELECT * FROM T2");
        createView("CREATE VIEW V3 AS SELECT * FROM T3 JOIN (SELECT * FROM V2) T ON T3.ID3 = T.ID2");
    }

    @Test
    public void testParseView() {
        System.out.println(parse(testSql.get(currentTestCaseId)).treeString());
    }

    @Test
    public void testAnalyzeView() {
        System.out.println(analyze(parse(testSql.get(currentTestCaseId))).treeString());
    }

    @Test
    public void testPlanView() throws AnalysisException {
        System.out.println(plan(parse(testSql.get(currentTestCaseId))).treeString());
    }

    @Test
    public void testTranslate() throws AnalysisException {
        System.out.println(translate(plan(parse(testSql.get(currentTestCaseId)))).getPlanRoot().getPlanTreeExplainStr());
    }

    private LogicalPlan parse(String sql) {
        return new NereidsParser().parseSingle(sql);
    }

    private LogicalPlan analyze(LogicalPlan plan) {
        return new NereidsAnalyzer(connectContext).analyze(plan);
    }

    private PhysicalPlan plan(LogicalPlan plan) throws AnalysisException {
        return new NereidsPlanner(connectContext).plan(plan, new PhysicalProperties(), connectContext);
    }

    private PlanFragment translate(PhysicalPlan plan) {
        return new PhysicalPlanTranslator().translatePlan(plan, new PlanTranslatorContext());
    }
}
