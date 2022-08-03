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
            "SELECT * FROM V1",
            "SELECT Y.ID1 FROM (SELECT * FROM V3) Y"
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
        createView("CREATE VIEW V1 AS SELECT ID FROM T1");
        createView("CREATE VIEW V2 AS SELECT ID FROM T2");
        createView("CREATE VIEW V3 AS SELECT SUM(A.ID) ID1, B.SS ID2 FROM V1 A, (SELECT SUM(X.ID) * 2 SS FROM V2 X GROUP BY X.ID) B GROUP BY B.SS");
    }

    @Test
    public void testParseView() {
        System.out.println(parse(testSql.get(0)).treeString());
    }

    @Test
    public void testAnalyzeView() {
        System.out.println(analyze(parse(testSql.get(1))).treeString());
    }

    @Test
    public void testPlanView() throws AnalysisException {
        System.out.println(plan(parse(testSql.get(1))).treeString());
    }

    @Test
    public void testTranslate() throws AnalysisException {
        System.out.println(translate(plan(parse(testSql.get(0)))).getPlanRoot().getPlanTreeExplainStr());
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
