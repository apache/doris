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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.NereidsAnalyzer;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.EliminateAliasNode;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSubQueryTest extends TestWithFeService implements PatternMatchSupported {
    private final NereidsParser parser = new NereidsParser();

    private final List<String> testSql = Lists.newArrayList(
            "SELECT * FROM (SELECT * FROM T1 T) T2",
            "SELECT * FROM T1 TT1 JOIN (SELECT * FROM T2 TT2) T ON TT1.ID = T.ID",
            "SELECT * FROM T1 TT1 JOIN (SELECT TT2.ID FROM T2 TT2) T ON TT1.ID = T.ID",
            "SELECT T.ID FROM T1 T",
            "SELECT A.ID FROM T1 A, T2 B WHERE A.ID = B.ID",
            "SELECT * FROM T1 JOIN T1 T2 ON T1.ID = T2.ID"
    );

    private final int currentTestCase = 0;

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
    public void testTranslateCase() throws AnalysisException {
        for (String sql : testSql) {
            System.out.println("/n/n***** " + sql + " *****/n/n");
            PhysicalPlan plan = new NereidsPlanner(connectContext).plan(
                    parser.parseSingle(sql),
                    new PhysicalProperties(),
                    connectContext
            );
            // Just to check whether translate will throw exception
            new PhysicalPlanTranslator().translatePlan(plan, new PlanTranslatorContext());
        }
    }

    @Test
    public void testCase1() {
        FieldChecker projectChecker = new FieldChecker(Lists.newArrayList());
        new PlanChecker().plan(new NereidsAnalyzer(connectContext).analyze(testSql.get(0)))
                .applyTopDown(new EliminateAliasNode())
                .matches(
                        logicalProject(
                                logicalProject().when(projectChecker.check(Lists.newArrayList()))
                        ).when(projectChecker.check(Lists.newArrayList()))
                );
    }
}

