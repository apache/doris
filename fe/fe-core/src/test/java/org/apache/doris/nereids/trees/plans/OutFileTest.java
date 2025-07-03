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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OutFileTest extends TestWithFeService implements PlanPatternMatchSupported {
    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

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
    public void testWriteOutFile() throws Exception {
        String sql = "select * from T1 join T2 on T1.id = T2.id into outfile 'file://~/result.csv'\n"
                + " format as csv\n"
                + " properties (\n"
                + "    \"column_separator\" = \",\",\n"
                + "    \"line_delimiter\" = \"\\n\",\n"
                + "    \"broker.name\" = \"my_broker\",\n"
                + "    \"max_file_size\" = \"100MB\"\n"
                + ")";
        Assertions.assertTrue(getOutputFragment(sql).getExplainString(TExplainLevel.BRIEF)
                .contains("FILE SINK"));
    }

    private PlanFragment getOutputFragment(String sql) throws Exception {
        StatementScopeIdGenerator.clear();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        PhysicalPlan plan = planner.planWithLock(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        return new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext())).translatePlan(plan);
    }
}
