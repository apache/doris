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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class CTEInlineTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
    }

    @Test
    public void recCteInline() {
        String sql = new StringBuilder().append("with recursive t1 as (\n").append("    select\n")
                .append("        1 as c1,\n").append("        1 as c2\n").append("),\n").append("t2 as (\n")
                .append("    select\n").append("        2 as c1,\n").append("        2 as c2\n").append("),\n")
                .append("t3 as (\n").append("    select\n").append("        3 as c1,\n").append("        3 as c2\n")
                .append("),\n").append("xx as (\n").append("    select\n").append("        c1,\n")
                .append("        c2\n").append("    from\n").append("        t1\n").append("    union\n")
                .append("    select\n").append("        t2.c1,\n").append("        t2.c2\n").append("    from\n")
                .append("        t2,\n").append("        xx\n").append("    where\n").append("        t2.c1 = xx.c1\n")
                .append("),\n").append("yy as (\n").append("    select\n").append("        c1,\n")
                .append("        c2\n").append("    from\n").append("        t3\n").append("    union\n")
                .append("    select\n").append("        t3.c1,\n").append("        t3.c2\n").append("    from\n")
                .append("        t3,\n").append("        yy,\n").append("        xx\n").append("    where\n")
                .append("        t3.c1 = yy.c1\n").append("        and t3.c2 = xx.c1\n").append(")\n")
                .append("select\n").append("    *\n").append("from\n").append("    yy y1,\n").append("    yy y2;")
                .toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                ExplainCommand.ExplainLevel.REWRITTEN_PLAN);
        MemoTestUtils.initMemoAndValidState(planner.getCascadesContext());
        PlanChecker.from(planner.getCascadesContext()).matches(
                this.logicalRecursiveUnion(
                        any(
                        ),
                        logicalRecursiveUnionProducer(
                                logicalProject(
                                        logicalJoin(
                                                any(),
                                                logicalProject(
                                                        logicalFilter(
                                                                logicalRecursiveUnion().when(cte -> cte.getCteName().equals("xx"))
                                                        )
                                                )
                                        )
                                )
                        )
                ).when(cte -> cte.getCteName().equals("yy"))
        );
    }
}
