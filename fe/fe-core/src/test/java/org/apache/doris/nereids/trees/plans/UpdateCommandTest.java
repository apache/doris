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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UpdateCommandTest extends TestWithFeService implements PlanPatternMatchSupported {
    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("create table t1 (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t2 (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table src (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "duplicate key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
    }

    @Test
    public void testSimpleUpdate() {
        String sql = "update t1 set v1 = v1 + 2, v2 = v1 * 2 where k1 = 3";
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, parsed);
        UpdateCommand command = ((UpdateCommand) parsed);
        LogicalPlan plan = command.completeQueryPlan(connectContext, command.getLogicalQuery());
        PlanChecker.from(connectContext, plan)
                .analyze(plan)
                .rewrite()
                .matches(
                        logicalOlapTableSink(
                                logicalProject(
                                        logicalFilter(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    public void testFromClauseUpdate() {
        String sql = "update t1 a set v1 = t2.v1 + 2, v2 = a.v1 * 2 "
                + "from src join t2 on src.k1 = t2.k1 where t2.k1 = a.k1";
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, parsed);
        UpdateCommand command = ((UpdateCommand) parsed);
        LogicalPlan plan = command.completeQueryPlan(connectContext, command.getLogicalQuery());
        PlanChecker.from(connectContext, plan)
                .analyze(plan)
                .matches(
                        logicalFilter(
                                logicalJoin(
                                        logicalSubQueryAlias(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalJoin(
                                                logicalOlapScan(),
                                                logicalFilter(
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                );
    }
}
