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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReadLockTest extends SSBTestBase {

    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testSimple() {
        String sql = "SELECT s_suppkey FROM supplier";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );

        Map<List<String>, TableIf> f = statementContext.getTables();
        Assertions.assertEquals(1, f.size());
        Set<String> tableNames = new HashSet<>();
        for (Map.Entry<List<String>, TableIf> entry : f.entrySet()) {
            TableIf table = entry.getValue();
            tableNames.add(table.getName());
        }
        Assertions.assertTrue(tableNames.contains("supplier"));
    }

    @Test
    public void testCTE() {
        String sql = "        WITH cte1 AS (\n"
                + "            SELECT s_suppkey\n"
                + "            FROM supplier\n"
                + "            WHERE s_suppkey < 30\n"
                + "        )\n"
                + "        SELECT *\n"
                + "        FROM cte1 as t1, cte1 as t2";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        Map<List<String>, TableIf> f = statementContext.getTables();
        Assertions.assertEquals(1, f.size());
        for (Map.Entry<List<String>, TableIf> entry : f.entrySet()) {
            TableIf table = entry.getValue();
            Assertions.assertEquals("supplier", table.getName());
        }
    }

    @Test
    public void testSubQuery() {
        String sql = "SELECT s_suppkey FROM (SELECT * FROM supplier) t";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        Map<List<String>, TableIf> f = statementContext.getTables();
        Assertions.assertEquals(1, f.size());
        Set<String> tableNames = new HashSet<>();
        for (Map.Entry<List<String>, TableIf> entry : f.entrySet()) {
            TableIf table = entry.getValue();
            tableNames.add(table.getName());
        }
        Assertions.assertTrue(tableNames.contains("supplier"));
    }

    @Test
    public void testScalarSubQuery() {
        String sql = "SELECT s_suppkey FROM supplier WHERE s_suppkey > (SELECT MAX(lo_orderkey) FROM lineorder)";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        Map<List<String>, TableIf> f = statementContext.getTables();
        Assertions.assertEquals(2, f.size());
        Set<String> tableNames = new HashSet<>();
        for (Map.Entry<List<String>, TableIf> entry : f.entrySet()) {
            TableIf table = entry.getValue();
            tableNames.add(table.getName());
        }
        Assertions.assertTrue(tableNames.contains("supplier"));
        Assertions.assertTrue(tableNames.contains("lineorder"));
    }

    @Test
    public void testInserInto() {
        String sql = "INSERT INTO supplier(s_suppkey) SELECT lo_orderkey FROM lineorder";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        InsertIntoTableCommand insertIntoTableCommand = (InsertIntoTableCommand) parser.parseSingle(sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(
                (LogicalPlan) insertIntoTableCommand.getExplainPlan(connectContext),
                PhysicalProperties.ANY
        );
        Map<List<String>, TableIf> f = statementContext.getTables();
        Assertions.assertEquals(1, f.size());
        Set<String> tableNames = new HashSet<>();
        for (Map.Entry<List<String>, TableIf> entry : f.entrySet()) {
            TableIf table = entry.getValue();
            tableNames.add(table.getName());
        }
        Assertions.assertTrue(tableNames.contains("lineorder"));
        f = statementContext.getInsertTargetTables();
        Assertions.assertEquals(1, f.size());
        tableNames = new HashSet<>();
        for (Map.Entry<List<String>, TableIf> entry : f.entrySet()) {
            TableIf table = entry.getValue();
            tableNames.add(table.getName());
        }
        Assertions.assertTrue(tableNames.contains("supplier"));
    }
}
