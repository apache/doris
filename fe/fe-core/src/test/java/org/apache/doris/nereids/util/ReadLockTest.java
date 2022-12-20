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

import org.apache.doris.catalog.Table;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ReadLockTest extends SSBTestBase {

    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testSimple() {
        String sql = "SELECT s_suppkey FROM supplier";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.plan(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        CascadesContext cascadesContext = planner.getCascadesContext();
        List<Table> f = (List<Table>) Deencapsulation.getField(cascadesContext, "tables");
        Assertions.assertEquals(1, f.size());
        Assertions.assertEquals("supplier", f.stream().map(Table::getName).findFirst().get());

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
        planner.plan(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        CascadesContext cascadesContext = planner.getCascadesContext();
        List<Table> f = (List<Table>) Deencapsulation.getField(cascadesContext, "tables");
        Assertions.assertEquals(1, f.size());
        Assertions.assertEquals("supplier", f.stream().map(Table::getName).findFirst().get());
    }

    @Test
    public void testSubQuery() {
        String sql = "SELECT s_suppkey FROM (SELECT * FROM supplier) t";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.plan(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        CascadesContext cascadesContext = planner.getCascadesContext();
        List<Table> f = (List<Table>) Deencapsulation.getField(cascadesContext, "tables");

        Assertions.assertEquals(1, f.size());
        Assertions.assertEquals("supplier", f.stream().map(Table::getName).findFirst().get());
    }

    @Test
    public void testScalarSubQuery() {
        String sql = "SELECT s_suppkey FROM supplier WHERE s_suppkey > (SELECT MAX(lo_orderkey) FROM lineorder)";
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.plan(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        CascadesContext cascadesContext = planner.getCascadesContext();
        List<Table> f = (List<Table>) Deencapsulation.getField(cascadesContext, "tables");
        Assertions.assertEquals(2, f.size());
        Assertions.assertEquals("supplier", f.stream().map(Table::getName).findFirst().get());

    }
}
