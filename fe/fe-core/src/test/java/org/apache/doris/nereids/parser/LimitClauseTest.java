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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LimitClauseTest {
    @Test
    public void testLimit() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "SELECT b FROM test order by a limit 3 offset 100";
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof LogicalLimit);
        LogicalLimit limit = (LogicalLimit) logicalPlan;
        Assertions.assertEquals(3, limit.getLimit());
        Assertions.assertEquals(100, limit.getOffset());
        Assertions.assertEquals(1, limit.children().size());
        Assertions.assertTrue(limit.child(0) instanceof  LogicalSort);

        sql = "SELECT b FROM test order by a limit 100, 3";
        logicalPlan = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof LogicalLimit);
        limit = (LogicalLimit) logicalPlan;
        Assertions.assertEquals(3, limit.getLimit());
        Assertions.assertEquals(100, limit.getOffset());
        Assertions.assertEquals(1, limit.children().size());
        Assertions.assertTrue(limit.child(0) instanceof LogicalSort);

        sql = "SELECT b FROM test limit 3";
        logicalPlan = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof LogicalLimit);
        limit = (LogicalLimit) logicalPlan;
        Assertions.assertEquals(3, limit.getLimit());
        Assertions.assertEquals(0, limit.getOffset());
        Assertions.assertEquals(1, limit.children().size());
        Assertions.assertTrue(limit.child(0) instanceof LogicalProject);

        sql = "SELECT b FROM test order by a limit 3";
        logicalPlan = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof LogicalLimit);
        limit = (LogicalLimit) logicalPlan;
        Assertions.assertEquals(3, limit.getLimit());
        Assertions.assertEquals(0, limit.getOffset());
        Assertions.assertEquals(1, limit.children().size());
        Assertions.assertTrue(limit.child(0) instanceof LogicalSort);
    }

    @Test
    public void testLimitExceptionCase() {
        NereidsParser nereidsParser = new NereidsParser();
        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    String sql = "SELECT b FROM test limit 3 offset 100";
                    nereidsParser.parseSingle(sql);
                });
        Assertions.assertEquals("OFFSET requires an ORDER BY clause",
                    exception.getMessage());

        exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    String sql = "SELECT b FROM test limit 100, 3";
                    nereidsParser.parseSingle(sql);
                });
        Assertions.assertEquals("OFFSET requires an ORDER BY clause",
                    exception.getMessage());

    }

    @Test
    public void testNoLimit() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "select a from tbl order by x";
        LogicalPlan root = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(root instanceof LogicalSort);
    }


    @Test
    public void testNoQueryOrganization() {
        NereidsParser nereidsParser = new NereidsParser();
        String sql = "select a from tbl";
        LogicalPlan root = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(root instanceof LogicalProject);
    }
}
