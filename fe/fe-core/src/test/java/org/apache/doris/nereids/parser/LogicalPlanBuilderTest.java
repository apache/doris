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

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromUsingCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for LogicalPlanBuilder to verify DELETE/UPDATE with ORDER BY and LIMIT
 * produce the correct logical plan tree structure.
 */
public class LogicalPlanBuilderTest {

    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testDeleteWithOrderByLimitProducesCorrectPlanTree() {
        String sql = "DELETE FROM t ORDER BY c1 LIMIT 10";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromUsingCommand.class, plan);
        LogicalPlan query = ((DeleteFromUsingCommand) plan).getLogicalQuery();
        // plan tree: LogicalLimit -> LogicalSort -> CheckPolicy(UnboundRelation)
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalLimit<?> limit = (LogicalLimit<?>) query;
        Assertions.assertEquals(10, limit.getLimit());
        Assertions.assertEquals(0, limit.getOffset());
        Assertions.assertInstanceOf(LogicalSort.class, limit.child());
        LogicalSort<?> sort = (LogicalSort<?>) limit.child();
        Assertions.assertEquals(1, sort.getOrderKeys().size());
    }

    @Test
    public void testDeleteWithOrderByLimitOffset() {
        String sql = "DELETE FROM t ORDER BY c1 ASC NULLS FIRST LIMIT 10, 3";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromUsingCommand.class, plan);
        LogicalPlan query = ((DeleteFromUsingCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalLimit<?> limit = (LogicalLimit<?>) query;
        // LIMIT offset, count: offset=10, limit=3
        Assertions.assertEquals(3, limit.getLimit());
        Assertions.assertEquals(10, limit.getOffset());
        Assertions.assertInstanceOf(LogicalSort.class, limit.child());
    }

    @Test
    public void testDeleteWithWhereOrderByLimit() {
        String sql = "DELETE FROM t WHERE c1 > 0 ORDER BY c1 DESC NULLS LAST LIMIT 5";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromUsingCommand.class, plan);
        LogicalPlan query = ((DeleteFromUsingCommand) plan).getLogicalQuery();
        // plan tree: LogicalLimit -> LogicalSort -> LogicalFilter -> CheckPolicy(UnboundRelation)
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalLimit<?> limit = (LogicalLimit<?>) query;
        Assertions.assertEquals(5, limit.getLimit());
        Assertions.assertInstanceOf(LogicalSort.class, limit.child());
        LogicalSort<?> sort = (LogicalSort<?>) limit.child();
        Assertions.assertInstanceOf(LogicalFilter.class, sort.child());
    }

    @Test
    public void testDeleteWithOrderByOnlyProducesDeleteFromUsingCommand() {
        String sql = "DELETE FROM t ORDER BY c1";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromUsingCommand.class, plan);
        LogicalPlan query = ((DeleteFromUsingCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalSort.class, query);
    }

    @Test
    public void testDeleteWithLimitOnlyProducesDeleteFromUsingCommand() {
        String sql = "DELETE FROM t LIMIT 5";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromUsingCommand.class, plan);
        LogicalPlan query = ((DeleteFromUsingCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalLimit<?> limit = (LogicalLimit<?>) query;
        Assertions.assertEquals(5, limit.getLimit());
    }

    @Test
    public void testDeleteWithoutOrderByLimitProducesDeleteFromCommand() {
        String sql = "DELETE FROM t WHERE c1 = 1";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromCommand.class, plan);
        Assertions.assertFalse(plan instanceof DeleteFromUsingCommand);
    }

    @Test
    public void testUpdateWithOrderByLimitProducesCorrectPlanTree() {
        String sql = "UPDATE t SET c1 = 10 ORDER BY c2 LIMIT 100";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, plan);
        LogicalPlan query = ((UpdateCommand) plan).getLogicalQuery();
        // plan tree: LogicalLimit -> LogicalSort -> CheckPolicy(UnboundRelation)
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalLimit<?> limit = (LogicalLimit<?>) query;
        Assertions.assertEquals(100, limit.getLimit());
        Assertions.assertEquals(0, limit.getOffset());
        Assertions.assertInstanceOf(LogicalSort.class, limit.child());
        LogicalSort<?> sort = (LogicalSort<?>) limit.child();
        Assertions.assertEquals(1, sort.getOrderKeys().size());
    }

    @Test
    public void testUpdateWithOrderByLimitOffset() {
        String sql = "UPDATE t SET c1 = 10 ORDER BY c2 LIMIT 100, 20";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, plan);
        LogicalPlan query = ((UpdateCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalLimit<?> limit = (LogicalLimit<?>) query;
        // LIMIT offset, count: offset=100, limit=20
        Assertions.assertEquals(20, limit.getLimit());
        Assertions.assertEquals(100, limit.getOffset());
        Assertions.assertInstanceOf(LogicalSort.class, limit.child());
    }

    @Test
    public void testUpdateWithWhereOrderByLimit() {
        String sql = "UPDATE t SET c1 = 10 WHERE c2 > 5 ORDER BY c2 DESC LIMIT 50";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, plan);
        LogicalPlan query = ((UpdateCommand) plan).getLogicalQuery();
        // plan tree: LogicalLimit -> LogicalSort -> LogicalFilter -> CheckPolicy(UnboundRelation)
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalLimit<?> limit = (LogicalLimit<?>) query;
        Assertions.assertInstanceOf(LogicalSort.class, limit.child());
        LogicalSort<?> sort = (LogicalSort<?>) limit.child();
        Assertions.assertInstanceOf(LogicalFilter.class, sort.child());
    }

    @Test
    public void testUpdateWithOrderByOnlyProducesCorrectPlanTree() {
        String sql = "UPDATE t SET c1 = 10 ORDER BY c2";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, plan);
        LogicalPlan query = ((UpdateCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalSort.class, query);
    }

    @Test
    public void testUpdateWithLimitOnlyProducesCorrectPlanTree() {
        String sql = "UPDATE t SET c1 = 10 LIMIT 50";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, plan);
        LogicalPlan query = ((UpdateCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalLimit<?> limit = (LogicalLimit<?>) query;
        Assertions.assertEquals(50, limit.getLimit());
    }

    @Test
    public void testUpdateWithoutOrderByLimitProducesUpdateCommand() {
        String sql = "UPDATE t SET c1 = 10 WHERE c2 = 1";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, plan);
        LogicalPlan query = ((UpdateCommand) plan).getLogicalQuery();
        // No sort or limit in query
        Assertions.assertInstanceOf(LogicalFilter.class, query);
    }

    @Test
    public void testDeleteWithMultipleOrderByColumns() {
        String sql = "DELETE FROM t ORDER BY c1 ASC, c2 DESC NULLS LAST LIMIT 10";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromUsingCommand.class, plan);
        LogicalPlan query = ((DeleteFromUsingCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalSort<?> sort = (LogicalSort<?>) ((LogicalLimit<?>) query).child();
        Assertions.assertEquals(2, sort.getOrderKeys().size());
    }

    @Test
    public void testUpdateWithMultipleOrderByColumns() {
        String sql = "UPDATE t SET c1 = 10 ORDER BY c2 ASC, c3 DESC NULLS FIRST LIMIT 5";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, plan);
        LogicalPlan query = ((UpdateCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalSort<?> sort = (LogicalSort<?>) ((LogicalLimit<?>) query).child();
        Assertions.assertEquals(2, sort.getOrderKeys().size());
    }

    @Test
    public void testDeleteOrderByIntegerOrdinalConvertedToUnboundSlot() {
        String sql = "DELETE FROM t ORDER BY 1 LIMIT 10";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromUsingCommand.class, plan);
        LogicalPlan query = ((DeleteFromUsingCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalSort<?> sort = (LogicalSort<?>) ((LogicalLimit<?>) query).child();
        Assertions.assertEquals(1, sort.getOrderKeys().size());
        // Integer ordinal should be converted to UnboundSlot, not remain as IntegerLikeLiteral
        Assertions.assertInstanceOf(UnboundSlot.class, sort.getOrderKeys().get(0).getExpr());
        Assertions.assertFalse(sort.getOrderKeys().get(0).getExpr() instanceof IntegerLikeLiteral);
    }

    @Test
    public void testUpdateOrderByIntegerOrdinalConvertedToUnboundSlot() {
        String sql = "UPDATE t SET c1 = 10 ORDER BY 1 LIMIT 100";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, plan);
        LogicalPlan query = ((UpdateCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalSort<?> sort = (LogicalSort<?>) ((LogicalLimit<?>) query).child();
        Assertions.assertEquals(1, sort.getOrderKeys().size());
        Assertions.assertInstanceOf(UnboundSlot.class, sort.getOrderKeys().get(0).getExpr());
        Assertions.assertFalse(sort.getOrderKeys().get(0).getExpr() instanceof IntegerLikeLiteral);
    }

    @Test
    public void testDeleteOrderByMixedOrdinalAndColumn() {
        String sql = "DELETE FROM t ORDER BY 1, c2 DESC LIMIT 5";
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(DeleteFromUsingCommand.class, plan);
        LogicalPlan query = ((DeleteFromUsingCommand) plan).getLogicalQuery();
        Assertions.assertInstanceOf(LogicalLimit.class, query);
        LogicalSort<?> sort = (LogicalSort<?>) ((LogicalLimit<?>) query).child();
        Assertions.assertEquals(2, sort.getOrderKeys().size());
        // First key: integer ordinal converted to UnboundSlot
        Assertions.assertInstanceOf(UnboundSlot.class, sort.getOrderKeys().get(0).getExpr());
        // Second key: column name remains as UnboundSlot
        Assertions.assertInstanceOf(UnboundSlot.class, sort.getOrderKeys().get(1).getExpr());
    }
}
