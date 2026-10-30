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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * The tests for utils
 */
public class UtilsTest {
    @Test
    public void testExecutePlanRestoresShowStatement() {
        StatementContext statementContext = new StatementContext();
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        NereidsParser parser = new NereidsParser();
        LogicalPlanAdapter showStmt = new LogicalPlanAdapter(
                parser.parseSingle("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"), statementContext);
        LogicalPlan selectPlan = parser.parseSingle("SELECT 1");
        Mockito.when(ctx.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getParsedStmt()).thenReturn(showStmt);
        Mockito.when(executor.executeInternalQueryWithoutAudit()).thenReturn(Collections.emptyList());

        Utils.executePlan(ctx, executor, selectPlan);

        InOrder calls = Mockito.inOrder(executor);
        calls.verify(executor).setParsedStmt(Mockito.argThat(stmt ->
                ((LogicalPlanAdapter) stmt).getLogicalPlan().stmtType() == StmtType.SELECT));
        calls.verify(executor).executeInternalQueryWithoutAudit();
        calls.verify(executor).setParsedStmt(showStmt);
        Assertions.assertEquals(StmtType.SHOW, showStmt.getLogicalPlan().stmtType());
    }

    @Test
    public void containChinese() {
        String chinese = "123数据库";
        Assertions.assertTrue(Utils.containChinese(chinese));

        String en = "database123";
        Assertions.assertFalse(Utils.containChinese(en));
    }

    @Test
    public void testAddOverflows() {
        // sums that fit in the long range do not overflow
        Assertions.assertFalse(Utils.addOverflows(0, 0));
        Assertions.assertFalse(Utils.addOverflows(1, 2));
        Assertions.assertFalse(Utils.addOverflows(Long.MAX_VALUE, 0));
        Assertions.assertFalse(Utils.addOverflows(0, Long.MAX_VALUE));
        Assertions.assertFalse(Utils.addOverflows(Long.MAX_VALUE - 1, 1));

        // limit + offset that exceeds the long range overflows. This is the case (e.g. LIMIT and
        // OFFSET both BIGINT_MAX) that previously produced a negative, illegal child limit when
        // pushing TopN/Limit down; callers now skip the rewrite instead.
        Assertions.assertTrue(Utils.addOverflows(Long.MAX_VALUE, 1));
        Assertions.assertTrue(Utils.addOverflows(Long.MAX_VALUE, Long.MAX_VALUE));
        Assertions.assertTrue(Utils.addOverflows(Long.MAX_VALUE - 1, 2));
    }
}
