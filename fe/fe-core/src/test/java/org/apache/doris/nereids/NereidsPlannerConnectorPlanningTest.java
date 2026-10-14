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

package org.apache.doris.nereids;

import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** {@link NereidsPlanner#plan} ends the planning of the statement's connector scope. */
public class NereidsPlannerConnectorPlanningTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("connector_planning");
    }

    @Test
    public void planEndsConnectorPlanningWhetherItSucceedsOrFails() {
        // Identical scans share their planned ranges only within one plan, so what a connector keeps for planning
        // must not outlive the plan, whether the plan is complete or planning fails; the values the statement keeps
        // stay. MUTATION: planWithLock's cleanup (releasePlannerResources) not ending connector planning -> red for
        // both statements.
        assertPlanEndsConnectorPlanning("select 1", true);
        assertPlanEndsConnectorPlanning("select * from no_such_table", false);
    }

    private void assertPlanEndsConnectorPlanning(String sql, boolean planned) {
        StatementContext statementContext = new StatementContext();
        ConnectorStatementScope scope = statementContext.getOrCreateConnectorStatementScope();
        Object memo = scope.computeIfAbsentForPlanning("memo", Object::new);
        Object table = scope.computeIfAbsent("table", Object::new);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        LogicalPlanAdapter statement = new LogicalPlanAdapter(new NereidsParser().parseSingle(sql), statementContext);

        if (planned) {
            planner.plan(statement);
        } else {
            Assertions.assertThrows(RuntimeException.class, () -> planner.plan(statement));
        }

        Assertions.assertNotSame(memo, scope.computeIfAbsentForPlanning("memo", Object::new), sql);
        Assertions.assertSame(table, scope.computeIfAbsent("table", Object::new), sql);
    }
}
