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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class ExpressionDefaultPartialUpdateGuardTest extends TestWithFeService {

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        String tableSql = "create table test.tbl_expr_default_mow (\n"
                + "  k1 int not null,\n"
                + "  v1 int null,\n"
                + "  d datev2 not null default to_date(now())\n"
                + ")\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\",\"enable_unique_key_merge_on_write\"=\"true\")";
        createTable(tableSql);
    }

    private static boolean anyCauseMessageContains(Throwable t, String substring) {
        while (t != null) {
            String message = t.getMessage();
            if (message != null && message.contains(substring)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private void analyzeInsertWithoutTxn(String sql) throws Exception {
        connectContext.setThreadLocalInfo();

        StatementContext statementContext = createStatementCtx(sql);
        LogicalPlan parsedPlan = new NereidsParser().parseSingle(sql);

        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

        InsertIntoTableCommand insertIntoTableCommand = (InsertIntoTableCommand) parsedPlan;
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(parsedPlan, statementContext);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, logicalPlanAdapter);

        insertIntoTableCommand.initPlan(connectContext, stmtExecutor, false);
    }

    @Test
    public void testInsertPartialUpdateRejectedWhenExpressionDefaultAndSessionVarDisabled() {
        connectContext.getSessionVariable().setEnableUniqueKeyPartialUpdate(true);
        connectContext.getSessionVariable().setAllowPartialUpdateWithExpressionDefault(false);

        Throwable t = Assertions.assertThrows(Throwable.class,
                () -> analyzeInsertWithoutTxn("insert into tbl_expr_default_mow(k1) values (1)"));

        Assertions.assertTrue(
                anyCauseMessageContains(t, "Partial update is not supported for table with expression default value"),
                () -> "Unexpected exception: " + t
        );
    }

    @Test
    public void testInsertPartialUpdateAllowedWhenSessionVarEnabled() {
        connectContext.getSessionVariable().setEnableUniqueKeyPartialUpdate(true);
        connectContext.getSessionVariable().setAllowPartialUpdateWithExpressionDefault(true);

        Assertions.assertDoesNotThrow(
                () -> analyzeInsertWithoutTxn("insert into tbl_expr_default_mow(k1) values (1)"));
    }

    @Test
    public void testInsertValuesDowngradedToUpsertNotRejectedWhenExpressionDefaultAndSessionVarDisabled() {
        connectContext.getSessionVariable().setEnableUniqueKeyPartialUpdate(true);
        connectContext.getSessionVariable().setAllowPartialUpdateWithExpressionDefault(false);

        Assertions.assertDoesNotThrow(
                () -> analyzeInsertWithoutTxn(
                        "insert into tbl_expr_default_mow values (1, 2, to_date('2024-01-01'))"));
    }
}
