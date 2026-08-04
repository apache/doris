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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the {@code skipAuth} flag lifecycle in
 * {@link InsertOverwriteTableCommand#run}.
 *
 * <p>For an OLAP target the command runs its internal partition-replacement work with the
 * auth check skipped, and must reset that flag before returning. The flag used to be flipped
 * on <em>before</em> the surrounding try/finally, so any statement that threw in between (for
 * example the {@code @branch}-on-non-iceberg guard) returned without the finally ever running,
 * leaving the flag set for the rest of the connection. This test drives exactly that failing
 * path and asserts the flag is back to its original value afterwards.</p>
 */
public class InsertOverwriteSkipAuthResetTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("iot_skipauth");
        createTable("CREATE TABLE t (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
    }

    @Test
    public void skipAuthIsResetAfterFailingBranchOverwrite() throws Exception {
        // @branch is only valid for iceberg; against an OLAP table run() plans the sink, marks the
        // overwrite as needing the auth-skip, then throws the guard below. The flag must not leak.
        String sql = "INSERT OVERWRITE TABLE iot_skipauth.t@BRANCH(anything) SELECT * FROM iot_skipauth.t";

        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof InsertOverwriteTableCommand,
                "an INSERT OVERWRITE ... @branch statement should parse to InsertOverwriteTableCommand");
        InsertOverwriteTableCommand command = (InsertOverwriteTableCommand) parsed;

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        statementContext.setConnectContext(connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, sql);

        // Baseline: the connection starts with the flag off.
        connectContext.setSkipAuth(false);

        Exception thrown = Assertions.assertThrows(Exception.class,
                () -> command.run(connectContext, executor));
        // Prove we actually reached the guard that fires AFTER the overwrite is marked as needing the
        // auth-skip -- otherwise this test would pass without exercising the leak path at all.
        Assertions.assertTrue(thrown.getMessage() != null
                        && thrown.getMessage().contains("Only support insert overwrite into iceberg table's branch"),
                "expected the @branch-on-non-iceberg guard to fire, but got: " + thrown.getMessage());

        // The flag must have been reset even though run() exited via an exception.
        Assertions.assertFalse(connectContext.isSkipAuth(),
                "skipAuth must be reset after a failed INSERT OVERWRITE, but it was left set");
    }
}
