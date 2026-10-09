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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.Status;
import org.apache.doris.datasource.lance.LanceIndexPrewarm;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.WarmupSelectCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

public class WarmupSelectIndexExecutionTest {
    private WarmupSelectCommand command() {
        return (WarmupSelectCommand) new NereidsParser().parseSingle(
                "WARM UP SELECT * FROM lake.db.items PROPERTIES (read_index_only=true)");
    }

    private StmtExecutor executor(ConnectContext context, WarmupSelectCommand command) {
        OriginStatement sql = new OriginStatement("WARM UP SELECT * FROM lake.db.items PROPERTIES (read_index_only=true)", 0);
        LogicalPlanAdapter adapter = new LogicalPlanAdapter(command, new StatementContext(context, sql));
        adapter.setOrigStmt(sql);
        return new StmtExecutor(context, adapter, true);
    }

    @Test
    public void advertisesTheExecutionSchemaDuringPrepare() {
        Assertions.assertEquals(5, command().getResultSetMetaData().getColumns().size());
        Assertions.assertEquals("DatasetVersion", command().getResultSetMetaData().getColumns().get(2).getName());
    }

    @Test
    public void preparedRetryDoesNotReuseCancellationAndEarlyCancelIsPreserved() throws Exception {
        AtomicReference<BooleanSupplier> cancellation = new AtomicReference<>();
        new MockUp<LanceIndexPrewarm>() {
            @Mock
            public void run(ConnectContext ctx, StmtExecutor executor, TableNameInfo table,
                    List<String> columns, BooleanSupplier cancelled) {
                cancellation.set(cancelled);
            }
        };
        ConnectContext context = new ConnectContext();
        WarmupSelectCommand retained = command();
        StmtExecutor first = executor(context, retained);
        retained.run(context, first);
        Assertions.assertFalse(cancellation.get().getAsBoolean());
        first.cancel(new Status(TStatusCode.CANCELLED, "test cancellation"));
        Assertions.assertTrue(cancellation.get().getAsBoolean());

        StmtExecutor retry = executor(context, retained);
        retained.run(context, retry);
        Assertions.assertFalse(cancellation.get().getAsBoolean(), "A new EXECUTE must not inherit KILL QUERY");

        StmtExecutor earlyCancelled = executor(context, retained);
        earlyCancelled.cancel(new Status(TStatusCode.CANCELLED, "cancel before run"));
        retained.run(context, earlyCancelled);
        Assertions.assertTrue(cancellation.get().getAsBoolean(),
                "Starting the command must not erase a concurrent cancellation");
    }
}
