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

package org.apache.doris.qe;

import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.QueryState.MysqlStateType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class MysqlConnectProcessorExecuteCloseTest {

    /**
     * A COM_STMT_EXECUTE is a statement of its own: what its planning kept on this frontend for
     * the BE (a remote Doris scan's Flight SQL session on the other frontend) is released when
     * the execution ends, not when the prepared statement is executed next, when the statement
     * failed before a coordinator took the plan (a SQL block rule on the scan).
     */
    @Test
    public void testEveryExecuteEndsItsStatement() throws Exception {
        ConnectContext context = new ConnectContext(null, false);
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        PrepareCommand command = Mockito.mock(PrepareCommand.class);
        Mockito.when(command.getOriginalStmt()).thenReturn(new OriginStatement("select 1", 0));
        StatementContext statementContext = new StatementContext(context, new OriginStatement("select 1", 0));
        context.setStatementContext(statementContext);
        PreparedStatementContext prepared = new PreparedStatementContext(
                command, context, statementContext, "select 1");
        ByteBuffer packet = ByteBuffer.allocate(9);
        packet.position(packet.limit()); // COM_STMT_EXECUTE header consumed, no parameter payload.
        ScanNode scanNode = Mockito.mock(ScanNode.class);

        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                (executor, settings) -> Mockito.doAnswer(invocation -> {
                    context.getStatementContext().stopScanNodeAtClose(scanNode);
                    throw new RuntimeException("sql block rule");
                }).when(executor).execute());
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            new MysqlConnectProcessor(context).handleExecute(command, 7, prepared, packet, null);
            Assertions.assertEquals(1, executors.constructed().size());
        }

        Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
        Mockito.verify(scanNode).stop();
    }
}
