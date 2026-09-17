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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MysqlConnectProcessorCursorFetchTest {
    private static final int CURSOR_TYPE_READ_ONLY = 1;

    @Test
    public void testZeroParameterExecutePreservesForwardingBuffer() throws Exception {
        for (boolean proxy : new boolean[] {false, true}) {
            ConnectContext context = new ConnectContext(null, proxy);
            context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
            PrepareCommand command = Mockito.mock(PrepareCommand.class);
            Mockito.when(command.getOriginalStmt()).thenReturn(new OriginStatement("select 1", 0));
            StatementContext statementContext = new StatementContext();
            PreparedStatementContext prepared = new PreparedStatementContext(
                    command, context, statementContext, "select 1");
            ByteBuffer packet = ByteBuffer.allocate(9);
            packet.position(packet.limit()); // COM_STMT_EXECUTE header consumed, no parameter payload.
            try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                    MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
                new MysqlConnectProcessor(context).handleExecute(command, 7, prepared, packet, null);
                Assertions.assertEquals(1, executors.constructed().size());
                Mockito.verify(executors.constructed().get(0)).execute();
                if (proxy) {
                    Assertions.assertNull(context.getPrepareExecuteBuffer());
                } else {
                    Assertions.assertNotNull(context.getPrepareExecuteBuffer());
                    Assertions.assertNotSame(packet, context.getPrepareExecuteBuffer());
                    Assertions.assertEquals(0, context.getPrepareExecuteBuffer().remaining());
                    Assertions.assertEquals(9, packet.position());
                }
            }
        }
    }

    @Test
    public void testUnidentifiedDeprecatedEofCursorReachesPreparedStatementLookup() throws Exception {
        ConnectContext context = execute(true, true, false);
        Assertions.assertTrue(context.getState().getErrorMessage().contains(
                "Unknown prepared statement handler"));
    }

    @Test
    public void testCompatibilityGateOnlyAppliesToAmbiguousProtocol() throws Exception {
        Assertions.assertTrue(execute(false, true, false).getState().getErrorMessage().contains(
                "Unknown prepared statement handler"));
        Assertions.assertTrue(execute(true, false, false).getState().getErrorMessage().contains(
                "Unknown prepared statement handler"));
        Assertions.assertTrue(execute(true, true, true).getState().getErrorMessage().contains(
                "Unknown prepared statement handler"));
    }

    private ConnectContext execute(boolean cursorRequested, boolean clientDeprecatedEof,
            boolean identifiedClient) throws Exception {
        ConnectContext context = new ConnectContext();
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        if (clientDeprecatedEof) {
            context.getMysqlChannel().setClientDeprecatedEOF();
        }
        if (identifiedClient) {
            context.setConnectAttributes(ImmutableMap.of(
                    "_client_name", "MySQL Connector/J", "_client_version", "8.2.0"));
        }

        ByteBuffer packet = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        packet.putInt(7);
        packet.put((byte) (cursorRequested ? CURSOR_TYPE_READ_ONLY : 0));
        packet.putInt(1);
        packet.flip();

        MysqlConnectProcessor processor = new MysqlConnectProcessor(context);
        Field packetField = MysqlConnectProcessor.class.getDeclaredField("packetBuf");
        packetField.setAccessible(true);
        packetField.set(processor, packet);
        Method handleExecute = MysqlConnectProcessor.class.getDeclaredMethod("handleExecute");
        handleExecute.setAccessible(true);
        handleExecute.invoke(processor);
        return context;
    }
}
