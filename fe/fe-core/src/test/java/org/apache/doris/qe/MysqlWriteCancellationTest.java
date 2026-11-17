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

import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Status;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.xnio.StreamConnection;
import org.xnio.conduits.ConduitStreamSinkChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class MysqlWriteCancellationTest {
    @Test
    public void testPreparedCancelledPartialWriteCannotResume() throws Exception {
        StreamConnection connection = Mockito.mock(StreamConnection.class);
        Mockito.when(connection.getPeerAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 12345));
        ConduitStreamSinkChannel sink = Mockito.mock(ConduitStreamSinkChannel.class);
        Mockito.when(connection.getSinkChannel()).thenReturn(sink);
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        ctx.setQueryId(new TUniqueId(7, 7));
        MysqlChannel channel = ctx.getMysqlChannel();
        AtomicReference<Status> reason = new AtomicReference<>();
        AtomicInteger bytesWritten = new AtomicInteger();
        channel.setQueryCancelReason(reason::get);
        Mockito.when(sink.write(Mockito.any(ByteBuffer.class))).thenAnswer(inv -> {
            ByteBuffer buffer = inv.getArgument(0);
            buffer.position(buffer.position() + 8);
            bytesWritten.addAndGet(8);
            reason.set(new Status(TStatusCode.CANCELLED, "cancelled partial prepared result"));
            return 8;
        }).thenReturn(0).thenAnswer(inv -> {
            // The client resumes reading before finalizeCommand tries to send an ERR.
            ByteBuffer buffer = inv.getArgument(0);
            int length = buffer.remaining();
            buffer.position(buffer.limit());
            bytesWritten.addAndGet(length);
            return length;
        });
        Mockito.when(sink.flush()).thenReturn(true);
        PrepareCommand command = Mockito.mock(PrepareCommand.class);
        Mockito.when(command.getOriginalStmt()).thenReturn(new OriginStatement("select 1", 0));
        PreparedStatementContext prepared = new PreparedStatementContext(command, ctx, new StatementContext(), "7");
        MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
        ByteBuffer execute = ByteBuffer.allocate(9);
        execute.position(execute.limit());
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                (executor, construction) -> Mockito.doAnswer(inv -> {
                    try {
                        channel.sendAndFlush(ByteBuffer.allocate(1024 * 1024));
                    } catch (ConnectionException failure) {
                        // Command execution wraps transport exceptions before handleExecute catches Throwable.
                        throw new NereidsException(failure);
                    }
                    return null;
                }).when(executor).execute());
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            processor.handleExecute(command, 7, prepared, execute, null);
            Assertions.assertEquals(8, bytesWritten.get());
            Assertions.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.isKilled());
            Mockito.verify(connection).close();
            channel.clearQueryCancelReason();
            Assertions.assertThrows(ConnectionException.class, processor::finalizeCommand);
            channel.reset();
            Assertions.assertThrows(ConnectionException.class,
                    () -> channel.sendAndFlush(ByteBuffer.wrap(new byte[] {0})));
            Assertions.assertEquals(8, bytesWritten.get());
        }
    }

    @Test
    public void testCommandClearsCancellationOnSuccessAndFailure() throws Exception {
        for (boolean fail : new boolean[] {false, true}) {
            MysqlChannel channel = Mockito.mock(MysqlChannel.class);
            Mockito.when(channel.fetchOnePacket())
                    .thenReturn(ByteBuffer.wrap(new byte[] {14 /* COM_PING */}));
            ConnectContext context = new ConnectContext(new MysqlProtocolAdapter(channel));
            context.getSessionVariable().setWorkloadGroup("normal");
            StmtExecutor cancelled = Mockito.mock(StmtExecutor.class);
            Status reason = new Status(TStatusCode.CANCELLED, "previous command cancelled");
            Mockito.when(cancelled.getPendingCancelReason()).thenReturn(reason);
            MysqlConnectProcessor processor = new MysqlConnectProcessor(context) {
                @Override
                public void finalizeCommand() throws IOException {
                    executor = cancelled;
                    if (fail) {
                        throw new ConnectionException("write failed");
                    }
                }
            };
            if (fail) {
                Assertions.assertThrows(ConnectionException.class, processor::processOnce);
            } else {
                processor.processOnce();
            }
            Mockito.verify(channel).setQueryCancelReason(Mockito.<Supplier<Status>>any());
            Mockito.verify(channel).clearQueryCancelReason();
        }
    }
}
