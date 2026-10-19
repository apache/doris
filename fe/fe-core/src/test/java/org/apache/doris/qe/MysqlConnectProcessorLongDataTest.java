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

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.PrepareCommandPlanner;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MysqlConnectProcessorLongDataTest {
    @Test
    public void testUploadHasNoResponseAndPingStillWorks() throws Exception {
        Harness h = new Harness(MysqlColType.MYSQL_TYPE_BLOB);
        h.upload(7, 0, new byte[] {0, (byte) 0xff});
        h.upload(7, 0, new byte[] {1});
        h.upload(123, 0, new byte[] {2}); // Unknown statements must not produce a response either.
        Assertions.assertArrayEquals(new byte[] {0, (byte) 0xff, 1}, h.prepared.getLongData(0));
        Mockito.verify(h.channel, Mockito.never()).sendAndFlush(Mockito.any());
        h.command(ByteBuffer.wrap(new byte[] {(byte) 0x0e}));
        Mockito.verify(h.channel).sendAndFlush(Mockito.any());
    }

    @Test
    public void testMixedParametersAndForwardingWithCachedTypes() throws Exception {
        Harness h = new Harness(MysqlColType.MYSQL_TYPE_BLOB, MysqlColType.MYSQL_TYPE_LONG,
                MysqlColType.MYSQL_TYPE_VARSTRING, MysqlColType.MYSQL_TYPE_VARSTRING);
        h.upload(7, 0, new byte[] {0, (byte) 0xff});
        h.upload(7, 0, new byte[] {(byte) 0x80});
        // Split one UTF-8 character across upload packets.
        byte[] text = "中文".getBytes(StandardCharsets.UTF_8);
        h.upload(7, 2, new byte[] {text[0]});
        h.upload(7, 2, java.util.Arrays.copyOfRange(text, 1, text.length));
        // The fourth parameter is NULL. Uploaded parameters have no inline values.
        h.execute(new byte[] {8, 0, 42, 0, 0, 0});
        Assertions.assertEquals(new VarBinaryLiteral(new byte[] {0, (byte) 0xff, (byte) 0x80}), h.bound(0));
        Assertions.assertEquals(new IntegerLiteral(42), h.bound(1));
        Assertions.assertEquals(new StringLiteral("中文"), h.bound(2));
        Assertions.assertInstanceOf(NullLiteral.class, h.bound(3));
        Assertions.assertFalse(h.prepared.hasLongData());

        ByteBuffer forwarded = h.context.getPrepareExecuteBuffer();
        byte[] payload = new byte[forwarded.remaining()];
        forwarded.get(payload);
        Harness master = new Harness(MysqlColType.MYSQL_TYPE_BLOB, MysqlColType.MYSQL_TYPE_LONG,
                MysqlColType.MYSQL_TYPE_VARSTRING, MysqlColType.MYSQL_TYPE_VARSTRING);
        master.execute(payload);
        for (int i = 0; i < 4; ++i) {
            Assertions.assertEquals(h.bound(i), master.bound(i));
        }
    }

    @Test
    public void testEmptyUploadAndStatementIsolation() throws Exception {
        Harness h = new Harness(MysqlColType.MYSQL_TYPE_VARSTRING);
        PreparedStatementContext other = new PreparedStatementContext(h.prepared.command, h.context,
                new StatementContext(), "select ?");
        h.context.addPreparedStatementContext("8", other);
        h.upload(8, 0, new byte[] {65});
        h.upload(7, 0, new byte[0]);
        Assertions.assertTrue(h.prepared.hasLongData(0));
        h.execute(new byte[] {0, 0});
        Assertions.assertEquals(new StringLiteral(""), h.bound(0));
        Assertions.assertArrayEquals(new byte[] {65}, other.getLongData(0));
        // The next execution must consume its inline value instead of reusing uploaded data.
        h.execute(new byte[] {0, 0, 1, 66});
        Assertions.assertEquals(new StringLiteral("B"), h.bound(0));
    }

    @Test
    public void testUploadErrorsAreDeferredAndResetClearsThem() throws Exception {
        Harness h = new Harness(MysqlColType.MYSQL_TYPE_VARSTRING);
        h.context.getSessionVariable().maxAllowedPacket = 3;
        h.upload(7, 0, new byte[] {65, 66});
        h.upload(7, 0, new byte[] {67, 68});
        Assertions.assertNotNull(h.prepared.getLongDataError());
        Assertions.assertFalse(h.prepared.hasLongData());
        Mockito.verify(h.channel, Mockito.never()).sendAndFlush(Mockito.any());
        h.executeError(new byte[] {0, 0}, "max_allowed_packet");
        Assertions.assertNull(h.prepared.getLongDataError());
        h.upload(7, 65535, new byte[0]);
        Assertions.assertNotNull(h.prepared.getLongDataError());
        h.processor.handleStmtResetById(7);
        Assertions.assertNull(h.prepared.getLongDataError());
        h.upload(7, 0, new byte[] {65});
        h.processor.handleStmtResetById(7);
        Assertions.assertFalse(h.prepared.hasLongData());
        h.execute(new byte[] {0, 0, 1, 66});
        Assertions.assertEquals(new StringLiteral("B"), h.bound(0));
    }

    @Test
    public void testBindingFailureClearsUploadWithAuditEnabled() throws Exception {
        Harness h = new Harness(MysqlColType.MYSQL_TYPE_LONG);
        h.context.getSessionVariable().enablePreparedStmtAuditLog = true;
        h.upload(7, 0, new byte[] {65});
        h.executeError(new byte[] {0, 0}, "Invalid MySQL type for long data");
        Assertions.assertFalse(h.prepared.hasLongData());
    }

    @Test
    public void testMalformedUploadAndCloseHaveNoResponse() throws Exception {
        Harness h = new Harness(MysqlColType.MYSQL_TYPE_VARSTRING);
        h.command(ByteBuffer.wrap(new byte[] {0x18}));
        h.command(ByteBuffer.wrap(new byte[] {0x18, 7, 0, 0, 0}));
        Assertions.assertNotNull(h.prepared.getLongDataError());
        Mockito.verify(h.channel, Mockito.never()).sendAndFlush(Mockito.any());
        h.processor.handleStmtResetById(7);
        h.upload(7, 0, new byte[] {65});
        h.context.setThreadLocalInfo();
        try {
            h.command(ByteBuffer.wrap(new byte[] {0x19, 7, 0, 0, 0}));
            Assertions.assertNull(h.context.getPreparedStementContext("7"));
            Mockito.verify(h.channel, Mockito.never()).sendAndFlush(Mockito.any());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testUploadOverridesNullBitmap() throws Exception {
        Harness h = new Harness(MysqlColType.MYSQL_TYPE_VARSTRING);
        h.upload(7, 0, new byte[] {65});
        h.execute(new byte[] {1, 0});
        Assertions.assertEquals(new StringLiteral("A"), h.bound(0));
        Assertions.assertEquals(0, h.context.getPrepareExecuteBuffer().get(0));
    }

    @Test
    public void testBlobTypesPreserveBytes() throws Exception {
        for (MysqlColType type : new MysqlColType[] {MysqlColType.MYSQL_TYPE_TINY_BLOB,
                MysqlColType.MYSQL_TYPE_MEDIUM_BLOB, MysqlColType.MYSQL_TYPE_LONG_BLOB,
                MysqlColType.MYSQL_TYPE_BLOB}) {
            MysqlSerializer value = MysqlSerializer.newInstance();
            byte[] bytes = new byte[] {0, (byte) 0x80, (byte) 0xff, 65};
            value.writeLenEncodedBytes(bytes);
            Assertions.assertEquals(new VarBinaryLiteral(bytes),
                    Literal.getLiteralByMysqlType(type, false, value.toByteBuffer()));
        }
    }

    @Test
    public void testTruncatedBlobFailsBinding() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Literal.getLiteralByMysqlType(MysqlColType.MYSQL_TYPE_BLOB, false,
                        ByteBuffer.wrap(new byte[] {5, 65})));
    }

    @Test
    public void testForwardedPrepareUsesParserContext() throws Exception {
        ConnectContext context = new ConnectContext(null, true);
        OriginStatement sql = new OriginStatement("SELECT HEX(?)", 0);
        StmtExecutor executor = new StmtExecutor(context, sql, true);
        StatementContext constructorContext = context.getStatementContext();
        StatementContext parsedContext = new StatementContext(context, sql);
        parsedContext.setPrepareStage(true);
        LogicalPlan plan = Mockito.mock(LogicalPlan.class);
        executor.setParsedStmt(new LogicalPlanAdapter(plan, parsedContext));
        context.setStatementContext(parsedContext);
        CascadesContext cascades = Mockito.mock(CascadesContext.class);
        Mockito.when(cascades.getRewritePlan()).thenReturn(plan);
        Mockito.when(plan.getOutput()).thenReturn(List.of());
        try (MockedConstruction<PrepareCommandPlanner> planners = Mockito.mockConstruction(
                PrepareCommandPlanner.class, (planner, construction) -> {
                    Assertions.assertSame(parsedContext, construction.arguments().get(0));
                    Mockito.when(planner.getCascadesContext()).thenReturn(cascades);
                })) {
            Assertions.assertFalse(constructorContext.isPrepareStage());
            Assertions.assertTrue(executor.planPrepareStatementSlots().isEmpty());
            Assertions.assertEquals(1, planners.constructed().size());
        }
    }

    private static class Harness {
        private final ConnectContext context = new ConnectContext();
        private final MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        private final MysqlConnectProcessor processor;
        private final PreparedStatementContext prepared;

        Harness(MysqlColType... types) throws Exception {
            Field adapter = ConnectContext.class.getDeclaredField("protocolAdapter");
            adapter.setAccessible(true);
            adapter.set(context, new MysqlProtocolAdapter(channel));
            Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
            context.getSessionVariable().enableServeSidePreparedStatement = true;
            List<Placeholder> placeholders = new ArrayList<>();
            for (int i = 0; i < types.length; ++i) {
                placeholders.add(new Placeholder(new PlaceholderId(i), types[i].getCode()));
            }
            PrepareCommand command = new PrepareCommand("7", null, placeholders,
                    new OriginStatement("select ?", 0));
            prepared = new PreparedStatementContext(command, context, new StatementContext(), "select ?");
            context.addPreparedStatementContext("7", prepared);
            processor = new MysqlConnectProcessor(context) {
                @Override
                protected void resolveWorkloadGroupName() {
                }
            };
        }

        void upload(int statement, int parameter, byte[] bytes) throws Exception {
            ByteBuffer packet = ByteBuffer.allocate(7 + bytes.length).order(ByteOrder.LITTLE_ENDIAN);
            packet.put((byte) 0x18);
            packet.putInt(statement).putShort((short) parameter).put(bytes).flip();
            command(packet);
            Assertions.assertEquals(QueryState.MysqlStateType.NOOP, context.getState().getStateType());
        }

        void command(ByteBuffer packet) throws Exception {
            Mockito.when(channel.fetchOnePacket()).thenReturn(packet);
            processor.processOnce();
        }

        void execute(byte[] payload) throws Exception {
            try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                    MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
                context.getState().reset();
                processor.handleExecute(prepared.command, 7, prepared,
                        ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN), null);
                Assertions.assertEquals(QueryState.MysqlStateType.OK, context.getState().getStateType(),
                        context.getState().getErrorMessage());
                Assertions.assertEquals(1, executors.constructed().size());
                Mockito.verify(executors.constructed().get(0)).execute();
            }
        }

        void executeError(byte[] payload, String message) throws Exception {
            try (MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
                context.getState().reset();
                processor.handleExecute(prepared.command, 7, prepared,
                        ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN), null);
                Assertions.assertEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
                Assertions.assertTrue(context.getState().getErrorMessage().contains(message),
                        context.getState().getErrorMessage());
            }
        }

        Literal bound(int parameter) {
            return (Literal) prepared.getStatementContext().getIdToPlaceholderRealExpr()
                    .get(prepared.command.getPlaceholders().get(parameter).getPlaceholderId());
        }
    }
}
