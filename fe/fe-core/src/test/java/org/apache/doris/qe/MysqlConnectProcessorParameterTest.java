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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.thrift.TNetworkAddress;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class MysqlConnectProcessorParameterTest {
    private static final MysqlColType[] STRING_TYPES = {MysqlColType.MYSQL_TYPE_STRING,
            MysqlColType.MYSQL_TYPE_VARSTRING, MysqlColType.MYSQL_TYPE_VARCHAR};

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testBinaryStringsWithNewAndCachedTypesAndForwarding() throws Exception {
        byte[] bytes = {0, (byte) 0x80, (byte) 0xff, 65, (byte) 0xc3, 40};
        for (MysqlColType type : STRING_TYPES) {
            Harness client = new Harness(type);
            Expression expected = new Cast(new VarBinaryLiteral(bytes), type == MysqlColType.MYSQL_TYPE_VARCHAR
                    ? VarcharType.createVarcharType(bytes.length) : StringType.INSTANCE);
            for (boolean newTypes : new boolean[] {true, false}) {
                client.execute(valuePacket(type, bytes, newTypes));
                Assertions.assertEquals(expected, client.bound(0));
                Assertions.assertEquals(new BigIntLiteral(4294967295L), client.bound(1));

                FEOpExecutor forwarder = new FEOpExecutor(new TNetworkAddress("127.0.0.1", 9030),
                        new OriginStatement("select ?, ?", 0), client.context, true);
                ByteBuffer forwarded = Deencapsulation.invoke(forwarder, "buildPrepareExecuteBuffer");
                Harness proxy = new Harness();
                Assertions.assertFalse(proxy.prepared.command.getPlaceholders().get(0).hasMysqlColType());
                proxy.execute(forwarded);
                Assertions.assertEquals(expected, proxy.bound(0));
                Assertions.assertEquals(client.bound(1), proxy.bound(1));
                Assertions.assertTrue(proxy.prepared.command.getPlaceholders().get(1).isUnsigned());
            }
        }
    }

    @Test
    public void testValidStringsAndNullRemainLiterals() throws Exception {
        for (MysqlColType type : STRING_TYPES) {
            Harness client = new Harness(type);
            for (String text : new String[] {"", "ASCII", "中文", "a\0b", "🙂", "�"}) {
                client.execute(valuePacket(type, text.getBytes(StandardCharsets.UTF_8), true));
                Assertions.assertEquals(type == MysqlColType.MYSQL_TYPE_VARCHAR
                        ? new VarcharLiteral(text) : new StringLiteral(text), client.bound(0));
            }
            MysqlSerializer nullPacket = MysqlSerializer.newInstance();
            nullPacket.writeInt1(1);
            nullPacket.writeInt1(0);
            nullPacket.writeInt4(-1);
            client.execute(nullPacket.toByteBuffer());
            Assertions.assertInstanceOf(NullLiteral.class, client.bound(0));
            Assertions.assertEquals(new BigIntLiteral(4294967295L), client.bound(1));
        }
    }

    private static ByteBuffer valuePacket(MysqlColType type, byte[] bytes, boolean newTypes) {
        MysqlSerializer packet = MysqlSerializer.newInstance();
        packet.writeInt1(0);
        packet.writeInt1(newTypes ? 1 : 0);
        if (newTypes) {
            packet.writeInt2(type.getCode());
            packet.writeInt2(MysqlColType.MYSQL_TYPE_LONG.getCode() | MysqlColType.UNSIGNED_MASK);
        }
        packet.writeLenEncodedBytes(bytes);
        packet.writeInt4(-1);
        return packet.toByteBuffer();
    }

    private static class Harness {
        private final ConnectContext context;
        private final PreparedStatementContext prepared;
        private final MysqlConnectProcessor processor;

        Harness(MysqlColType type) throws Exception {
            this(new ConnectContext(), Arrays.asList(new Placeholder(new PlaceholderId(0), type.getCode()),
                    new Placeholder(new PlaceholderId(1),
                            MysqlColType.MYSQL_TYPE_LONG.getCode() | MysqlColType.UNSIGNED_MASK)));
        }

        Harness() throws Exception {
            this(new ConnectContext(null, true), Arrays.asList(new Placeholder(new PlaceholderId(0)),
                    new Placeholder(new PlaceholderId(1))));
        }

        private Harness(ConnectContext context, List<Placeholder> placeholders) throws Exception {
            this.context = context;
            PrepareCommand command = new PrepareCommand("7", null, placeholders,
                    new OriginStatement("select ?, ?", 0));
            prepared = new PreparedStatementContext(command, context, new StatementContext(), "select ?, ?");
            context.addPreparedStatementContext("7", prepared);
            processor = new MysqlConnectProcessor(context);
        }

        void execute(ByteBuffer packet) throws Exception {
            try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                    MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
                context.getState().reset();
                processor.handleExecute(prepared.command, 7, prepared, packet.order(ByteOrder.LITTLE_ENDIAN), null);
                Assertions.assertEquals(QueryState.MysqlStateType.OK, context.getState().getStateType(),
                        context.getState().getErrorMessage());
                Mockito.verify(executors.constructed().get(0)).execute();
                Assertions.assertFalse(packet.hasRemaining());
            }
        }

        Expression bound(int parameter) {
            return prepared.getStatementContext().getIdToPlaceholderRealExpr()
                    .get(prepared.command.getPlaceholders().get(parameter).getPlaceholderId());
        }
    }
}
