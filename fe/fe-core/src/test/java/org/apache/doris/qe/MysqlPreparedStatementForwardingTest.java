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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MysqlPreparedStatementForwardingTest {
    @Test
    public void testForwardedExecutionsRetainTypesAndNullBitmap() throws Exception {
        ConnectContext follower = new ConnectContext();
        follower.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        follower.setCurrentUserIdentity(UserIdentity.ROOT);
        follower.setCapability(MysqlCapability.DEFAULT_CAPABILITY);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        PreparedStatementContext prepared = prepare(follower);
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (int execution = 0; execution < 5; execution++) {
                boolean newTypes = execution == 0 || execution == 3;
                boolean wideInteger = execution >= 3;
                ByteBuffer packet = packet(execution, newTypes, wideInteger);
                byte[] original = Arrays.copyOf(packet.array(), packet.limit());
                new MysqlConnectProcessor(follower).handleExecute(
                        prepared.command, 7, prepared, packet, null);
                Assertions.assertNotEquals(QueryState.MysqlStateType.ERR, follower.getState().getStateType(),
                        follower.getState().getErrorMessage());
                Assertions.assertEquals(packet.limit(), packet.position());
                Assertions.assertArrayEquals(original, Arrays.copyOf(packet.array(), packet.limit()));

                // Every master RPC reparses PREPARE, so it starts without cached parameter types.
                ConnectContext master = new ConnectContext(null, true);
                master.setCommand(MysqlCommand.COM_STMT_EXECUTE);
                PreparedStatementContext proxyPrepared = prepare(master);
                Assertions.assertSame(packet.array(), follower.getPrepareExecuteBuffer().array());
                TMasterOpRequest request = new FEOpExecutor(new TNetworkAddress("127.0.0.1", 9010),
                        prepared.command.getOriginalStmt(), follower, true).buildStmtForwardParams();
                TMasterOpRequest restored = new TMasterOpRequest();
                new TDeserializer().deserialize(restored, new TSerializer().serialize(request));
                ByteBuffer forwarded = ByteBuffer.wrap(restored.getPrepareExecuteBuffer()).order(ByteOrder.LITTLE_ENDIAN);
                new MysqlConnectProcessor(master).handleExecute(
                        proxyPrepared.command, 7, proxyPrepared, forwarded, null);
                Assertions.assertNotEquals(QueryState.MysqlStateType.ERR, master.getState().getStateType(),
                        master.getState().getErrorMessage());
                Assertions.assertFalse(forwarded.hasRemaining());
                Assertions.assertEquals((execution + 1) * 2, executors.constructed().size());
                for (int i = 0; i < 10; i++) {
                    PlaceholderId id = new PlaceholderId(i);
                    Literal expected = (Literal) prepared.statementContext.getIdToPlaceholderRealExpr().get(id);
                    Literal actual = (Literal) proxyPrepared.statementContext.getIdToPlaceholderRealExpr().get(id);
                    Assertions.assertEquals(expected.getDataType(), actual.getDataType());
                    Assertions.assertEquals(expected.getStringValue(), actual.getStringValue());
                }
                Literal integer = (Literal) proxyPrepared.statementContext.getIdToPlaceholderRealExpr()
                        .get(new PlaceholderId(1));
                Assertions.assertEquals(wideInteger ? "1099511627776" : "4294967295", integer.getStringValue());
                Assertions.assertEquals(!wideInteger, proxyPrepared.command.getPlaceholders().get(1).isUnsigned());
            }
        }
    }

    private PreparedStatementContext prepare(ConnectContext context) {
        List<Placeholder> parameters = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            parameters.add(new Placeholder(new PlaceholderId(i)));
        }
        OriginStatement origin = new OriginStatement("select ?, ?, ?, ?, ?, ?, ?, ?, ?, ?", 0);
        PrepareCommand command = new PrepareCommand("7", Mockito.mock(LogicalPlan.class), parameters, origin);
        return new PreparedStatementContext(command, context, new StatementContext(context, origin), "7");
    }

    private ByteBuffer packet(int execution, boolean newTypes, boolean wideInteger) {
        ByteBuffer packet = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
        packet.position(9); // The MySQL dispatcher has already consumed the execute header.
        boolean withNulls = execution == 1;
        packet.put((byte) (withNulls ? 4 : 0));
        packet.put((byte) (withNulls ? 1 : 0)); // Exercise both bytes of the null bitmap.
        packet.put((byte) (newTypes ? 1 : 0));
        if (newTypes) {
            packet.putChar((char) MysqlColType.MYSQL_TYPE_VARSTRING.getCode());
            packet.putChar((char) (wideInteger ? MysqlColType.MYSQL_TYPE_LONGLONG.getCode()
                    : MysqlColType.MYSQL_TYPE_LONG.getCode() | MysqlColType.UNSIGNED_MASK));
            for (int i = 2; i < 10; i++) {
                packet.putChar((char) MysqlColType.MYSQL_TYPE_LONG.getCode());
            }
        }
        byte[] vector = (execution == 1 ? "[3,4]" : "[1,2]").getBytes(StandardCharsets.UTF_8);
        packet.put((byte) vector.length).put(vector);
        if (wideInteger) {
            packet.putLong(1L << 40);
        } else {
            packet.putInt(-1);
        }
        for (int i = 2; i < 10; i++) {
            if (!withNulls || (i != 2 && i != 8)) {
                packet.putInt(execution + i);
            }
        }
        packet.flip();
        packet.position(9);
        return packet;
    }
}
