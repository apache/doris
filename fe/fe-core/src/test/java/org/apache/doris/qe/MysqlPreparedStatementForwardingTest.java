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
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

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

/**
 * A follower forwards a COM_STMT_EXECUTE to the master in a TMasterOpRequest. The master prepares the statement
 * again for every forwarded execution, so it never has the parameter types a client caches after the first
 * execution: the request has to carry them.
 */
public class MysqlPreparedStatementForwardingTest {
    // More than eight, so that the NULL bitmap takes two bytes
    private static final int PARAMETER_COUNT = 10;

    @Test
    public void testForwardedExecutionsRetainTypesAndNullBitmap() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSqlCacheManager()).thenReturn(new NereidsSqlCacheManager());
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            ConnectContext follower = new ConnectContext();
            follower.setCurrentUserIdentity(UserIdentity.ROOT);
            follower.setRemoteIP("127.0.0.1");
            follower.setCapability(MysqlCapability.DEFAULT_CAPABILITY);
            PreparedStatementContext prepared = prepare(follower);
            for (int execution = 0; execution < 5; execution++) {
                // The client sends the parameter types with the first execution and when they change. The other
                // executions leave them out and have the server reuse the cached ones.
                boolean newTypes = execution == 0 || execution == 3;
                boolean wideInteger = execution >= 3;
                ByteBuffer packet = packet(execution, newTypes, wideInteger);
                byte[] original = Arrays.copyOf(packet.array(), packet.limit());
                execute(follower, prepared, packet);
                Assertions.assertEquals(packet.limit(), packet.position());
                Assertions.assertArrayEquals(original, Arrays.copyOf(packet.array(), packet.limit()));

                TMasterOpRequest request = new FEOpExecutor(new TNetworkAddress("127.0.0.1", 9010),
                        prepared.command.getOriginalStmt(), follower, true).buildStmtForwardParams();
                TMasterOpRequest restored = new TMasterOpRequest();
                new TDeserializer().deserialize(restored, new TSerializer().serialize(request));
                ConnectContext master = new ConnectContext(null, true);
                PreparedStatementContext reprepared = prepare(master);
                ByteBuffer forwarded = ByteBuffer.wrap(restored.getPrepareExecuteBuffer())
                        .order(ByteOrder.LITTLE_ENDIAN);
                execute(master, reprepared, forwarded);
                Assertions.assertFalse(forwarded.hasRemaining());
                Assertions.assertEquals((execution + 1) * 2, executors.constructed().size());

                for (int i = 0; i < PARAMETER_COUNT; i++) {
                    Assertions.assertEquals(bound(prepared, i), bound(reprepared, i), "parameter " + i);
                }
                Literal integer = (Literal) bound(reprepared, 1);
                Assertions.assertEquals(wideInteger ? "1099511627776" : "4294967295", integer.getStringValue());
                Assertions.assertEquals(!wideInteger, reprepared.command.getPlaceholders().get(1).isUnsigned());
            }
        }
    }

    private PreparedStatementContext prepare(ConnectContext context) throws Exception {
        List<Placeholder> placeholders = new ArrayList<>();
        for (int i = 0; i < PARAMETER_COUNT; i++) {
            placeholders.add(new Placeholder(new PlaceholderId(i)));
        }
        OriginStatement origin = new OriginStatement("select ?, ?, ?, ?, ?, ?, ?, ?, ?, ?", 0);
        PrepareCommand command = new PrepareCommand("7", null, placeholders, origin);
        PreparedStatementContext prepared = new PreparedStatementContext(command, context,
                new StatementContext(context, origin), "7");
        context.addPreparedStatementContext("7", prepared);
        // The query id the COM_STMT_PREPARE of the statement left on the connection; the mocked executors never
        // execute, so they do not set one.
        context.setQueryId(new TUniqueId(7L, 7L));
        return prepared;
    }

    private void execute(ConnectContext context, PreparedStatementContext prepared, ByteBuffer packet) {
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        context.getState().reset();
        new MysqlConnectProcessor(context).handleExecute(prepared.command, 7, prepared, packet, null);
        Assertions.assertEquals(QueryState.MysqlStateType.OK, context.getState().getStateType(),
                context.getState().getErrorMessage());
    }

    private Expression bound(PreparedStatementContext prepared, int parameter) {
        return prepared.getStatementContext().getIdToPlaceholderRealExpr()
                .get(prepared.command.getPlaceholders().get(parameter).getPlaceholderId());
    }

    // The parameter part of a COM_STMT_EXECUTE: a string, an unsigned INT that later becomes a BIGINT, and eight
    // INTs, two of which, one in each byte of the NULL bitmap, are NULL in the second execution.
    private ByteBuffer packet(int execution, boolean newTypes, boolean wideInteger) {
        ByteBuffer packet = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
        packet.position(9); // The MySQL dispatcher has already read the statement id, flags and iteration count.
        boolean withNulls = execution == 1;
        packet.put((byte) (withNulls ? 4 : 0));
        packet.put((byte) (withNulls ? 1 : 0));
        packet.put((byte) (newTypes ? 1 : 0));
        if (newTypes) {
            packet.putChar((char) MysqlColType.MYSQL_TYPE_VARSTRING.getCode());
            packet.putChar((char) (wideInteger ? MysqlColType.MYSQL_TYPE_LONGLONG.getCode()
                    : MysqlColType.MYSQL_TYPE_LONG.getCode() | MysqlColType.UNSIGNED_MASK));
            for (int i = 2; i < PARAMETER_COUNT; i++) {
                packet.putChar((char) MysqlColType.MYSQL_TYPE_LONG.getCode());
            }
        }
        byte[] text = (execution == 1 ? "[3,4]" : "[1,2]").getBytes(StandardCharsets.UTF_8);
        packet.put((byte) text.length).put(text);
        if (wideInteger) {
            packet.putLong(1L << 40);
        } else {
            packet.putInt(-1);
        }
        for (int i = 2; i < PARAMETER_COUNT; i++) {
            if (!withNulls || (i != 2 && i != 8)) {
                packet.putInt(execution + i);
            }
        }
        packet.flip();
        packet.position(9);
        return packet;
    }
}
