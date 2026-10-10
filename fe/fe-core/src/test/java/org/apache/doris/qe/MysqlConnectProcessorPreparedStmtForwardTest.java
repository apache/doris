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
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MysqlConnectProcessorPreparedStmtForwardTest {
    @Test
    public void testCachedExecuteEmbedsKnownTypesIntoForwardedBuffer() throws Exception {
        // A follower FE that already ran the first COM_STMT_EXECUTE (new_params_bind_flag = 1)
        // has a cached command with typed placeholders. A cached repeat execute omits the types
        // (new_params_bind_flag = 0). When such a write statement is forwarded to the master FE,
        // the master re-prepares it and never sees the types, so handleExecute must embed the
        // types known from the first execute into the forwarded buffer and set the flag.
        ConnectContext context = new ConnectContext(null, false);
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        PrepareCommand command = new PrepareCommand(
                "7",
                null,
                ImmutableList.of(
                        new Placeholder(new PlaceholderId(0), MysqlColType.MYSQL_TYPE_LONG.getCode()),
                        new Placeholder(new PlaceholderId(1), MysqlColType.MYSQL_TYPE_SHORT.getCode())),
                new OriginStatement("insert into t values (?, ?)", 0));
        StatementContext statementContext = new StatementContext();
        PreparedStatementContext prepared = new PreparedStatementContext(
                command, context, statementContext, "insert into t values (?, ?)");

        // COM_STMT_EXECUTE packet: header(9 bytes) + null bitmap(1) + new_params_bind_flag(1)
        // + values (4-byte LONG + 2-byte SHORT). The types are omitted on purpose.
        ByteBuffer packet = ByteBuffer.allocate(9 + 1 + 1 + 4 + 2).order(ByteOrder.LITTLE_ENDIAN);
        packet.putInt(7);            // statement id
        packet.put((byte) 0);        // flags
        packet.putInt(1);            // iteration count
        packet.put((byte) 0);        // null bitmap: both parameters are not null
        packet.put((byte) 0);        // new_params_bind_flag = 0 (cached execute)
        packet.putInt(42);           // value of the MYSQL_TYPE_LONG parameter
        packet.putShort((short) 7);  // value of the MYSQL_TYPE_SHORT parameter
        packet.flip();
        packet.position(9); // the header has already been consumed

        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            new MysqlConnectProcessor(context).handleExecute(command, 7, prepared, packet, null);

            ByteBuffer forwarded = context.getPrepareExecuteBuffer().duplicate();
            System.out.println("DEBUG order=" + forwarded.order() + " remaining=" + forwarded.remaining());
            byte[] dbg = new byte[forwarded.remaining()];
            ByteBuffer cp = forwarded.duplicate();
            cp.get(dbg);
            StringBuilder sb = new StringBuilder();
            for (byte b : dbg) {
                sb.append(String.format("%02x ", b));
            }
            System.out.println("DEBUG bytes=" + sb);
            Assertions.assertEquals(0, forwarded.get());       // null bitmap preserved
            Assertions.assertEquals(1, forwarded.get());       // new_params_bind_flag = 1
            // the known types are embedded before the values
            Assertions.assertEquals((char) MysqlColType.MYSQL_TYPE_LONG.getCode(), forwarded.getChar());
            Assertions.assertEquals((char) MysqlColType.MYSQL_TYPE_SHORT.getCode(), forwarded.getChar());
            Assertions.assertEquals(42, forwarded.getInt());
            Assertions.assertEquals((short) 7, forwarded.getShort());
            Assertions.assertEquals(0, forwarded.remaining());
        }
    }

    @Test
    public void testFirstExecuteKeepsForwardedBufferUnchanged() throws Exception {
        // The first execute already carries the types (new_params_bind_flag = 1), so the
        // forwarded buffer must be the raw packet, not rewritten.
        ConnectContext context = new ConnectContext(null, false);
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        PrepareCommand command = new PrepareCommand(
                "7",
                null,
                ImmutableList.of(new Placeholder(new PlaceholderId(0))),
                new OriginStatement("insert into t values (?)", 0));
        StatementContext statementContext = new StatementContext();
        PreparedStatementContext prepared = new PreparedStatementContext(
                command, context, statementContext, "insert into t values (?)");

        ByteBuffer packet = ByteBuffer.allocate(9 + 1 + 1 + 2 + 4).order(ByteOrder.LITTLE_ENDIAN);
        packet.putInt(7);
        packet.put((byte) 0);
        packet.putInt(1);
        packet.put((byte) 0);                          // null bitmap
        packet.put((byte) 1);                          // new_params_bind_flag = 1
        packet.putChar((char) MysqlColType.MYSQL_TYPE_LONG.getCode());
        packet.putInt(42);
        packet.flip();
        packet.position(9);

        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            new MysqlConnectProcessor(context).handleExecute(command, 7, prepared, packet, null);

            // The raw packet is forwarded as-is: null bitmap then the flag and types the
            // client already sent.
            ByteBuffer forwarded = context.getPrepareExecuteBuffer().duplicate();
            Assertions.assertEquals(0, forwarded.get());   // null bitmap
            Assertions.assertEquals(1, forwarded.get());   // flag stays 1
            Assertions.assertEquals((char) MysqlColType.MYSQL_TYPE_LONG.getCode(), forwarded.getChar());
        }
    }

    @Test
    public void testZeroParameterExecuteKeepsEmptyForwardedBuffer() throws Exception {
        // A zero-parameter execute has no null bitmap, flag or values; the forwarded buffer
        // is the raw (empty) packet, which still identifies COM_STMT_EXECUTE on the master.
        ConnectContext context = new ConnectContext(null, false);
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        PrepareCommand command = new PrepareCommand(
                "7", null, ImmutableList.of(),
                new OriginStatement("delete from t where 1 = 0", 0));
        StatementContext statementContext = new StatementContext();
        PreparedStatementContext prepared = new PreparedStatementContext(
                command, context, statementContext, "delete from t where 1 = 0");
        ByteBuffer packet = ByteBuffer.allocate(9);
        packet.position(packet.limit());

        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class);
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            new MysqlConnectProcessor(context).handleExecute(command, 7, prepared, packet, null);
            Assertions.assertNotNull(context.getPrepareExecuteBuffer());
            Assertions.assertEquals(0, context.getPrepareExecuteBuffer().remaining());
        }
    }
}
