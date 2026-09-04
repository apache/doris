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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MysqlConnectProcessorCursorFetchTest {
    private static final int CURSOR_TYPE_READ_ONLY = 1;

    @Test
    public void testUnidentifiedDeprecatedEofCursorFailsBeforeExecution() throws Exception {
        ConnectContext context = execute(true, true, false);
        Assert.assertTrue(context.getState().getErrorMessage().contains(
                "Cannot safely execute cursor fetch because the client did not provide identifiable"));
    }

    @Test
    public void testCompatibilityGateOnlyAppliesToAmbiguousProtocol() throws Exception {
        Assert.assertTrue(execute(false, true, false).getState().getErrorMessage().contains(
                "Unknown prepared statement handler"));
        Assert.assertTrue(execute(true, false, false).getState().getErrorMessage().contains(
                "Unknown prepared statement handler"));
        Assert.assertTrue(execute(true, true, true).getState().getErrorMessage().contains(
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
