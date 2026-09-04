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

import org.apache.doris.mysql.DummyMysqlChannel;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ConnectProcessorForwardProtocolTest {
    @Test
    public void testOldMasterQueryFailsBeforeRawPacketsAreSent() throws Exception {
        TestContext context = new TestContext();
        StmtExecutor executor = forwardedExecutor();
        Mockito.when(executor.hasForwardedQueryResultPackets()).thenReturn(true);

        new TestProcessor(context, executor).finalizeCommand();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, context.getState().getStateType());
        Assert.assertEquals(0xFF, MysqlProto.readInt1(context.channel.packet));
        Mockito.verify(executor, Mockito.never()).sendProxyQueryResult();
    }

    @Test
    public void testOldMasterDmlRebuildsOkWithoutRetryRisk() throws Exception {
        TestContext context = new TestContext();
        StmtExecutor executor = forwardedExecutor();
        Mockito.when(executor.getForwardedAffectedRows()).thenReturn(7L);

        new TestProcessor(context, executor).finalizeCommand();

        Assert.assertEquals(QueryState.MysqlStateType.OK, context.getState().getStateType());
        Assert.assertEquals(0x00, MysqlProto.readInt1(context.channel.packet));
        Assert.assertEquals(7L, MysqlProto.readVInt(context.channel.packet));
        Mockito.verify(executor, Mockito.never()).sendProxyQueryResult();
    }

    @Test
    public void testRemoteErrorsRemainUnchanged() throws Exception {
        TestContext context = new TestContext();
        StmtExecutor executor = forwardedExecutor();
        Mockito.when(executor.getProxyStatusCode()).thenReturn(1064);
        Mockito.when(executor.getOutputPacket()).thenReturn(ByteBuffer.wrap(new byte[] {(byte) 0xFF, 1}));

        new TestProcessor(context, executor).finalizeCommand();

        Assert.assertEquals(0xFF, MysqlProto.readInt1(context.channel.packet));
        Mockito.verify(executor).sendProxyQueryResult();
    }

    @Test
    public void testNewMasterPacketsRemainUnchanged() throws Exception {
        TestContext context = new TestContext();
        StmtExecutor executor = forwardedExecutor();
        Mockito.when(executor.isForwardedClientDeprecatedEofApplied()).thenReturn(true);

        new TestProcessor(context, executor).finalizeCommand();

        Mockito.verify(executor).sendProxyQueryResult();
    }

    @Test
    public void testLegacyEofClientDoesNotRequireConfirmation() throws Exception {
        TestContext context = new TestContext(false);
        StmtExecutor executor = forwardedExecutor();

        new TestProcessor(context, executor).finalizeCommand();

        Mockito.verify(executor).sendProxyQueryResult();
    }

    private StmtExecutor forwardedExecutor() {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.hasForwardedToMaster()).thenReturn(true);
        Mockito.when(executor.getProxyStatusCode()).thenReturn(0);
        return executor;
    }

    private static class TestProcessor extends MysqlConnectProcessor {
        private TestProcessor(ConnectContext context, StmtExecutor executor) {
            super(context);
            this.executor = executor;
        }
    }

    private static class TestContext extends ConnectContext {
        private final RecordingChannel channel;

        private TestContext() {
            this(true);
        }

        private TestContext(boolean clientDeprecatedEof) {
            channel = new RecordingChannel(clientDeprecatedEof);
        }

        @Override
        public RecordingChannel getMysqlChannel() {
            return channel;
        }
    }

    private static class RecordingChannel extends DummyMysqlChannel {
        private ByteBuffer packet;

        private RecordingChannel(boolean clientDeprecatedEof) {
            int flags = MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit();
            if (clientDeprecatedEof) {
                flags |= MysqlCapability.Flag.CLIENT_DEPRECATE_EOF.getFlagBit();
            }
            serializer = MysqlSerializer.newInstance(new MysqlCapability(flags));
            if (clientDeprecatedEof) {
                setClientDeprecatedEOF();
            }
        }

        @Override
        public void sendAndFlush(ByteBuffer packet) throws IOException {
            this.packet = packet.duplicate();
        }
    }
}
