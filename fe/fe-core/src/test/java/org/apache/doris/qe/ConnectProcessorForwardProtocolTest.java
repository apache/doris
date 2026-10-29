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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ConnectProcessorForwardProtocolTest {
    @Test
    public void testOldMasterSuccessfulEofDoesNotBecomeError() throws Exception {
        TestContext context = new TestContext();
        StmtExecutor executor = forwardedExecutor();
        Mockito.when(executor.getProxyStatusCode()).thenReturn(1105);
        ByteBuffer packet = ByteBuffer.wrap(new byte[] {(byte) 0xFE, 0, 0, 2, 0, 3, 0, 0});
        Mockito.when(executor.getOutputPacket()).thenReturn(packet);

        new TestProcessor(context, executor).finalizeCommand();

        Assertions.assertEquals(packet, context.channel.packet);
        Mockito.verify(executor).sendProxyQueryResult();
    }

    @Test
    public void testOldMasterDmlPreservesCompleteOk() throws Exception {
        TestContext context = new TestContext();
        StmtExecutor executor = forwardedExecutor();
        QueryState state = new QueryState();
        state.setOk(7, 3, "label=load_1,txnId=123,status=VISIBLE");
        state.serverStatus = 2;
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        state.toResponsePacket().writeTo(serializer);
        ByteBuffer packet = serializer.toByteBuffer();
        Mockito.when(executor.getOutputPacket()).thenReturn(packet);

        new TestProcessor(context, executor).finalizeCommand();

        Assertions.assertEquals(packet, context.channel.packet);
        Mockito.verify(executor).sendProxyQueryResult();
    }

    @Test
    public void testRemoteErrorsRemainUnchanged() throws Exception {
        TestContext context = new TestContext();
        StmtExecutor executor = forwardedExecutor();
        Mockito.when(executor.getProxyStatusCode()).thenReturn(1064);
        Mockito.when(executor.getOutputPacket()).thenReturn(ByteBuffer.wrap(new byte[] {(byte) 0xFF, 1}));

        new TestProcessor(context, executor).finalizeCommand();

        Assertions.assertEquals(0xFF, MysqlProto.readInt1(context.channel.packet));
        Mockito.verify(executor).sendProxyQueryResult();
    }

    @Test
    public void testNewMasterPacketsRemainUnchanged() throws Exception {
        TestContext context = new TestContext();
        StmtExecutor executor = forwardedExecutor();

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
