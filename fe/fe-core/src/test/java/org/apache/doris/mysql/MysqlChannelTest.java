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

package org.apache.doris.mysql;


import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.xnio.StreamConnection;
import org.xnio.conduits.ConduitStreamSinkChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MysqlChannelTest {
    private static final int MAX_PACKET = MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH;

    StreamConnection streamConnection = Mockito.mock(StreamConnection.class);

    @Test
    public void testSendAfterException() throws IOException {
        // Mock StreamConnection.getPeerAddress() to avoid NPE in MysqlChannel constructor
        Mockito.when(streamConnection.getPeerAddress())
                .thenReturn(new java.net.InetSocketAddress("127.0.0.1", 12345));

        // Mock.
        ConduitStreamSinkChannel mockSinkChannel = Mockito.mock(ConduitStreamSinkChannel.class);
        Mockito.when(streamConnection.getSinkChannel()).thenReturn(mockSinkChannel);
        Mockito.when(mockSinkChannel.write(ArgumentMatchers.any(ByteBuffer.class)))
                // The first call to `write()` throws IOException.
                .thenThrow(new IOException())
                // The second call to `write()` executes normally.
                .thenAnswer(inv -> {
                    ByteBuffer buffer = inv.getArgument(0);
                    int writeLen = buffer.remaining();
                    buffer.position(buffer.limit());
                    return writeLen;
                });
        Mockito.when(mockSinkChannel.flush()).thenReturn(true);

        ConnectContext ctx = new ConnectContext(streamConnection);
        MysqlChannel mysqlChannel = new MysqlChannel(streamConnection, ctx);
        Deencapsulation.setField(mysqlChannel, "sendBuffer", ByteBuffer.allocate(5));
        // The first call to `realNetSend()` in `flush()` throws IOException.
        // If `flush()` doesn't consider this exception, `sendBuffer` won't be reset to write mode,
        // which will cause BufferOverflowException at the next calling `sendOnePacket()`.
        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.putInt(1);
        buf.putInt(2);
        // limit=8
        buf.flip();
        try {
            mysqlChannel.sendOnePacket(buf);
            Assertions.fail();
        } catch (IOException ignore) {
            // do nothing
        }
        buf.clear();

        buf.putInt(1);
        // limit=4
        buf.flip();
        mysqlChannel.sendOnePacket(buf);
        buf.clear();

        buf.putInt(1);
        buf.putInt(2);
        // limit=8
        buf.flip();
        mysqlChannel.sendOnePacket(buf);
    }

    @Test
    public void testByteArrayPacketsMatchWireFraming() throws IOException {
        RecordingChannel bytes = new RecordingChannel();
        RecordingChannel buffers = new RecordingChannel();
        bytes.sequenceId = buffers.sequenceId = 254;
        for (int size : new int[] {0, 1, 7, 8, 9, MAX_PACKET, MAX_PACKET + 1}) {
            byte[] packet = new byte[size];
            if (size > 0) {
                packet[size - 1] = 42;
            }
            bytes.sendOnePacket(packet);
            buffers.sendOnePacket(ByteBuffer.wrap(packet));
            bytes.flush();
            buffers.flush();
            Assertions.assertArrayEquals(buffers.output.toByteArray(), bytes.output.toByteArray());
            Assertions.assertEquals(buffers.sequenceId, bytes.sequenceId);
            bytes.output.reset();
            buffers.output.reset();
        }
    }

    @Test
    public void testByteArrayFlushBoundaryPreservesWireBytes() throws IOException {
        byte[] bufferedPrefix = {11, 12, 13, 14};
        byte[] secondPacket = {91, 92, 93};
        for (int size : new int[] {
                RecordingChannel.SEND_BUFFER_CAPACITY - MysqlChannel.PACKET_HEADER_LEN,
                RecordingChannel.SEND_BUFFER_CAPACITY,
                RecordingChannel.SEND_BUFFER_CAPACITY + 1}) {
            byte[] firstPacket = new byte[size];
            for (int i = 0; i < size; i++) {
                firstPacket[i] = (byte) (i + 1);
            }
            RecordingChannel bytes = new RecordingChannel();
            RecordingChannel buffers = new RecordingChannel();
            bytes.sequenceId = buffers.sequenceId = 255;
            bytes.sendBuffer.put(bufferedPrefix);
            buffers.sendBuffer.put(bufferedPrefix);

            bytes.sendOnePacket(firstPacket);
            bytes.sendOnePacket(secondPacket);
            buffers.sendOnePacket(ByteBuffer.wrap(firstPacket));
            buffers.sendOnePacket(ByteBuffer.wrap(secondPacket));
            bytes.flush();
            buffers.flush();

            ByteArrayOutputStream expected = new ByteArrayOutputStream();
            expected.write(bufferedPrefix, 0, bufferedPrefix.length);
            expected.write(firstPacket.length & 0xFF);
            expected.write((firstPacket.length >>> 8) & 0xFF);
            expected.write((firstPacket.length >>> 16) & 0xFF);
            expected.write(255);
            expected.write(firstPacket, 0, firstPacket.length);
            expected.write(secondPacket.length & 0xFF);
            expected.write((secondPacket.length >>> 8) & 0xFF);
            expected.write((secondPacket.length >>> 16) & 0xFF);
            expected.write(0);
            expected.write(secondPacket, 0, secondPacket.length);

            Assertions.assertArrayEquals(expected.toByteArray(), bytes.output.toByteArray());
            Assertions.assertArrayEquals(buffers.output.toByteArray(), bytes.output.toByteArray());
            Assertions.assertEquals(1, bytes.sequenceId);
            Assertions.assertEquals(buffers.sequenceId, bytes.sequenceId);
        }
    }

    @Test
    public void testByteArrayPreservesProxyAndHandshake() throws IOException {
        byte[] packet = {1, 2, 3};
        ProxyMysqlChannel proxy = new ProxyMysqlChannel();
        proxy.sendOnePacket(packet);
        Assertions.assertEquals(ByteBuffer.wrap(packet), proxy.getProxyResultBufferList().get(0));
        new DummyMysqlChannel().sendOnePacket(packet);
        RecordingChannel channel = new RecordingChannel();
        channel.isSslHandshaking = true;
        channel.sendOnePacket(packet);
        channel.flush();
        Assertions.assertArrayEquals(packet, channel.output.toByteArray());
        Assertions.assertEquals(0, channel.sequenceId);
    }

    private static class RecordingChannel extends MysqlChannel {
        private static final int SEND_BUFFER_CAPACITY = 8;
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        RecordingChannel() {
            sendBuffer = ByteBuffer.allocate(SEND_BUFFER_CAPACITY);
        }

        @Override
        protected void realNetSend(ByteBuffer buffer) {
            while (buffer.hasRemaining()) {
                output.write(buffer.get());
            }
        }
    }

}
