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
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;

public class MysqlChannelTest {

    StreamConnection streamConnection = Mockito.mock(StreamConnection.class);
    private ConnectContext receivingContext;
    private ByteArrayOutputStream response;

    @Test
    public void testOversizedPacketRejectedBeforeBody() throws IOException {
        for (boolean writeFails : new boolean[]{false, true}) {
            ByteBuffer input = ByteBuffer.allocate(4);
            putHeader(input, 9, 1);
            input.flip();
            MysqlChannel channel = receivingChannel(input, 8);
            channel.setSequenceId(1);
            if (writeFails) {
                ConduitStreamSinkChannel sink = streamConnection.getSinkChannel();
                Mockito.doThrow(new IOException("write failed")).when(sink)
                        .write(ArgumentMatchers.any(ByteBuffer.class));
            }

            Assertions.assertThrows(IOException.class, channel::fetchOnePacket);
            Assertions.assertEquals(4, input.position());
            Mockito.verify(streamConnection.getSourceChannel()).read(ArgumentMatchers.any(ByteBuffer.class));
            Assertions.assertTrue(receivingContext.isKilled());
            Mockito.verify(streamConnection).close();
            Assertions.assertNull(channel.fetchOnePacket());
            if (writeFails) {
                continue;
            }
            assertPacketTooLarge(2);
        }
    }

    @Test
    public void testContinuationPacketLimit() throws IOException {
        int fullLength = MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH;
        int maxLength = fullLength + 8;
        for (int tailLength : new int[]{0, 8, 9}) {
            ByteBuffer input = ByteBuffer.allocate(fullLength + 8 + tailLength);
            putHeader(input, fullLength, 1);
            input.position(fullLength + 4);
            putHeader(input, tailLength, 2);
            input.position(input.capacity());
            input.flip();
            MysqlChannel channel = receivingChannel(input, maxLength);
            channel.setSequenceId(1);

            if (tailLength < 9) {
                ByteBuffer packet = channel.fetchOnePacket();
                Assertions.assertEquals(fullLength + tailLength, packet.remaining());
                Assertions.assertTrue(packet.capacity() <= maxLength);
                Assertions.assertEquals(input.limit(), input.position());
                Assertions.assertFalse(receivingContext.isKilled());
                Mockito.verify(streamConnection, Mockito.never()).close();
                continue;
            }
            Assertions.assertThrows(IOException.class, channel::fetchOnePacket);
            Assertions.assertEquals(fullLength + 8, input.position());
            Mockito.verify(streamConnection.getSourceChannel(), Mockito.times(3))
                    .read(ArgumentMatchers.any(ByteBuffer.class));
            Assertions.assertTrue(receivingContext.isKilled());
            Mockito.verify(streamConnection).close();
            assertPacketTooLarge(3);
        }
    }

    @Test
    public void testSslPacketLimitAcrossRecords() throws IOException {
        byte[] handshakeRecord = {0x16, 3, 3, 0, 2, 1, 0};
        MysqlChannel handshake = receivingChannel(ByteBuffer.wrap(handshakeRecord), 1);
        handshake.initSslBuffer();
        handshake.setSslHandshaking(true);
        handshake.setSequenceId(1);
        Assertions.assertEquals(ByteBuffer.wrap(handshakeRecord), handshake.fetchOnePacket());
        Assertions.assertEquals(1, handshake.sequenceId);
        Assertions.assertFalse(receivingContext.isKilled());
        Assertions.assertEquals(0, response.size());
        Mockito.verify(streamConnection, Mockito.never()).close();

        ByteBuffer first = ByteBuffer.allocate(9);
        putHeader(first, 5, 1);
        first.put(new byte[]{1, 2, 3, 4, 5});
        ByteBuffer lastRecord = ByteBuffer.allocate(16);
        lastRecord.put(first.array(), 6, 3);
        putHeader(lastRecord, 5, 2);
        lastRecord.put(new byte[]{6, 7, 8, 9, 10});
        putHeader(lastRecord, 6, 3);
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        putSslRecord(wire, Arrays.copyOfRange(first.array(), 0, 2));
        putSslRecord(wire, Arrays.copyOfRange(first.array(), 2, 6));
        putSslRecord(wire, lastRecord.array());
        MysqlChannel channel = receivingChannel(ByteBuffer.wrap(wire.toByteArray()), 5);
        channel.initSslBuffer();
        SSLEngine engine = mockSslEngine();
        // Leave cryptography to JSSE; exercise the real TLS-record and MySQL framing with plaintext records.
        Mockito.when(engine.unwrap(ArgumentMatchers.any(ByteBuffer.class), ArgumentMatchers.any(ByteBuffer.class)))
                .thenAnswer(inv -> {
                    ByteBuffer src = inv.getArgument(0);
                    ByteBuffer dst = inv.getArgument(1);
                    int consumed = src.remaining();
                    src.position(src.position() + 5);
                    int produced = src.remaining();
                    dst.put(src);
                    return new SSLEngineResult(SSLEngineResult.Status.OK,
                            SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, consumed, produced);
                });
        Mockito.when(engine.wrap(ArgumentMatchers.any(ByteBuffer.class), ArgumentMatchers.any(ByteBuffer.class)))
                .thenAnswer(inv -> {
                    ByteBuffer src = inv.getArgument(0);
                    ByteBuffer dst = inv.getArgument(1);
                    int length = src.remaining();
                    dst.put(src);
                    return new SSLEngineResult(SSLEngineResult.Status.OK,
                            SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, length, length);
                });
        channel.setSslEngine(engine);
        channel.setSslMode(true);
        channel.setSequenceId(1);

        ByteBuffer packet = channel.fetchOnePacket();
        byte[] payload = new byte[packet.remaining()];
        packet.get(payload);
        Assertions.assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, payload);
        packet = channel.fetchOnePacket();
        packet.get(payload);
        Assertions.assertArrayEquals(new byte[]{6, 7, 8, 9, 10}, payload);
        Assertions.assertThrows(IOException.class, channel::fetchOnePacket);
        Mockito.verify(streamConnection).close();
        assertPacketTooLarge(4);
    }

    @Test
    public void testSslCloseNotifyStopsReading() throws IOException {
        ByteBuffer input = ByteBuffer.wrap(new byte[]{0x17, 3, 3, 0, 1, 0, 0x15, 3, 3, 0, 2, 1, 0});
        MysqlChannel channel = receivingChannel(input, 16);
        channel.initSslBuffer();
        SSLEngine engine = mockSslEngine();
        AtomicBoolean inboundDone = new AtomicBoolean();
        Mockito.when(engine.isInboundDone()).thenAnswer(inv -> inboundDone.get());
        Mockito.when(engine.unwrap(ArgumentMatchers.any(ByteBuffer.class), ArgumentMatchers.any(ByteBuffer.class)))
                .thenAnswer(inv -> {
                    ByteBuffer src = inv.getArgument(0);
                    int consumed = src.remaining();
                    boolean closed = src.get(0) == 0x15;
                    src.position(src.limit());
                    inboundDone.set(closed);
                    // An empty application record keeps TLS open; close_notify ends inbound data.
                    return new SSLEngineResult(closed ? SSLEngineResult.Status.CLOSED : SSLEngineResult.Status.OK,
                            SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, consumed, 0);
                });
        channel.setSslEngine(engine);
        channel.setSslMode(true);

        Assertions.assertNull(channel.fetchOnePacket());
        Assertions.assertTrue(inboundDone.get());
        Assertions.assertEquals(input.limit(), input.position());
        Mockito.verify(streamConnection.getSourceChannel(), Mockito.times(4))
                .read(ArgumentMatchers.any(ByteBuffer.class));
    }

    private static SSLEngine mockSslEngine() {
        SSLEngine engine = Mockito.mock(SSLEngine.class);
        SSLSession session = Mockito.mock(SSLSession.class);
        Mockito.when(engine.getSession()).thenReturn(session);
        Mockito.when(session.getApplicationBufferSize()).thenReturn(16384);
        Mockito.when(session.getPacketBufferSize()).thenReturn(16384);
        return engine;
    }

    private MysqlChannel receivingChannel(ByteBuffer input, int maxPacketSize) throws IOException {
        streamConnection = Mockito.mock(StreamConnection.class);
        Mockito.when(streamConnection.getPeerAddress())
                .thenReturn(new java.net.InetSocketAddress("127.0.0.1", 12345));
        ConduitStreamSourceChannel source = Mockito.mock(ConduitStreamSourceChannel.class);
        Mockito.when(streamConnection.getSourceChannel()).thenReturn(source);
        Mockito.when(source.read(ArgumentMatchers.any(ByteBuffer.class))).thenAnswer(inv -> {
            if (!input.hasRemaining()) {
                return -1;
            }
            ByteBuffer dst = inv.getArgument(0);
            int length = Math.min(dst.remaining(), input.remaining());
            int limit = input.limit();
            input.limit(input.position() + length);
            dst.put(input);
            input.limit(limit);
            return length;
        });
        ConduitStreamSinkChannel sink = Mockito.mock(ConduitStreamSinkChannel.class);
        Mockito.when(streamConnection.getSinkChannel()).thenReturn(sink);
        response = new ByteArrayOutputStream();
        Mockito.when(sink.write(ArgumentMatchers.any(ByteBuffer.class))).thenAnswer(inv -> {
            ByteBuffer buffer = inv.getArgument(0);
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            response.write(bytes);
            return bytes.length;
        });
        Mockito.when(sink.flush()).thenReturn(true);
        receivingContext = new ConnectContext(streamConnection);
        receivingContext.getSessionVariable().maxAllowedPacket = maxPacketSize;
        return receivingContext.getMysqlChannel();
    }

    private void assertPacketTooLarge(int sequenceId) {
        ByteBuffer packet = ByteBuffer.wrap(response.toByteArray());
        packet.position(3);
        Assertions.assertEquals(sequenceId, packet.get() & 0xff);
        Assertions.assertEquals(0xff, packet.get() & 0xff);
        Assertions.assertEquals(1153, (packet.get() & 0xff) | ((packet.get() & 0xff) << 8));
    }

    private static void putHeader(ByteBuffer buffer, int length, int sequenceId) {
        buffer.put((byte) length).put((byte) (length >> 8)).put((byte) (length >> 16)).put((byte) sequenceId);
    }

    private static void putSslRecord(ByteArrayOutputStream output, byte[] payload) {
        output.write(0x17);
        output.write(3);
        output.write(3);
        output.write(payload.length >> 8);
        output.write(payload.length);
        output.write(payload, 0, payload.length);
    }

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
}
