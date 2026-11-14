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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * MySQL packets read out of TLS records: a packet may span many records, a record may carry several
 * packets, and a packet of exactly 0xFFFFFF bytes continues in the next one, exactly as on a plain socket.
 */
public class MysqlChannelTlsPacketTest {

    private static final int MAX_PACKET = MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH;
    private static final int RECORD_HEADER = 5;
    private static final int MAX_RECORD_PLAINTEXT = 16384;

    /** A channel in SSL mode whose cipher is the identity: records keep their real 5-byte header. */
    private static final class FakeTlsChannel extends MysqlChannel {
        FakeTlsChannel(StreamConnection connection, ConnectContext ctx) {
            super(connection, ctx);
            initSslBuffer();
            setSslMode(true);
        }

        @Override
        protected void decryptData(ByteBuffer dstBuf, boolean isHeader) {
            if (isHeader) {
                return;
            }
            // the record (header + body) is in dstBuf with the position at its end, like the real one
            dstBuf.flip();
            dstBuf.position(RECORD_HEADER);
            dstBuf.compact();
            dstBuf.flip();
        }
    }

    private static MysqlChannel channelOver(byte[] wire, boolean tls) throws IOException {
        ByteBuffer input = ByteBuffer.wrap(wire);
        StreamConnection connection = Mockito.mock(StreamConnection.class);
        Mockito.when(connection.getPeerAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 3306));
        ConduitStreamSinkChannel sink = Mockito.mock(ConduitStreamSinkChannel.class);
        Mockito.when(connection.getSinkChannel()).thenReturn(sink);
        ConduitStreamSourceChannel source = Mockito.mock(ConduitStreamSourceChannel.class);
        Mockito.when(connection.getSourceChannel()).thenReturn(source);
        Mockito.when(source.read(ArgumentMatchers.any(ByteBuffer.class))).thenAnswer(inv -> {
            ByteBuffer buffer = inv.getArgument(0);
            if (!input.hasRemaining()) {
                return -1;
            }
            // hand over at most a few KiB per read so that reads straddle record and packet boundaries
            int len = Math.min(Math.min(buffer.remaining(), input.remaining()), 3000);
            byte[] bytes = new byte[len];
            input.get(bytes);
            buffer.put(bytes);
            return len;
        });
        ConnectContext ctx = new ConnectContext(connection);
        return tls ? new FakeTlsChannel(connection, ctx) : new MysqlChannel(connection, ctx);
    }

    /** One MySQL packet: 3-byte little-endian length, sequence id, payload. */
    private static byte[] packet(int seq, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + payload.length);
        buffer.put((byte) payload.length).put((byte) (payload.length >> 8)).put((byte) (payload.length >> 16));
        buffer.put((byte) seq).put(payload);
        return buffer.array();
    }

    /** A payload as the MySQL protocol sends it: 0xFFFFFF-byte packets, then the rest, possibly empty. */
    private static byte[] packets(byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int pos = 0;
        int seq = 0;
        while (true) {
            int n = Math.min(MAX_PACKET, payload.length - pos);
            byte[] piece = Arrays.copyOfRange(payload, pos, pos + n);
            out.write(packet(seq++, piece), 0, 4 + n);
            pos += n;
            if (n < MAX_PACKET) {
                return out.toByteArray();
            }
        }
    }

    private static byte[] record(byte[] plaintext) {
        ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER + plaintext.length);
        buffer.put((byte) 0x17).put((byte) 3).put((byte) 3);
        buffer.put((byte) (plaintext.length >> 8)).put((byte) plaintext.length);
        buffer.put(plaintext);
        return buffer.array();
    }

    /** Cuts a plain byte stream into records of the given sizes; the remainder goes in full-size records. */
    private static byte[] records(byte[] plain, int... sizes) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int pos = 0;
        for (int size : sizes) {
            byte[] piece = Arrays.copyOfRange(plain, pos, pos + size);
            out.write(record(piece), 0, RECORD_HEADER + piece.length);
            pos += size;
        }
        while (pos < plain.length) {
            byte[] piece = Arrays.copyOfRange(plain, pos, Math.min(plain.length, pos + MAX_RECORD_PLAINTEXT));
            out.write(record(piece), 0, RECORD_HEADER + piece.length);
            pos += piece.length;
        }
        return out.toByteArray();
    }

    private static byte[] randomPayload(int len) {
        byte[] bytes = new byte[len];
        new Random(len).nextBytes(bytes);
        return bytes;
    }

    private static byte[] bytesOf(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    @Test
    public void largeStatementOverTlsIsJoined() throws IOException {
        byte[] payload = randomPayload(MAX_PACKET + 100_000);
        MysqlChannel channel = channelOver(records(packets(payload), 7, 16384, 1), true);
        ByteBuffer got = channel.fetchOnePacket();
        Assertions.assertNotNull(got);
        Assertions.assertArrayEquals(payload, bytesOf(got));
        Assertions.assertNull(channel.fetchOnePacket(), "nothing follows the statement");
    }

    @Test
    public void exactMaxPacketOverTlsEndsWithTheEmptyContinuation() throws IOException {
        byte[] payload = randomPayload(MAX_PACKET);
        byte[] plain = packets(payload);
        Assertions.assertEquals(4 + MAX_PACKET + 4, plain.length, "an empty packet closes it");
        MysqlChannel channel = channelOver(records(plain), true);
        Assertions.assertArrayEquals(payload, bytesOf(channel.fetchOnePacket()));
        Assertions.assertNull(channel.fetchOnePacket());
    }

    @Test
    public void twoContinuationsOverTls() throws IOException {
        byte[] payload = randomPayload(2 * MAX_PACKET + 12_345);
        MysqlChannel channel = channelOver(records(packets(payload)), true);
        Assertions.assertArrayEquals(payload, bytesOf(channel.fetchOnePacket()));
    }

    @Test
    public void severalPacketsInOneRecordOverTls() throws IOException {
        byte[] first = randomPayload(100);
        byte[] second = randomPayload(200);
        byte[] third = randomPayload(300);
        byte[] plain = concat(packet(0, first), packet(1, second), packet(2, third));
        MysqlChannel channel = channelOver(records(plain), true);
        Assertions.assertArrayEquals(first, bytesOf(channel.fetchOnePacket()));
        Assertions.assertArrayEquals(second, bytesOf(channel.fetchOnePacket()));
        Assertions.assertArrayEquals(third, bytesOf(channel.fetchOnePacket()));
        Assertions.assertNull(channel.fetchOnePacket());
    }

    @Test
    public void packetSpanningOddRecordsOverTls() throws IOException {
        byte[] payload = randomPayload(50_000);
        // the header itself is cut across records, then a 1-byte record, then uneven pieces
        MysqlChannel channel = channelOver(records(packet(0, payload), 2, 1, 1, 9_999, 16384, 3), true);
        Assertions.assertArrayEquals(payload, bytesOf(channel.fetchOnePacket()));
    }

    @Test
    public void continuationOutOfSequenceOverTlsIsRefused() throws IOException {
        byte[] plain = concat(packet(0, randomPayload(MAX_PACKET)), packet(5, randomPayload(10)));
        MysqlChannel channel = channelOver(records(plain), true);
        IOException e = Assertions.assertThrows(IOException.class, channel::fetchOnePacket);
        Assertions.assertEquals("Bad packet sequence.", e.getMessage());
    }

    @Test
    public void largeStatementOverPlainSocketIsStillJoined() throws IOException {
        byte[] payload = randomPayload(MAX_PACKET + 100_000);
        MysqlChannel channel = channelOver(packets(payload), false);
        Assertions.assertArrayEquals(payload, bytesOf(channel.fetchOnePacket()));
        Assertions.assertNull(channel.fetchOnePacket());
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }
}
