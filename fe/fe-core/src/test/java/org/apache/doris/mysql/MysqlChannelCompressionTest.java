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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * The MySQL compressed protocol on the FE channel: frames written to the wire carry exactly the plain packet
 * stream, split and numbered as the protocol says, and frames read from the wire are reassembled into the
 * packets the plain reader would have seen.
 */
public class MysqlChannelCompressionTest {

    private static final int HEADER = MysqlChannel.COMPRESSED_HEADER_LEN;

    /** One compressed frame as decoded from the wire. */
    private static final class Frame {
        final int compressedLen;
        final int seq;
        final int uncompressedLen;
        final byte[] payload;

        Frame(int compressedLen, int seq, int uncompressedLen, byte[] payload) {
            this.compressedLen = compressedLen;
            this.seq = seq;
            this.uncompressedLen = uncompressedLen;
            this.payload = payload;
        }

        byte[] plain() throws Exception {
            if (uncompressedLen == 0) {
                return payload;
            }
            Inflater inflater = new Inflater();
            inflater.setInput(payload);
            byte[] out = new byte[uncompressedLen];
            int n = 0;
            while (n < uncompressedLen) {
                int got = inflater.inflate(out, n, uncompressedLen - n);
                Assertions.assertTrue(got > 0, "stream ended before the declared length");
                n += got;
            }
            Assertions.assertTrue(inflater.finished(), "stream longer than the declared length");
            inflater.end();
            return out;
        }
    }

    /** A channel over a mocked connection: everything written lands in {@code wire}, reads come from {@code input}. */
    private static final class Harness {
        final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        final ByteBuffer input;
        final MysqlChannel channel;

        Harness(byte[] inputBytes) throws IOException {
            this(inputBytes, false);
        }

        /** With {@code tls}, the channel is in SSL mode over an identity cipher: records are real, keys are not. */
        Harness(byte[] inputBytes, boolean tls) throws IOException {
            input = ByteBuffer.wrap(inputBytes);
            StreamConnection connection = Mockito.mock(StreamConnection.class);
            Mockito.when(connection.getPeerAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 3306));
            ConduitStreamSinkChannel sink = Mockito.mock(ConduitStreamSinkChannel.class);
            Mockito.when(connection.getSinkChannel()).thenReturn(sink);
            Mockito.when(sink.write(ArgumentMatchers.any(ByteBuffer.class))).thenAnswer(inv -> {
                ByteBuffer buffer = inv.getArgument(0);
                int len = buffer.remaining();
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                wire.write(bytes);
                return len;
            });
            Mockito.when(sink.flush()).thenReturn(true);
            ConduitStreamSourceChannel source = Mockito.mock(ConduitStreamSourceChannel.class);
            Mockito.when(connection.getSourceChannel()).thenReturn(source);
            Mockito.when(source.read(ArgumentMatchers.any(ByteBuffer.class))).thenAnswer(inv -> {
                ByteBuffer buffer = inv.getArgument(0);
                if (!input.hasRemaining()) {
                    return -1;
                }
                int len = Math.min(buffer.remaining(), input.remaining());
                byte[] bytes = new byte[len];
                input.get(bytes);
                buffer.put(bytes);
                return len;
            });
            ConnectContext ctx = new ConnectContext(connection);
            channel = tls ? new FakeTlsChannel(connection, ctx) : new MysqlChannel(connection, ctx);
        }

        /**
         * The handshake's outcome: compression negotiated, the one plain packet that stands for the
         * authentication OK flushed, then the compressed protocol started, as AcceptListener does.
         */
        void negotiateAndStart() throws IOException {
            channel.setCompressionNegotiated(1);
            channel.sendAndFlush(ByteBuffer.wrap(new byte[] {0}));
            Assertions.assertFalse(channel.isCompressionActive(), "nothing is compressed before the OK is out");
            channel.startCompressionIfNegotiated();
            Assertions.assertTrue(channel.isCompressionActive());
        }

        byte[] wireBytes() {
            return wire.toByteArray();
        }
    }

    /**
     * A channel in SSL mode whose cipher is the identity: TLS records keep their real 5-byte header
     * (type, version, 2-byte length) and carry the plaintext as is. It exercises the record-versus-frame
     * boundaries of the compressed protocol over TLS without a key store.
     */
    private static final class FakeTlsChannel extends MysqlChannel {
        static final int RECORD_HEADER = 5;
        static final int MAX_RECORD_PLAINTEXT = 16384;

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

        @Override
        protected ByteBuffer encryptData(ByteBuffer dstBuf) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while (dstBuf.hasRemaining()) {
                int n = Math.min(MAX_RECORD_PLAINTEXT, dstBuf.remaining());
                byte[] body = new byte[n];
                dstBuf.get(body);
                out.write(record(body), 0, RECORD_HEADER + n);
            }
            return ByteBuffer.wrap(out.toByteArray());
        }

        static byte[] record(byte[] plaintext) {
            ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER + plaintext.length);
            buffer.put((byte) 0x17).put((byte) 3).put((byte) 3);
            buffer.put((byte) (plaintext.length >> 8)).put((byte) plaintext.length);
            buffer.put(plaintext);
            return buffer.array();
        }

        /** Cuts a plain byte stream into records of the given sizes; the remainder is one last record. */
        static byte[] records(byte[] plain, int... sizes) {
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

        /** The plaintext carried by a run of records. */
        static byte[] unwrap(byte[] wire, int from) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int pos = from;
            while (pos < wire.length) {
                Assertions.assertEquals(0x17, wire[pos] & 0xFF, "record type at " + pos);
                int len = ((wire[pos + 3] & 0xFF) << 8) | (wire[pos + 4] & 0xFF);
                out.write(wire, pos + RECORD_HEADER, len);
                pos += RECORD_HEADER + len;
            }
            Assertions.assertEquals(wire.length, pos, "records end exactly at the wire's end");
            return out.toByteArray();
        }
    }

    private static List<Frame> decodeFrames(byte[] wire, int from) {
        List<Frame> frames = new ArrayList<>();
        int pos = from;
        while (pos < wire.length) {
            Assertions.assertTrue(pos + HEADER <= wire.length, "truncated compressed header at " + pos);
            int compressedLen = (wire[pos] & 0xFF) | ((wire[pos + 1] & 0xFF) << 8) | ((wire[pos + 2] & 0xFF) << 16);
            int seq = wire[pos + 3] & 0xFF;
            int uncompressedLen = (wire[pos + 4] & 0xFF) | ((wire[pos + 5] & 0xFF) << 8)
                    | ((wire[pos + 6] & 0xFF) << 16);
            pos += HEADER;
            Assertions.assertTrue(pos + compressedLen <= wire.length, "truncated compressed payload at " + pos);
            byte[] payload = new byte[compressedLen];
            System.arraycopy(wire, pos, payload, 0, compressedLen);
            pos += compressedLen;
            frames.add(new Frame(compressedLen, seq, uncompressedLen, payload));
        }
        return frames;
    }

    private static byte[] plainStream(List<Frame> frames) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Frame frame : frames) {
            out.write(frame.plain());
        }
        return out.toByteArray();
    }

    private static byte[] textPayload(int len) {
        byte[] bytes = new byte[len];
        byte[] line = "2026-10-05 order 1234567 | shop.orders | 1.00 | delivered\n"
                .getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < len; i++) {
            bytes[i] = line[i % line.length];
        }
        return bytes;
    }

    private static byte[] randomPayload(int len) {
        byte[] bytes = new byte[len];
        new Random(7).nextBytes(bytes);
        return bytes;
    }

    /** The packets of this test, sent through a channel; returns the bytes a plain channel would write. */
    private static void sendPackets(MysqlChannel channel, List<byte[]> packets) throws IOException {
        for (byte[] packet : packets) {
            channel.sendOnePacket(ByteBuffer.wrap(packet));
        }
        channel.flush();
    }

    @Test
    public void testWriteFramesCarryThePlainPacketStream() throws Exception {
        byte[] tiny = new byte[] {1, 2, 3};                 // under MIN_COMPRESS_LENGTH: travels raw
        byte[] noise = randomPayload(4 * 1024);             // incompressible: zlib cannot shrink it, travels raw
        List<byte[]> bulk = new ArrayList<>();
        bulk.add(textPayload(200 * 1024));                  // compressible: a deflated frame
        bulk.add(textPayload(MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH + 1024 * 1024)); // split over frames

        // the reference: the same packets through a plain channel, whatever the flush boundaries
        Harness plain = new Harness(new byte[0]);
        plain.channel.setSequenceId(0);
        plain.channel.sendAndFlush(ByteBuffer.wrap(tiny));
        plain.channel.sendAndFlush(ByteBuffer.wrap(noise));
        sendPackets(plain.channel, bulk);
        byte[] expected = plain.wireBytes();

        Harness compressed = new Harness(new byte[0]);
        compressed.negotiateAndStart();
        int authResponseLen = compressed.wireBytes().length;
        Assertions.assertEquals(4 + 1, authResponseLen, "the authentication response travels plain");
        compressed.channel.setSequenceId(0);
        compressed.channel.sendAndFlush(ByteBuffer.wrap(tiny));
        compressed.channel.sendAndFlush(ByteBuffer.wrap(noise));
        sendPackets(compressed.channel, bulk);

        List<Frame> frames = decodeFrames(compressed.wireBytes(), authResponseLen);
        Assertions.assertArrayEquals(expected, plainStream(frames));
        for (int i = 0; i < frames.size(); i++) {
            Assertions.assertEquals(i & 0xFF, frames.get(i).seq, "compressed sequence id of frame " + i);
            Assertions.assertTrue(frames.get(i).compressedLen <= MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH);
            Assertions.assertTrue(frames.get(i).uncompressedLen <= MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH);
        }
        Assertions.assertEquals(0, frames.get(0).uncompressedLen, "a short frame is sent raw");
        Assertions.assertEquals(4 + tiny.length, frames.get(0).compressedLen);
        Assertions.assertEquals(0, frames.get(1).uncompressedLen, "an incompressible frame is sent raw");
        Assertions.assertEquals(4 + noise.length, frames.get(1).compressedLen);
        Assertions.assertTrue(frames.stream().anyMatch(f -> f.uncompressedLen > 0
                && f.compressedLen < f.uncompressedLen), "a text frame is deflated");
        Assertions.assertTrue(frames.size() >= 5, "a packet past 16 MiB spans more than one frame");
        Assertions.assertTrue(compressed.wireBytes().length < expected.length / 4, "text shrinks on the wire");
    }

    private static byte[] packet(int seq, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + payload.length);
        buffer.put((byte) payload.length).put((byte) (payload.length >> 8)).put((byte) (payload.length >> 16));
        buffer.put((byte) seq);
        buffer.put(payload);
        return buffer.array();
    }

    private static byte[] frame(int seq, byte[] plain, boolean deflate) {
        byte[] payload = plain;
        int uncompressedLen = 0;
        if (deflate) {
            Deflater deflater = new Deflater(6);
            deflater.setInput(plain);
            deflater.finish();
            byte[] out = new byte[plain.length + 64];
            int n = deflater.deflate(out);
            Assertions.assertTrue(deflater.finished());
            deflater.end();
            payload = new byte[n];
            System.arraycopy(out, 0, payload, 0, n);
            uncompressedLen = plain.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(HEADER + payload.length);
        buffer.put((byte) payload.length).put((byte) (payload.length >> 8)).put((byte) (payload.length >> 16));
        buffer.put((byte) seq);
        buffer.put((byte) uncompressedLen).put((byte) (uncompressedLen >> 8)).put((byte) (uncompressedLen >> 16));
        buffer.put(payload);
        return buffer.array();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    @Test
    public void testReadReassemblesPacketsAcrossFrames() throws Exception {
        byte[] p1 = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = textPayload(100 * 1024);
        byte[] p3 = textPayload(60);
        byte[] stream = concat(packet(0, p1), packet(1, p2), packet(2, p3));
        // frame 0 raw: the first packet plus the head of the second; frame 1 deflated: the rest of the second and
        // the whole third; frame 2 raw: nothing but the tail of the third
        int cut1 = 4 + p1.length + 5;
        int cut2 = stream.length - 10;
        byte[] wire = concat(
                frame(0, Arrays.copyOfRange(stream, 0, cut1), false),
                frame(1, Arrays.copyOfRange(stream, cut1, cut2), true),
                frame(2, Arrays.copyOfRange(stream, cut2, stream.length), false));

        Harness harness = new Harness(wire);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);

        ByteBuffer got = harness.channel.fetchOnePacket();
        Assertions.assertArrayEquals(p1, remaining(got));
        got = harness.channel.fetchOnePacket();
        Assertions.assertArrayEquals(p2, remaining(got));
        got = harness.channel.fetchOnePacket();
        Assertions.assertArrayEquals(p3, remaining(got));
        // the peer closed the connection: no header to read
        Assertions.assertNull(harness.channel.fetchOnePacket());
    }

    @Test
    public void testReadRefusesAFrameOutOfSequence() throws Exception {
        byte[] wire = frame(1, packet(0, "abc".getBytes(StandardCharsets.UTF_8)), false);
        Harness harness = new Harness(wire);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        IOException e = Assertions.assertThrows(IOException.class, harness.channel::fetchOnePacket);
        Assertions.assertTrue(e.getMessage().contains("compressed packet sequence"), e.getMessage());
    }

    @Test
    public void testReadRefusesAFrameThatInflatesToAnotherLength() throws Exception {
        byte[] plain = packet(0, textPayload(300));
        byte[] good = frame(0, plain, true);
        // declare one byte less than the stream inflates to
        good[4] = (byte) (plain.length - 1);
        good[5] = (byte) ((plain.length - 1) >> 8);
        Harness harness = new Harness(good);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        IOException e = Assertions.assertThrows(IOException.class, harness.channel::fetchOnePacket);
        Assertions.assertTrue(e.getMessage().contains("header declared"), e.getMessage());
    }

    @Test
    public void testCompressedSequenceResetsWithThePacketSequence() throws Exception {
        // a command (frame 0) answered on the same connection: the response frames continue the counter, and
        // the next command boundary resets both counters to 0
        byte[] wire = concat(frame(0, packet(0, new byte[] {3, 'x'}), false),
                frame(0, packet(0, new byte[] {3, 'y'}), false));
        Harness harness = new Harness(wire);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(new byte[] {3, 'x'}, remaining(harness.channel.fetchOnePacket()));
        int before = harness.wireBytes().length;
        harness.channel.sendAndFlush(ByteBuffer.wrap(textPayload(500)));
        List<Frame> response = decodeFrames(harness.wireBytes(), before);
        Assertions.assertEquals(1, response.size());
        Assertions.assertEquals(1, response.get(0).seq, "the response continues the command's compressed sequence");
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(new byte[] {3, 'y'}, remaining(harness.channel.fetchOnePacket()));
    }

    @Test
    public void testPacketCounterFollowsTheFramesAtEachTurn() throws Exception {
        // a command whose single packet (seq 0) arrives split over two raw frames (seq 0 and 1), the way
        // libmysqlclient sends a statement longer than its net buffer: mysqld numbers its reply from the
        // frames it read (2), not from the packets (1), and so must we
        byte[] command = packet(0, textPayload(120));
        byte[] wire = concat(frame(0, Arrays.copyOfRange(command, 0, 70), false),
                frame(1, Arrays.copyOfRange(command, 70, command.length), false));
        Harness harness = new Harness(wire);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(textPayload(120), remaining(harness.channel.fetchOnePacket()));
        int before = harness.wireBytes().length;
        harness.channel.sendAndFlush(ByteBuffer.wrap(new byte[] {7}));
        List<Frame> reply = decodeFrames(harness.wireBytes(), before);
        Assertions.assertEquals(1, reply.size());
        Assertions.assertEquals(2, reply.get(0).seq, "the reply's frame continues the frame count");
        byte[] plain = reply.get(0).plain();
        Assertions.assertEquals(2, plain[3] & 0xFF, "the reply's first packet is numbered from the frames read");
    }

    @Test
    public void testInboundInnerSequenceIsNotVerifiedWhileCompressed() throws Exception {
        // a client numbers inner packets from its own frame count; mysqld verifies only the frame sequence
        byte[] payload = "abc".getBytes(StandardCharsets.UTF_8);
        Harness harness = new Harness(frame(0, packet(9, payload), false));
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(payload, remaining(harness.channel.fetchOnePacket()));
    }

    @Test
    public void testRefusedLoginNeverStartsCompression() throws Exception {
        Harness harness = new Harness(new byte[0]);
        harness.channel.setCompressionNegotiated(1);
        harness.channel.setSequenceId(0);
        // the refusal that closes the handshake is flushed, but nothing starts the compressed protocol
        byte[] refusal = new byte[] {(byte) 0xff, 1, 2};
        harness.channel.sendAndFlush(ByteBuffer.wrap(refusal));
        Assertions.assertFalse(harness.channel.isCompressionActive());
        harness.channel.sendAndFlush(ByteBuffer.wrap(textPayload(300)));
        Assertions.assertArrayEquals(concat(packet(0, refusal), packet(1, textPayload(300))), harness.wireBytes());
    }

    @Test
    public void testCloseEndsTheCompressorAndLaterFramesFailAsClosed() throws Exception {
        Harness harness = new Harness(frame(0, packet(0, textPayload(100)), true));
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        // a KILL or the idle reaper closes the channel from another thread through this same call
        harness.channel.close();
        IOException sendFailure = Assertions.assertThrows(IOException.class,
                () -> harness.channel.sendAndFlush(ByteBuffer.wrap(textPayload(300))));
        Assertions.assertTrue(sendFailure.getMessage().contains("closed"), sendFailure.getMessage());
        IOException readFailure = Assertions.assertThrows(IOException.class, harness.channel::fetchOnePacket);
        Assertions.assertTrue(readFailure.getMessage().contains("closed"), readFailure.getMessage());
    }

    @Test
    public void testStartAfterCloseIsANoOp() throws Exception {
        Harness harness = new Harness(new byte[0]);
        harness.channel.setCompressionNegotiated(1);
        harness.channel.close();
        harness.channel.startCompressionIfNegotiated();
        Assertions.assertFalse(harness.channel.isCompressionActive());
    }

    @Test
    public void testReadRefusesAStreamCutBeforeItsTrailer() throws Exception {
        byte[] whole = frame(0, packet(0, textPayload(300)), true);
        // drop the zlib trailer (adler32, 4 bytes) and declare the shorter compressed length
        int cut = whole.length - 4;
        byte[] wire = Arrays.copyOf(whole, cut);
        int compressedLen = cut - HEADER;
        wire[0] = (byte) compressedLen;
        wire[1] = (byte) (compressedLen >> 8);
        wire[2] = (byte) (compressedLen >> 16);
        Harness harness = new Harness(wire);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        IOException e = Assertions.assertThrows(IOException.class, harness.channel::fetchOnePacket);
        Assertions.assertTrue(e.getMessage().contains("trailer") || e.getMessage().contains("header declared"),
                e.getMessage());
    }

    @Test
    public void testCompressedFramesAreReadOutOfTlsRecords() throws Exception {
        // the same three packets over three frames as the plain reassembly test (frame 0 raw, 19 bytes;
        // frame 1 deflated, a few hundred bytes; frame 2 raw, 17 bytes), but the frame stream arrives cut
        // into TLS records at awkward places: inside frame 0's header (3), inside its payload (9 more),
        // one record holding the tail of frame 0, the whole of frame 1 and most of frame 2, and the last
        // 7 bytes of frame 2 as the final record
        byte[] p1 = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = textPayload(100 * 1024);
        byte[] p3 = textPayload(60);
        byte[] stream = concat(packet(0, p1), packet(1, p2), packet(2, p3));
        int cut1 = 4 + p1.length + 5;
        int cut2 = stream.length - 10;
        byte[] frames = concat(
                frame(0, Arrays.copyOfRange(stream, 0, cut1), false),
                frame(1, Arrays.copyOfRange(stream, cut1, cut2), true),
                frame(2, Arrays.copyOfRange(stream, cut2, stream.length), false));
        Assertions.assertTrue(frames.length > 12 + 7 + HEADER, "the fixture must span more than its cuts");
        byte[] wire = FakeTlsChannel.records(frames, 3, 9, frames.length - 12 - 7);

        Harness harness = new Harness(wire, true);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(p1, remaining(harness.channel.fetchOnePacket()));
        Assertions.assertArrayEquals(p2, remaining(harness.channel.fetchOnePacket()));
        Assertions.assertArrayEquals(p3, remaining(harness.channel.fetchOnePacket()));
        Assertions.assertNull(harness.channel.fetchOnePacket());
    }

    @Test
    public void testTlsRecordsMayBeEmptyAndTheStreamMayEndMidRecord() throws Exception {
        byte[] p1 = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = textPayload(300);
        byte[] frames = concat(frame(0, packet(0, p1), false), frame(1, packet(1, p2), true));
        // frame 0 is 14 bytes (7-byte frame header + a 7-byte packet): cut it into records of 3, an empty
        // one, and 11, so frame 1 is the last record on its own; the empty record carries nothing and
        // costs nothing
        Assertions.assertEquals(14, HEADER + 4 + p1.length);
        byte[] wire = FakeTlsChannel.records(frames, 3, 0, 11);
        Harness harness = new Harness(wire, true);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(p1, remaining(harness.channel.fetchOnePacket()));
        Assertions.assertArrayEquals(p2, remaining(harness.channel.fetchOnePacket()));
        Assertions.assertNull(harness.channel.fetchOnePacket());

        // the peer goes away inside the last record: the packet it carried is never produced, while
        // frame 0, complete in the earlier records, still is
        byte[] truncated = Arrays.copyOf(wire, wire.length - 5);
        harness = new Harness(truncated, true);
        harness.negotiateAndStart();
        harness.channel.setSequenceId(0);
        Assertions.assertArrayEquals(p1, remaining(harness.channel.fetchOnePacket()));
        Assertions.assertNull(harness.channel.fetchOnePacket());
    }

    @Test
    public void testCompressedFramesAreWrittenInsideTlsRecords() throws Exception {
        List<byte[]> bulk = new ArrayList<>();
        bulk.add(textPayload(200 * 1024));
        bulk.add(textPayload(MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH + 1024 * 1024));

        Harness plain = new Harness(new byte[0]);
        plain.channel.setSequenceId(0);
        sendPackets(plain.channel, bulk);
        byte[] expected = plain.wireBytes();

        Harness tls = new Harness(new byte[0], true);
        tls.negotiateAndStart();
        int handshakeLen = tls.wireBytes().length;
        Assertions.assertEquals(FakeTlsChannel.RECORD_HEADER + 4 + 1, handshakeLen,
                "the authentication response travels as one plain record");
        tls.channel.setSequenceId(0);
        sendPackets(tls.channel, bulk);

        // records on the wire, frames inside the records, the plain packet stream inside the frames
        byte[] framesOnTheWire = FakeTlsChannel.unwrap(tls.wireBytes(), handshakeLen);
        List<Frame> frames = decodeFrames(framesOnTheWire, 0);
        Assertions.assertArrayEquals(expected, plainStream(frames));
        Assertions.assertTrue(frames.size() >= 3, "a packet past 16 MiB spans more than one frame");
        for (int i = 0; i < frames.size(); i++) {
            Assertions.assertEquals(i & 0xFF, frames.get(i).seq);
        }
    }

    @Test
    public void testPlainTlsSessionIsUntouched() throws Exception {
        // no compression negotiated: the existing TLS packet path serves a packet out of a record and
        // sends a packet as a record
        byte[] payload = textPayload(300);
        Harness harness = new Harness(FakeTlsChannel.records(packet(0, payload)), true);
        harness.channel.setSequenceId(0);
        Assertions.assertFalse(harness.channel.isCompressionActive());
        Assertions.assertArrayEquals(payload, remaining(harness.channel.fetchOnePacket()));
        harness.channel.sendAndFlush(ByteBuffer.wrap(new byte[] {8, 9}));
        Assertions.assertArrayEquals(packet(1, new byte[] {8, 9}), FakeTlsChannel.unwrap(harness.wireBytes(), 0));
    }

    @Test
    public void testPlainChannelIsUntouched() throws Exception {
        Harness harness = new Harness(packet(0, new byte[] {7}));
        harness.channel.setSequenceId(0);
        Assertions.assertFalse(harness.channel.isCompressionActive());
        Assertions.assertArrayEquals(new byte[] {7}, remaining(harness.channel.fetchOnePacket()));
        harness.channel.sendAndFlush(ByteBuffer.wrap(new byte[] {8, 9}));
        Assertions.assertArrayEquals(packet(1, new byte[] {8, 9}), harness.wireBytes());
    }

    private static byte[] remaining(ByteBuffer buffer) {
        Assertions.assertNotNull(buffer);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
