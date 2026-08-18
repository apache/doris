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

package org.apache.doris.paimon;

import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes Paimon commit messages into the DPCM (Doris-Paimon Commit Message) payload
 * format forwarded to FE.
 *
 * <h3>DPCM framing format</h3>
 * Each payload is framed as:
 * <pre>
 *   ┌──────────┬─────────────┬────────────┬──────────────────────┐
 *   │ Magic (4)│ Version (4) │ Length (4) │ Serialized Messages  │
 *   │  "DPCM"  │  big-endian │ big-endian │     (varies)         │
 *   └──────────┴─────────────┴────────────┴──────────────────────┘
 * </pre>
 *
 * <p>Messages are serialized using Paimon's {@link CommitMessageSerializer} and
 * split into chunks if the serialized payload exceeds {@link #MAX_PAYLOAD_BYTES}
 * (8 MiB). Chunk size starts at {@link #DEFAULT_CHUNK_SIZE} (512 messages) and
 * is halved adaptively until each chunk fits within the size limit.
 */
final class PaimonCommitCodec {
    static final int HEADER_BYTES = 12;
    /** Maximum framed payload size per chunk (8 MiB). */
    static final int MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;
    /** Starting number of commit messages per chunk. */
    static final int DEFAULT_CHUNK_SIZE = 512;

    private final CommitMessageSerializer serializer = new CommitMessageSerializer();
    private final int maxPayloadBytes;
    private final int defaultChunkSize;

    PaimonCommitCodec() {
        this(MAX_PAYLOAD_BYTES, DEFAULT_CHUNK_SIZE);
    }

    PaimonCommitCodec(int maxPayloadBytes, int defaultChunkSize) {
        if (maxPayloadBytes <= HEADER_BYTES || defaultChunkSize <= 0) {
            throw new IllegalArgumentException("Invalid Paimon commit payload limits");
        }
        this.maxPayloadBytes = maxPayloadBytes;
        this.defaultChunkSize = defaultChunkSize;
    }

    /**
     * Encode commit messages into DPCM-framed byte chunks.
     *
     * @param messages Paimon commit messages from {@code prepareCommit()}
     * @return byte[][] where each element is a complete DPCM-framed chunk
     */
    byte[][] encode(List<CommitMessage> messages) throws Exception {
        if (messages.isEmpty()) {
            return new byte[0][];
        }

        // Adaptive chunking uses a size-limited output. An oversized attempt
        // therefore stops before allocating beyond one chunk's budget.
        int chunkSize = defaultChunkSize;
        List<byte[]> payloads = new ArrayList<>();
        int offset = 0;
        while (offset < messages.size()) {
            int end = Math.min(offset + chunkSize, messages.size());
            byte[] payload;
            try {
                payload = encodeChunk(messages.subList(offset, end));
            } catch (PayloadTooLargeException e) {
                if (chunkSize > 1) {
                    chunkSize = Math.max(1, chunkSize / 2);
                    continue;
                }
                throw new IOException("A single Paimon commit message exceeds the "
                        + maxPayloadBytes + " byte framed payload limit", e);
            }
            payloads.add(payload);
            offset = end;
        }
        return payloads.toArray(new byte[0][]);
    }

    /** Serialize one chunk of messages and wrap it in a DPCM frame. */
    private byte[] encodeChunk(List<CommitMessage> messages) throws Exception {
        BoundedOutputStream output = new BoundedOutputStream(maxPayloadBytes);
        output.write(new byte[HEADER_BYTES]);
        serializer.serializeList(messages, new DataOutputViewStreamWrapper(output));

        byte[] payload = output.toByteArray();
        payload[0] = 'D';
        payload[1] = 'P';
        payload[2] = 'C';
        payload[3] = 'M';
        writeInt(payload, 4, serializer.getVersion());
        writeInt(payload, 8, payload.length - HEADER_BYTES);
        return payload;
    }

    /**
     * Wrap serialized data in a DPCM frame: 4-byte magic "DPCM", 4-byte version
     * (big-endian), 4-byte data length (big-endian), followed by the data.
     */
    static byte[] frame(byte[] data, int version) {
        byte[] payload = new byte[HEADER_BYTES + data.length];
        payload[0] = 'D';
        payload[1] = 'P';
        payload[2] = 'C';
        payload[3] = 'M';
        writeInt(payload, 4, version);
        writeInt(payload, 8, data.length);
        System.arraycopy(data, 0, payload, HEADER_BYTES, data.length);
        return payload;
    }

    /** Write a 32-bit integer in big-endian byte order. */
    private static void writeInt(byte[] output, int offset, int value) {
        output[offset] = (byte) ((value >>> 24) & 0xFF);
        output[offset + 1] = (byte) ((value >>> 16) & 0xFF);
        output[offset + 2] = (byte) ((value >>> 8) & 0xFF);
        output[offset + 3] = (byte) (value & 0xFF);
    }

    private static final class PayloadTooLargeException extends IOException {
        private PayloadTooLargeException(int limit) {
            super("Paimon commit serialization exceeds " + limit + " bytes");
        }
    }

    /** Output stream which fails before Paimon serialization can exceed one framed chunk. */
    private static final class BoundedOutputStream extends OutputStream {
        private final ByteArrayOutputStream output;
        private final int limit;

        private BoundedOutputStream(int limit) {
            this.output = new ByteArrayOutputStream(Math.min(1024, limit));
            this.limit = limit;
        }

        private void reserve(int bytes) throws IOException {
            if (bytes < 0 || bytes > limit - output.size()) {
                throw new PayloadTooLargeException(limit);
            }
        }

        @Override
        public void write(int value) throws IOException {
            reserve(1);
            output.write(value);
        }

        @Override
        public void write(byte[] value, int offset, int length) throws IOException {
            reserve(length);
            output.write(value, offset, length);
        }

        private byte[] toByteArray() {
            return output.toByteArray();
        }
    }
}
