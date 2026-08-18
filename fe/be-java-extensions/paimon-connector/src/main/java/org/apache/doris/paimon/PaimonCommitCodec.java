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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
    private static final int SERIALIZATION_WORKING_COPIES = 2;

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
        return encode(messages, Long.MAX_VALUE);
    }

    byte[][] encode(List<CommitMessage> messages, long maxTotalPayloadBytes) throws Exception {
        if (maxTotalPayloadBytes <= 0) {
            throw new IllegalArgumentException(
                    "Paimon commit payload memory limit must be positive");
        }
        if (messages.isEmpty()) {
            return new byte[0][];
        }

        // Adaptive chunking uses a size-limited output. An oversized attempt
        // therefore stops before allocating beyond one chunk's budget.
        int chunkSize = defaultChunkSize;
        List<byte[]> payloads = new ArrayList<>();
        long totalPayloadBytes = 0;
        int offset = 0;
        while (offset < messages.size()) {
            long remainingPayloadMemory = maxTotalPayloadBytes - totalPayloadBytes;
            int chunkWorkingLimit = (int) Math.min(
                    maxPayloadBytes, remainingPayloadMemory / SERIALIZATION_WORKING_COPIES);
            if (chunkWorkingLimit <= HEADER_BYTES) {
                throw totalPayloadMemoryException(
                        maxTotalPayloadBytes, totalPayloadBytes, chunkWorkingLimit);
            }
            int end = Math.min(offset + chunkSize, messages.size());
            byte[] payload;
            try {
                payload = encodeChunk(messages.subList(offset, end), chunkWorkingLimit);
            } catch (PayloadTooLargeException e) {
                if (chunkSize > 1) {
                    chunkSize = Math.max(1, chunkSize / 2);
                    continue;
                }
                if (chunkWorkingLimit < maxPayloadBytes) {
                    throw totalPayloadMemoryException(
                            maxTotalPayloadBytes, totalPayloadBytes, chunkWorkingLimit);
                }
                throw new CommitPayloadMemoryException(
                        "A single Paimon commit message exceeds the "
                                + maxPayloadBytes + " byte framed payload limit",
                        e);
            }
            if (payload.length > maxTotalPayloadBytes - totalPayloadBytes) {
                throw new CommitPayloadMemoryException(
                        "Paimon commit payloads exceed their total memory limit: limit="
                                + maxTotalPayloadBytes + ", completed=" + totalPayloadBytes
                                + ", nextChunk=" + payload.length);
            }
            payloads.add(payload);
            totalPayloadBytes += payload.length;
            offset = end;
        }
        return payloads.toArray(new byte[0][]);
    }

    /** Serialize one chunk of messages and wrap it in a DPCM frame. */
    private byte[] encodeChunk(List<CommitMessage> messages, int chunkWorkingLimit)
            throws Exception {
        BoundedOutputStream output = new BoundedOutputStream(chunkWorkingLimit);
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

    private static CommitPayloadMemoryException totalPayloadMemoryException(
            long maxTotalPayloadBytes, long completedPayloadBytes, int nextWorkingBufferBytes) {
        return new CommitPayloadMemoryException(
                "Paimon commit payload serialization exceeds its total memory limit: limit="
                        + maxTotalPayloadBytes + ", completed=" + completedPayloadBytes
                        + ", nextWorkingBuffer=" + nextWorkingBufferBytes);
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

    static final class CommitPayloadMemoryException extends IOException {
        private CommitPayloadMemoryException(String message) {
            super(message);
        }

        private CommitPayloadMemoryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Output stream which fails before Paimon serialization can exceed one framed chunk. */
    private static final class BoundedOutputStream extends OutputStream {
        private byte[] output;
        private final int limit;
        private int size;

        private BoundedOutputStream(int limit) {
            this.output = new byte[Math.min(1024, limit)];
            this.limit = limit;
        }

        private void reserve(int bytes) throws IOException {
            if (bytes < 0 || bytes > limit - size) {
                throw new PayloadTooLargeException(limit);
            }
            int required = size + bytes;
            if (required > output.length) {
                int doubled = output.length > limit / 2 ? limit : output.length * 2;
                output = Arrays.copyOf(output, Math.max(required, doubled));
            }
        }

        @Override
        public void write(int value) throws IOException {
            reserve(1);
            output[size++] = (byte) value;
        }

        @Override
        public void write(byte[] value, int offset, int length) throws IOException {
            reserve(length);
            System.arraycopy(value, offset, output, size, length);
            size += length;
        }

        private byte[] toByteArray() {
            return Arrays.copyOf(output, size);
        }
    }
}
