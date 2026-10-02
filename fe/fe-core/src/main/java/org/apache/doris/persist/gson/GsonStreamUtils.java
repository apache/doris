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

package org.apache.doris.persist.gson;

import com.google.gson.JsonParseException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Streams JSON objects without materializing the complete JSON string or byte array.
 *
 * <p>The chunked wire format is:
 * <pre>
 *   int32 -1                         // chunked-format marker
 *   repeated {
 *     int32 chunkLength              // positive number of bytes in this chunk
 *     byte[chunkLength] chunk
 *   }
 *   int32 0                          // record terminator
 * </pre>
 * Chunk boundaries are byte-oriented and may split a multi-byte UTF-8 character. Readers must consume the
 * terminator before reading the next field from the {@link DataInput}.
 *
 * <p>For image compatibility, {@link #readJson(DataInput, Class)} also accepts the legacy
 * {@code Text.writeString} format: a non-negative UTF-8 byte length followed by that many bytes. New values are
 * always written in the chunked format. Neither method closes the caller-owned input or output.
 */
public final class GsonStreamUtils {
    // Text.writeString uses only non-negative lengths, so this marker unambiguously identifies the new format.
    private static final int CHUNKED_JSON_MARKER = -1;
    // Bounds temporary memory while keeping the framing overhead small for large image entries.
    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    private GsonStreamUtils() {
    }

    /**
     * Writes one JSON value using the chunked wire format documented on this class.
     */
    public static void writeJson(DataOutput out, Object value) throws IOException {
        writeJson(out, value, DEFAULT_CHUNK_SIZE);
    }

    static void writeJson(DataOutput out, Object value, int chunkSize) throws IOException {
        out.writeInt(CHUNKED_JSON_MARKER);
        ChunkedOutputStream output = new ChunkedOutputStream(out, chunkSize);
        Writer writer = new OutputStreamWriter(output, StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT));
        // The caller owns DataOutput; do not close the writer after a failed serialization.
        try {
            GsonUtils.GSON.toJson(value, writer);
        } catch (JsonParseException e) {
            rethrowIOException(e);
            throw e;
        }
        writer.flush();
        output.finish();
    }

    /**
     * Reads either a chunked JSON value or a legacy length-prefixed JSON value.
     */
    public static <T> T readJson(DataInput in, Class<T> type) throws IOException {
        int length = in.readInt();
        InputStream input;
        if (length == CHUNKED_JSON_MARKER) {
            input = new ChunkedInputStream(in);
        } else if (length >= 0) {
            input = new BoundedInputStream(in, length);
        } else {
            throw new IOException("Invalid JSON length: " + length);
        }
        try {
            T value = GsonUtils.GSON.fromJson(new InputStreamReader(input, StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)), type);
            while (input.read() != -1) {
                // Consume the complete framed JSON value, including the terminal chunk.
            }
            return value;
        } catch (JsonParseException e) {
            rethrowIOException(e);
            throw e;
        }
    }

    private static void rethrowIOException(JsonParseException exception) throws IOException {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            cause = cause.getCause();
        }
    }

    private static class ChunkedOutputStream extends OutputStream {
        private final DataOutput out;
        private final byte[] buffer;
        private int count;

        private ChunkedOutputStream(DataOutput out, int chunkSize) {
            this.out = out;
            this.buffer = new byte[chunkSize];
        }

        @Override
        public void write(int value) throws IOException {
            if (count == buffer.length) {
                writeChunk();
            }
            buffer[count++] = (byte) value;
        }

        @Override
        public void write(byte[] data, int offset, int length) throws IOException {
            while (length > 0) {
                if (count == buffer.length) {
                    writeChunk();
                }
                int bytesToCopy = Math.min(length, buffer.length - count);
                System.arraycopy(data, offset, buffer, count, bytesToCopy);
                count += bytesToCopy;
                offset += bytesToCopy;
                length -= bytesToCopy;
            }
        }

        @Override
        public void flush() throws IOException {
            writeChunk();
        }

        private void finish() throws IOException {
            writeChunk();
            out.writeInt(0);
        }

        private void writeChunk() throws IOException {
            if (count > 0) {
                out.writeInt(count);
                out.write(buffer, 0, count);
                count = 0;
            }
        }
    }

    private static class ChunkedInputStream extends InputStream {
        private final DataInput in;
        private int remaining;
        private boolean finished;

        private ChunkedInputStream(DataInput in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            if (!prepareChunk()) {
                return -1;
            }
            remaining--;
            return in.readUnsignedByte();
        }

        @Override
        public int read(byte[] data, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (!prepareChunk()) {
                return -1;
            }
            int bytesToRead = Math.min(length, remaining);
            in.readFully(data, offset, bytesToRead);
            remaining -= bytesToRead;
            return bytesToRead;
        }

        private boolean prepareChunk() throws IOException {
            while (remaining == 0 && !finished) {
                remaining = in.readInt();
                if (remaining < 0) {
                    throw new IOException("Invalid JSON chunk length: " + remaining);
                }
                finished = remaining == 0;
            }
            return !finished;
        }
    }

    private static class BoundedInputStream extends InputStream {
        private final DataInput in;
        private int remaining;

        private BoundedInputStream(DataInput in, int length) {
            this.in = in;
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int value = in.readUnsignedByte();
            remaining--;
            return value;
        }

        @Override
        public int read(byte[] data, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (remaining == 0) {
                return -1;
            }
            int bytesToRead = Math.min(length, remaining);
            in.readFully(data, offset, bytesToRead);
            remaining -= bytesToRead;
            return bytesToRead;
        }
    }
}
