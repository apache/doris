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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.spi.RequestBody;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Unit tests for {@link S3OutputStream}.
 *
 * <p>Covers the M3 fix: size-guard and idempotent close behaviour.
 *
 * <p>{@link S3OutputStream} is package-private; this test lives in the same package.
 * Because the class is small and self-contained, all tests are purely in-memory —
 * no AWS SDK calls are made.
 */
class S3OutputStreamTest {

    // ------------------------------------------------------------------
    // M3-A: write() size guard
    // ------------------------------------------------------------------

    /**
     * M3: Writing exactly MAX_SINGLE_UPLOAD_BYTES bytes must succeed (boundary — at the limit).
     *
     * <p>We fake the buffer's internal {@code count} field to avoid allocating 256 MB in the test.
     * Then write one more byte which fills the last slot — this must succeed.
     */
    @Test
    void testWriteAtLimitSucceeds() throws Exception {
        // null objStorage is safe because close() is never called in this test.
        S3OutputStream stream = new S3OutputStream("s3://bucket/key", null);

        // Fill the buffer to MAX - 1 bytes via reflection so we don't allocate 256 MB.
        long max = getMaxSingleUploadBytes();
        setBufferCount(stream, (int) (max - 1));

        // Writing 1 more byte should bring us exactly to MAX — allowed.
        Assertions.assertDoesNotThrow(() -> stream.write(new byte[1], 0, 1),
                "Writing exactly at the limit must not throw");
    }

    /**
     * M3: Writing one byte beyond MAX_SINGLE_UPLOAD_BYTES must throw {@link IOException} with
     * a message containing "buffer limit exceeded".
     */
    @Test
    void testWriteExceedingLimitThrowsIOException() throws Exception {
        S3OutputStream stream = new S3OutputStream("s3://bucket/key", null);

        // Pre-fill the buffer to MAX bytes.
        long max = getMaxSingleUploadBytes();
        setBufferCount(stream, (int) max);

        // Writing one more byte must exceed the limit.
        IOException ex = Assertions.assertThrows(IOException.class,
                () -> stream.write(new byte[1], 0, 1),
                "Writing past the limit must throw IOException");
        Assertions.assertTrue(ex.getMessage().contains("buffer limit exceeded"),
                "Exception message must contain 'buffer limit exceeded', was: " + ex.getMessage());
    }

    /**
     * M3: single-byte {@code write(int)} must also respect the size guard.
     */
    @Test
    void testSingleByteWriteExceedingLimitThrowsIOException() throws Exception {
        S3OutputStream stream = new S3OutputStream("s3://bucket/key", null);

        long max = getMaxSingleUploadBytes();
        setBufferCount(stream, (int) max);

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> stream.write(0xFF),
                "Single-byte write past the limit must throw IOException");
        Assertions.assertTrue(ex.getMessage().contains("buffer limit exceeded"),
                "Exception message must contain 'buffer limit exceeded'");
    }

    // ------------------------------------------------------------------
    // M3-B: write-after-close guard
    // ------------------------------------------------------------------

    /**
     * M3: Calling {@code write()} after {@code close()} must throw {@link IOException} with
     * a message containing "already closed".
     */
    @Test
    void testWriteAfterCloseThrowsIOException() throws Exception {
        // Provide a no-op ObjStorage so close() can complete successfully.
        S3OutputStream stream = new S3OutputStream("s3://bucket/key", new CapturingStorage());
        stream.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> stream.write(new byte[1], 0, 1),
                "write() after close() must throw IOException");
        Assertions.assertTrue(ex.getMessage().contains("already closed"),
                "Exception message must contain 'already closed', was: " + ex.getMessage());
    }

    // ------------------------------------------------------------------
    // M3-C: idempotent close()
    // ------------------------------------------------------------------

    /**
     * M3: Calling {@code close()} twice must not trigger a second {@code putObject()} call —
     * the second invocation must be a no-op.
     */
    @Test
    void testCloseIsIdempotent() throws IOException {
        AtomicBoolean putObjectCalled = new AtomicBoolean(false);

        CapturingStorage storage = new CapturingStorage() {
            @Override
            public void putObject(String remotePath, RequestBody body) throws IOException {
                if (putObjectCalled.getAndSet(true)) {
                    throw new AssertionError("putObject() called more than once");
                }
            }
        };

        S3OutputStream stream = new S3OutputStream("s3://bucket/key", storage);
        stream.close(); // first close — must call putObject once
        stream.close(); // second close — must be a no-op (no second putObject call)

        Assertions.assertTrue(putObjectCalled.get(), "putObject() must have been called on first close()");
    }

    /**
     * M3: {@code close()} must pass the correct data to {@code putObject()}.
     */
    @Test
    void testCloseUploadsBufferedData() throws IOException {
        AtomicReference<byte[]> captured = new AtomicReference<>();

        CapturingStorage storage = new CapturingStorage() {
            @Override
            public void putObject(String remotePath, RequestBody body) throws IOException {
                try {
                    captured.set(body.content().readAllBytes());
                } catch (IOException e) {
                    throw new IOException("Failed to read captured body", e);
                }
            }
        };

        byte[] payload = "hello-s3".getBytes();
        S3OutputStream stream = new S3OutputStream("s3://bucket/key", storage);
        stream.write(payload, 0, payload.length);
        stream.close();

        Assertions.assertNotNull(captured.get(), "putObject() must have been called");
        Assertions.assertEquals(new String(payload), new String(captured.get()),
                "Buffered data must match what was written");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Reads the {@code MAX_SINGLE_UPLOAD_BYTES} static field via reflection.
     */
    private static long getMaxSingleUploadBytes() throws Exception {
        Field f = S3OutputStream.class.getDeclaredField("MAX_SINGLE_UPLOAD_BYTES");
        f.setAccessible(true);
        return (long) f.get(null);
    }

    /**
     * Sets the internal {@code count} of the {@link ByteArrayOutputStream} buffer inside an
     * {@link S3OutputStream} to {@code count} bytes, faking that data has already been written.
     * This avoids allocating hundreds of MB in unit tests.
     */
    private static void setBufferCount(S3OutputStream stream, int count) throws Exception {
        // Get the S3OutputStream.buffer (ByteArrayOutputStream) field.
        Field bufferField = S3OutputStream.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        ByteArrayOutputStream buf = (ByteArrayOutputStream) bufferField.get(stream);

        // Manipulate ByteArrayOutputStream.count (the number of valid bytes) directly.
        Field countField = ByteArrayOutputStream.class.getDeclaredField("count");
        countField.setAccessible(true);
        countField.set(buf, count);
    }

    /**
     * Subclass (via the {@link ObjStorage} interface route) that records {@code putObject()} calls.
     * Because {@link S3ObjStorage} has a concrete constructor (requires {@code Map<String,String>})
     * we subclass {@link S3ObjStorage} with a minimal property map that never builds a client.
     */
    private static class CapturingStorage extends S3ObjStorage {

        CapturingStorage() {
            super(Map.of()); // empty properties — no AWS client is built
        }

        @Override
        public void putObject(String remotePath, RequestBody body) throws IOException {
            // default no-op; subclass overrides as needed
        }
    }
}
