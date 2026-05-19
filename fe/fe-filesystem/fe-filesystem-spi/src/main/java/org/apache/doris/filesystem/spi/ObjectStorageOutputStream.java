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

package org.apache.doris.filesystem.spi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream that buffers writes in memory and uploads to object storage on close.
 *
 * <p>This implementation is intentionally simple and suitable for small metadata files
 * (manifests, snapshots, job info, etc.). Writes are rejected when the in-memory buffer
 * would exceed {@link #MAX_SINGLE_UPLOAD_BYTES} to prevent OOM on large payloads.
 * For large file writes (Hive data files, Backup archives), multipart upload must be
 * used instead.
 *
 * <p><strong>Empty-close semantics (#22):</strong> if {@link #close()} is called without
 * any preceding {@code write(...)} call, no object is uploaded. This avoids polluting the
 * bucket with phantom 0-byte placeholders when the caller opens an output stream and
 * aborts before writing. To explicitly create a zero-byte object, call
 * {@code write(new byte[0])} or {@code write(b, off, 0)} before {@link #close()}.
 */
public class ObjectStorageOutputStream extends OutputStream {

    private static final long MAX_SINGLE_UPLOAD_BYTES = 256L * 1024 * 1024; // 256 MB

    private final String remotePath;
    private final ObjStorage<?> objStorage;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private boolean closed = false;
    private boolean writeCalled = false;

    public ObjectStorageOutputStream(String remotePath, ObjStorage<?> objStorage) {
        this.remotePath = remotePath;
        this.objStorage = objStorage;
    }

    @Override
    public void write(int b) throws IOException {
        checkNotClosed();
        checkCapacity(1);
        writeCalled = true;
        buffer.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkNotClosed();
        checkCapacity(len);
        writeCalled = true;
        buffer.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (!writeCalled) {
            return;
        }
        byte[] data = buffer.toByteArray();
        objStorage.putObject(remotePath,
                RequestBody.of(new ByteArrayInputStream(data), data.length));
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("Stream already closed: " + remotePath);
        }
    }

    private void checkCapacity(int additionalBytes) throws IOException {
        if ((long) buffer.size() + additionalBytes > MAX_SINGLE_UPLOAD_BYTES) {
            throw new IOException(String.format(
                    "ObjectStorageOutputStream buffer limit exceeded (max %d MB) for path: %s. "
                    + "Use multipart upload for large files.",
                    MAX_SINGLE_UPLOAD_BYTES / (1024 * 1024), remotePath));
        }
    }
}
