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

package org.apache.doris.fs.io.s3;

import org.apache.doris.fs.io.DorisInput;
import org.apache.doris.fs.io.DorisPath;

import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Objects;

final class S3Input implements DorisInput {
    private final DorisPath path;
    private final S3Client client;
    private final GetObjectRequest request;
    private boolean closed;

    public S3Input(DorisPath path, S3Client client, GetObjectRequest request) {
        this.path = Objects.requireNonNull(path, "path is null");
        this.client = Objects.requireNonNull(client, "client is null");
        this.request = Objects.requireNonNull(request, "request is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        checkClosed();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        Objects.checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return;
        }

        String range = String.format("bytes=%d-%d", position, (position + length) - 1);
        GetObjectRequest rangeRequest = request.toBuilder().range(range).build();
        int n = read(buffer, offset, length, rangeRequest);
        if (n < length) {
            throw new EOFException(String.format("Read %d of %d requested bytes: %s", n, length, path));
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Input is closed: " + path.toString());
        }
    }

    private int read(byte[] buffer, int offset, int length, GetObjectRequest rangeRequest) throws IOException {
        try {
            return client.getObject(rangeRequest, (response, inputStream) -> {
                try {
                    return inputStream.readNBytes(buffer, offset, length);
                } catch (AbortedException e) {
                    throw new InterruptedIOException();
                } catch (IOException e) {
                    throw RetryableException.create("Error reading getObject response", e);
                }
            });
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException(path.toString());
        } catch (SdkException e) {
            throw new IOException("Failed to open S3 file: " + path, e);
        }
    }
}
