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

import org.apache.doris.common.io.IOUtils;
import org.apache.doris.fs.io.DorisInputStream;
import org.apache.doris.fs.io.DorisPath;

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import static software.amazon.awssdk.utils.IoUtils.drainInputStream;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;

public class S3InputStream extends DorisInputStream {
    private static final long DEFAULT_TCP_BUFFER_SIZE = 1024 * 8;
    private static final int MAX_SKIP_BYTES = 1024 * 1024;

    private final DorisPath path;
    private final S3Client client;
    private final GetObjectRequest request;
    private final long length;

    private boolean closed;
    private ResponseInputStream<GetObjectResponse> in;
    private long streamPosition;
    private long nextReadPosition;

    public S3InputStream(DorisPath path, S3Client client, GetObjectRequest request, long length) {
        this.path = requireNonNull(path, "path is null");
        this.client = requireNonNull(client, "client is null");
        this.request = requireNonNull(request, "request is null");
        this.length = length;
    }

    @Override
    public int available() throws IOException {
        checkClosed();
        if ((in != null) && (nextReadPosition == streamPosition)) {
            return getAvailable();
        }
        return 0;
    }

    @Override
    public long getPosition() {
        return nextReadPosition;
    }

    @Override
    public void seek(long position) throws IOException {
        checkClosed();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if ((length != -1) && (position > length)) {
            throw new IOException(String.format("Cannot seek to %d. File size is %d: %s", position, length, path));
        }

        nextReadPosition = position;
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        seekStream(false);

        return retryableRead(() -> {
            int value = doRead();
            if (value >= 0) {
                streamPosition++;
                nextReadPosition++;
            }
            return value;
        });
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        checkClosed();
        seekStream(false);

        return retryableRead(() -> {
            int n = doRead(bytes, offset, length);
            if (n > 0) {
                streamPosition += n;
                nextReadPosition += n;
            }
            return n;
        });
    }

    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        long skipSize = IOUtils.clamp(n, 0, length != -1 ? length - nextReadPosition : Integer.MAX_VALUE);
        nextReadPosition += skipSize;
        return skipSize;
    }

    @Override
    public void skipNBytes(long n) throws IOException {
        checkClosed();

        if (n <= 0) {
            return;
        }
        long position = nextReadPosition + n;
        if ((position < 0) || (length != -1 && position > length)) {
            throw new EOFException(String.format("Unable to skip %d bytes (position=%d, fileSize=%d): %s",
                    n, nextReadPosition, length, path));
        }
        nextReadPosition = position;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeStream();
    }

    private <T> T retryableRead(IOExceptionThrowingSupplier<T> supplier) throws IOException {
        try {
            return supplier.get();
        } catch (IOException e) {
            seekStream(true);
        }

        return supplier.get();
    }

    private interface IOExceptionThrowingSupplier<T> {
        T get() throws IOException;
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Input is closed: " + path.toString());
        }
    }

    private void seekStream(boolean forceStreamReset) throws IOException {
        if (!forceStreamReset && (in != null) && (nextReadPosition == streamPosition)) {
            // already at specified position
            return;
        }

        if (!forceStreamReset && (in != null) && (nextReadPosition > streamPosition)) {
            // seeking forwards
            long skip = nextReadPosition - streamPosition;
            if (skip <= max(getAvailable(), MAX_SKIP_BYTES)) {
                // already buffered or seek is small enough
                if (doSkip(skip) == skip) {
                    streamPosition = nextReadPosition;
                    return;
                }
            }
        }

        // close the stream and open at desired position
        streamPosition = nextReadPosition;
        closeStream();

        try {
            GetObjectRequest rangeRequest = request;
            if (nextReadPosition != 0) {
                String range = String.format("bytes=%d-", nextReadPosition);
                rangeRequest = request.toBuilder().range(range).build();
            }
            in = client.getObject(rangeRequest);
            // a workaround for https://github.com/aws/aws-sdk-java-v2/issues/3538
            if (in.response().contentLength() != null && in.response().contentLength() == 0) {
                in = new ResponseInputStream<>(in.response(), nullInputStream());
            }
            streamPosition = nextReadPosition;
        } catch (NoSuchKeyException e) {
            var ex = new FileNotFoundException(path.toString());
            ex.initCause(e);
            throw ex;
        } catch (SdkException e) {
            throw new IOException("Failed to open S3 file: " + path, e);
        }
    }

    private void closeStream() {
        if (in == null) {
            return;
        }

        try (var v = in) {
            // According to the documentation: Abort will close the underlying connection,
            // dropping all remaining data in the stream, and not leaving the connection
            // open to be used for future requests. This can be more expensive
            // than just reading remaining data.
            if (length != -1 && length - streamPosition <= DEFAULT_TCP_BUFFER_SIZE) {
                drainInputStream(in);
            } else {
                in.abort();
                in.release();
            }
        } catch (AbortedException | IOException e) {
        } finally {
            in = null;
        }
    }

    private int getAvailable() throws IOException {
        try {
            return in.available();
        } catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    private long doSkip(long n) throws IOException {
        try {
            return in.skip(n);
        } catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    private int doRead() throws IOException {
        try {
            return in.read();
        } catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    private int doRead(byte[] bytes, int offset, int length) throws IOException {
        try {
            return in.read(bytes, offset, length);
        } catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }
}
