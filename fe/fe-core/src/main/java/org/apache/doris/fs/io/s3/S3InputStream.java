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
import org.apache.doris.fs.io.ParsedPath;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.utils.IoUtils;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Objects;

/**
 * An implementation of DorisInputStream that reads data from Amazon S3.
 * This class provides functionality to read S3 objects with support for seeking,
 * buffering, and handling various S3-specific edge cases.
 */
public class S3InputStream extends DorisInputStream {
    // Default TCP buffer size for network operations (8KB)
    private static final long DEFAULT_TCP_BUFFER_SIZE = 1024 * 8;
    // Maximum number of bytes to skip when seeking forward
    private static final int MAX_SKIP_BYTES = 1024 * 1024;

    // The S3 path being read
    private final ParsedPath filePath;
    // AWS S3 client for making requests
    private final S3Client s3Client;
    // The base request object for S3 GET operations
    private final GetObjectRequest baseRequest;
    // Total length of the S3 object, -1 if unknown
    private final long objectLength;

    // Whether the stream has been closed
    private boolean isClosed;
    // Current input stream for reading data
    private ResponseInputStream<GetObjectResponse> inputStream;
    // Current position in the stream where data is being read
    private long currentPosition;
    // Position where next read operation should start
    private long nextReadPosition;

    /**
     * Constructs a new S3InputStream.
     *
     * @param path The Doris path representing the S3 object
     * @param client The S3 client to use for requests
     * @param request The base GET object request
     * @param length The total length of the object, or -1 if unknown
     */
    public S3InputStream(ParsedPath path, S3Client client, GetObjectRequest request, long length) {
        this.filePath = Objects.requireNonNull(path, "path is null");
        this.s3Client = Objects.requireNonNull(client, "client is null");
        this.baseRequest = Objects.requireNonNull(request, "request is null");
        this.objectLength = length;
    }

    // **************** Public Interface Methods ****************

    @Override
    public int available() throws IOException {
        validateStreamState();
        if ((inputStream != null) && (nextReadPosition == currentPosition)) {
            return getAvailableBytes();
        }
        return 0;
    }

    @Override
    public long getPosition() {
        return nextReadPosition;
    }

    @Override
    public void seek(long position) throws IOException {
        validateStreamState();
        if (position < 0) {
            throw new IOException("Cannot seek to negative position");
        }
        if ((objectLength != -1) && (position > objectLength)) {
            throw new IOException(String.format("Cannot seek to %d. File size is %d: %s",
                    position, objectLength, filePath));
        }
        nextReadPosition = position;
    }

    @Override
    public int read() throws IOException {
        validateStreamState();
        seekToPosition(false);

        return executeWithRetry(() -> {
            int value = readByte();
            if (value >= 0) {
                currentPosition++;
                nextReadPosition++;
            }
            return value;
        });
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        validateStreamState();
        seekToPosition(false);

        return executeWithRetry(() -> {
            int bytesRead = readBytes(buffer, offset, length);
            if (bytesRead > 0) {
                currentPosition += bytesRead;
                nextReadPosition += bytesRead;
            }
            return bytesRead;
        });
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
        validateStreamState();
        long actualSkip = IOUtils.clamp(bytesToSkip, 0,
                objectLength != -1 ? objectLength - nextReadPosition : Integer.MAX_VALUE);
        nextReadPosition += actualSkip;
        return actualSkip;
    }

    @Override
    public void skipNBytes(long n) throws IOException {
        validateStreamState();

        if (n <= 0) {
            return;
        }
        long targetPosition = nextReadPosition + n;
        if ((targetPosition < 0) || (objectLength != -1 && targetPosition > objectLength)) {
            throw new EOFException(String.format("Unable to skip %d bytes (position=%d, fileSize=%d): %s",
                    n, nextReadPosition, objectLength, filePath));
        }
        nextReadPosition = targetPosition;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        closeCurrentStream();
    }

    // **************** Private Helper Methods ****************

    /**
     * Executes a read operation with one retry attempt on failure.
     */
    private <T> T executeWithRetry(IOExceptionThrowingSupplier<T> operation) throws IOException {
        try {
            return operation.get();
        } catch (IOException e) {
            seekToPosition(true);
        }
        return operation.get();
    }

    /**
     * Functional interface for operations that may throw IOException.
     */
    private interface IOExceptionThrowingSupplier<T> {
        T get() throws IOException;
    }

    /**
     * Validates that the stream is not closed.
     */
    private void validateStreamState() throws IOException {
        if (isClosed) {
            throw new IOException("Stream is closed: " + filePath.toString());
        }
    }

    /**
     * Seeks the stream to the next read position.
     */
    private void seekToPosition(boolean forceReset) throws IOException {
        if (!forceReset && (inputStream != null) && (nextReadPosition == currentPosition)) {
            return;
        }

        if (!forceReset && (inputStream != null) && (nextReadPosition > currentPosition)) {
            long skipDistance = nextReadPosition - currentPosition;
            if (skipDistance <= Math.max(getAvailableBytes(), MAX_SKIP_BYTES)) {
                if (skipStreamBytes(skipDistance) == skipDistance) {
                    currentPosition = nextReadPosition;
                    return;
                }
            }
        }

        currentPosition = nextReadPosition;
        closeCurrentStream();

        try {
            GetObjectRequest rangeRequest = baseRequest;
            if (nextReadPosition != 0) {
                String range = String.format("bytes=%d-", nextReadPosition);
                rangeRequest = baseRequest.toBuilder().range(range).build();
            }
            inputStream = s3Client.getObject(rangeRequest);

            if (inputStream.response().contentLength() != null
                    && inputStream.response().contentLength() == 0) {
                inputStream = new ResponseInputStream<>(inputStream.response(), nullInputStream());
            }
            currentPosition = nextReadPosition;
        } catch (NoSuchKeyException e) {
            var ex = new FileNotFoundException(filePath.toString());
            ex.initCause(e);
            throw ex;
        } catch (SdkException e) {
            throw new IOException("Failed to open S3 file: " + filePath, e);
        }
    }

    /**
     * Closes the current input stream.
     */
    private void closeCurrentStream() {
        if (inputStream == null) {
            return;
        }

        try (var stream = inputStream) {
            if (objectLength != -1 && objectLength - currentPosition <= DEFAULT_TCP_BUFFER_SIZE) {
                IoUtils.drainInputStream(stream);
            } else {
                stream.abort();
                stream.release();
            }
        } catch (AbortedException | IOException e) {
            // Ignore exceptions during close
        } finally {
            inputStream = null;
        }
    }

    /**
     * Gets available bytes in the current stream.
     */
    private int getAvailableBytes() throws IOException {
        try {
            return inputStream.available();
        } catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    /**
     * Skips bytes in the current stream.
     */
    private long skipStreamBytes(long n) throws IOException {
        try {
            return inputStream.skip(n);
        } catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    /**
     * Reads a single byte from the current stream.
     */
    private int readByte() throws IOException {
        try {
            return inputStream.read();
        } catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    /**
     * Reads multiple bytes from the current stream.
     */
    private int readBytes(byte[] bytes, int offset, int length) throws IOException {
        try {
            return inputStream.read(bytes, offset, length);
        } catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }
}
