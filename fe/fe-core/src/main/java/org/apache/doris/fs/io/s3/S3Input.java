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
import org.apache.doris.fs.io.ParsedPath;

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

/**
 * An implementation of DorisInput that reads data from Amazon S3 storage.
 * This class provides functionality to read bytes from S3 objects using range requests,
 * which allows for efficient partial object reads without downloading the entire object.
 */
final class S3Input implements DorisInput {
    // The path to the S3 object being read
    private final ParsedPath path;

    // The S3 client used for making requests to AWS S3
    private final S3Client client;

    // The base request object for S3 GET operations
    private final GetObjectRequest request;

    // Flag indicating whether this input stream has been closed
    private boolean closed;

    /**
     * Constructs a new S3Input instance with the specified parameters.
     *
     * @param path The path to the S3 object
     * @param client The S3 client to use for requests
     * @param request The base GET object request
     */
    public S3Input(ParsedPath path, S3Client client, GetObjectRequest request) {
        this.path = Objects.requireNonNull(path, "path is null");
        this.client = Objects.requireNonNull(client, "client is null");
        this.request = Objects.requireNonNull(request, "request is null");
    }

    /**
     * Reads the requested number of bytes from the S3 object at the specified position.
     * Uses S3 range requests to efficiently read only the required bytes.
     *
     * @param position The position in the object to start reading from
     * @param buffer The buffer to read the data into
     * @param offset The offset in the buffer to start writing at
     * @param length The number of bytes to read
     * @throws IOException If an I/O error occurs during reading
     */
    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        validateReadParameters(position, buffer, offset, length);

        if (length == 0) {
            return;
        }

        String rangeHeader = formatRangeHeader(position, length);
        GetObjectRequest rangeRequest = request.toBuilder().range(rangeHeader).build();

        int bytesRead = executeRead(buffer, offset, length, rangeRequest);
        if (bytesRead < length) {
            throw new EOFException(String.format("Read %d of %d requested bytes: %s",
                    bytesRead, length, path));
        }
    }

    /**
     * Closes this input stream and releases any system resources associated with it.
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * Validates the parameters for a read operation.
     *
     * @param position The read position
     * @param buffer The buffer to read into
     * @param offset The buffer offset
     * @param length The number of bytes to read
     * @throws IOException If any parameters are invalid
     */
    private void validateReadParameters(long position, byte[] buffer, int offset, int length)
            throws IOException {
        checkClosed();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        Objects.checkFromIndexSize(offset, length, buffer.length);
    }

    /**
     * Formats the range header for S3 range requests.
     *
     * @param position Start position
     * @param length Number of bytes to read
     * @return Formatted range header string
     */
    private String formatRangeHeader(long position, int length) {
        return String.format("bytes=%d-%d", position, (position + length) - 1);
    }

    /**
     * Executes the actual read operation against S3.
     *
     * @param buffer The buffer to read into
     * @param offset The buffer offset
     * @param length The number of bytes to read
     * @param rangeRequest The S3 range request
     * @return The number of bytes read
     * @throws IOException If an I/O error occurs
     */
    private int executeRead(byte[] buffer, int offset, int length, GetObjectRequest rangeRequest)
            throws IOException {
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

    /**
     * Checks if this input stream has been closed.
     *
     * @throws IOException If the stream is closed
     */
    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Input is closed: " + path.toString());
        }
    }
}
