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
import org.apache.doris.common.util.S3URI;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * An OutputStream implementation that writes data to Amazon S3.
 * This class supports both single-part and multi-part uploads to S3.
 * For files larger than the part size (5MB), it automatically switches to multi-part upload.
 * The implementation is thread-safe and handles upload failures gracefully.
 */
public class S3OutputStream extends OutputStream {
    // List to track completed parts for multi-part upload
    private final List<CompletedPart> completedParts = new ArrayList<>();

    // Executor for handling asynchronous upload operations
    private final Executor uploadExecutor;

    // S3 client for interacting with Amazon S3
    private final S3Client s3Client;

    // URI containing bucket and key information for the S3 object
    private final S3URI s3Uri;

    // Size of each part for multi-part upload (5MB)
    private final int partSize;

    // Current part number for multi-part upload
    private int currentPartNum;

    // Buffer for storing data before upload
    private byte[] dataBuffer = new byte[0];

    // Current size of data in buffer
    private int bufferLength;

    // Initial size of the buffer
    private int initialBufferLength = 64;

    // Flag indicating if the stream is closed
    private boolean isClosed;

    // Flag indicating if an upload operation has failed
    private boolean hasFailed;

    // Flag indicating if multi-part upload has been initiated
    private boolean isMultipartStarted;

    // Future representing the current upload operation
    private Future<CompletedPart> currentUploadFuture;

    // Upload ID for multi-part upload, shared between threads
    private Optional<String> uploadId = Optional.empty();

    /**
     * Constructs a new S3OutputStream.
     *
     * @param uploadExecutor executor for handling asynchronous upload operations
     * @param s3Client client for interacting with Amazon S3
     * @param s3Uri URI containing bucket and key information
     */
    public S3OutputStream(Executor uploadExecutor, S3Client s3Client, S3URI s3Uri) {
        this.uploadExecutor = Objects.requireNonNull(uploadExecutor, "uploadExecutor cannot be null");
        this.s3Client = Objects.requireNonNull(s3Client, "s3Client cannot be null");
        this.s3Uri = Objects.requireNonNull(s3Uri, "s3Uri cannot be null");
        this.partSize = 5 * 1024 * 1024; // 5 MB
    }

    // ===============================
    // OutputStream Implementation
    // ===============================

    /**
     * Writes a single byte to the output stream.
     *
     * @param byteValue the byte to write
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(int byteValue) throws IOException {
        validateStreamOpen();
        ensureBufferCapacity(1);
        dataBuffer[bufferLength] = (byte) byteValue;
        bufferLength++;
        flushBufferIfNeeded(false);
    }

    /**
     * Writes a portion of byte array to the output stream.
     *
     * @param data the data to write
     * @param offset starting offset in the data
     * @param length number of bytes to write
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(byte[] data, int offset, int length) throws IOException {
        validateStreamOpen();

        while (length > 0) {
            ensureBufferCapacity(length);

            int bytesToCopy = Math.min(dataBuffer.length - bufferLength, length);
            System.arraycopy(data, offset, dataBuffer, bufferLength, bytesToCopy);
            bufferLength += bytesToCopy;

            flushBufferIfNeeded(false);

            offset += bytesToCopy;
            length -= bytesToCopy;
        }
    }

    /**
     * Flushes the stream, ensuring any buffered data is written.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void flush() throws IOException {
        validateStreamOpen();
        flushBufferIfNeeded(false);
    }

    /**
     * Closes the stream, uploading any remaining data.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;

        if (hasFailed) {
            try {
                abortMultipartUpload();
                return;
            } catch (SdkException e) {
                throw new IOException(e);
            }
        }

        try {
            flushBufferIfNeeded(true);
            waitForUploadCompletion();
        } catch (IOException | RuntimeException e) {
            abortUploadAndAddSuppressed(e);
            throw e;
        }

        try {
            uploadId.ifPresent(this::completeMultipartUpload);
        } catch (SdkException e) {
            abortUploadAndAddSuppressed(e);
            throw new IOException(e);
        }
    }

    // ===============================
    // Buffer Management
    // ===============================

    /**
     * Ensures the buffer has enough capacity for additional data.
     *
     * @param additionalBytes number of additional bytes needed
     */
    private void ensureBufferCapacity(int additionalBytes) {
        int requiredCapacity = Math.min(partSize, bufferLength + additionalBytes);
        if (dataBuffer.length < requiredCapacity) {
            int newCapacity = Math.max(dataBuffer.length, initialBufferLength);
            if (newCapacity < requiredCapacity) {
                newCapacity += newCapacity / 2;
                newCapacity = (int) IOUtils.clamp(newCapacity, requiredCapacity, partSize);
            }
            dataBuffer = Arrays.copyOf(dataBuffer, newCapacity);
        }
    }

    /**
     * Flushes the buffer if it's full or if final flush is requested.
     *
     * @param isFinalFlush whether this is the final flush
     * @throws IOException if an I/O error occurs
     */
    private void flushBufferIfNeeded(boolean isFinalFlush) throws IOException {
        if (isFinalFlush && !isMultipartStarted) {
            uploadSinglePart();
            return;
        }

        if ((bufferLength == partSize) || (isFinalFlush && bufferLength > 0)) {
            uploadPart();
        }
    }

    // ===============================
    // Upload Operations
    // ===============================

    /**
     * Uploads the current buffer as a single part.
     *
     * @throws IOException if an I/O error occurs
     */
    private void uploadSinglePart() throws IOException {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(s3Uri.getBucket())
                .key(s3Uri.getKey())
                .contentLength((long) bufferLength)
                .build();

        ByteBuffer byteBuffer = ByteBuffer.wrap(dataBuffer, 0, bufferLength);

        try {
            s3Client.putObject(request, RequestBody.fromByteBuffer(byteBuffer));
        } catch (S3Exception e) {
            hasFailed = true;
            if (e.statusCode() == HttpURLConnection.HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(s3Uri.toString());
            }
            throw new IOException(String.format("Failed to upload to bucket [%s] key [%s]: %s",
                    s3Uri.getBucket(), s3Uri.getKey(), e.getMessage()), e);
        } catch (SdkException e) {
            hasFailed = true;
            throw new IOException(String.format("Failed to upload to bucket [%s] key [%s]: %s",
                    s3Uri.getBucket(), s3Uri.getKey(), e.getMessage()), e);
        }
    }

    /**
     * Uploads the current buffer as a part in multi-part upload.
     *
     * @throws IOException if an I/O error occurs
     */
    private void uploadPart() throws IOException {
        byte[] partData = dataBuffer;
        int partLength = bufferLength;

        if (currentUploadFuture != null) {
            waitForUploadCompletion();
        }

        dataBuffer = new byte[0];
        initialBufferLength = partSize;
        bufferLength = 0;
        isMultipartStarted = true;

        currentUploadFuture = CompletableFuture.supplyAsync(() -> uploadPartToS3(partData, partLength), uploadExecutor);
    }

    /**
     * Uploads a part to S3 as part of multi-part upload.
     *
     * @param data the data to upload
     * @param length the length of data
     * @return the completed part information
     */
    private CompletedPart uploadPartToS3(byte[] data, int length) {
        if (uploadId.isEmpty()) {
            CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                    .bucket(s3Uri.getBucket())
                    .key(s3Uri.getKey())
                    .build();
            uploadId = Optional.of(s3Client.createMultipartUpload(request).uploadId());
        }

        currentPartNum++;
        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(s3Uri.getBucket())
                .key(s3Uri.getKey())
                .uploadId(uploadId.get())
                .partNumber(currentPartNum)
                .contentLength((long) length)
                .build();

        ByteBuffer byteBuffer = ByteBuffer.wrap(data, 0, length);
        UploadPartResponse response = s3Client.uploadPart(request, RequestBody.fromByteBuffer(byteBuffer));

        CompletedPart completedPart = CompletedPart.builder()
                .partNumber(currentPartNum)
                .eTag(response.eTag())
                .build();

        completedParts.add(completedPart);
        return completedPart;
    }

    // ===============================
    // Helper Methods
    // ===============================

    /**
     * Validates that the stream is not closed.
     *
     * @throws IOException if the stream is closed
     */
    private void validateStreamOpen() throws IOException {
        if (isClosed) {
            throw new IOException("Stream is closed: " + s3Uri.toString());
        }
    }

    /**
     * Waits for the current upload operation to complete.
     *
     * @throws IOException if the upload fails
     */
    private void waitForUploadCompletion() throws IOException {
        if (currentUploadFuture == null) {
            return;
        }

        try {
            currentUploadFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        } catch (ExecutionException e) {
            throw new IOException("Upload failed", e);
        }
    }

    /**
     * Completes the multi-part upload.
     *
     * @param uploadId the ID of the multi-part upload
     */
    private void completeMultipartUpload(String uploadId) {
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                .bucket(s3Uri.getBucket())
                .key(s3Uri.getKey())
                .uploadId(uploadId)
                .multipartUpload(builder -> builder.parts(completedParts))
                .build();
        s3Client.completeMultipartUpload(request);
    }

    /**
     * Aborts the multi-part upload.
     */
    private void abortMultipartUpload() {
        uploadId.map(id -> AbortMultipartUploadRequest.builder()
                        .bucket(s3Uri.getBucket())
                        .key(s3Uri.getKey())
                        .uploadId(id)
                        .build())
                .ifPresent(s3Client::abortMultipartUpload);
    }

    /**
     * Aborts the upload and adds any suppressed exceptions.
     *
     * @param throwable the exception to add suppressed exceptions to
     */
    private void abortUploadAndAddSuppressed(Throwable throwable) {
        try {
            abortMultipartUpload();
        } catch (Throwable t) {
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }
}
