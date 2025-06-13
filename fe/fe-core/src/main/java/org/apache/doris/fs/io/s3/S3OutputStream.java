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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static java.util.concurrent.CompletableFuture.supplyAsync;
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
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public class S3OutputStream extends OutputStream {
    private final List<CompletedPart> parts = new ArrayList<>();
    private final Executor uploadExecutor;
    private final S3Client client;
    private final S3URI s3URI;
    private final int partSize;

    private int currentPartNumber;
    private byte[] buffer = new byte[0];
    private int bufferSize;
    private int initialBufferSize = 64;

    private boolean closed;
    private boolean failed;
    private boolean multipartUploadStarted;
    private Future<CompletedPart> inProgressUploadFuture;

    // Mutated by background thread which does the multipart upload.
    // Read by both main thread and background thread.
    // Visibility is ensured by calling get() on inProgressUploadFuture.
    private Optional<String> uploadId = Optional.empty();

    public S3OutputStream(Executor uploadExecutor, S3Client client, S3URI s3URI) {
        this.uploadExecutor = Objects.requireNonNull(uploadExecutor, "uploadExecutor is null");
        this.client = Objects.requireNonNull(client, "client is null");
        this.s3URI = Objects.requireNonNull(s3URI, "location is null");
        this.partSize = 5 * 1024 * 1024; // 5 MB
    }

    @Override
    public void write(int b) throws IOException {
        checkClosed();
        ensureCapacity(1);
        buffer[bufferSize] = (byte) b;
        bufferSize++;
        flushBuffer(false);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        checkClosed();

        while (length > 0) {
            ensureCapacity(length);

            int copied = min(buffer.length - bufferSize, length);
            arraycopy(bytes, offset, buffer, bufferSize, copied);
            bufferSize += copied;

            flushBuffer(false);

            offset += copied;
            length -= copied;
        }
    }

    @Override
    public void flush() throws IOException {
        checkClosed();
        flushBuffer(false);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        if (failed) {
            try {
                abortUpload();
                return;
            } catch (SdkException e) {
                throw new IOException(e);
            }
        }

        try {
            flushBuffer(true);
            waitForPreviousUploadFinish();
        } catch (IOException | RuntimeException e) {
            abortUploadSuppressed(e);
            throw e;
        }

        try {
            uploadId.ifPresent(this::finishUpload);
        } catch (SdkException e) {
            abortUploadSuppressed(e);
            throw new IOException(e);
        }
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Output stream is closed: " + s3URI.toString());
        }
    }

    private void ensureCapacity(int extra) {
        int capacity = min(partSize, bufferSize + extra);
        if (buffer.length < capacity) {
            int target = max(buffer.length, initialBufferSize);
            if (target < capacity) {
                target += target / 2;
                target = (int) IOUtils.clamp(target, capacity, partSize);
            }
            buffer = Arrays.copyOf(buffer, target);
        }
    }

    private PutObjectRequest newPutObjectRequest() {
        return PutObjectRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .contentLength((long) bufferSize)
                .build();
    }

    private void flushBuffer(boolean finished)
            throws IOException {
        // skip multipart upload if there would only be one part
        if (finished && !multipartUploadStarted) {
            PutObjectRequest request = newPutObjectRequest();
            ByteBuffer bytes = ByteBuffer.wrap(buffer, 0, bufferSize);

            try {
                client.putObject(request, RequestBody.fromByteBuffer(bytes));
                return;
            } catch (S3Exception e) {
                failed = true;
                // when `location` already exists, the operation will fail with `412 Precondition Failed`
                if (e.statusCode() == HTTP_PRECON_FAILED) {
                    throw new FileAlreadyExistsException(s3URI.toString());
                }
                throw new IOException(String.format("Put failed for bucket [%s] key [%s]: %s",
                        s3URI.getBucket(), s3URI.getKey(), e.getMessage()), e);
            } catch (SdkException e) {
                failed = true;
                throw new IOException(String.format("Put failed for bucket [%s] key [%s]: %s",
                        s3URI.getBucket(), s3URI.getKey(), e.getMessage()), e);
            }
        }

        // the multipart upload API only allows the last part to be smaller than 5MB
        if ((bufferSize == partSize) || (finished && (bufferSize > 0))) {
            byte[] data = buffer;
            int length = bufferSize;

            if (finished) {
                this.buffer = null;
            } else {
                this.buffer = new byte[0];
                this.initialBufferSize = partSize;
                bufferSize = 0;
            }

            try {
                waitForPreviousUploadFinish();
            } catch (IOException e) {
                failed = true;
                abortUploadSuppressed(e);
                throw e;
            }
            multipartUploadStarted = true;
            inProgressUploadFuture = supplyAsync(() -> uploadPage(data, length), uploadExecutor);
        }
    }

    private void waitForPreviousUploadFinish()
            throws IOException {
        if (inProgressUploadFuture == null) {
            return;
        }

        try {
            inProgressUploadFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        } catch (ExecutionException e) {
            throw new IOException("Streaming upload failed", e);
        }
    }

    private CreateMultipartUploadRequest newCreateMultipartUploadRequest() {
        return CreateMultipartUploadRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .build();
    }

    private UploadPartRequest newUploadPartRequest(long length) {
        return UploadPartRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getBucket())
                .contentLength((long) length)
                .uploadId(uploadId.get())
                .partNumber(currentPartNumber)
                .build();
    }

    private CompleteMultipartUploadRequest newCompleteMultipartUploadRequest(String uploadId) {
        return CompleteMultipartUploadRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .uploadId(uploadId)
                .multipartUpload(x -> x.parts(parts))
                .build();
    }

    private AbortMultipartUploadRequest newAbortMultipartUploadRequest(String uploadId) {
        return AbortMultipartUploadRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .uploadId(uploadId)
                .build();
    }

    private CompletedPart uploadPage(byte[] data, int length) {
        if (uploadId.isEmpty()) {
            CreateMultipartUploadRequest request = newCreateMultipartUploadRequest();
            uploadId = Optional.of(client.createMultipartUpload(request).uploadId());
        }

        currentPartNumber++;
        UploadPartRequest request = newUploadPartRequest(length);

        ByteBuffer bytes = ByteBuffer.wrap(data, 0, length);
        UploadPartResponse response = client.uploadPart(request, RequestBody.fromByteBuffer(bytes));
        CompletedPart part = CompletedPart.builder()
                .partNumber(currentPartNumber)
                .eTag(response.eTag())
                .build();

        parts.add(part);
        return part;
    }

    private void finishUpload(String uploadId) {
        CompleteMultipartUploadRequest request = newCompleteMultipartUploadRequest(uploadId);
        client.completeMultipartUpload(request);
    }

    private void abortUpload() {
        uploadId.map(id -> newAbortMultipartUploadRequest(id))
                .ifPresent(client::abortMultipartUpload);
    }

    private void abortUploadSuppressed(Throwable throwable) {
        try {
            abortUpload();
        } catch (Throwable t) {
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }
}
