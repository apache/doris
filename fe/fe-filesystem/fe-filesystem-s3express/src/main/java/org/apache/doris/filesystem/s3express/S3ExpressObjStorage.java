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

package org.apache.doris.filesystem.s3express;

import org.apache.doris.filesystem.UploadPartResult;
import org.apache.doris.filesystem.s3.S3CredentialsProviderFactory;
import org.apache.doris.filesystem.s3.S3ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.ObjectStorageUri;
import org.apache.doris.filesystem.spi.RemoteObjects;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * S3 Express object storage specialization.
 *
 * <p>The AWS SDK selects S3 Express Session Auth from the bucket name, so client creation and
 * ordinary object operations are inherited. This class owns the S3 Express LIST constraints and
 * multipart checksum flow.
 */
public final class S3ExpressObjStorage extends S3ObjStorage {

    private final S3ExpressFileSystemProperties properties;

    public S3ExpressObjStorage(S3ExpressFileSystemProperties properties) {
        super(properties);
        this.properties = properties;
    }

    @Override
    protected S3Client buildClient() throws IOException {
        // Let the SDK resolve the directory bucket's zonal endpoint from its bucket name.
        return buildClient(null, properties.getRegion(), buildCredentialsProvider());
    }

    @Override
    protected AwsCredentialsProvider buildCredentialsProvider() {
        return S3CredentialsProviderFactory.createClientProvider(
                properties, this::buildStsClient, false);
    }

    @Override
    public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        String listSubPrefix = subPrefix;
        if (StringUtils.isNotEmpty(listSubPrefix) && !listSubPrefix.endsWith("/")) {
            int slash = listSubPrefix.lastIndexOf('/');
            listSubPrefix = slash < 0 ? "" : listSubPrefix.substring(0, slash + 1);
        }
        return super.listObjectsWithPrefix(prefix, listSubPrefix, continuationToken);
    }

    @Override
    public RemoteObjects listObjectsWithOptions(String remotePath, ObjectListOptions options)
            throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(
                remotePath, isUsePathStyle(), getSupportedSchemes());
        if (StringUtils.isNotEmpty(uri.key()) && !uri.key().endsWith("/")) {
            throw new IOException("S3 Express LIST prefix must end with '/': " + uri.key());
        }
        if (options != null && StringUtils.isNotEmpty(options.startAfter())) {
            throw new IOException("StartAfter is not supported for S3 Express");
        }
        if (options != null && StringUtils.isNotEmpty(options.delimiter())
                && !"/".equals(options.delimiter())) {
            throw new IOException("S3 Express delimiter must be '/'");
        }
        return super.listObjectsWithOptions(remotePath, options);
    }

    @Override
    public String initiateMultipartUpload(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(
                remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            CreateMultipartUploadResponse response = getClient().createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .bucket(uri.bucket())
                            .key(uri.key())
                            .checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
                            .build());
            return response.uploadId();
        } catch (SdkException e) {
            throw new IOException("initiateMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            org.apache.doris.filesystem.spi.RequestBody body) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(
                remotePath, isUsePathStyle(), getSupportedSchemes());
        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(uri.bucket())
                .key(uri.key())
                .uploadId(uploadId)
                .partNumber(partNum)
                .contentLength(body.contentLength())
                .checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
                .build();
        try (InputStream content = body.content()) {
            UploadPartResponse response = getClient().uploadPart(request,
                    software.amazon.awssdk.core.sync.RequestBody.fromInputStream(
                            content, body.contentLength()));
            if (StringUtils.isBlank(response.checksumCRC32C())) {
                throw new IOException("S3 Express UploadPart response is missing CRC32C, part="
                        + partNum);
            }
            return new UploadPartResult(partNum, response.eTag(), response.checksumCRC32C());
        } catch (SdkException e) {
            throw new IOException("uploadPart " + partNum + " failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(
                remotePath, isUsePathStyle(), getSupportedSchemes());
        List<CompletedPart> completedParts = new ArrayList<>(parts.size());
        List<UploadPartResult> sortedParts = new ArrayList<>(parts);
        sortedParts.sort(Comparator.comparingInt(UploadPartResult::partNumber));
        for (int i = 0; i < sortedParts.size(); i++) {
            UploadPartResult part = sortedParts.get(i);
            int expectedPartNumber = i + 1;
            if (part.partNumber() != expectedPartNumber) {
                throw new IOException("S3 Express multipart parts must be consecutive: expected "
                        + expectedPartNumber + ", actual " + part.partNumber());
            }
            if (StringUtils.isBlank(part.checksumCrc32c())) {
                throw new IOException("S3 Express multipart part " + part.partNumber()
                        + " is missing CRC32C");
            }
            completedParts.add(CompletedPart.builder()
                    .partNumber(part.partNumber())
                    .eTag(part.etag())
                    .checksumCRC32C(part.checksumCrc32c())
                    .build());
        }
        try {
            getClient().completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(uri.bucket())
                    .key(uri.key())
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder()
                            .parts(completedParts)
                            .build())
                    .build());
        } catch (SdkException e) {
            throw new IOException("completeMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }
}
