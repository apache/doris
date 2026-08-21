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
import org.apache.doris.filesystem.s3.S3ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.RequestBody;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

class S3ExpressObjStorageTest {

    @Test
    void buildClient_leavesEndpointResolutionToSdk() throws IOException {
        S3ExpressFileSystemProperties properties = properties();
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties);

        try (S3Client client = storage.getClient()) {
            Assertions.assertTrue(client.serviceClientConfiguration().endpointOverride().isEmpty());
        }
    }

    @Test
    void listObjectsWithPrefix_broadensGlobToContainingDirectory() throws Exception {
        S3Client mockClient = Mockito.mock(S3Client.class);
        Mockito.when(mockClient.listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().isTruncated(false).build());
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties());
        setClient(storage, mockClient);

        storage.listObjectsWithPrefix("stage/root", "partition/file*.parquet", "token");

        ArgumentCaptor<ListObjectsV2Request> request =
                ArgumentCaptor.forClass(ListObjectsV2Request.class);
        Mockito.verify(mockClient).listObjectsV2(request.capture());
        Assertions.assertEquals("stage/root/partition/", request.getValue().prefix());
        Assertions.assertEquals("token", request.getValue().continuationToken());
    }

    @Test
    void listObjectsWithOptions_rejectsUnsupportedDirectoryBucketOptions() {
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties());

        Assertions.assertThrows(IOException.class,
                () -> storage.listObjectsWithOptions("s3://bucket/not-a-directory", null));
        Assertions.assertThrows(IOException.class,
                () -> storage.listObjectsWithOptions("s3://bucket/directory/",
                        ObjectListOptions.builder().startAfter("previous-key").build()));
        Assertions.assertThrows(IOException.class,
                () -> storage.listObjectsWithOptions("s3://bucket/directory/",
                        ObjectListOptions.builder().delimiter(":").build()));
    }

    @Test
    void initiateMultipartUpload_requestsCrc32c() throws Exception {
        S3Client mockClient = Mockito.mock(S3Client.class);
        Mockito.when(mockClient.createMultipartUpload(
                        ArgumentMatchers.any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder()
                        .uploadId("upload-123")
                        .build());
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties());
        setClient(storage, mockClient);

        String uploadId = storage.initiateMultipartUpload("s3://bucket/path/to/object");

        ArgumentCaptor<CreateMultipartUploadRequest> request =
                ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        Mockito.verify(mockClient).createMultipartUpload(request.capture());
        Assertions.assertEquals("upload-123", uploadId);
        Assertions.assertEquals("bucket", request.getValue().bucket());
        Assertions.assertEquals("path/to/object", request.getValue().key());
        Assertions.assertEquals(ChecksumAlgorithm.CRC32_C,
                request.getValue().checksumAlgorithm());
    }

    @Test
    void uploadPart_requestsAndPropagatesCrc32c() throws Exception {
        S3Client mockClient = Mockito.mock(S3Client.class);
        Mockito.when(mockClient.uploadPart(
                        ArgumentMatchers.any(UploadPartRequest.class),
                        ArgumentMatchers.any(software.amazon.awssdk.core.sync.RequestBody.class)))
                .thenReturn(UploadPartResponse.builder()
                        .eTag("etag-2")
                        .checksumCRC32C("crc32c-2")
                        .build());
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties());
        setClient(storage, mockClient);
        RequestBody body = RequestBody.of(
                new ByteArrayInputStream(new byte[] {1, 2, 3}), 3);

        UploadPartResult result = storage.uploadPart(
                "s3://bucket/path/to/object", "upload-123", 2, body);

        ArgumentCaptor<UploadPartRequest> request =
                ArgumentCaptor.forClass(UploadPartRequest.class);
        Mockito.verify(mockClient).uploadPart(request.capture(),
                ArgumentMatchers.any(software.amazon.awssdk.core.sync.RequestBody.class));
        Assertions.assertEquals("bucket", request.getValue().bucket());
        Assertions.assertEquals("path/to/object", request.getValue().key());
        Assertions.assertEquals("upload-123", request.getValue().uploadId());
        Assertions.assertEquals(2, request.getValue().partNumber());
        Assertions.assertEquals(3, request.getValue().contentLength());
        Assertions.assertEquals(ChecksumAlgorithm.CRC32_C,
                request.getValue().checksumAlgorithm());
        Assertions.assertEquals(2, result.partNumber());
        Assertions.assertEquals("etag-2", result.etag());
        Assertions.assertEquals("crc32c-2", result.checksumCrc32c());
    }

    @Test
    void uploadPart_rejectsMissingResponseChecksum() throws Exception {
        S3Client mockClient = Mockito.mock(S3Client.class);
        Mockito.when(mockClient.uploadPart(
                        ArgumentMatchers.any(UploadPartRequest.class),
                        ArgumentMatchers.any(software.amazon.awssdk.core.sync.RequestBody.class)))
                .thenReturn(UploadPartResponse.builder()
                        .eTag("etag-1")
                        .build());
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties());
        setClient(storage, mockClient);
        RequestBody body = RequestBody.of(new ByteArrayInputStream(new byte[] {1}), 1);

        IOException exception = Assertions.assertThrows(IOException.class,
                () -> storage.uploadPart(
                        "s3://bucket/path/to/object", "upload-123", 1, body));

        Assertions.assertTrue(exception.getMessage().contains("missing CRC32C"));
    }

    @Test
    void completeMultipartUpload_sendsPerPartCrc32c() throws Exception {
        S3Client mockClient = Mockito.mock(S3Client.class);
        Mockito.when(mockClient.completeMultipartUpload(
                        ArgumentMatchers.any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompleteMultipartUploadResponse.builder().build());
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties());
        setClient(storage, mockClient);

        storage.completeMultipartUpload("s3://bucket/path/to/object", "upload-123", List.of(
                new UploadPartResult(2, "etag-2", "crc32c-2"),
                new UploadPartResult(1, "etag-1", "crc32c-1")));

        ArgumentCaptor<CompleteMultipartUploadRequest> request =
                ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
        Mockito.verify(mockClient).completeMultipartUpload(request.capture());
        Assertions.assertEquals("bucket", request.getValue().bucket());
        Assertions.assertEquals("path/to/object", request.getValue().key());
        Assertions.assertEquals("upload-123", request.getValue().uploadId());
        List<CompletedPart> completedParts = request.getValue().multipartUpload().parts();
        Assertions.assertEquals(2, completedParts.size());
        Assertions.assertEquals(1, completedParts.get(0).partNumber());
        Assertions.assertEquals("etag-1", completedParts.get(0).eTag());
        Assertions.assertEquals("crc32c-1", completedParts.get(0).checksumCRC32C());
        Assertions.assertEquals(2, completedParts.get(1).partNumber());
        Assertions.assertEquals("etag-2", completedParts.get(1).eTag());
        Assertions.assertEquals("crc32c-2", completedParts.get(1).checksumCRC32C());
    }

    @Test
    void completeMultipartUpload_rejectsMissingPartChecksum() throws Exception {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties());
        setClient(storage, mockClient);

        IOException exception = Assertions.assertThrows(IOException.class,
                () -> storage.completeMultipartUpload(
                        "s3://bucket/path/to/object", "upload-123",
                        List.of(new UploadPartResult(1, "etag-1"))));

        Assertions.assertTrue(exception.getMessage().contains("missing CRC32C"));
        Mockito.verifyNoInteractions(mockClient);
    }

    @Test
    void completeMultipartUpload_rejectsNonconsecutiveParts() throws Exception {
        S3Client mockClient = Mockito.mock(S3Client.class);
        S3ExpressObjStorage storage = new S3ExpressObjStorage(properties());
        setClient(storage, mockClient);

        IOException exception = Assertions.assertThrows(IOException.class,
                () -> storage.completeMultipartUpload(
                        "s3://bucket/path/to/object", "upload-123", List.of(
                                new UploadPartResult(1, "etag-1", "crc32c-1"),
                                new UploadPartResult(3, "etag-3", "crc32c-3"))));

        Assertions.assertTrue(exception.getMessage().contains("expected 2, actual 3"));
        Mockito.verifyNoInteractions(mockClient);
    }

    private static S3ExpressFileSystemProperties properties() {
        return S3ExpressFileSystemProperties.of(Map.of(
                "s3.endpoint", "https://s3express-usw2-az1.us-west-2.amazonaws.com",
                "s3.region", "us-west-2",
                "s3.access_key", "ak",
                "s3.secret_key", "sk",
                "s3.bucket", "bucket"));
    }

    private static void setClient(S3ExpressObjStorage storage, S3Client client)
            throws ReflectiveOperationException {
        Field clientField = S3ObjStorage.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(storage, client);
    }
}
