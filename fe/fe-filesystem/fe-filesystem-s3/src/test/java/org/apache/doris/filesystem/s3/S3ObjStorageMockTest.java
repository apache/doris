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

import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.UploadPartResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.sts.StsClient;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Full unit tests for {@link S3ObjStorage} using a testable subclass that overrides
 * {@link S3ObjStorage#buildClient()} to inject a mock S3Client.
 */
class S3ObjStorageMockTest {

    private S3Client mockS3;
    private S3ObjStorage storage;

    @BeforeEach
    void setUp() {
        mockS3 = Mockito.mock(S3Client.class);
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.amazonaws.com");
        props.put("AWS_REGION", "us-east-1");
        props.put("AWS_ACCESS_KEY", "testAK");
        props.put("AWS_SECRET_KEY", "testSK");
        props.put("AWS_BUCKET", "my-bucket");
        storage = new TestableS3ObjStorage(props, mockS3);
    }

    // ------------------------------------------------------------------
    // getClient()
    // ------------------------------------------------------------------

    @Test
    void getClient_returnsInjectedMockClient() throws IOException {
        Assertions.assertEquals(mockS3, storage.getClient());
    }

    // ------------------------------------------------------------------
    // listObjects()
    // ------------------------------------------------------------------

    @Test
    void listObjects_returnsRemoteObjectsFromS3Response() throws IOException {
        Instant now = Instant.now();
        S3Object obj = S3Object.builder()
                .key("data/file1.csv")
                .eTag("abc123")
                .size(1024L)
                .lastModified(now)
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(obj)
                .isTruncated(false)
                .build();
        Mockito.when(mockS3.listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request.class))).thenReturn(response);

        RemoteObjects result = storage.listObjects("s3://my-bucket/data/", null);

        Assertions.assertEquals(1, result.getObjectList().size());
        RemoteObject ro = result.getObjectList().get(0);
        Assertions.assertEquals("data/file1.csv", ro.getKey());
        Assertions.assertEquals(1024L, ro.getSize());
        Assertions.assertEquals("abc123", ro.getEtag());
        Assertions.assertFalse(result.isTruncated());
    }

    @Test
    void listObjects_passesContinuationToken() throws IOException {
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(List.of())
                .isTruncated(false)
                .build();
        Mockito.when(mockS3.listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request.class))).thenReturn(response);

        storage.listObjects("s3://my-bucket/prefix/", "token-abc");

        ArgumentCaptor<ListObjectsV2Request> captor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        Mockito.verify(mockS3).listObjectsV2(captor.capture());
        Assertions.assertEquals("token-abc", captor.getValue().continuationToken());
    }

    @Test
    void listObjects_truncatedResultReturnsContinuationToken() throws IOException {
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(List.of())
                .isTruncated(true)
                .nextContinuationToken("next-token")
                .build();
        Mockito.when(mockS3.listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request.class))).thenReturn(response);

        RemoteObjects result = storage.listObjects("s3://my-bucket/prefix/", null);

        Assertions.assertTrue(result.isTruncated());
        Assertions.assertEquals("next-token", result.getContinuationToken());
    }

    // ------------------------------------------------------------------
    // headObject()
    // ------------------------------------------------------------------

    @Test
    void headObject_returnsRemoteObjectOnSuccess() throws IOException {
        Instant now = Instant.now();
        HeadObjectResponse headResp = (HeadObjectResponse) HeadObjectResponse.builder()
                .eTag("etag-xyz")
                .contentLength(2048L)
                .lastModified(now)
                .build();
        Mockito.when(mockS3.headObject(ArgumentMatchers.any(HeadObjectRequest.class))).thenReturn(headResp);

        RemoteObject result = storage.headObject("s3://my-bucket/data/file.csv");

        Assertions.assertEquals("data/file.csv", result.getKey());
        Assertions.assertEquals(2048L, result.getSize());
        Assertions.assertEquals("etag-xyz", result.getEtag());
    }

    @Test
    void headObject_throwsFileNotFoundForNoSuchKeyException() {
        Mockito.when(mockS3.headObject(ArgumentMatchers.any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("not found").build());

        Assertions.assertThrows(FileNotFoundException.class,
                () -> storage.headObject("s3://my-bucket/missing"));
    }

    @Test
    void headObject_throwsFileNotFoundFor404S3Exception() {
        Mockito.when(mockS3.headObject(ArgumentMatchers.any(HeadObjectRequest.class)))
                .thenThrow(S3Exception.builder().message("not found").statusCode(404).build());

        Assertions.assertThrows(FileNotFoundException.class,
                () -> storage.headObject("s3://my-bucket/missing"));
    }

    // ------------------------------------------------------------------
    // putObject()
    // ------------------------------------------------------------------

    @Test
    void putObject_delegatesToS3Client() throws IOException {
        Mockito.when(mockS3.putObject(ArgumentMatchers.any(PutObjectRequest.class),
                ArgumentMatchers.any(software.amazon.awssdk.core.sync.RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        RequestBody body = RequestBody.of(new ByteArrayInputStream(new byte[]{1, 2, 3}), 3);
        storage.putObject("s3://my-bucket/obj", body);

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockS3).putObject(captor.capture(),
                ArgumentMatchers.any(software.amazon.awssdk.core.sync.RequestBody.class));
        Assertions.assertEquals("my-bucket", captor.getValue().bucket());
        Assertions.assertEquals("obj", captor.getValue().key());
    }

    // ------------------------------------------------------------------
    // deleteObject()
    // ------------------------------------------------------------------

    @Test
    void deleteObject_delegatesToS3Client() throws IOException {
        Mockito.when(mockS3.deleteObject(ArgumentMatchers.any(DeleteObjectRequest.class)))
                .thenReturn(DeleteObjectResponse.builder().build());

        storage.deleteObject("s3://my-bucket/to-delete");

        ArgumentCaptor<DeleteObjectRequest> captor = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        Mockito.verify(mockS3).deleteObject(captor.capture());
        Assertions.assertEquals("my-bucket", captor.getValue().bucket());
        Assertions.assertEquals("to-delete", captor.getValue().key());
    }

    @Test
    void deleteObject_swallows404S3Exception() throws IOException {
        Mockito.when(mockS3.deleteObject(ArgumentMatchers.any(DeleteObjectRequest.class)))
                .thenThrow(S3Exception.builder().message("not found").statusCode(404).build());

        // Should not throw
        storage.deleteObject("s3://my-bucket/already-gone");
    }

    // ------------------------------------------------------------------
    // copyObject()
    // ------------------------------------------------------------------

    @Test
    void copyObject_delegatesToS3Client() throws IOException {
        Mockito.when(mockS3.copyObject(ArgumentMatchers.any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        storage.copyObject("s3://my-bucket/src", "s3://my-bucket/dst");

        ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        Mockito.verify(mockS3).copyObject(captor.capture());
        Assertions.assertEquals("my-bucket/src", captor.getValue().copySource());
        Assertions.assertEquals("my-bucket", captor.getValue().destinationBucket());
        Assertions.assertEquals("dst", captor.getValue().destinationKey());
    }

    // ------------------------------------------------------------------
    // initiateMultipartUpload()
    // ------------------------------------------------------------------

    @Test
    void initiateMultipartUpload_returnsUploadId() throws IOException {
        Mockito.when(mockS3.createMultipartUpload(ArgumentMatchers.any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-123").build());

        String uploadId = storage.initiateMultipartUpload("s3://my-bucket/large-file");

        Assertions.assertEquals("upload-123", uploadId);
    }

    // ------------------------------------------------------------------
    // uploadPart()
    // ------------------------------------------------------------------

    @Test
    void uploadPart_returnsPartResult() throws IOException {
        Mockito.when(mockS3.uploadPart(ArgumentMatchers.any(UploadPartRequest.class),
                ArgumentMatchers.any(software.amazon.awssdk.core.sync.RequestBody.class)))
                .thenReturn(UploadPartResponse.builder().eTag("part-etag").build());

        RequestBody body = RequestBody.of(new ByteArrayInputStream(new byte[5]), 5);
        UploadPartResult result = storage.uploadPart("s3://my-bucket/large-file", "upload-123", 1, body);

        Assertions.assertEquals(1, result.partNumber());
        Assertions.assertEquals("part-etag", result.etag());
    }

    // ------------------------------------------------------------------
    // completeMultipartUpload()
    // ------------------------------------------------------------------

    @Test
    void completeMultipartUpload_delegatesToS3Client() throws IOException {
        Mockito.when(mockS3.completeMultipartUpload(ArgumentMatchers.any(CompleteMultipartUploadRequest.class)))
                .thenReturn(software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse.builder().build());

        List<UploadPartResult> parts = List.of(
                new UploadPartResult(1, "etag-1"),
                new UploadPartResult(2, "etag-2"));
        storage.completeMultipartUpload("s3://my-bucket/file", "upload-123", parts);

        ArgumentCaptor<CompleteMultipartUploadRequest> captor =
                ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
        Mockito.verify(mockS3).completeMultipartUpload(captor.capture());
        Assertions.assertEquals("my-bucket", captor.getValue().bucket());
        Assertions.assertEquals("upload-123", captor.getValue().uploadId());
        Assertions.assertEquals(2, captor.getValue().multipartUpload().parts().size());
    }

    // ------------------------------------------------------------------
    // abortMultipartUpload()
    // ------------------------------------------------------------------

    @Test
    void abortMultipartUpload_delegatesToS3Client() throws IOException {
        Mockito.when(mockS3.abortMultipartUpload(ArgumentMatchers.any(AbortMultipartUploadRequest.class)))
                .thenReturn(software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse.builder().build());

        storage.abortMultipartUpload("s3://my-bucket/file", "upload-123");

        ArgumentCaptor<AbortMultipartUploadRequest> captor =
                ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        Mockito.verify(mockS3).abortMultipartUpload(captor.capture());
        Assertions.assertEquals("upload-123", captor.getValue().uploadId());
    }

    // ------------------------------------------------------------------
    // getPresignedUrl() - requires bucket
    // ------------------------------------------------------------------

    @Test
    void getPresignedUrl_throwsWhenBucketNotConfigured() {
        Map<String, String> noBucketProps = new HashMap<>();
        noBucketProps.put("AWS_ENDPOINT", "https://s3.amazonaws.com");
        noBucketProps.put("AWS_ACCESS_KEY", "ak");
        noBucketProps.put("AWS_SECRET_KEY", "sk");
        S3ObjStorage noBucket = new TestableS3ObjStorage(noBucketProps, mockS3);

        Assertions.assertThrows(IOException.class, () -> noBucket.getPresignedUrl("some/key"),
                "Should throw when AWS_BUCKET not configured");
    }

    // ------------------------------------------------------------------
    // getStsToken() - requires role ARN
    // ------------------------------------------------------------------

    @Test
    void getStsToken_throwsWhenRoleArnNotConfigured() {
        Assertions.assertThrows(IOException.class, () -> storage.getStsToken(),
                "Should throw when AWS_ROLE_ARN not configured");
    }

    @Test
    void buildCredentialsProvider_returnsAssumeRoleProviderWhenRoleArnConfigured() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.amazonaws.com");
        props.put("AWS_REGION", "us-east-1");
        props.put("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/MyRole");
        props.put("AWS_EXTERNAL_ID", "snapshot-external-id");
        InspectableS3ObjStorage roleArnStorage = new InspectableS3ObjStorage(props, mockS3);

        AwsCredentialsProvider credentialsProvider = roleArnStorage.inspectBuildCredentialsProvider();

        Assertions.assertEquals("StsAssumeRoleCredentialsProvider",
                credentialsProvider.getClass().getSimpleName());
    }

    // ------------------------------------------------------------------
    // close()
    // ------------------------------------------------------------------

    @Test
    void close_closesS3Client() throws IOException {
        // Force client creation
        storage.getClient();
        storage.close();
        Mockito.verify(mockS3).close();
    }

    // ------------------------------------------------------------------
    // Test infrastructure
    // ------------------------------------------------------------------

    private static class TestableS3ObjStorage extends S3ObjStorage {
        private final S3Client mockClient;

        TestableS3ObjStorage(Map<String, String> properties, S3Client mockClient) {
            super(properties);
            this.mockClient = mockClient;
        }

        @Override
        protected S3Client buildClient() {
            return mockClient;
        }
    }

    private static class InspectableS3ObjStorage extends TestableS3ObjStorage {
        InspectableS3ObjStorage(Map<String, String> properties, S3Client mockClient) {
            super(properties, mockClient);
        }

        AwsCredentialsProvider inspectBuildCredentialsProvider() {
            return buildCredentialsProvider();
        }

        @Override
        protected StsClient buildStsClient(AwsCredentialsProvider credentialsProvider, String region) {
            return Mockito.mock(StsClient.class);
        }
    }
}
