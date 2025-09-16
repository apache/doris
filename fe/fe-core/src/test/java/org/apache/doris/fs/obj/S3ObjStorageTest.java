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

package org.apache.doris.fs.obj;

import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.fs.remote.RemoteFile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class S3ObjStorageTest {
    private S3ObjStorage storage;
    private S3Client mockClient;
    private AbstractS3CompatibleProperties mockProperties;

    @BeforeEach
    void setUp() throws UserException {
        mockProperties = Mockito.mock(AbstractS3CompatibleProperties.class);
        Mockito.when(mockProperties.getEndpoint()).thenReturn("http://s3.example.com");
        Mockito.when(mockProperties.getRegion()).thenReturn("us-east-1");
        Mockito.when(mockProperties.getUsePathStyle()).thenReturn("false");
        Mockito.when(mockProperties.getForceParsingByStandardUrl()).thenReturn("false");
        // storage = new S3ObjStorage(mockProperties);
        mockClient = Mockito.mock(S3Client.class);
        storage = Mockito.spy(new S3ObjStorage(mockProperties));
        Mockito.doReturn(mockClient).when(storage).getClient();
    }

    @Test
    @DisplayName("getClient should return a valid S3Client instance")
    void getClientReturnsValidS3Client() throws UserException {
        S3Client client = storage.getClient();
        Assertions.assertNotNull(client);
    }

    @Test
    @DisplayName("headObject should return OK status when object exists")
    void headObjectReturnsOkWhenObjectExists() throws UserException {
        Mockito.when(mockClient.headObject(Mockito.any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().build());

        Status status = storage.headObject("s3://bucket/key");
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    @DisplayName("headObject should return NOT_FOUND status when object does not exist")
    void headObjectReturnsNotFoundWhenObjectDoesNotExist() throws UserException {
        Mockito.when(mockClient.headObject(Mockito.any(HeadObjectRequest.class)))
                .thenThrow(S3Exception.builder().statusCode(404).build());

        Status status = storage.headObject("s3://bucket/nonexistent-key");
        Assertions.assertEquals(Status.ErrCode.NOT_FOUND, status.getErrCode());
    }

    @Test
    @DisplayName("headObject should return COMMON_ERROR status for other exceptions")
    void headObjectReturnsErrorForOtherExceptions() throws UserException {
        Mockito.when(mockClient.headObject(Mockito.any(HeadObjectRequest.class)))
                .thenThrow(S3Exception.builder().statusCode(500).build());

        Status status = storage.headObject("s3://bucket/key");
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
    }

    @Test
    @DisplayName("putObject should return OK status when upload succeeds")
    void putObjectReturnsOkWhenUploadSucceeds() throws UserException {
        Mockito.when(mockClient.putObject(Mockito.any(PutObjectRequest.class), Mockito.any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        InputStream content = new ByteArrayInputStream("test content".getBytes());
        Status status = storage.putObject("s3://bucket/key", content, 12);
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    @DisplayName("putObject should return COMMON_ERROR status when upload fails")
    void putObjectReturnsErrorWhenUploadFails() throws UserException {
        Mockito.when(mockClient.putObject(Mockito.any(PutObjectRequest.class), Mockito.any(RequestBody.class)))
                .thenThrow(S3Exception.builder().statusCode(500).build());

        InputStream content = new ByteArrayInputStream("test content".getBytes());
        Status status = storage.putObject("s3://bucket/key", content, 12);
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
    }

    @Test
    @DisplayName("deleteObject should return OK status when object is deleted successfully")
    void deleteObjectReturnsOkWhenDeletionSucceeds() throws UserException {
        Mockito.when(mockClient.deleteObject(Mockito.any(DeleteObjectRequest.class)))
                .thenReturn(DeleteObjectResponse.builder().build());

        Status status = storage.deleteObject("s3://bucket/key");
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    @DisplayName("deleteObject should return OK status when object does not exist")
    void deleteObjectReturnsOkWhenObjectDoesNotExist() throws UserException {
        Mockito.when(mockClient.deleteObject(Mockito.any(DeleteObjectRequest.class)))
                .thenThrow(S3Exception.builder().statusCode(404).build());

        Status status = storage.deleteObject("s3://bucket/nonexistent-key");
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    @DisplayName("deleteObject should return COMMON_ERROR status for other exceptions")
    void deleteObjectReturnsErrorForOtherExceptions() throws UserException {
        Mockito.when(mockClient.deleteObject(Mockito.any(DeleteObjectRequest.class)))
                .thenThrow(S3Exception.builder().statusCode(500).build());

        Status status = storage.deleteObject("s3://bucket/key");
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
    }

    @Test
    @DisplayName("listObjects should return a list of objects when objects exist")
    void listObjectsReturnsObjectsWhenObjectsExist() throws UserException {
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("prefix/key1").size(100L).build(),
                        S3Object.builder().key("prefix/key2").size(200L).build())
                .isTruncated(false)
                .build();
        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(response);

        RemoteObjects objects = storage.listObjects("s3://bucket/prefix", null);
        Assertions.assertEquals(2, objects.getObjectList().size());
    }

    @Test
    @DisplayName("listObjects should throw DdlException for errors")
    void listObjectsThrowsExceptionForErrors() throws UserException {
        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenThrow(S3Exception.builder().statusCode(500).build());

        Assertions.assertThrows(DdlException.class, () -> storage.listObjects("s3://bucket/prefix", null));
    }

    @Test
    @DisplayName("multipartUpload should return OK status when upload succeeds")
    void multipartUploadReturnsOkWhenUploadSucceeds() throws Exception {
        Mockito.when(mockClient.createMultipartUpload(Mockito.any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("uploadId").build());
        Mockito.when(mockClient.uploadPart(Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class)))
                .thenReturn(UploadPartResponse.builder().eTag("etag").build());
        Mockito.when(mockClient.completeMultipartUpload(Mockito.any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompleteMultipartUploadResponse.builder().build());

        InputStream content = new ByteArrayInputStream(new byte[10 * 1024 * 1024]); // 10 MB
        Status status = storage.multipartUpload("s3://bucket/key", content, 10 * 1024 * 1024);
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    @DisplayName("multipartUpload should return COMMON_ERROR status when upload fails")
    void multipartUploadReturnsErrorWhenUploadFails() throws Exception {
        Mockito.when(mockClient.createMultipartUpload(Mockito.any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("uploadId").build());
        Mockito.when(mockClient.uploadPart(Mockito.any(UploadPartRequest.class), Mockito.any(RequestBody.class)))
                .thenThrow(S3Exception.builder().statusCode(500).build());

        InputStream content = new ByteArrayInputStream(new byte[10 * 1024 * 1024]); // 10 MB
        Status status = storage.multipartUpload("s3://bucket/key", content, 10 * 1024 * 1024);
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
    }

    @Test
    public void testListFiles_nonRecursive_filesOnly() {
        List<RemoteFile> result = new ArrayList<>();

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(S3Object.builder()
                        .key("folder/file.txt")
                        .lastModified(Instant.now())
                        .size(1024L)
                        .build())
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenReturn(response);

        Status status = storage.listFiles("s3://test-bucket/folder/", false, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(1, result.size());
        Assertions.assertFalse(result.get(0).isDirectory());
    }

    @Test
    public void testListFiles_nonRecursive_withDirectory() {
        List<RemoteFile> result = new ArrayList<>();

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .commonPrefixes(CommonPrefix.builder().prefix("folder/subdir/").build())
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenReturn(response);

        Status status = storage.listFiles("s3://test-bucket/folder/", false, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).isDirectory());
    }

    @Test
    public void testListFiles_recursive_filesAndSubdirs() {
        List<RemoteFile> result = new ArrayList<>();

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(
                        S3Object.builder().key("folder/a.txt").lastModified(Instant.now()).size(120L).build(),
                        S3Object.builder().key("folder/sub/b.txt").lastModified(Instant.now()).size(20L).build()
                ).build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenReturn(response);

        Status status = storage.listFiles("s3://test-bucket/folder/", true, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testListFiles_emptyDirectory() {
        List<RemoteFile> result = new ArrayList<>();

        ListObjectsV2Response response = ListObjectsV2Response.builder().build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenReturn(response);

        Status status = storage.listFiles("s3://test-bucket/empty/", false, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFiles_keyEqualsPrefix_shouldBeSkipped() {
        List<RemoteFile> result = new ArrayList<>();

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("folder/").lastModified(Instant.now()).build())
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenReturn(response);

        Status status = storage.listFiles("s3://test-bucket/folder/", true, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void testListFiles_withPagination() {
        List<RemoteFile> result = new ArrayList<>();

        ListObjectsV2Response page1 = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("folder/file1.txt").lastModified(Instant.now()).size(1L).build())
                .nextContinuationToken("next-token")
                .build();

        ListObjectsV2Response page2 = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("folder/file2.txt").size(2L).lastModified(Instant.now()).build())
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenReturn(page1)
                .thenReturn(page2);

        Status status = storage.listFiles("s3://test-bucket/folder/", true, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testListFiles_noSuchKeyException() {
        List<RemoteFile> result = new ArrayList<>();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenThrow(NoSuchKeyException.builder().message("not found").build());

        Status status = storage.listFiles("s3://test-bucket/folder/", false, result);

        Assertions.assertEquals(Status.ErrCode.NOT_FOUND, status.getErrCode());
    }

    @Test
    public void testListFiles_otherException() {
        List<RemoteFile> result = new ArrayList<>();
        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenThrow(new RuntimeException("something went wrong"));
        Status status = storage.listFiles("s3://test-bucket/folder/", false, result);
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
    }

    @Test
    public void testCopyObject_success() {
        CopyObjectResponse response = CopyObjectResponse.builder().build();
        Mockito.when(mockClient.copyObject(Mockito.any(CopyObjectRequest.class))).thenReturn(response);
        Status status = storage.copyObject("s3://bucket/source.txt", "s3://bucket/target.txt");
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    public void testCopyObject_s3Exception() {
        Mockito.when(mockClient.copyObject(Mockito.any(CopyObjectRequest.class)))
                .thenThrow(S3Exception.builder().message("Access denied").build());
        Status status = storage.copyObject("s3://bucket/source.txt", "s3://bucket/target.txt");
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("copy file failed"));
    }

    @Test
    public void testDeleteObjects_success_singlePage() throws DdlException {
        DeleteObjectsResponse mockResp = DeleteObjectsResponse.builder().deleted(
                DeletedObject.builder().key("folder/file1.txt").build(),
                DeletedObject.builder().key("folder/file2.txt").build()
        ).build();
        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(
                ListObjectsV2Response.builder().contents(
                        S3Object.builder().key("folder/file1.txt").lastModified(Instant.now()).size(1L).build(),
                        S3Object.builder().key("folder/file2.txt").lastModified(Instant.now()).size(1L).build()
                ).isTruncated(false).build()
        );
        Mockito.when(mockClient.deleteObjects(Mockito.any(DeleteObjectsRequest.class))).thenReturn(mockResp);
        Status status = storage.deleteObjects("s3://bucket/folder/");
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    public void testDeleteObjects_success_multiPage() throws DdlException {
        ListObjectsV2Response page1 = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("folder/file1.txt").lastModified(Instant.now()).size(1L).build())
                .nextContinuationToken("token-2")
                .isTruncated(true)
                .build();

        ListObjectsV2Response page2 = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("folder/file2.txt").lastModified(Instant.now()).size(1L).build())
                .isTruncated(false)
                .build();

        DeleteObjectsResponse deleteResp = DeleteObjectsResponse.builder()
                .deleted(
                        DeletedObject.builder().key("folder/file1.txt").build(),
                        DeletedObject.builder().key("folder/file2.txt").build()
                )
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenReturn(page1)
                .thenReturn(page2);
        Mockito.when(mockClient.deleteObjects(Mockito.any(DeleteObjectsRequest.class))).thenReturn(deleteResp);

        Status status = storage.deleteObjects("s3://bucket/folder/");
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    public void testDeleteObjects_deleteThrowsRuntimeException() throws DdlException {
        ListObjectsV2Response listResp = ListObjectsV2Response.builder().contents(
                S3Object.builder().key("folder/file1.txt").lastModified(Instant.now()).size(1L).build()
        ).isTruncated(false).build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(listResp);
        Mockito.when(mockClient.deleteObjects(Mockito.any(DeleteObjectsRequest.class)))
                .thenThrow(new RuntimeException("delete failed"));

        Status status = storage.deleteObjects("s3://bucket/folder/");
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("delete objects failed"));
    }

    @Test
    public void testDeleteObjects_emptyList() throws DdlException {
        ListObjectsV2Response listResp = ListObjectsV2Response.builder()
                .contents(Collections.emptyList())
                .isTruncated(false)
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(listResp);

        Status status = storage.deleteObjects("s3://bucket/empty/");
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    public void testListDirectories_success() {
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .commonPrefixes(
                        CommonPrefix.builder().prefix("folder/a/").build(),
                        CommonPrefix.builder().prefix("folder/b/").build()
                )
                .isTruncated(false)
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(response);

        Set<String> result = new HashSet<>();
        Status status = storage.listDirectories("s3://bucket/folder/", result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertTrue(result.contains("s3://bucket/folder/a/"));
        Assertions.assertTrue(result.contains("s3://bucket/folder/b/"));
    }

    @Test
    public void testListDirectories_empty() {
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .commonPrefixes(Collections.emptyList())
                .isTruncated(false)
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(response);

        Set<String> result = new HashSet<>();
        Status status = storage.listDirectories("s3://bucket/folder/", result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListDirectories_multiPage() {
        ListObjectsV2Response page1 = ListObjectsV2Response.builder()
                .commonPrefixes(CommonPrefix.builder().prefix("folder/a/").build())
                .isTruncated(true)
                .nextContinuationToken("token-2")
                .build();

        ListObjectsV2Response page2 = ListObjectsV2Response.builder()
                .commonPrefixes(CommonPrefix.builder().prefix("folder/b/").build())
                .isTruncated(false)
                .build();

        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenReturn(page1)
                .thenReturn(page2);

        Set<String> result = new HashSet<>();
        Status status = storage.listDirectories("s3://bucket/folder/", result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertTrue(result.contains("s3://bucket/folder/a/"));
        Assertions.assertTrue(result.contains("s3://bucket/folder/b/"));
    }

    @Test
    public void testListDirectories_exception() {
        Mockito.when(mockClient.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
                .thenThrow(new RuntimeException("S3 error"));

        Set<String> result = new HashSet<>();
        Status status = storage.listDirectories("s3://bucket/folder/", result);

        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("S3 error"));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGlobListWithDirectoryBucket() throws UserException {
        storage = new S3ObjStorage(mockProperties);
        mockClient = Mockito.mock(S3Client.class);
        storage = Mockito.spy(storage);
        Mockito.doReturn(mockClient).when(storage).getClient();

        String remotePath = "s3://my-bucket--usw2-az1--x-s3/data/files*.csv";
        List<RemoteFile> result = new ArrayList<>();

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("data/files1.csv").size(1024L).lastModified(Instant.now()).build())
                .isTruncated(false)
                .build();

        // Use an ArgumentCaptor to capture the request passed to listObjectsV2
        org.mockito.ArgumentCaptor<ListObjectsV2Request> captor =
                org.mockito.ArgumentCaptor.forClass(ListObjectsV2Request.class);
        // since storage is a spy, storage.globList will call real method.
        // the real method calls listObjectsV2 in the same class, which is also a real method call.
        // the real listObjectsV2 method calls getClient().listObjectsV2()
        // getClient() is mocked to return mockClient.
        // So we just need to mock mockClient.listObjectsV2()
        Mockito.when(mockClient.listObjectsV2(captor.capture())).thenReturn(response);

        Status status = storage.globList(remotePath, result, false);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(1, result.size());
        // Verify that the prefix was adjusted correctly for the directory bucket.
        // "data/files*.csv" -> longest prefix is "data/files", adjusted to "data/"
        Assertions.assertEquals("data/", captor.getValue().prefix());
    }
}
