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

package org.apache.doris.fsv2.obj;

import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

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
}
