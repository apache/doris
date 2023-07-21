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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectNotInActiveTierErrorException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * use for unit test
 */
public class MockedS3Client implements S3Client {

    private byte[] mockedData;
    private boolean canMakeData;
    private final List<S3Object> mockedObjectList = new ArrayList<>();

    @Override
    public String serviceName() {
        return "MockedS3";
    }

    @Override
    public void close() {}

    public void setMockedData(byte[] mockedData) {
        this.mockedData = mockedData;
    }

    public void setCanMakeData(boolean canMakeData) {
        this.canMakeData = canMakeData;
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) throws NoSuchKeyException,
                AwsServiceException, SdkClientException, S3Exception {
        return HeadObjectResponse.builder().deleteMarker(false).eTag("head-tag").build();
    }

    @Override
    public GetObjectResponse getObject(GetObjectRequest getObjectRequest, Path destinationPath) throws
                NoSuchKeyException, InvalidObjectStateException,
                AwsServiceException, SdkClientException, S3Exception {
        if (canMakeData) {
            try (OutputStream os = Files.newOutputStream(destinationPath)) {
                os.write(mockedData);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return GetObjectResponse.builder().eTag("get-etag").build();
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
                throws AwsServiceException, SdkClientException, S3Exception {
        Long size = requestBody.optionalContentLength().orElse(0L);
        mockedObjectList.add(S3Object.builder().key(putObjectRequest.key()).size(size).build());
        return PutObjectResponse.builder().eTag("put-etag").build();
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest) throws AwsServiceException,
            SdkClientException, S3Exception {
        mockedObjectList.removeIf(e -> Objects.equals(e.key(), deleteObjectRequest.key()));
        return DeleteObjectResponse.builder().deleteMarker(true).build();
    }

    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest copyObjectRequest) throws ObjectNotInActiveTierErrorException,
            AwsServiceException, SdkClientException, S3Exception {
        CopyObjectResult result = CopyObjectResult.builder().eTag("copy-etag").build();
        return CopyObjectResponse.builder().copyObjectResult(result).build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request) throws NoSuchBucketException,
            AwsServiceException, SdkClientException, S3Exception {
        return ListObjectsV2Response.builder()
                .contents(mockedObjectList)
                .isTruncated(true)
                .delimiter(",")
                .nextContinuationToken("next-token")
                .prefix("prefix")
                .maxKeys(5)
                .build();
    }
}
