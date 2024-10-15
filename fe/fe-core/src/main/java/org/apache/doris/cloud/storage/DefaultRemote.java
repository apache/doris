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

package org.apache.doris.cloud.storage;

import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of {@link RemoteBase} use {@link S3Client}.
 * If one object storage such as OSS can not use {@link S3Client} to access, please override these methods.
 */
public class DefaultRemote extends RemoteBase {
    private static final Logger LOG = LogManager.getLogger(DefaultRemote.class);
    private S3Client s3Client;

    public DefaultRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public ListObjectsResult listObjects(String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(), continuationToken);
    }

    @Override
    public ListObjectsResult listObjects(String subPrefix, String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(subPrefix), continuationToken);
    }

    @Override
    public ListObjectsResult headObject(String subKey) throws DdlException {
        initClient();
        try {
            String key = normalizePrefix(subKey);
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(obj.getBucket()).key(key)
                    .build();
            HeadObjectResponse response = s3Client.headObject(request);
            ObjectFile objectFile = new ObjectFile(key, getRelativePath(key), response.eTag(),
                    response.contentLength());
            return new ListObjectsResult(Lists.newArrayList(objectFile), false, null);
        } catch (NoSuchKeyException e) {
            LOG.warn("NoSuchKey when head object for S3, subKey={}", subKey);
            return new ListObjectsResult(Lists.newArrayList(), false, null);
        } catch (SdkException e) {
            LOG.warn("Failed to head object for S3, subKey={}", subKey, e);
            throw new DdlException(
                    "Failed to head object for S3, subKey=" + subKey + " Error message=" + e.getMessage());
        }
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        throw new DdlException("Get sts token is unsupported");
    }

    private ListObjectsResult listObjectsInner(String prefix, String continuationToken) throws DdlException {
        initClient();
        try {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(obj.getBucket())
                    .prefix(prefix);
            if (!StringUtils.isEmpty(continuationToken)) {
                requestBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
            List<ObjectFile> objectFiles = new ArrayList<>();
            for (S3Object c : response.contents()) {
                objectFiles.add(new ObjectFile(c.key(), getRelativePath(c.key()), c.eTag(), c.size()));
            }
            return new ListObjectsResult(objectFiles, response.isTruncated(), response.nextContinuationToken());
        } catch (SdkException e) {
            LOG.warn("Failed to list objects for S3, prefix {}", prefix, e);
            throw new DdlException("Failed to list objects for S3, Error message=" + e.getMessage());
        }
    }

    private void initClient() {
        if (s3Client == null) {
            /*
             * https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#131-client-http-configuration
             * https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#133-client-override-configuration
             * There are several timeout configuration, please config if needed.
             */
            AwsCredentials credentials;
            if (obj.getToken() != null) {
                credentials = AwsSessionCredentials.create(obj.getAk(), obj.getSk(), obj.getToken());
            } else {
                credentials = AwsBasicCredentials.create(obj.getAk(), obj.getSk());
            }
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(credentials);
            URI endpointUri = URI.create("http://" + obj.getEndpoint());
            s3Client = S3Client.builder().endpointOverride(endpointUri).credentialsProvider(scp)
                    .region(Region.of(obj.getRegion())).build();
        }
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }

    @Override
    public void deleteObjects(List<String> keys) throws DdlException {
        checkDeleteKeys(keys);
        initClient();
        try {
            int maxDelete = 1000;
            for (int i = 0; i < keys.size() / maxDelete + 1; i++) {
                ArrayList<ObjectIdentifier> toDelete = new ArrayList<>();
                for (int j = maxDelete * i; j < keys.size() && toDelete.size() < maxDelete; j++) {
                    toDelete.add(ObjectIdentifier.builder().key(keys.get(j)).build());
                }
                DeleteObjectsRequest.Builder requestBuilder = DeleteObjectsRequest.builder().bucket(obj.getBucket())
                        .delete(Delete.builder().objects(toDelete).build());
                LOG.info("Delete objects for bucket={}, keys={}", obj.getBucket(), keys);
                DeleteObjectsResponse response = s3Client.deleteObjects(requestBuilder.build());
                if (!response.errors().isEmpty()) {
                    S3Error error = response.errors().get(0);
                    throw new DdlException(
                            "Failed delete objects, bucket=" + obj.getBucket() + ", key=" + error.key() + ", error="
                                    + error.message() + ", code=" + error.code());
                }
            }
        } catch (SdkException e) {
            LOG.warn("Failed to delete objects for S3", e);
            throw new DdlException("Failed to delete objects for S3, Error message=" + e.getMessage());
        }
    }
}
