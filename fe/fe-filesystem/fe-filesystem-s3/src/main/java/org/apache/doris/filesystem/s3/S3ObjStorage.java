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

import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.UploadPartResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Object storage implementation backed by AWS S3 SDK v2.
 * Accepts only Map<String, String> in constructor; no dependency on fe-core or fe-common.
 *
 * <p>Recognized property keys:
 * <ul>
 *   <li>AWS_ENDPOINT     - S3 endpoint URL (required)</li>
 *   <li>AWS_REGION       - AWS region identifier (required)</li>
 *   <li>AWS_ACCESS_KEY   - AWS access key ID</li>
 *   <li>AWS_SECRET_KEY   - AWS secret access key</li>
 *   <li>AWS_TOKEN        - AWS session token (optional)</li>
 *   <li>use_path_style   - "true" to enable path-style bucket access</li>
 * </ul>
 */
public class S3ObjStorage implements ObjStorage<S3Client> {

    private static final Logger LOG = LogManager.getLogger(S3ObjStorage.class);

    static final String PROP_ENDPOINT = "AWS_ENDPOINT";
    static final String PROP_REGION = "AWS_REGION";
    static final String PROP_ACCESS_KEY = "AWS_ACCESS_KEY";
    static final String PROP_SECRET_KEY = "AWS_SECRET_KEY";
    static final String PROP_TOKEN = "AWS_TOKEN";
    static final String PROP_PATH_STYLE = "use_path_style";

    private final Map<String, String> properties;
    private final boolean usePathStyle;
    private volatile S3Client client;

    public S3ObjStorage(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(properties);
        this.usePathStyle = Boolean.parseBoolean(properties.getOrDefault(PROP_PATH_STYLE, "false"));
    }

    @Override
    public S3Client getClient() throws IOException {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = buildClient();
                }
            }
        }
        return client;
    }

    private S3Client buildClient() throws IOException {
        String endpointStr = properties.get(PROP_ENDPOINT);
        if (endpointStr == null || endpointStr.isEmpty()) {
            throw new IOException("S3 property " + PROP_ENDPOINT + " is required");
        }
        if (!endpointStr.contains("://")) {
            endpointStr = "https://" + endpointStr;
        }
        String region = properties.getOrDefault(PROP_REGION, "us-east-1");
        AwsCredentialsProvider credentialsProvider = buildCredentialsProvider();

        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder()
                        .socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30))
                        .build())
                .endpointOverride(URI.create(endpointStr))
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(usePathStyle)
                        .build())
                .build();
    }

    private AwsCredentialsProvider buildCredentialsProvider() {
        String accessKey = properties.get(PROP_ACCESS_KEY);
        String secretKey = properties.get(PROP_SECRET_KEY);
        String token = properties.get(PROP_TOKEN);

        if (accessKey != null && !accessKey.isEmpty() && secretKey != null && !secretKey.isEmpty()) {
            if (token != null && !token.isEmpty()) {
                return StaticCredentialsProvider.create(
                        AwsSessionCredentials.create(accessKey, secretKey, token));
            }
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }
        return DefaultCredentialsProvider.create();
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .bucket(uri.bucket())
                .prefix(uri.key());
        if (continuationToken != null && !continuationToken.isEmpty()) {
            builder.continuationToken(continuationToken);
        }
        try {
            ListObjectsV2Response response = getClient().listObjectsV2(builder.build());
            List<org.apache.doris.filesystem.spi.RemoteObject> objects = response.contents().stream()
                    .map(s3Obj -> new org.apache.doris.filesystem.spi.RemoteObject(
                            s3Obj.key(),
                            getRelativePath(uri.key(), s3Obj.key()),
                            s3Obj.eTag(),
                            s3Obj.size()))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, response.isTruncated(),
                    response.nextContinuationToken());
        } catch (S3Exception e) {
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    private static String getRelativePath(String prefix, String key) {
        return key.startsWith(prefix) ? key.substring(prefix.length()) : key;
    }

    @Override
    public org.apache.doris.filesystem.spi.RemoteObject headObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            HeadObjectResponse response = getClient().headObject(
                    HeadObjectRequest.builder().bucket(uri.bucket()).key(uri.key()).build());
            return new org.apache.doris.filesystem.spi.RemoteObject(
                    uri.key(), uri.key(), response.eTag(), response.contentLength());
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("Object not found: " + remotePath);
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void putObject(String remotePath, org.apache.doris.filesystem.spi.RequestBody requestBody)
            throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try (InputStream content = requestBody.content()) {
            getClient().putObject(
                    PutObjectRequest.builder().bucket(uri.bucket()).key(uri.key()).build(),
                    software.amazon.awssdk.core.sync.RequestBody.fromInputStream(
                            content, requestBody.contentLength()));
        } catch (S3Exception e) {
            throw new IOException("putObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            getClient().deleteObject(DeleteObjectRequest.builder()
                    .bucket(uri.bucket()).key(uri.key()).build());
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return; // already deleted
            }
            throw new IOException("deleteObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void copyObject(String srcPath, String dstPath) throws IOException {
        S3Uri srcUri = S3Uri.parse(srcPath, usePathStyle);
        S3Uri dstUri = S3Uri.parse(dstPath, usePathStyle);
        try {
            getClient().copyObject(CopyObjectRequest.builder()
                    .copySource(srcUri.bucket() + "/" + srcUri.key())
                    .destinationBucket(dstUri.bucket())
                    .destinationKey(dstUri.key())
                    .build());
        } catch (S3Exception e) {
            throw new IOException("copyObject from " + srcPath + " to " + dstPath
                    + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String initiateMultipartUpload(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            CreateMultipartUploadResponse response = getClient().createMultipartUpload(
                    CreateMultipartUploadRequest.builder().bucket(uri.bucket()).key(uri.key()).build());
            return response.uploadId();
        } catch (S3Exception e) {
            throw new IOException("initiateMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            org.apache.doris.filesystem.spi.RequestBody body) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try (InputStream content = body.content()) {
            UploadPartResponse response = getClient().uploadPart(
                    UploadPartRequest.builder()
                            .bucket(uri.bucket()).key(uri.key())
                            .uploadId(uploadId).partNumber(partNum)
                            .contentLength(body.contentLength())
                            .build(),
                    software.amazon.awssdk.core.sync.RequestBody.fromInputStream(
                            content, body.contentLength()));
            return new UploadPartResult(partNum, response.eTag());
        } catch (S3Exception e) {
            throw new IOException("uploadPart " + partNum + " failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void completeMultipartUpload(String remotePath, String uploadId,
            List<org.apache.doris.filesystem.spi.UploadPartResult> parts) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        List<CompletedPart> completedParts = parts.stream()
                .map(p -> CompletedPart.builder().partNumber(p.partNumber()).eTag(p.etag()).build())
                .collect(Collectors.toList());
        try {
            getClient().completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(uri.bucket()).key(uri.key()).uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build());
        } catch (S3Exception e) {
            throw new IOException("completeMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            getClient().abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(uri.bucket()).key(uri.key()).uploadId(uploadId).build());
        } catch (S3Exception e) {
            LOG.warn("abortMultipartUpload failed for {}: {}", remotePath, e.getMessage());
        }
    }

    /**
     * Open an InputStream for reading an S3 object.
     */
    InputStream openInputStream(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            return getClient().getObject(
                    GetObjectRequest.builder().bucket(uri.bucket()).key(uri.key()).build());
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("Object not found: " + remotePath);
        } catch (S3Exception e) {
            throw new IOException("getObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
