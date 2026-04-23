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
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.StsCredentials;
import org.apache.doris.filesystem.spi.UploadPartResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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
    static final String PROP_BUCKET = "AWS_BUCKET";
    static final String PROP_ROLE_ARN = "AWS_ROLE_ARN";
    static final String PROP_EXTERNAL_ID = "AWS_EXTERNAL_ID";

    /** Validity period for pre-signed URLs and STS tokens (seconds). */
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    /**
     * Normalizes property keys to canonical AWS_* form so that callers using
     * alternate key formats (e.g. "s3.access_key", "access_key") are treated
     * identically to callers that already use canonical keys like "AWS_ACCESS_KEY".
     *
     * <p>Only adds a canonical entry when the canonical key is absent; explicit
     * canonical values are never overridden.
     */
    static Map<String, String> normalizeProperties(Map<String, String> props) {
        Map<String, String> result = new HashMap<>(props);
        addIfAbsent(result, PROP_ACCESS_KEY, "s3.access_key", "access_key", "ACCESS_KEY");
        addIfAbsent(result, PROP_SECRET_KEY, "s3.secret_key", "secret_key", "SECRET_KEY");
        addIfAbsent(result, PROP_ENDPOINT, "s3.endpoint", "endpoint", "ENDPOINT");
        addIfAbsent(result, PROP_REGION, "s3.region", "region", "REGION");
        addIfAbsent(result, PROP_TOKEN, "s3.session_token", "session_token");
        return result;
    }

    /** Copies the first non-null alias value into {@code canonicalKey} if not already present. */
    private static void addIfAbsent(Map<String, String> map, String canonicalKey, String... aliases) {
        if (map.containsKey(canonicalKey)) {
            return;
        }
        for (String alias : aliases) {
            String value = map.get(alias);
            if (value != null) {
                map.put(canonicalKey, value);
                return;
            }
        }
    }

    private final Map<String, String> properties;
    private final boolean usePathStyle;
    /** Bucket name; may be null if not provided (listObjectsWithPrefix and related methods will fail). */
    private final String bucket;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile S3Client client;

    public S3ObjStorage(Map<String, String> properties) {
        // Always normalize so that subclasses (OssObjStorage, CosObjStorage, etc.)
        // which pass s3.* property keys also get them mapped to canonical AWS_* form.
        Map<String, String> normalized = normalizeProperties(properties);
        this.properties = Collections.unmodifiableMap(normalized);
        this.usePathStyle = Boolean.parseBoolean(normalized.getOrDefault(PROP_PATH_STYLE, "false"));
        this.bucket = normalized.get(PROP_BUCKET);
    }

    /**
     * Returns whether path-style (vs virtual-hosted-style) bucket access is enabled.
     * Used by {@link S3FileSystem} when parsing URIs that may be path-style
     * ({@code https://endpoint/bucket/key}) instead of virtual-hosted ({@code s3://bucket/key}).
     */
    public boolean isUsePathStyle() {
        return usePathStyle;
    }

    @Override
    public S3Client getClient() throws IOException {
        if (closed.get()) {
            throw new IOException("S3ObjStorage is already closed");
        }
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = buildClient();
                }
            }
        }
        return client;
    }

    protected S3Client buildClient() throws IOException {
        String endpointStr = properties.get(PROP_ENDPOINT);
        // #23: Region is required for SigV4 signing. Historically we silently fell back to
        // "us-east-1" when none was configured, which can mis-route requests to the wrong AWS
        // region for standard S3 (no endpoint override). Soft-deprecate by logging a WARN
        // rather than throwing, to avoid breaking clusters that rely on the implicit default.
        String region = properties.get(PROP_REGION);
        if (region == null || region.isEmpty()) {
            region = "us-east-1";
            if (endpointStr == null || endpointStr.isEmpty()) {
                LOG.warn("S3 region is not configured (set s3.region / region / AWS_REGION); "
                        + "falling back to '{}'. This is deprecated and may mis-route requests "
                        + "for non-us-east-1 buckets — configure the region explicitly.", region);
            } else {
                LOG.warn("S3 region is not configured but endpoint '{}' is set; using '{}' as a "
                        + "placeholder solely for SigV4 signing.", endpointStr, region);
            }
        }
        AwsCredentialsProvider credentialsProvider = buildCredentialsProvider();

        S3ClientBuilder builder = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder()
                        .socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30))
                        .build())
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(usePathStyle)
                        .build());

        // endpointOverride is only set for non-AWS endpoints (MinIO, COS, OSS, etc.).
        // Standard AWS S3 access uses region-only routing without an explicit endpoint.
        if (endpointStr != null && !endpointStr.isEmpty()) {
            if (!endpointStr.contains("://")) {
                endpointStr = "https://" + endpointStr;
            }
            builder.endpointOverride(URI.create(endpointStr));
        }

        // Switch TCCL to the plugin classloader so that AWS SDK's
        // ClasspathInterceptorChainFactory scans the plugin's classpath rather than
        // the FE parent classloader.  Without this the interceptor type check fails
        // with "does not implement ExecutionInterceptor API" because the interceptor
        // class and the interface are loaded by different classloaders.
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(S3ObjStorage.class.getClassLoader());
        try {
            return builder.build();
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    protected AwsCredentialsProvider buildCredentialsProvider() {
        String roleArn = properties.get(PROP_ROLE_ARN);
        if (roleArn != null && !roleArn.isEmpty()) {
            return buildAssumeRoleCredentialsProvider(roleArn, properties.get(PROP_EXTERNAL_ID));
        }
        return buildClientBaseCredentialsProvider();
    }

    private AwsCredentialsProvider buildClientBaseCredentialsProvider() {
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
        // Allow anonymous access: chain DefaultCredentialsProvider with AnonymousCredentialsProvider
        // as fallback, so public buckets can be accessed without any credentials.
        return AwsCredentialsProviderChain.builder()
                .credentialsProviders(
                        DefaultCredentialsProvider.create(),
                        AnonymousCredentialsProvider.create())
                .build();
    }

    private AwsCredentialsProvider buildStsSourceCredentialsProvider() {
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

    protected StsClient buildStsClient(AwsCredentialsProvider credentialsProvider, String region) {
        return StsClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();
    }

    private AwsCredentialsProvider buildAssumeRoleCredentialsProvider(String roleArn, String externalId) {
        String region = properties.getOrDefault(PROP_REGION, "us-east-1");
        StsClient stsClient = buildStsClient(buildStsSourceCredentialsProvider(), region);
        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(builder -> {
                    builder.roleArn(roleArn)
                            .roleSessionName("doris_" + UUID.randomUUID().toString().replace("-", ""));
                    if (externalId != null && !externalId.isEmpty()) {
                        builder.externalId(externalId);
                    }
                }).build();
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
                            s3Obj.size(),
                            s3Obj.lastModified() != null ? s3Obj.lastModified().toEpochMilli() : 0L))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, response.isTruncated(),
                    response.nextContinuationToken());
        } catch (SdkException e) {
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    /**
     * Strips {@code prefix} (with implicit trailing-slash normalisation) from {@code key}
     * to produce the per-listing relative path. Identical normalisation to
     * {@link #getRelativePathSafe(String, String)} so callers see the same relative-path
     * shape regardless of which list entry point ({@link #listObjects},
     * {@link #listObjectsNonRecursive}, {@link #listObjectsWithPrefix}) was used.
     * If {@code prefix} does not end in {@code "/"} (e.g. user passed
     * {@code s3://bucket/foo} expecting "directory" semantics), {@code "/"} is appended
     * before stripping; if it already ends in {@code "/"} no double-slash is introduced.
     * Bucket-root prefixes (empty key) leave {@code key} unchanged.
     */
    private static String getRelativePath(String prefix, String key) {
        return getRelativePathSafe(prefix, key);
    }

    /**
     * Bounded variant of {@link #listObjects(String, String)}: caps the number of keys
     * returned in this single call via {@code maxKeys}. Useful for "is this prefix
     * non-empty?" probes that should not pull a full page of 1000 keys.
     *
     * @param maxKeys upper bound of keys to fetch; values {@code <= 0} are treated as no cap
     */
    public RemoteObjects listObjects(String remotePath, String continuationToken, int maxKeys)
            throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .bucket(uri.bucket())
                .prefix(uri.key());
        if (maxKeys > 0) {
            builder.maxKeys(maxKeys);
        }
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
                            s3Obj.size(),
                            s3Obj.lastModified() != null ? s3Obj.lastModified().toEpochMilli() : 0L))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, response.isTruncated(),
                    response.nextContinuationToken());
        } catch (SdkException e) {
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    /**
     * Lists objects using S3 delimiter mode (delimiter {@code "/"}) so only the direct
     * children under {@code remotePath} are returned in {@code Contents}. Sub-directories
     * (which appear as {@code CommonPrefixes}) are intentionally NOT exposed here; callers
     * that need them should issue a separate request. Used to give the FileSystem
     * abstraction true POSIX-like "list one directory level" semantics on object stores.
     */
    public RemoteObjects listObjectsNonRecursive(String remotePath, String continuationToken)
            throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .bucket(uri.bucket())
                .prefix(uri.key())
                .delimiter("/");
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
                            s3Obj.size(),
                            s3Obj.lastModified() != null ? s3Obj.lastModified().toEpochMilli() : 0L))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, response.isTruncated(),
                    response.nextContinuationToken());
        } catch (SdkException e) {
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public org.apache.doris.filesystem.spi.RemoteObject headObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            HeadObjectResponse response = getClient().headObject(
                    HeadObjectRequest.builder().bucket(uri.bucket()).key(uri.key()).build());
            return new org.apache.doris.filesystem.spi.RemoteObject(
                    uri.key(), uri.key(), response.eTag(), response.contentLength(),
                    response.lastModified() != null ? response.lastModified().toEpochMilli() : 0L);
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("Object not found: " + remotePath);
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        } catch (SdkException e) {
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
        } catch (SdkException e) {
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
        } catch (SdkException e) {
            throw new IOException("deleteObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void copyObject(String srcPath, String dstPath) throws IOException {
        S3Uri srcUri = S3Uri.parse(srcPath, usePathStyle);
        S3Uri dstUri = S3Uri.parse(dstPath, usePathStyle);
        try {
            getClient().copyObject(CopyObjectRequest.builder()
                    .copySource(SdkHttpUtils.urlEncodeIgnoreSlashes(
                            srcUri.bucket() + "/" + srcUri.key()))
                    .destinationBucket(dstUri.bucket())
                    .destinationKey(dstUri.key())
                    .build());
        } catch (SdkException e) {
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
        } catch (SdkException e) {
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
        } catch (SdkException e) {
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
        } catch (SdkException e) {
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
            // Re-throw so callers know the abort failed; orphaned parts may still exist
            // and require manual cleanup or a lifecycle rule.
            throw new IOException("abortMultipartUpload failed for " + remotePath
                    + " (uploadId=" + uploadId + "): HTTP " + e.statusCode()
                    + " " + e.awsErrorDetails().errorCode() + ": " + e.getMessage(), e);
        } catch (SdkException e) {
            throw new IOException("abortMultipartUpload failed for " + remotePath
                    + " (uploadId=" + uploadId + "): " + e.getMessage(), e);
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
        } catch (SdkException e) {
            throw new IOException("getObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    /**
     * Open an InputStream for reading an S3 object starting at a byte offset (HTTP Range request).
     */
    InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            GetObjectRequest.Builder req = GetObjectRequest.builder()
                    .bucket(uri.bucket()).key(uri.key());
            if (fromByte > 0) {
                req.range("bytes=" + fromByte + "-");
            }
            return getClient().getObject(req.build());
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("Object not found: " + remotePath);
        } catch (SdkException e) {
            throw new IOException("getObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    /**
     * Returns the last-modified time of an S3 object in milliseconds since epoch.
     */
    long headObjectLastModified(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            HeadObjectResponse resp = getClient().headObject(
                    HeadObjectRequest.builder().bucket(uri.bucket()).key(uri.key()).build());
            return resp.lastModified() != null ? resp.lastModified().toEpochMilli() : 0L;
        } catch (NoSuchKeyException e) {
            throw new FileNotFoundException("Object not found: " + remotePath);
        } catch (SdkException e) {
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Cloud-specific extensions
    // -----------------------------------------------------------------------

    @Override
    public StsCredentials getStsToken() throws IOException {
        String roleArn = properties.get(PROP_ROLE_ARN);
        String externalId = properties.get(PROP_EXTERNAL_ID);
        if (roleArn == null || roleArn.isEmpty()) {
            throw new IOException("STS role ARN (AWS_ROLE_ARN) is not configured");
        }
        String region = properties.getOrDefault(PROP_REGION, "us-east-1");
        try {
            try (StsClient stsClient = buildStsClient(buildStsSourceCredentialsProvider(), region)) {
                AssumeRoleRequest.Builder reqBuilder = AssumeRoleRequest.builder()
                        .roleArn(roleArn)
                        .durationSeconds(SESSION_EXPIRE_SECONDS)
                        .roleSessionName("doris_" + UUID.randomUUID().toString().replace("-", ""));
                if (externalId != null && !externalId.isEmpty()) {
                    reqBuilder.externalId(externalId);
                }
                AssumeRoleResponse resp = stsClient.assumeRole(reqBuilder.build());
                Credentials cred = resp.credentials();
                return new StsCredentials(cred.accessKeyId(), cred.secretAccessKey(), cred.sessionToken());
            }
        } catch (Exception e) {
            LOG.warn("Failed to get STS token, roleArn={}", roleArn, e);
            throw new IOException("Failed to get STS token: " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        requireBucket("listObjectsWithPrefix");
        String fullPrefix = normalizeAndCombinePrefix(prefix, subPrefix);
        try {
            ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(fullPrefix);
            if (continuationToken != null && !continuationToken.isEmpty()) {
                reqBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response resp = getClient().listObjectsV2(reqBuilder.build());
            List<RemoteObject> files = resp.contents().stream()
                    .map(s3Obj -> new RemoteObject(
                            s3Obj.key(),
                            getRelativePathSafe(prefix, s3Obj.key()),
                            s3Obj.eTag(),
                            s3Obj.size(),
                            s3Obj.lastModified() != null ? s3Obj.lastModified().toEpochMilli() : 0L))
                    .collect(Collectors.toList());
            return new RemoteObjects(files, resp.isTruncated(),
                    resp.isTruncated() ? resp.nextContinuationToken() : null);
        } catch (SdkException e) {
            LOG.warn("Failed to listObjectsWithPrefix, fullPrefix={}", fullPrefix, e);
            throw new IOException("Failed to listObjectsWithPrefix: " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        requireBucket("headObjectWithMeta");
        String fullKey = normalizeAndCombinePrefix(prefix, subKey);
        try {
            HeadObjectResponse resp = getClient().headObject(
                    HeadObjectRequest.builder()
                            .bucket(bucket)
                            .key(fullKey)
                            .build());
            RemoteObject obj = new RemoteObject(
                    fullKey, getRelativePathSafe(prefix, fullKey), resp.eTag(), resp.contentLength(),
                    resp.lastModified() != null ? resp.lastModified().toEpochMilli() : 0L);
            return new RemoteObjects(Collections.singletonList(obj), false, null);
        } catch (NoSuchKeyException e) {
            LOG.warn("Key not found in headObjectWithMeta, key={}", fullKey);
            return new RemoteObjects(Collections.emptyList(), false, null);
        } catch (SdkException e) {
            LOG.warn("Failed to headObjectWithMeta, key={}", fullKey, e);
            throw new IOException("Failed to headObjectWithMeta: " + e.getMessage(), e);
        }
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        requireBucket("getPresignedUrl");
        String accessKey = properties.get(PROP_ACCESS_KEY);
        String secretKey = properties.get(PROP_SECRET_KEY);
        String region = properties.getOrDefault(PROP_REGION, "us-east-1");
        try {
            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKey)
                    .build();
            PutObjectPresignRequest presignReq = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofSeconds(SESSION_EXPIRE_SECONDS))
                    .putObjectRequest(putReq)
                    .build();
            AwsBasicCredentials cred = AwsBasicCredentials.create(accessKey, secretKey);
            try (S3Presigner presigner = S3Presigner.builder()
                    .region(Region.of(region))
                    .credentialsProvider(StaticCredentialsProvider.create(cred))
                    .build()) {
                PresignedPutObjectRequest presigned = presigner.presignPutObject(presignReq);
                LOG.info("Generated S3 presigned URL for key={}", objectKey);
                return presigned.url().toString();
            }
        } catch (SdkException e) {
            LOG.warn("Failed to generate S3 presigned URL for key={}", objectKey, e);
            throw new IOException("Failed to generate S3 presigned URL: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        // S3 DeleteObjects supports up to 1000 keys per request
        int batchSize = 1000;
        List<String> failedKeys = new ArrayList<>();
        try {
            for (int i = 0; i < keys.size(); i += batchSize) {
                List<String> batch = keys.subList(i, Math.min(i + batchSize, keys.size()));
                List<ObjectIdentifier> identifiers = batch.stream()
                        .map(k -> ObjectIdentifier.builder().key(k).build())
                        .collect(Collectors.toList());
                DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                        .bucket(bucket)
                        .delete(Delete.builder().objects(identifiers).quiet(true).build())
                        .build();
                DeleteObjectsResponse response = getClient().deleteObjects(request);
                if (response.hasErrors()) {
                    for (S3Error error : response.errors()) {
                        LOG.warn("Failed to delete object key={} from bucket={}: {} {}",
                                error.key(), bucket, error.code(), error.message());
                        failedKeys.add(error.key());
                    }
                }
            }
        } catch (SdkException e) {
            throw new IOException("Failed to batch delete objects from bucket=" + bucket + ": " + e.getMessage(), e);
        }
        if (!failedKeys.isEmpty()) {
            // Surface the full failure count and a bounded sample of failing keys in
            // the message so operators can correlate logs with the truncated list
            // without flooding the exception when DeleteObjects rejects many keys
            // at once (S3 batch up to 1000). The complete list is already at WARN
            // in the loop above; do not repeat it here.
            int sampleSize = Math.min(10, failedKeys.size());
            String sample = String.join(", ", failedKeys.subList(0, sampleSize));
            String suffix = failedKeys.size() > sampleSize
                    ? " (and " + (failedKeys.size() - sampleSize) + " more, see WARN log for full list)"
                    : "";
            throw new IOException("Failed to delete " + failedKeys.size() + " object(s) from bucket="
                    + bucket + "; failing keys [" + sample + "]" + suffix);
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private void requireBucket(String operation) throws IOException {
        if (bucket == null || bucket.isEmpty()) {
            throw new IOException(operation + " requires AWS_BUCKET to be configured");
        }
    }

    private static String normalizeAndCombinePrefix(String prefix, String subPrefix) {
        String normalized = (prefix == null || prefix.isEmpty()) ? ""
                : (prefix.endsWith("/") ? prefix : prefix + "/");
        if (subPrefix == null || subPrefix.isEmpty()) {
            return normalized;
        }
        return normalized.isEmpty() ? subPrefix : normalized + subPrefix;
    }

    private static String getRelativePathSafe(String prefix, String key) {
        String normalized = (prefix == null || prefix.isEmpty()) ? ""
                : (prefix.endsWith("/") ? prefix : prefix + "/");
        if (!key.startsWith(normalized)) {
            return key;
        }
        return key.substring(normalized.length());
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }
}
