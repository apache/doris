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

import org.apache.doris.filesystem.UploadPartResult;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.ObjectStorageUri;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.StsCredentials;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Object storage implementation backed by AWS S3 SDK v2.
 * The Map constructor binds through {@link S3FileSystemProperties} so all runtime
 * paths use the same typed S3 parameter model.
 */
public class S3ObjStorage implements ObjStorage<S3Client> {

    private static final Logger LOG = LogManager.getLogger(S3ObjStorage.class);
    private static final String DIRECTORY_BUCKET_SUFFIX = "--x-s3";
    private static final Map<String, String> S3_EXPRESS_REGIONS = Map.of(
            "use1", "us-east-1",
            "use2", "us-east-2",
            "usw2", "us-west-2",
            "aps1", "ap-south-1",
            "apne1", "ap-northeast-1",
            "euw1", "eu-west-1",
            "eun1", "eu-north-1");

    /** Validity period for pre-signed URLs and STS tokens (seconds). */
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final S3FileSystemProperties s3Properties;
    private final boolean usePathStyle;
    /** Bucket name; may be null if not provided (listObjectsWithPrefix and related methods will fail). */
    private final String bucket;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<String, S3Client> expressClients = new HashMap<>();
    private volatile S3Client client;

    public S3ObjStorage(S3FileSystemProperties properties) {
        this.s3Properties = properties;
        this.usePathStyle = properties.isUsePathStyle();
        this.bucket = properties.getBucket();
    }

    public S3ObjStorage(Map<String, String> properties) {
        this(S3FileSystemProperties.of(properties));
    }

    /**
     * Returns whether path-style (vs virtual-hosted-style) bucket access is enabled.
     * Used by {@link S3FileSystem} when parsing URIs that may be path-style
     * ({@code https://endpoint/bucket/key}) instead of virtual-hosted ({@code s3://bucket/key}).
     */
    public boolean isUsePathStyle() {
        return usePathStyle;
    }

    /** Returns the URI schemes this provider accepts (e.g. {@code {s3, s3a, s3n}}). */
    public Set<String> getSupportedSchemes() {
        return s3Properties.getSupportedSchemes();
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
        return buildClient(
                s3Properties.getEndpoint(),
                s3Properties.getRegion(),
                buildCredentialsProvider(), false);
    }

    protected S3Client buildExpressClient(String region) throws IOException {
        return buildClient(
                "",
                region,
                buildCredentialsProvider(region), true);
    }

    private S3Client buildClient(String endpointStr, String region,
            AwsCredentialsProvider clientCredentialsProvider, boolean express)
            throws IOException {
        S3ClientBuilder builder = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder()
                        .socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30))
                        .build())
                .credentialsProvider(clientCredentialsProvider)
                .region(Region.of(region))
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(express ? false : usePathStyle)
                        .build());
        if (express) {
            builder.disableS3ExpressSessionAuth(false);
        }

        if (!express && StringUtils.isNotBlank(endpointStr)) {
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
        return S3CredentialsProviderFactory.createClientProvider(s3Properties, this::buildStsClient);
    }

    protected AwsCredentialsProvider buildCredentialsProvider(String region) {
        return S3CredentialsProviderFactory.createClientProvider(
                s3Properties, (sourceCredentials, ignoredUserRegion) ->
                        buildStsClient(sourceCredentials, region));
    }

    boolean usesS3ExpressRead(String requestBucket) {
        return s3Properties.isScopedAwsS3ExpressImport()
                && getS3ExpressZoneId(requestBucket).isPresent();
    }

    private S3Client getExpressClient(String requestBucket) throws IOException {
        if (closed.get()) {
            throw new IOException("S3ObjStorage is already closed");
        }
        String region = getS3ExpressRegion(requestBucket);
        synchronized (expressClients) {
            if (closed.get()) {
                throw new IOException("S3ObjStorage is already closed");
            }
            S3Client expressClient = expressClients.get(region);
            if (expressClient == null) {
                expressClient = buildExpressClient(region);
                expressClients.put(region, expressClient);
            }
            return expressClient;
        }
    }

    private AwsCredentialsProvider buildStsSourceCredentialsProvider() {
        return S3CredentialsProviderFactory.createStsSourceProvider(s3Properties);
    }

    protected StsClient buildStsClient(AwsCredentialsProvider credentialsProvider, String region) {
        return StsClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException {
        return listObjectsWithOptions(remotePath, ObjectListOptions.builder()
                .continuationToken(continuationToken)
                .build());
    }

    @Override
    public RemoteObjects listObjectsWithOptions(String remotePath, ObjectListOptions options) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
        boolean expressRead = usesS3ExpressRead(uri.bucket());
        String requestPrefix = expressRead ? slashTerminatedPrefix(uri.key()) : uri.key();
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .bucket(uri.bucket());
        if (!expressRead || !requestPrefix.isEmpty()) {
            builder.prefix(requestPrefix);
        }
        if (options != null) {
            if (StringUtils.isNotBlank(options.continuationToken())) {
                builder.continuationToken(options.continuationToken());
            } else if (StringUtils.isNotBlank(options.startAfter())) {
                if (expressRead) {
                    throw new IOException("StartAfter is not supported for AWS Directory Bucket listings");
                }
                builder.startAfter(options.startAfter());
            }
            if (options.maxKeys() > 0) {
                builder.maxKeys(options.maxKeys());
            }
            if (StringUtils.isNotBlank(options.delimiter())) {
                builder.delimiter(options.delimiter());
            }
        }
        try {
            S3Client listClient = expressRead ? getExpressClient(uri.bucket()) : getClient();
            ListObjectsV2Response response = listClient.listObjectsV2(builder.build());
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
        } catch (S3Exception e) {
            if (expressRead) {
                throw new IOException("Create S3 Express session or list directory bucket failed for "
                        + remotePath + ": " + formatS3Exception(e), e);
            }
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        } catch (SdkException e) {
            if (expressRead) {
                throw new IOException("Create S3 Express session or list directory bucket failed for "
                        + remotePath + ": " + e.getMessage(), e);
            }
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    private static String formatS3Exception(S3Exception exception) {
        return "HTTP status=" + exception.statusCode()
                + ", AWS error code=" + exception.awsErrorDetails().errorCode()
                + ", message=" + exception.awsErrorDetails().errorMessage()
                + ", request ID=" + exception.requestId();
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
        return listObjectsWithOptions(remotePath, ObjectListOptions.builder()
                .continuationToken(continuationToken)
                .maxKeys(maxKeys)
                .build());
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
        return listObjectsWithOptions(remotePath, ObjectListOptions.builder()
                .continuationToken(continuationToken)
                .delimiter("/")
                .build());
    }

    @Override
    public org.apache.doris.filesystem.spi.RemoteObject headObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
        ObjectStorageUri srcUri = ObjectStorageUri.parse(srcPath, usePathStyle, getSupportedSchemes());
        ObjectStorageUri dstUri = ObjectStorageUri.parse(dstPath, usePathStyle, getSupportedSchemes());
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
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
            List<UploadPartResult> parts) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
    @Override
    public InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
    @Override
    public long headObjectLastModified(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, usePathStyle, getSupportedSchemes());
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
        if (StringUtils.isBlank(s3Properties.getRoleArn())) {
            throw new IOException("STS role ARN (AWS_ROLE_ARN) is not configured");
        }
        try {
            try (StsClient stsClient = buildStsClient(
                    buildStsSourceCredentialsProvider(), s3Properties.getRegion())) {
                AssumeRoleRequest.Builder reqBuilder = AssumeRoleRequest.builder()
                        .roleArn(s3Properties.getRoleArn())
                        .durationSeconds(SESSION_EXPIRE_SECONDS)
                        .roleSessionName("doris_" + UUID.randomUUID().toString().replace("-", ""));
                if (StringUtils.isNotBlank(s3Properties.getExternalId())) {
                    reqBuilder.externalId(s3Properties.getExternalId());
                }
                AssumeRoleResponse resp = stsClient.assumeRole(reqBuilder.build());
                Credentials cred = resp.credentials();
                return new StsCredentials(cred.accessKeyId(), cred.secretAccessKey(), cred.sessionToken());
            }
        } catch (Exception e) {
            LOG.warn("Failed to get STS token, roleArn={}", s3Properties.getRoleArn(), e);
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
            if (StringUtils.isNotBlank(continuationToken)) {
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
        try {
            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKey)
                    .build();
            PutObjectPresignRequest presignReq = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofSeconds(SESSION_EXPIRE_SECONDS))
                    .putObjectRequest(putReq)
                    .build();
            try (S3Presigner presigner = S3Presigner.builder()
                    .region(Region.of(s3Properties.getRegion()))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(s3Properties.getAccessKey(), s3Properties.getSecretKey())))
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
        if (StringUtils.isBlank(bucket)) {
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

    private static Optional<String> getS3ExpressZoneId(String bucketName) {
        if (bucketName == null || !bucketName.endsWith(DIRECTORY_BUCKET_SUFFIX)) {
            return Optional.empty();
        }
        String nameAndZone = bucketName.substring(
                0, bucketName.length() - DIRECTORY_BUCKET_SUFFIX.length());
        int zoneSeparator = nameAndZone.lastIndexOf("--");
        if (zoneSeparator <= 0 || zoneSeparator + 2 == nameAndZone.length()) {
            return Optional.empty();
        }
        String zoneId = nameAndZone.substring(zoneSeparator + 2);
        int azSeparator = zoneId.lastIndexOf("-az");
        if (azSeparator <= 0 || azSeparator + 3 == zoneId.length()) {
            return Optional.empty();
        }
        for (int i = 0; i < azSeparator; i++) {
            char c = zoneId.charAt(i);
            if (!(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '-') {
                return Optional.empty();
            }
        }
        for (int i = azSeparator + 3; i < zoneId.length(); i++) {
            char c = zoneId.charAt(i);
            if (c < '0' || c > '9') {
                return Optional.empty();
            }
        }
        return Optional.of(zoneId);
    }

    private String getS3ExpressRegion(String bucketName) throws IOException {
        String zoneId = getS3ExpressZoneId(bucketName).orElseThrow(
                () -> new IOException("Invalid AWS Directory Bucket name " + bucketName
                        + "; expected <bucket-base-name>--<zone-id>--x-s3"));
        String zonePrefix = zoneId.substring(0, zoneId.indexOf('-'));
        String inferredRegion = S3_EXPRESS_REGIONS.get(zonePrefix);
        if (inferredRegion != null) {
            return inferredRegion;
        }
        if (StringUtils.isBlank(s3Properties.getRegion())) {
            throw new IOException("Cannot infer AWS region from Directory Bucket zone " + zoneId
                    + "; set s3.region for newly introduced AWS regions");
        }
        return s3Properties.getRegion();
    }

    private static String slashTerminatedPrefix(String key) {
        if (key.isEmpty() || key.endsWith("/")) {
            return key;
        }
        int slash = key.lastIndexOf('/');
        return slash < 0 ? "" : key.substring(0, slash + 1);
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (client != null) {
                client.close();
                client = null;
            }
            synchronized (expressClients) {
                for (S3Client expressClient : expressClients.values()) {
                    expressClient.close();
                }
                expressClients.clear();
            }
        }
    }
}
