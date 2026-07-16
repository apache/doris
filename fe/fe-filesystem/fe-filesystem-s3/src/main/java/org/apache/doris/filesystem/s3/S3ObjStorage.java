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

import org.apache.doris.filesystem.S3BucketCapabilities;
import org.apache.doris.filesystem.UploadPartResult;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
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
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
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
import java.util.List;
import java.util.Map;
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

    /** Validity period for pre-signed URLs and STS tokens (seconds). */
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final S3FileSystemProperties s3Properties;
    private final boolean usePathStyle;
    /** Bucket name; may be null if not provided (listObjectsWithPrefix and related methods will fail). */
    private final String bucket;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile S3Client generalClient;
    private volatile S3Client directoryClient;
    private volatile AwsCredentialsProvider credentialsProvider;

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

    @Override
    public S3Client getClient() throws IOException {
        if (closed.get()) {
            throw new IOException("S3ObjStorage is already closed");
        }
        if (generalClient == null) {
            synchronized (this) {
                if (generalClient == null) {
                    generalClient = buildClient();
                }
            }
        }
        return generalClient;
    }

    protected S3Client buildClient() throws IOException {
        return buildClient(
                s3Properties.getEndpoint(),
                s3Properties.getRegion(),
                getCredentialsProvider(), false);
    }

    protected S3Client buildDirectoryClient() throws IOException {
        return buildClient(s3Properties.getEndpoint(), s3Properties.getRegion(),
                getCredentialsProvider(), true);
    }

    private S3Client buildClient(String endpointStr, String region,
            AwsCredentialsProvider clientCredentialsProvider, boolean directory)
            throws IOException {
        S3ClientBuilder builder = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder()
                        .socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30))
                        .build())
                .credentialsProvider(clientCredentialsProvider)
                .region(Region.of(region))
                .disableS3ExpressSessionAuth(!directory
                        && !S3BucketCapabilities.isAwsS3Endpoint(endpointStr))
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(directory ? false : usePathStyle)
                        .build());

        // Preserve the configured override for the general-purpose client. Directory Bucket
        // requests use the separate SDK-routed client and never enter this branch.
        if (!directory && StringUtils.isNotBlank(endpointStr)) {
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

    private AwsCredentialsProvider getCredentialsProvider() {
        if (credentialsProvider == null) {
            synchronized (this) {
                if (credentialsProvider == null) {
                    credentialsProvider = buildCredentialsProvider();
                }
            }
        }
        return credentialsProvider;
    }

    S3BucketCapabilities capabilitiesFor(String requestBucket) {
        return S3BucketCapabilities.resolve(requestBucket, s3Properties.getEndpoint());
    }

    boolean isDirectoryBucket(String requestBucket) {
        return capabilitiesFor(requestBucket).isDirectoryBucket();
    }

    private S3Client clientFor(String requestBucket) throws IOException {
        S3BucketCapabilities capabilities = capabilitiesFor(requestBucket);
        if (!capabilities.isDirectoryBucket()) {
            return getClient();
        }
        try {
            capabilities.validateDirectoryConfiguration(s3Properties.getEndpoint(),
                    s3Properties.getRegion(), usePathStyle);
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
        if (closed.get()) {
            throw new IOException("S3ObjStorage is already closed");
        }
        if (directoryClient == null) {
            synchronized (this) {
                if (directoryClient == null) {
                    directoryClient = buildDirectoryClient();
                }
            }
        }
        return directoryClient;
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
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        S3BucketCapabilities capabilities = capabilitiesFor(uri.bucket());
        String requestPrefix = capabilities.isDirectoryBucket()
                ? directoryListPrefix(uri.key()) : uri.key();
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .bucket(uri.bucket())
                .prefix(requestPrefix);
        if (options != null) {
            if (StringUtils.isNotBlank(options.continuationToken())) {
                builder.continuationToken(options.continuationToken());
            } else if (capabilities.supportsStartAfter()
                    && StringUtils.isNotBlank(options.startAfter())) {
                builder.startAfter(options.startAfter());
            }
            if (options.maxKeys() > 0) {
                builder.maxKeys(options.maxKeys());
            }
            if (StringUtils.isNotBlank(options.delimiter())) {
                if (capabilities.isDirectoryBucket() && !"/".equals(options.delimiter())) {
                    throw new IOException("AWS Directory Bucket only supports '/' as delimiter");
                }
                builder.delimiter(options.delimiter());
            }
        }
        try {
            List<org.apache.doris.filesystem.spi.RemoteObject> objects = new ArrayList<>();
            ListObjectsV2Response response = clientFor(uri.bucket()).listObjectsV2(builder.build());
            response.contents().stream()
                    .filter(s3Obj -> s3Obj.key().startsWith(uri.key()))
                    .map(s3Obj -> new org.apache.doris.filesystem.spi.RemoteObject(
                            s3Obj.key(),
                            getRelativePath(uri.key(), s3Obj.key()),
                            s3Obj.eTag(),
                            s3Obj.size(),
                            s3Obj.lastModified() != null
                                    ? s3Obj.lastModified().toEpochMilli() : 0L))
                    .forEach(objects::add);
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
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            HeadObjectResponse response = clientFor(uri.bucket()).headObject(
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
        S3BucketCapabilities capabilities = capabilitiesFor(uri.bucket());
        PutObjectRequest.Builder request = PutObjectRequest.builder()
                .bucket(uri.bucket()).key(uri.key());
        if (capabilities.checksumPolicy() == S3BucketCapabilities.ChecksumPolicy.CRC32C) {
            request.checksumAlgorithm(ChecksumAlgorithm.CRC32C);
        }
        try (InputStream content = requestBody.content()) {
            clientFor(uri.bucket()).putObject(
                    request.build(),
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
            clientFor(uri.bucket()).deleteObject(DeleteObjectRequest.builder()
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
        if (isDirectoryBucket(srcUri.bucket()) || isDirectoryBucket(dstUri.bucket())) {
            throw new IOException("CopyObject is not supported for AWS Directory Bucket");
        }
        try {
            clientFor(dstUri.bucket()).copyObject(CopyObjectRequest.builder()
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
        S3BucketCapabilities capabilities = capabilitiesFor(uri.bucket());
        CreateMultipartUploadRequest.Builder request = CreateMultipartUploadRequest.builder()
                .bucket(uri.bucket()).key(uri.key());
        if (capabilities.checksumPolicy() == S3BucketCapabilities.ChecksumPolicy.CRC32C) {
            request.checksumAlgorithm(ChecksumAlgorithm.CRC32C);
        }
        try {
            CreateMultipartUploadResponse response = clientFor(uri.bucket())
                    .createMultipartUpload(request.build());
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
        S3BucketCapabilities capabilities = capabilitiesFor(uri.bucket());
        UploadPartRequest.Builder request = UploadPartRequest.builder()
                .bucket(uri.bucket()).key(uri.key())
                .uploadId(uploadId).partNumber(partNum)
                .contentLength(body.contentLength());
        if (capabilities.checksumPolicy() == S3BucketCapabilities.ChecksumPolicy.CRC32C) {
            request.checksumAlgorithm(ChecksumAlgorithm.CRC32C);
        }
        try (InputStream content = body.content()) {
            UploadPartResponse response = clientFor(uri.bucket()).uploadPart(
                    request.build(),
                    software.amazon.awssdk.core.sync.RequestBody.fromInputStream(
                            content, body.contentLength()));
            if (capabilities.isDirectoryBucket() && StringUtils.isBlank(response.checksumCRC32C())) {
                throw new IOException("UploadPart response is missing CRC32C for AWS Directory Bucket, part="
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
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        S3BucketCapabilities capabilities = capabilitiesFor(uri.bucket());
        List<UploadPartResult> sortedParts = parts.stream()
                .sorted(java.util.Comparator.comparingInt(UploadPartResult::partNumber))
                .collect(Collectors.toList());
        List<CompletedPart> completedParts = new ArrayList<>(sortedParts.size());
        for (int i = 0; i < sortedParts.size(); i++) {
            UploadPartResult part = sortedParts.get(i);
            if (capabilities.isDirectoryBucket() && part.partNumber() != i + 1) {
                throw new IOException("Multipart upload parts must be consecutive from 1");
            }
            CompletedPart.Builder completed = CompletedPart.builder()
                    .partNumber(part.partNumber()).eTag(part.etag());
            if (capabilities.isDirectoryBucket()) {
                if (StringUtils.isBlank(part.checksumCrc32c())) {
                    throw new IOException(
                            "AWS Directory Bucket multipart completion requires CRC32C for every part");
                }
                completed.checksumCRC32C(part.checksumCrc32c());
            }
            completedParts.add(completed.build());
        }
        try {
            clientFor(uri.bucket()).completeMultipartUpload(CompleteMultipartUploadRequest.builder()
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
            clientFor(uri.bucket()).abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(uri.bucket()).key(uri.key()).uploadId(uploadId).build());
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return;
            }
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
            return clientFor(uri.bucket()).getObject(
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
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            GetObjectRequest.Builder req = GetObjectRequest.builder()
                    .bucket(uri.bucket()).key(uri.key());
            if (fromByte > 0) {
                req.range("bytes=" + fromByte + "-");
            }
            return clientFor(uri.bucket()).getObject(req.build());
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
        S3Uri uri = S3Uri.parse(remotePath, usePathStyle);
        try {
            HeadObjectResponse resp = clientFor(uri.bucket()).headObject(
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
        S3BucketCapabilities capabilities = capabilitiesFor(bucket);
        String requestPrefix = capabilities.isDirectoryBucket()
                ? directoryListPrefix(fullPrefix) : fullPrefix;
        try {
            ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(requestPrefix);
            if (StringUtils.isNotBlank(continuationToken)) {
                reqBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response resp = clientFor(bucket).listObjectsV2(reqBuilder.build());
            List<RemoteObject> files = resp.contents().stream()
                    .filter(s3Obj -> s3Obj.key().startsWith(fullPrefix))
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
            HeadObjectResponse resp = clientFor(bucket).headObject(
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
        if (!capabilitiesFor(bucket).supportsPresign()) {
            throw new IOException("Presigned URL is not supported for AWS Directory Bucket");
        }
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
                DeleteObjectsRequest.Builder request = DeleteObjectsRequest.builder()
                        .bucket(bucket)
                        .delete(Delete.builder().objects(identifiers).quiet(true).build());
                if (capabilitiesFor(bucket).checksumPolicy()
                        == S3BucketCapabilities.ChecksumPolicy.CRC32C) {
                    request.checksumAlgorithm(ChecksumAlgorithm.CRC32C);
                }
                DeleteObjectsResponse response = clientFor(bucket).deleteObjects(request.build());
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

    private static String directoryListPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty() || prefix.endsWith("/")) {
            return prefix == null ? "" : prefix;
        }
        int slash = prefix.lastIndexOf('/');
        return slash < 0 ? "" : prefix.substring(0, slash + 1);
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
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (generalClient != null) {
                generalClient.close();
            }
            if (directoryClient != null && directoryClient != generalClient) {
                directoryClient.close();
            }
            generalClient = null;
            directoryClient = null;
        }
    }
}
