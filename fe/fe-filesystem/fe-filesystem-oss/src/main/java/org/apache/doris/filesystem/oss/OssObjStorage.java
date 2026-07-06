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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.UploadPartResult;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.ObjectStorageUri;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.StsCredentials;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyun.oss.common.utils.HttpHeaders;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.BasicCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.profile.DefaultProfile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Alibaba Cloud OSS implementation backed by the native OSS SDK.
 *
 * <p>This class consumes typed OSS properties. Raw key aliases are resolved by
 * {@link OssFileSystemProperties}; client construction and authentication do not
 * translate through AWS-compatible keys.
 */
public class OssObjStorage implements ObjStorage<OSS> {

    private static final Logger LOG = LogManager.getLogger(OssObjStorage.class);

    private static final int SESSION_EXPIRE_SECONDS = 3600;
    private static final int DELETE_BATCH_SIZE = 1000;
    private static final Credentials ANONYMOUS_CREDENTIALS =
            new DefaultCredentials("anonymous", "anonymous");
    // A bucket name that can serve as a virtual-hosted subdomain: 3-63 chars, lowercase
    // letters/digits/hyphens, must start and end with an alphanumeric.
    private static final java.util.regex.Pattern VIRTUAL_HOST_BUCKET_NAME =
            java.util.regex.Pattern.compile("[a-z0-9][a-z0-9-]{1,61}[a-z0-9]");

    private final OssFileSystemProperties properties;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile OSS ossClient;

    public OssObjStorage(Map<String, String> properties) {
        this(OssFileSystemProperties.of(properties));
    }

    public OssObjStorage(OssFileSystemProperties properties) {
        this.properties = properties;
    }

    /** Whether path-style (vs virtual-hosted-style) bucket access is explicitly configured. */
    public boolean isUsePathStyle() {
        return properties.isUsePathStyle();
    }

    /** Returns the URI schemes this provider accepts (e.g. {@code {oss, s3, s3a}}). */
    public Set<String> getSupportedSchemes() {
        return properties.getSupportedSchemes();
    }

    @Override
    public OSS getClient() throws IOException {
        return getClient(properties.getBucket());
    }

    /**
     * Returns the lazily-built OSS client, choosing path-style vs virtual-hosted addressing
     * based on {@code bucket} (see {@link #resolvePathStyle(String)}).
     *
     * <p>The addressing decision is made once, when the client is first built, from the first
     * bucket accessed through this instance. An {@code OssObjStorage} is scoped to a single
     * endpoint/bucket in practice (e.g. one backup repository), so this matches the per-bucket
     * behavior the legacy AWS-SDK-based client relied on for that case.
     */
    private OSS getClient(String bucket) throws IOException {
        if (closed.get()) {
            throw new IOException("OssObjStorage is already closed");
        }
        if (ossClient == null) {
            synchronized (this) {
                if (ossClient == null) {
                    ossClient = buildOssClient(resolvePathStyle(bucket));
                }
            }
        }
        return ossClient;
    }

    /**
     * Decides whether to use path-style (SLD) addressing for {@code bucket}.
     *
     * <p>Honors an explicit {@code use_path_style=true}, and otherwise falls back to path-style
     * when the bucket name cannot be expressed as a virtual-hosted DNS label (for example it
     * contains an underscore, which is illegal in a hostname). This mirrors the AWS SDK v2
     * behavior the legacy S3-compatible client relied on, so buckets with non-DNS-safe names
     * keep working instead of failing with a virtual-hosted {@code NoSuchBucket}. The native OSS
     * SDK does no such fallback on its own.
     *
     * <p>Note: the OSS SDK's own {@code validateBucketName} is intentionally not used here — it
     * accepts underscores (a legal object-storage name) even though such a name is not a legal
     * DNS host label, which is precisely the case that must trigger path-style.
     */
    boolean resolvePathStyle(String bucket) {
        if (properties.isUsePathStyle()) {
            return true;
        }
        return hasText(bucket) && !isVirtualHostCompatible(bucket);
    }

    /**
     * Returns true when {@code bucket} is a valid virtual-hosted DNS label: 3-63 characters of
     * lowercase letters, digits and hyphens, starting and ending with an alphanumeric. Names
     * with underscores, uppercase letters or dots cannot be safely used as a virtual-hosted
     * subdomain and therefore require path-style addressing.
     */
    private static boolean isVirtualHostCompatible(String bucket) {
        return VIRTUAL_HOST_BUCKET_NAME.matcher(bucket).matches();
    }

    protected OSS buildOssClient(boolean pathStyle) throws IOException {
        String endpoint = properties.getEndpoint();
        String accessKey = properties.getAccessKey();
        String secretKey = properties.getSecretKey();
        if (!hasText(accessKey)) {
            return new OSSClientBuilder().build(endpoint, anonymousCredentialsProvider(),
                    anonymousClientConfiguration(pathStyle));
        }
        ClientBuilderConfiguration config = clientConfiguration(pathStyle);
        String token = properties.getSessionToken();
        if (hasText(token)) {
            return new OSSClientBuilder().build(endpoint, accessKey, secretKey, token, config);
        }
        return new OSSClientBuilder().build(endpoint, accessKey, secretKey, config);
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException {
        return listObjectsWithOptions(remotePath, ObjectListOptions.builder()
                .continuationToken(continuationToken)
                .build());
    }

    @Override
    public RemoteObjects listObjectsWithOptions(String remotePath, ObjectListOptions options) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        ListObjectsRequest request = new ListObjectsRequest(uri.bucket());
        request.setPrefix(uri.key());
        if (options != null) {
            String marker = hasText(options.continuationToken())
                    ? options.continuationToken() : options.startAfter();
            if (hasText(marker)) {
                request.setMarker(marker);
            }
            if (options.maxKeys() > 0) {
                request.setMaxKeys(options.maxKeys());
            }
            if (hasText(options.delimiter())) {
                request.setDelimiter(options.delimiter());
            }
        }
        try {
            ObjectListing listing = getClient(uri.bucket()).listObjects(request);
            List<RemoteObject> objects = listing.getObjectSummaries().stream()
                    .map(obj -> toRemoteObject(uri.key(), obj))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, listing.isTruncated(), resolveNextMarker(listing));
        } catch (OSSException e) {
            // OSSException (server-side errors such as NoSuchBucket) is a sibling of
            // ClientException, not a subclass, so it must be caught explicitly or it would
            // propagate as an unwrapped runtime exception.
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        } catch (ClientException e) {
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObject headObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            ObjectMetadata metadata = getClient(uri.bucket()).getObjectMetadata(uri.bucket(), uri.key());
            return new RemoteObject(uri.key(), uri.key(), metadata.getETag(), metadata.getContentLength(),
                    lastModifiedMs(metadata.getLastModified()));
        } catch (OSSException e) {
            if (isNotFound(e)) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        } catch (ClientException e) {
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void putObject(String remotePath, RequestBody requestBody) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(requestBody.contentLength());
        try (InputStream content = requestBody.content()) {
            getClient(uri.bucket()).putObject(new PutObjectRequest(uri.bucket(), uri.key(), content, metadata));
        } catch (OSSException | ClientException e) {
            // OSSException (server-side error) is a sibling of ClientException, not a subclass,
            // so both must be caught or a server error would escape as an unchecked exception.
            throw new IOException("putObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            getClient(uri.bucket()).deleteObject(uri.bucket(), uri.key());
        } catch (OSSException e) {
            if (isNotFound(e)) {
                return;
            }
            throw new IOException("deleteObject failed for " + remotePath + ": " + e.getMessage(), e);
        } catch (ClientException e) {
            throw new IOException("deleteObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void copyObject(String srcPath, String dstPath) throws IOException {
        ObjectStorageUri src = ObjectStorageUri.parse(srcPath, isUsePathStyle(), getSupportedSchemes());
        ObjectStorageUri dst = ObjectStorageUri.parse(dstPath, isUsePathStyle(), getSupportedSchemes());
        try {
            getClient(src.bucket()).copyObject(new CopyObjectRequest(
                    src.bucket(), src.key(), dst.bucket(), dst.key()));
        } catch (OSSException | ClientException e) {
            throw new IOException("copyObject from " + srcPath + " to " + dstPath
                    + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String initiateMultipartUpload(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            InitiateMultipartUploadResult result = getClient(uri.bucket()).initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(uri.bucket(), uri.key()));
            return result.getUploadId();
        } catch (OSSException | ClientException e) {
            throw new IOException("initiateMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try (InputStream content = body.content()) {
            com.aliyun.oss.model.UploadPartRequest request = new com.aliyun.oss.model.UploadPartRequest();
            request.setBucketName(uri.bucket());
            request.setKey(uri.key());
            request.setUploadId(uploadId);
            request.setPartNumber(partNum);
            request.setPartSize(body.contentLength());
            request.setInputStream(content);
            com.aliyun.oss.model.UploadPartResult result = getClient(uri.bucket()).uploadPart(request);
            return new UploadPartResult(partNum, result.getETag());
        } catch (OSSException | ClientException e) {
            throw new IOException("uploadPart " + partNum + " failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        List<PartETag> partEtags = parts.stream()
                .map(part -> new PartETag(part.partNumber(), part.etag()))
                .collect(Collectors.toList());
        try {
            getClient(uri.bucket()).completeMultipartUpload(new CompleteMultipartUploadRequest(
                    uri.bucket(), uri.key(), uploadId, partEtags));
        } catch (OSSException | ClientException e) {
            throw new IOException("completeMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            getClient(uri.bucket()).abortMultipartUpload(new AbortMultipartUploadRequest(
                    uri.bucket(), uri.key(), uploadId));
        } catch (OSSException | ClientException e) {
            throw new IOException("abortMultipartUpload failed for " + remotePath
                    + " (uploadId=" + uploadId + "): " + e.getMessage(), e);
        }
    }

    @Override
    public InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            GetObjectRequest request = new GetObjectRequest(uri.bucket(), uri.key());
            if (fromByte > 0) {
                request.setRange(fromByte, -1);
            }
            OSSObject object = getClient(uri.bucket()).getObject(request);
            return object.getObjectContent();
        } catch (OSSException e) {
            if (isNotFound(e)) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("getObject failed for " + remotePath + ": " + e.getMessage(), e);
        } catch (ClientException e) {
            throw new IOException("getObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public long headObjectLastModified(String remotePath) throws IOException {
        return headObject(remotePath).getModificationTime();
    }

    @Override
    public StsCredentials getStsToken() throws IOException {
        String region = properties.getRegion();
        String accessKey = requireProperty(properties.getAccessKey(), "OSS_ACCESS_KEY", "OSS access key");
        String secretKey = requireProperty(properties.getSecretKey(), "OSS_SECRET_KEY", "OSS secret key");
        String roleArn = requireProperty(properties.getRoleArn(), "OSS_ROLE_ARN", "OSS role ARN");
        try {
            DefaultProfile profile = DefaultProfile.getProfile(region);
            BasicCredentials basicCredentials = new BasicCredentials(accessKey, secretKey);
            DefaultAcsClient ramClient =
                    new DefaultAcsClient(profile, new StaticCredentialsProvider(basicCredentials));
            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setRoleArn(roleArn);
            request.setRoleSessionName("doris_" + java.util.UUID.randomUUID().toString().replace("-", ""));
            request.setDurationSeconds((long) SESSION_EXPIRE_SECONDS);
            AssumeRoleResponse response = ramClient.getAcsResponse(request);
            AssumeRoleResponse.Credentials credentials = response.getCredentials();
            return new StsCredentials(
                    credentials.getAccessKeyId(),
                    credentials.getAccessKeySecret(),
                    credentials.getSecurityToken());
        } catch (Exception e) {
            LOG.warn("Failed to get OSS STS token, roleArn={}", properties.getRoleArn(), e);
            throw new IOException("Failed to get OSS STS token: " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "OSS_BUCKET", "OSS bucket");
        String fullPrefix = normalizeAndCombinePrefix(prefix, subPrefix);
        return listObjects("oss://" + bucket + "/" + fullPrefix, continuationToken);
    }

    @Override
    public RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "OSS_BUCKET", "OSS bucket");
        String fullKey = normalizeAndCombinePrefix(prefix, subKey);
        try {
            RemoteObject object = headObject("oss://" + bucket + "/" + fullKey);
            return new RemoteObjects(Collections.singletonList(new RemoteObject(
                    object.getKey(), getRelativePathSafe(prefix, object.getKey()), object.getEtag(),
                    object.getSize(), object.getModificationTime())), false, null);
        } catch (FileNotFoundException e) {
            return new RemoteObjects(Collections.emptyList(), false, null);
        }
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "OSS_BUCKET", "OSS bucket for presigned URL");
        try {
            Date expiration = new Date(System.currentTimeMillis() + (long) SESSION_EXPIRE_SECONDS * 1000);
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket, objectKey, HttpMethod.PUT);
            request.setExpiration(expiration);
            URL signedUrl = getClient(bucket).generatePresignedUrl(request);
            LOG.debug("Generated OSS presigned URL for key={}", objectKey);
            return signedUrl.toString();
        } catch (OSSException | ClientException e) {
            LOG.warn("Failed to generate OSS presigned URL for key={}", objectKey, e);
            throw new IOException("Failed to generate OSS presigned URL: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        try {
            for (int i = 0; i < keys.size(); i += DELETE_BATCH_SIZE) {
                List<String> batch = keys.subList(i, Math.min(i + DELETE_BATCH_SIZE, keys.size()));
                DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
                request.setQuiet(true);
                request.setKeys(batch);
                getClient(bucket).deleteObjects(request);
            }
        } catch (OSSException | ClientException e) {
            throw new IOException("Failed to batch delete objects from bucket=" + bucket + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) && ossClient != null) {
            ossClient.shutdown();
            ossClient = null;
        }
    }

    private RemoteObject toRemoteObject(String prefix, OSSObjectSummary object) {
        return new RemoteObject(
                object.getKey(),
                getRelativePathSafe(prefix, object.getKey()),
                object.getETag(),
                object.getSize(),
                lastModifiedMs(object.getLastModified()));
    }

    private static String requireProperty(String value, String key, String description) throws IOException {
        if (!hasText(value)) {
            throw new IOException(description + " is required; set " + key + " in properties");
        }
        return value;
    }

    /**
     * Computes the marker that resumes listing on the next page.
     *
     * <p>OSS (like the S3 V1 {@code GetBucket} API) only fills
     * {@link ObjectListing#getNextMarker()} when a {@code delimiter} is set. For a delimiter-less
     * (recursive) listing the marker is {@code null} even when {@link ObjectListing#isTruncated()}
     * is {@code true}, so naively forwarding it would stop pagination after the first page (~1000
     * keys) and silently drop every object beyond it (recursive delete/rename/list).
     *
     * <p>When the SDK leaves the marker blank on a truncated page, fall back to the
     * lexicographically greatest key/common-prefix returned on this page — OSS lists in
     * lexicographic order and resumes strictly after the marker. This mirrors the last-key
     * fallback the COS/OBS SDKs perform internally.
     *
     * <p>If a page is marked truncated yet yields neither a marker nor any key/common-prefix to
     * resume from, there is no cursor for the next page; returning {@code null} would make the
     * paginating caller re-list from the start indefinitely, so this fails loudly instead.
     */
    private static String resolveNextMarker(ObjectListing listing) throws IOException {
        if (!listing.isTruncated()) {
            return null;
        }
        String nextMarker = listing.getNextMarker();
        if (hasText(nextMarker)) {
            return nextMarker;
        }
        String marker = null;
        List<OSSObjectSummary> summaries = listing.getObjectSummaries();
        if (summaries != null && !summaries.isEmpty()) {
            marker = summaries.get(summaries.size() - 1).getKey();
        }
        List<String> commonPrefixes = listing.getCommonPrefixes();
        if (commonPrefixes != null && !commonPrefixes.isEmpty()) {
            String lastPrefix = commonPrefixes.get(commonPrefixes.size() - 1);
            if (marker == null || lastPrefix.compareTo(marker) > 0) {
                marker = lastPrefix;
            }
        }
        if (marker == null) {
            throw new IOException("OSS reported a truncated listing but returned no marker and no "
                    + "keys to resume from; cannot paginate without looping from the start");
        }
        return marker;
    }

    private static boolean isNotFound(OSSException e) {
        return "NoSuchKey".equals(e.getErrorCode())
                || "NoSuchBucket".equals(e.getErrorCode());
    }

    private static long lastModifiedMs(Date lastModified) {
        return lastModified == null ? 0L : lastModified.getTime();
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

    private static boolean hasText(String value) {
        return value != null && !value.isEmpty();
    }

    private static CredentialsProvider anonymousCredentialsProvider() {
        return new CredentialsProvider() {
            @Override
            public void setCredentials(Credentials credentials) {
            }

            @Override
            public Credentials getCredentials() {
                return ANONYMOUS_CREDENTIALS;
            }
        };
    }

    private static ClientBuilderConfiguration clientConfiguration(boolean pathStyle) {
        ClientBuilderConfiguration config = new ClientBuilderConfiguration();
        // SLD (second-level-domain) access is the OSS SDK's name for path-style addressing.
        config.setSLDEnabled(pathStyle);
        return config;
    }

    static ClientBuilderConfiguration anonymousClientConfiguration(boolean pathStyle) {
        ClientBuilderConfiguration config = clientConfiguration(pathStyle);
        config.setSignerHandlers(Collections.singletonList(request -> {
            request.getHeaders().remove(HttpHeaders.AUTHORIZATION);
            request.getHeaders().remove(OSSHeaders.OSS_SECURITY_TOKEN);
        }));
        return config;
    }
}
