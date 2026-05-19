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

import org.apache.doris.filesystem.spi.ObjectStorageUri;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.StsCredentials;
import org.apache.doris.filesystem.spi.UploadPartResult;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Alibaba Cloud OSS implementation backed by the native OSS SDK.
 */
public class OssObjStorage implements ObjStorage<OSS> {

    private static final Logger LOG = LogManager.getLogger(OssObjStorage.class);

    private static final int SESSION_EXPIRE_SECONDS = 3600;
    private static final int DELETE_BATCH_SIZE = 1000;
    private static final Pattern STANDARD_ENDPOINT_PATTERN =
            Pattern.compile("^(?:https?://)?(?:s3\\.)?oss-([a-z0-9-]+?)(?:-internal)?\\.aliyuncs\\.com$");

    private final Map<String, String> ossProperties;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile OSS ossClient;

    public OssObjStorage(Map<String, String> properties) {
        this.ossProperties = new HashMap<>(properties);
        normalizeEndpointAndRegion();
    }

    /**
     * Translates OSS-specific property keys to AWS-compatible keys for legacy callers.
     * If both forms are present, the AWS_* key takes precedence.
     */
    static Map<String, String> toS3Props(Map<String, String> ossProps) {
        Map<String, String> s3Props = new HashMap<>(ossProps);
        if (ossProps.containsKey("OSS_ENDPOINT") && !ossProps.containsKey("AWS_ENDPOINT")) {
            s3Props.put("AWS_ENDPOINT", ossProps.get("OSS_ENDPOINT"));
        }
        if (ossProps.containsKey("OSS_ACCESS_KEY") && !ossProps.containsKey("AWS_ACCESS_KEY")) {
            s3Props.put("AWS_ACCESS_KEY", ossProps.get("OSS_ACCESS_KEY"));
        }
        if (ossProps.containsKey("OSS_SECRET_KEY") && !ossProps.containsKey("AWS_SECRET_KEY")) {
            s3Props.put("AWS_SECRET_KEY", ossProps.get("OSS_SECRET_KEY"));
        }
        if (ossProps.containsKey("OSS_TOKEN") && !ossProps.containsKey("AWS_TOKEN")) {
            s3Props.put("AWS_TOKEN", ossProps.get("OSS_TOKEN"));
        }
        if (ossProps.containsKey("OSS_BUCKET") && !ossProps.containsKey("AWS_BUCKET")) {
            s3Props.put("AWS_BUCKET", ossProps.get("OSS_BUCKET"));
        }
        if (ossProps.containsKey("OSS_REGION") && !ossProps.containsKey("AWS_REGION")) {
            s3Props.put("AWS_REGION", ossProps.get("OSS_REGION"));
        }
        if (ossProps.containsKey("OSS_ROLE_ARN") && !ossProps.containsKey("AWS_ROLE_ARN")) {
            s3Props.put("AWS_ROLE_ARN", ossProps.get("OSS_ROLE_ARN"));
        }
        s3Props.put("use_path_style", "false");
        return s3Props;
    }

    @Override
    public OSS getClient() throws IOException {
        if (closed.get()) {
            throw new IOException("OssObjStorage is already closed");
        }
        if (ossClient == null) {
            synchronized (this) {
                if (ossClient == null) {
                    ossClient = buildOssClient();
                }
            }
        }
        return ossClient;
    }

    protected OSS buildOssClient() throws IOException {
        String endpoint = resolveRequired("OSS_ENDPOINT", "AWS_ENDPOINT", "OSS endpoint");
        String accessKey = resolveRequired("OSS_ACCESS_KEY", "AWS_ACCESS_KEY", "OSS access key");
        String secretKey = resolveRequired("OSS_SECRET_KEY", "AWS_SECRET_KEY", "OSS secret key");
        String token = resolveOpt("OSS_TOKEN", "AWS_TOKEN");
        if (hasText(token)) {
            return new OSSClientBuilder().build(endpoint, accessKey, secretKey, token);
        }
        return new OSSClientBuilder().build(endpoint, accessKey, secretKey);
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException {
        return listObjectsWithOptions(remotePath, ObjectListOptions.builder()
                .continuationToken(continuationToken)
                .build());
    }

    @Override
    public RemoteObjects listObjectsWithOptions(String remotePath, ObjectListOptions options) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
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
            ObjectListing listing = getClient().listObjects(request);
            List<RemoteObject> objects = listing.getObjectSummaries().stream()
                    .map(obj -> toRemoteObject(uri.key(), obj))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, listing.isTruncated(),
                    listing.isTruncated() ? listing.getNextMarker() : null);
        } catch (ClientException e) {
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObject headObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try {
            ObjectMetadata metadata = getClient().getObjectMetadata(uri.bucket(), uri.key());
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
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(requestBody.contentLength());
        try (InputStream content = requestBody.content()) {
            getClient().putObject(new PutObjectRequest(uri.bucket(), uri.key(), content, metadata));
        } catch (ClientException e) {
            throw new IOException("putObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try {
            getClient().deleteObject(uri.bucket(), uri.key());
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
        ObjectStorageUri src = ObjectStorageUri.parse(srcPath, false);
        ObjectStorageUri dst = ObjectStorageUri.parse(dstPath, false);
        try {
            getClient().copyObject(new CopyObjectRequest(
                    src.bucket(), src.key(), dst.bucket(), dst.key()));
        } catch (ClientException e) {
            throw new IOException("copyObject from " + srcPath + " to " + dstPath
                    + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String initiateMultipartUpload(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try {
            InitiateMultipartUploadResult result = getClient().initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(uri.bucket(), uri.key()));
            return result.getUploadId();
        } catch (ClientException e) {
            throw new IOException("initiateMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try (InputStream content = body.content()) {
            com.aliyun.oss.model.UploadPartRequest request = new com.aliyun.oss.model.UploadPartRequest();
            request.setBucketName(uri.bucket());
            request.setKey(uri.key());
            request.setUploadId(uploadId);
            request.setPartNumber(partNum);
            request.setPartSize(body.contentLength());
            request.setInputStream(content);
            com.aliyun.oss.model.UploadPartResult result = getClient().uploadPart(request);
            return new UploadPartResult(partNum, result.getETag());
        } catch (ClientException e) {
            throw new IOException("uploadPart " + partNum + " failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        List<PartETag> partEtags = parts.stream()
                .map(part -> new PartETag(part.partNumber(), part.etag()))
                .collect(Collectors.toList());
        try {
            getClient().completeMultipartUpload(new CompleteMultipartUploadRequest(
                    uri.bucket(), uri.key(), uploadId, partEtags));
        } catch (ClientException e) {
            throw new IOException("completeMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try {
            getClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                    uri.bucket(), uri.key(), uploadId));
        } catch (ClientException e) {
            throw new IOException("abortMultipartUpload failed for " + remotePath
                    + " (uploadId=" + uploadId + "): " + e.getMessage(), e);
        }
    }

    @Override
    public InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try {
            GetObjectRequest request = new GetObjectRequest(uri.bucket(), uri.key());
            if (fromByte > 0) {
                request.setRange(fromByte, -1);
            }
            OSSObject object = getClient().getObject(request);
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
        String region = resolveRequired("OSS_REGION", "AWS_REGION", "OSS region for STS");
        String accessKey = resolveRequired("OSS_ACCESS_KEY", "AWS_ACCESS_KEY", "OSS access key");
        String secretKey = resolveRequired("OSS_SECRET_KEY", "AWS_SECRET_KEY", "OSS secret key");
        String roleArn = resolveRequired("OSS_ROLE_ARN", "AWS_ROLE_ARN", "OSS role ARN");
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
            LOG.warn("Failed to get OSS STS token, roleArn={}", resolveOpt("OSS_ROLE_ARN", "AWS_ROLE_ARN"), e);
            throw new IOException("Failed to get OSS STS token: " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        String bucket = resolveRequired("OSS_BUCKET", "AWS_BUCKET", "OSS bucket");
        String fullPrefix = normalizeAndCombinePrefix(prefix, subPrefix);
        return listObjects("oss://" + bucket + "/" + fullPrefix, continuationToken);
    }

    @Override
    public RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        String bucket = resolveRequired("OSS_BUCKET", "AWS_BUCKET", "OSS bucket");
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
        String bucket = resolveRequired("OSS_BUCKET", "AWS_BUCKET", "OSS bucket for presigned URL");
        try {
            Date expiration = new Date(System.currentTimeMillis() + (long) SESSION_EXPIRE_SECONDS * 1000);
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket, objectKey, HttpMethod.PUT);
            request.setExpiration(expiration);
            URL signedUrl = getClient().generatePresignedUrl(request);
            LOG.info("Generated OSS presigned URL for key={}", objectKey);
            return signedUrl.toString();
        } catch (ClientException e) {
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
                getClient().deleteObjects(request);
            }
        } catch (ClientException e) {
            throw new IOException("Failed to batch delete objects from bucket=" + bucket + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(ossProperties);
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

    private String resolveOpt(String primaryKey, String fallbackKey) {
        String value = ossProperties.get(primaryKey);
        if (hasText(value)) {
            return value;
        }
        return fallbackKey == null ? null : ossProperties.get(fallbackKey);
    }

    private String resolveRequired(String primaryKey, String fallbackKey, String description)
            throws IOException {
        String value = resolveOpt(primaryKey, fallbackKey);
        if (!hasText(value)) {
            throw new IOException(description + " is required; set " + primaryKey + " in properties");
        }
        return value;
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

    private void normalizeEndpointAndRegion() {
        String region = resolveOpt("OSS_REGION", "AWS_REGION");
        String endpoint = resolveOpt("OSS_ENDPOINT", "AWS_ENDPOINT");
        if (!hasText(region) && hasText(endpoint)) {
            region = extractRegionFromEndpoint(endpoint);
            if (hasText(region)) {
                ossProperties.put("OSS_REGION", region);
            }
        }
        if (!hasText(endpoint) && hasText(region)) {
            ossProperties.put("OSS_ENDPOINT", "oss-" + region + "-internal.aliyuncs.com");
        }
    }

    private static String extractRegionFromEndpoint(String endpoint) {
        Matcher matcher = STANDARD_ENDPOINT_PATTERN.matcher(endpoint.toLowerCase(Locale.ROOT));
        return matcher.matches() ? matcher.group(1) : null;
    }
}
