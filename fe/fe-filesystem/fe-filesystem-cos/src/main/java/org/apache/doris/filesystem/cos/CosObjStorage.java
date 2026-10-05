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

package org.apache.doris.filesystem.cos;

import org.apache.doris.filesystem.UploadPartResult;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.ObjectStorageUri;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.StsCredentials;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.AnonymousCOSCredentials;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.exception.MultiObjectDeleteException;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.AbortMultipartUploadRequest;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.DeleteObjectsRequest;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadResult;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.region.Region;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.sts.v20180813.StsClient;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleRequest;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleResponse;
import com.tencentcloudapi.sts.v20180813.models.Credentials;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Tencent Cloud COS implementation backed by the native COS SDK.
 *
 * <p>This class consumes typed COS properties. Raw key aliases are resolved by
 * {@link CosFileSystemProperties}; client construction and authentication do not
 * translate through AWS-compatible keys.
 */
public class CosObjStorage implements ObjStorage<COSClient> {

    private static final Logger LOG = LogManager.getLogger(CosObjStorage.class);

    private static final int SESSION_EXPIRE_SECONDS = 3600;
    private static final int DELETE_BATCH_SIZE = 1000;

    private final CosFileSystemProperties properties;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile COSClient cosClient;

    public CosObjStorage(Map<String, String> properties) {
        this(CosFileSystemProperties.of(properties));
    }

    public CosObjStorage(CosFileSystemProperties properties) {
        this.properties = properties;
    }

    /** Whether path-style (vs virtual-hosted-style) bucket access is configured. */
    public boolean isUsePathStyle() {
        return properties.isUsePathStyle();
    }

    /** Returns the URI schemes this provider accepts (e.g. {@code {cos, cosn, s3, s3a}}). */
    public Set<String> getSupportedSchemes() {
        return properties.getSupportedSchemes();
    }

    @Override
    public COSClient getClient() throws IOException {
        if (closed.get()) {
            throw new IOException("CosObjStorage is already closed");
        }
        if (cosClient == null) {
            synchronized (this) {
                if (cosClient == null) {
                    cosClient = buildCosClient(properties.getRegion());
                }
            }
        }
        return cosClient;
    }

    protected COSClient buildCosClient(String region) throws IOException {
        COSCredentials cred = buildCredentials();
        ClientConfig clientConfig = new ClientConfig();
        // Note on use_path_style: unlike the S3/OSS/OBS SDKs, the native COS SDK has no
        // path-style addressing mode — it always renders the bucket as a host subdomain
        // (bucket-appid.<endpoint-suffix>) and puts only the object key in the request path
        // (see COSClient#buildUrlAndHost). There is therefore no client setting to apply here.
        // This is harmless for COS: bucket names are always of the form name-appid, which is a
        // valid DNS label, so virtual-hosted access always works. The use_path_style flag is
        // still honored where it matters — URI parsing in S3CompatibleFileSystem — so a
        // path-style URL supplied by the user is parsed into the correct (bucket, key) and the
        // request is then re-issued virtual-hosted to that same bucket.
        clientConfig.setRegion(new Region(region));
        clientConfig.setHttpProtocol(HttpProtocol.https);
        clientConfig.setEndPointSuffix(stripScheme(properties.getEndpoint()));
        clientConfig.setMaxConnectionsCount(parseIntProperty(properties.getMaxConnections(),
                "COS max connections"));
        clientConfig.setConnectionRequestTimeout(parseIntProperty(properties.getRequestTimeoutMs(),
                "COS request timeout"));
        clientConfig.setConnectionTimeout(parseIntProperty(properties.getConnectionTimeoutMs(),
                "COS connection timeout"));
        clientConfig.setSocketTimeout(parseIntProperty(properties.getRequestTimeoutMs(),
                "COS socket timeout"));
        return new COSClient(cred, clientConfig);
    }

    private COSCredentials buildCredentials() {
        if (!hasText(properties.getAccessKey()) && !hasText(properties.getSecretKey())) {
            return new AnonymousCOSCredentials();
        }
        if (hasText(properties.getSessionToken())) {
            return new BasicSessionCredentials(properties.getAccessKey(), properties.getSecretKey(),
                    properties.getSessionToken());
        }
        return new BasicCOSCredentials(properties.getAccessKey(), properties.getSecretKey());
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
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(uri.bucket());
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
        } catch (CosClientException e) {
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObject headObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            ObjectMetadata metadata = getClient().getObjectMetadata(uri.bucket(), uri.key());
            return new RemoteObject(uri.key(), uri.key(), metadata.getETag(), metadata.getContentLength(),
                    lastModifiedMs(metadata.getLastModified()));
        } catch (CosServiceException e) {
            if (isNotFound(e)) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        } catch (CosClientException e) {
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void putObject(String remotePath, RequestBody requestBody) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(requestBody.contentLength());
        try (InputStream content = requestBody.content()) {
            getClient().putObject(new PutObjectRequest(uri.bucket(), uri.key(), content, metadata));
        } catch (CosClientException e) {
            throw new IOException("putObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            getClient().deleteObject(uri.bucket(), uri.key());
        } catch (CosServiceException e) {
            if (isNotFound(e)) {
                return;
            }
            throw new IOException("deleteObject failed for " + remotePath + ": " + e.getMessage(), e);
        } catch (CosClientException e) {
            throw new IOException("deleteObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void copyObject(String srcPath, String dstPath) throws IOException {
        ObjectStorageUri src = ObjectStorageUri.parse(srcPath, isUsePathStyle(), getSupportedSchemes());
        ObjectStorageUri dst = ObjectStorageUri.parse(dstPath, isUsePathStyle(), getSupportedSchemes());
        try {
            getClient().copyObject(new CopyObjectRequest(
                    src.bucket(), src.key(), dst.bucket(), dst.key()));
        } catch (CosClientException e) {
            throw new IOException("copyObject from " + srcPath + " to " + dstPath
                    + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String initiateMultipartUpload(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            InitiateMultipartUploadResult result = getClient().initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(uri.bucket(), uri.key()));
            return result.getUploadId();
        } catch (CosClientException e) {
            throw new IOException("initiateMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try (InputStream content = body.content()) {
            com.qcloud.cos.model.UploadPartRequest request = new com.qcloud.cos.model.UploadPartRequest();
            request.setBucketName(uri.bucket());
            request.setKey(uri.key());
            request.setUploadId(uploadId);
            request.setPartNumber(partNum);
            request.setPartSize(body.contentLength());
            request.setInputStream(content);
            com.qcloud.cos.model.UploadPartResult result = getClient().uploadPart(request);
            return new UploadPartResult(partNum, result.getETag());
        } catch (CosClientException e) {
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
            getClient().completeMultipartUpload(new CompleteMultipartUploadRequest(
                    uri.bucket(), uri.key(), uploadId, partEtags));
        } catch (CosClientException e) {
            throw new IOException("completeMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, isUsePathStyle(), getSupportedSchemes());
        try {
            getClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                    uri.bucket(), uri.key(), uploadId));
        } catch (CosClientException e) {
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
            COSObject object = getClient().getObject(request);
            return object.getObjectContent();
        } catch (CosServiceException e) {
            if (isNotFound(e)) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("getObject failed for " + remotePath + ": " + e.getMessage(), e);
        } catch (CosClientException e) {
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
        String accessKey = requireProperty(properties.getAccessKey(), "COS_ACCESS_KEY", "COS access key");
        String secretKey = requireProperty(properties.getSecretKey(), "COS_SECRET_KEY", "COS secret key");
        String roleArn = requireProperty(properties.getRoleArn(), "COS_ROLE_ARN", "COS role ARN");
        try {
            Credential credential = new Credential(accessKey, secretKey);
            StsClient stsClient = new StsClient(credential, region);
            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setRoleArn(roleArn);
            request.setRoleSessionName("doris_" + java.util.UUID.randomUUID().toString().replace("-", ""));
            request.setDurationSeconds((long) SESSION_EXPIRE_SECONDS);
            AssumeRoleResponse response = stsClient.AssumeRole(request);
            Credentials credentials = response.getCredentials();
            return new StsCredentials(
                    credentials.getTmpSecretId(),
                    credentials.getTmpSecretKey(),
                    credentials.getToken());
        } catch (Exception e) {
            LOG.warn("Failed to get COS STS token, roleArn={}", properties.getRoleArn(), e);
            throw new IOException("Failed to get COS STS token: " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "COS_BUCKET", "COS bucket");
        String fullPrefix = normalizeAndCombinePrefix(prefix, subPrefix);
        return listObjects("cos://" + bucket + "/" + fullPrefix, continuationToken);
    }

    @Override
    public RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "COS_BUCKET", "COS bucket");
        String fullKey = normalizeAndCombinePrefix(prefix, subKey);
        try {
            RemoteObject object = headObject("cos://" + bucket + "/" + fullKey);
            return new RemoteObjects(Collections.singletonList(new RemoteObject(
                    object.getKey(), getRelativePathSafe(prefix, object.getKey()), object.getEtag(),
                    object.getSize(), object.getModificationTime())), false, null);
        } catch (FileNotFoundException e) {
            return new RemoteObjects(Collections.emptyList(), false, null);
        }
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "COS_BUCKET", "COS bucket for presigned URL");
        try {
            COSClient cos = getClient();
            Date expiration = new Date(System.currentTimeMillis() + (long) SESSION_EXPIRE_SECONDS * 1000);
            URL url = cos.generatePresignedUrl(bucket, objectKey, expiration, HttpMethodName.PUT,
                    new HashMap<>(), new HashMap<>());
            LOG.debug("Generated COS presigned URL for key={}", objectKey);
            return url.toString();
        } catch (CosClientException e) {
            LOG.warn("Failed to generate COS presigned URL for key={} in region={}",
                    objectKey, properties.getRegion(), e);
            throw new IOException("Failed to generate COS presigned URL: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        try {
            for (int i = 0; i < keys.size(); i += DELETE_BATCH_SIZE) {
                List<String> batch = keys.subList(i, Math.min(i + DELETE_BATCH_SIZE, keys.size()));
                DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
                request.setQuiet(true);
                request.setKeys(batch.stream()
                        .map(DeleteObjectsRequest.KeyVersion::new)
                        .collect(Collectors.toList()));
                getClient().deleteObjects(request);
            }
        } catch (MultiObjectDeleteException e) {
            List<String> failedKeys = e.getErrors().stream()
                    .map(MultiObjectDeleteException.DeleteError::getKey)
                    .collect(Collectors.toList());
            throw deleteFailure(bucket, failedKeys, e);
        } catch (CosClientException e) {
            throw new IOException("Failed to batch delete objects from bucket=" + bucket + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) && cosClient != null) {
            cosClient.shutdown();
            cosClient = null;
        }
    }

    private RemoteObject toRemoteObject(String prefix, COSObjectSummary object) {
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

    private static IOException deleteFailure(String bucket, List<String> failedKeys, Exception cause) {
        int sampleSize = Math.min(10, failedKeys.size());
        String sample = String.join(", ", failedKeys.subList(0, sampleSize));
        String suffix = failedKeys.size() > sampleSize
                ? " (and " + (failedKeys.size() - sampleSize) + " more, see WARN log for full list)"
                : "";
        return new IOException("Failed to delete " + failedKeys.size() + " object(s) from bucket="
                + bucket + "; failing keys [" + sample + "]" + suffix, cause);
    }

    private static boolean isNotFound(CosServiceException e) {
        return e.getStatusCode() == 404
                || "NoSuchKey".equals(e.getErrorCode())
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

    private static int parseIntProperty(String value, String description) throws IOException {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IOException(description + " must be an integer: " + value, e);
        }
    }

    private static String stripScheme(String endpoint) {
        if (endpoint.startsWith("https://")) {
            return endpoint.substring("https://".length());
        }
        if (endpoint.startsWith("http://")) {
            return endpoint.substring("http://".length());
        }
        return endpoint;
    }
}
