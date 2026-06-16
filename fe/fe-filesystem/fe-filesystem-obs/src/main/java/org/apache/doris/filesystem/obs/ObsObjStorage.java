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

package org.apache.doris.filesystem.obs;

import org.apache.doris.filesystem.UploadPartResult;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.ObjectStorageUri;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.StsCredentials;

import com.huaweicloud.sdk.core.auth.GlobalCredentials;
import com.huaweicloud.sdk.core.auth.ICredential;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.AgencyAuth;
import com.huaweicloud.sdk.iam.v3.model.AgencyAuthIdentity;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyRequest;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyRequestBody;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.huaweicloud.sdk.iam.v3.model.IdentityAssumerole;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AbortMultipartUploadRequest;
import com.obs.services.model.CompleteMultipartUploadRequest;
import com.obs.services.model.CopyObjectRequest;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.InitiateMultipartUploadRequest;
import com.obs.services.model.InitiateMultipartUploadResult;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PartEtag;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.TemporarySignatureRequest;
import com.obs.services.model.TemporarySignatureResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Huawei Cloud OBS implementation backed by the native OBS SDK.
 *
 * <p>This class consumes typed OBS properties. Raw key aliases are resolved by
 * {@link ObsFileSystemProperties}; client construction and authentication do not
 * translate through AWS-compatible keys.
 */
public class ObsObjStorage implements ObjStorage<ObsClient> {

    private static final Logger LOG = LogManager.getLogger(ObsObjStorage.class);

    private static final int SESSION_EXPIRE_SECONDS = 3600;
    private static final int DELETE_BATCH_SIZE = 1000;

    private final ObsFileSystemProperties properties;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile ObsClient client;

    public ObsObjStorage(Map<String, String> properties) {
        this(ObsFileSystemProperties.of(properties));
    }

    public ObsObjStorage(ObsFileSystemProperties properties) {
        this.properties = properties;
    }

    /** Whether path-style (vs virtual-hosted-style) bucket access is configured. */
    public boolean isUsePathStyle() {
        return properties.isUsePathStyle();
    }

    @Override
    public ObsClient getClient() throws IOException {
        if (closed.get()) {
            throw new IOException("ObsObjStorage is already closed");
        }
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = buildObsClient(properties.getEndpoint(), properties.getAccessKey(),
                            properties.getSecretKey());
                }
            }
        }
        return client;
    }

    /**
     * Factory method for creating an {@link ObsClient}; protected for testability.
     */
    protected ObsClient buildObsClient(String endpoint, String accessKey, String secretKey) {
        ObsConfiguration config = buildObsConfiguration(endpoint);
        if (!hasText(accessKey) && !hasText(secretKey)) {
            return new ObsClient(config);
        }
        if (hasText(properties.getSessionToken())) {
            return new ObsClient(accessKey, secretKey, properties.getSessionToken(), config);
        }
        return new ObsClient(accessKey, secretKey, config);
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
            List<RemoteObject> objects = listing.getObjects().stream()
                    .map(obj -> toRemoteObject(uri.key(), obj))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, listing.isTruncated(),
                    listing.isTruncated() ? listing.getNextMarker() : null);
        } catch (ObsException e) {
            throw new IOException("Failed to list objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObject headObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try {
            ObjectMetadata metadata = getClient().getObjectMetadata(uri.bucket(), uri.key());
            return new RemoteObject(uri.key(), uri.key(), metadata.getEtag(), contentLength(metadata),
                    lastModifiedMs(metadata.getLastModified()));
        } catch (ObsException e) {
            if (isNotFound(e)) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void putObject(String remotePath, RequestBody requestBody) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try (InputStream content = requestBody.content()) {
            PutObjectRequest request = new PutObjectRequest(uri.bucket(), uri.key(), content);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(requestBody.contentLength());
            request.setMetadata(metadata);
            request.setAutoClose(false);
            getClient().putObject(request);
        } catch (ObsException e) {
            throw new IOException("putObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try {
            getClient().deleteObject(uri.bucket(), uri.key());
        } catch (ObsException e) {
            if (isNotFound(e)) {
                return;
            }
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
        } catch (ObsException e) {
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
        } catch (ObsException e) {
            throw new IOException("initiateMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        try (InputStream content = body.content()) {
            com.obs.services.model.UploadPartRequest request =
                    new com.obs.services.model.UploadPartRequest(uri.bucket(), uri.key());
            request.setUploadId(uploadId);
            request.setPartNumber(partNum);
            request.setPartSize(body.contentLength());
            request.setInput(content);
            request.setAutoClose(false);
            com.obs.services.model.UploadPartResult result = getClient().uploadPart(request);
            return new UploadPartResult(partNum, result.getEtag());
        } catch (ObsException e) {
            throw new IOException("uploadPart " + partNum + " failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException {
        ObjectStorageUri uri = ObjectStorageUri.parse(remotePath, false);
        List<PartEtag> partEtags = parts.stream()
                .map(part -> new PartEtag(part.etag(), part.partNumber()))
                .collect(Collectors.toList());
        try {
            getClient().completeMultipartUpload(new CompleteMultipartUploadRequest(
                    uri.bucket(), uri.key(), uploadId, partEtags));
        } catch (ObsException e) {
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
        } catch (ObsException e) {
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
                request.setRangeStart(fromByte);
            }
            ObsObject object = getClient().getObject(request);
            return object.getObjectContent();
        } catch (ObsException e) {
            if (isNotFound(e)) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
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
        String accessKey = requireProperty(properties.getAccessKey(), "OBS_ACCESS_KEY", "OBS access key");
        String secretKey = requireProperty(properties.getSecretKey(), "OBS_SECRET_KEY", "OBS secret key");
        String agencyName = requireProperty(properties.getAgencyName(), "OBS_AGENCY_NAME", "OBS agency name for STS");
        String domainName = requireProperty(properties.getDomainName(), "OBS_DOMAIN_NAME", "OBS domain name for STS");
        try {
            ICredential auth = new GlobalCredentials().withAk(accessKey).withSk(secretKey);
            IamClient client = IamClient.newBuilder()
                    .withEndpoint("iam." + region + ".myhuaweicloud.com")
                    .withCredential(auth)
                    .build();
            IdentityAssumerole assumeRoleIdentity = new IdentityAssumerole();
            assumeRoleIdentity.withAgencyName(agencyName)
                    .withDomainName(domainName)
                    .withDurationSeconds(SESSION_EXPIRE_SECONDS);
            List<AgencyAuthIdentity.MethodsEnum> methods = new ArrayList<>();
            methods.add(AgencyAuthIdentity.MethodsEnum.fromValue("assume_role"));
            AgencyAuthIdentity identityAuth = new AgencyAuthIdentity();
            identityAuth.withMethods(methods).withAssumeRole(assumeRoleIdentity);
            AgencyAuth authBody = new AgencyAuth();
            authBody.withIdentity(identityAuth);
            CreateTemporaryAccessKeyByAgencyRequestBody body =
                    new CreateTemporaryAccessKeyByAgencyRequestBody().withAuth(authBody);
            CreateTemporaryAccessKeyByAgencyRequest request =
                    new CreateTemporaryAccessKeyByAgencyRequest().withBody(body);
            CreateTemporaryAccessKeyByAgencyResponse response =
                    client.createTemporaryAccessKeyByAgency(request);
            Credential credential = response.getCredential();
            return new StsCredentials(
                    credential.getAccess(),
                    credential.getSecret(),
                    credential.getSecuritytoken());
        } catch (Exception e) {
            LOG.warn("Failed to get OBS STS token, agencyName={}", agencyName, e);
            throw new IOException("Failed to get OBS STS token: " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "OBS_BUCKET", "OBS bucket");
        String fullPrefix = normalizeAndCombinePrefix(prefix, subPrefix);
        return listObjects("obs://" + bucket + "/" + fullPrefix, continuationToken);
    }

    @Override
    public RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "OBS_BUCKET", "OBS bucket");
        String fullKey = normalizeAndCombinePrefix(prefix, subKey);
        try {
            RemoteObject object = headObject("obs://" + bucket + "/" + fullKey);
            return new RemoteObjects(Collections.singletonList(new RemoteObject(
                    object.getKey(), getRelativePathSafe(prefix, object.getKey()), object.getEtag(),
                    object.getSize(), object.getModificationTime())), false, null);
        } catch (FileNotFoundException e) {
            return new RemoteObjects(Collections.emptyList(), false, null);
        }
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String bucket = requireProperty(properties.getBucket(), "OBS_BUCKET", "OBS bucket for presigned URL");
        try {
            TemporarySignatureRequest request = new TemporarySignatureRequest(
                    HttpMethodEnum.PUT, SESSION_EXPIRE_SECONDS);
            request.setBucketName(bucket);
            request.setObjectKey(objectKey);
            request.setHeaders(new HashMap<>());
            TemporarySignatureResponse response = getClient().createTemporarySignature(request);
            String url = response.getSignedUrl();
            LOG.info("Generated OBS temporary signature URL for key={}", objectKey);
            return url;
        } catch (ObsException e) {
            throw new IOException("Failed to generate OBS presigned URL: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        List<String> failedKeys = new ArrayList<>();
        try {
            for (int i = 0; i < keys.size(); i += DELETE_BATCH_SIZE) {
                List<String> batch = keys.subList(i, Math.min(i + DELETE_BATCH_SIZE, keys.size()));
                DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
                request.setQuiet(true);
                for (String key : batch) {
                    request.addKeyAndVersion(key);
                }
                DeleteObjectsResult result = getClient().deleteObjects(request);
                for (DeleteObjectsResult.ErrorResult error : result.getErrorResults()) {
                    LOG.warn("Failed to delete OBS object key={} from bucket={}: {} {}",
                            error.getObjectKey(), bucket, error.getErrorCode(), error.getMessage());
                    failedKeys.add(error.getObjectKey());
                }
            }
        } catch (ObsException e) {
            throw new IOException("Failed to batch delete objects from bucket=" + bucket + ": " + e.getMessage(), e);
        }
        if (!failedKeys.isEmpty()) {
            int sampleSize = Math.min(10, failedKeys.size());
            String sample = String.join(", ", failedKeys.subList(0, sampleSize));
            String suffix = failedKeys.size() > sampleSize
                    ? " (and " + (failedKeys.size() - sampleSize) + " more, see WARN log for full list)"
                    : "";
            throw new IOException("Failed to delete " + failedKeys.size() + " object(s) from bucket="
                    + bucket + "; failing keys [" + sample + "]" + suffix);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) && client != null) {
            client.close();
            client = null;
        }
    }

    private RemoteObject toRemoteObject(String prefix, ObsObject object) {
        ObjectMetadata metadata = object.getMetadata();
        return new RemoteObject(
                object.getObjectKey(),
                getRelativePathSafe(prefix, object.getObjectKey()),
                metadata.getEtag(),
                contentLength(metadata),
                lastModifiedMs(metadata.getLastModified()));
    }

    private static String requireProperty(String value, String key, String description) throws IOException {
        if (!hasText(value)) {
            throw new IOException(description + " is required; set " + key + " in properties");
        }
        return value;
    }

    private static boolean isNotFound(ObsException e) {
        return e.getResponseCode() == 404
                || "NoSuchKey".equals(e.getErrorCode())
                || "NoSuchBucket".equals(e.getErrorCode());
    }

    private static long contentLength(ObjectMetadata metadata) {
        return metadata.getContentLength() == null ? 0L : metadata.getContentLength();
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

    private ObsConfiguration buildObsConfiguration(String endpoint) {
        ObsConfiguration config = new ObsConfiguration();
        config.setEndPoint(endpoint);
        config.setMaxConnections(parseIntProperty(properties.getMaxConnections(), "OBS max connections"));
        config.setConnectionRequestTimeout(parseIntProperty(properties.getRequestTimeoutMs(),
                "OBS request timeout"));
        config.setConnectionTimeout(parseIntProperty(properties.getConnectionTimeoutMs(),
                "OBS connection timeout"));
        config.setSocketTimeout(parseIntProperty(properties.getRequestTimeoutMs(),
                "OBS socket timeout"));
        config.setPathStyle(properties.isUsePathStyle());
        return config;
    }

    private static int parseIntProperty(String value, String description) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(description + " must be an integer: " + value, e);
        }
    }
}
