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

import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.fs.GlobListResult;
import org.apache.doris.fs.remote.RemoteFile;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.EcsRamRoleCredentialsProvider;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ObjStorage implementation backed by the Alibaba Cloud OSS Java SDK (aliyun-sdk-oss:3.15.0).
 * Used for FE-side file listing and metadata operations on OSS buckets.
 *
 * Handles all credential cases natively, avoiding AWS SDK SigV4 signing incompatibility
 * with Alibaba STS tokens:
 *   1. Static ak/sk                 → DefaultCredentialProvider(ak, sk)
 *   2. Static ak/sk + session_token → DefaultCredentialProvider(ak, sk, token)
 *   3. role_arn via ECS RAM role    → ECS metadata (100.100.100.200) → STS AssumeRole
 *   4. ECS direct (no role_arn)     → EcsRamRoleCredentialsProvider(roleName from env)
 */
public class OSSObjStorage implements ObjStorage<OSS> {
    private static final Logger LOG = LogManager.getLogger(OSSObjStorage.class);

    private final OSSProperties ossProperties;
    private volatile OSS client;

    private final boolean isUsePathStyle;
    private final boolean forceParsingByStandardUri;

    public OSSObjStorage(OSSProperties ossProperties) {
        this.ossProperties = ossProperties;
        // OSS only supports virtual-hosted style (bucket.endpoint/key).
        // Path style is not supported — always use false for S3URI parsing
        // regardless of what the user configured.
        this.isUsePathStyle = false;
        this.forceParsingByStandardUri = Boolean.parseBoolean(ossProperties.getForceParsingByStandardUrl());
    }

    @Override
    public OSS getClient() throws UserException {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = buildOssClient();
                }
            }
        }
        return client;
    }

    private OSS buildOssClient() {
        String endpoint = ossProperties.getEndpoint();
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            endpoint = "https://" + endpoint;
        }

        String ak = ossProperties.getAccessKey();
        String sk = ossProperties.getSecretKey();
        String token = ossProperties.getSessionToken();
        String roleArn = ossProperties.getOssRoleArn();
        String externalId = ossProperties.getOssExternalId();
        String region = ossProperties.getRegion();

        CredentialsProvider provider;

        if (StringUtils.isNotBlank(roleArn)) {
            // role_arn: ECS RAM role → STS AssumeRole → temporary credentials
            Triple<String, String, String> stsCreds =
                    resolveEcsRoleThenAssumeRole(roleArn, externalId, region);
            provider = new DefaultCredentialProvider(
                    stsCreds.getLeft(), stsCreds.getMiddle(), stsCreds.getRight());
        } else if (StringUtils.isNotBlank(ak) && StringUtils.isNotBlank(sk)) {
            if (StringUtils.isNotBlank(token)) {
                // STS temporary credentials: uses x-oss-security-token header correctly.
                provider = new DefaultCredentialProvider(ak, sk, token);
            } else {
                provider = new DefaultCredentialProvider(ak, sk);
            }
        } else {
            // No credentials: rely on ECS instance metadata directly.
            // ALIBABA_CLOUD_ECS_METADATA env var must be set to the ECS RAM role name.
            String roleName = System.getenv("ALIBABA_CLOUD_ECS_METADATA");
            if (StringUtils.isBlank(roleName)) {
                throw new IllegalArgumentException(
                        "OSS: no credentials provided and ALIBABA_CLOUD_ECS_METADATA is not set. "
                        + "Set oss.access_key/oss.secret_key, oss.role_arn, "
                        + "or ALIBABA_CLOUD_ECS_METADATA=<ecs-ram-role-name>.");
            }
            provider = new EcsRamRoleCredentialsProvider(roleName);
        }

        LOG.info("OSSObjStorage: building client endpoint={}", endpoint);
        return new OSSClientBuilder().build(endpoint, provider);
    }

    /**
     * Fetches ECS instance credentials via 100.100.100.200 then calls Alibaba STS AssumeRole.
     * Mirrors the C++ OSSSTSCredentialProvider implementation.
     */
    private Triple<String, String, String> resolveEcsRoleThenAssumeRole(
            String roleArn, String externalId, String region) {
        try {
            String roleName = fetchEcsRoleName();
            String[] ecsCredentials = fetchEcsCredentials(roleName);
            return callStsAssumeRole(
                    ecsCredentials[0], ecsCredentials[1], ecsCredentials[2],
                    roleArn, externalId, region);
        } catch (Exception e) {
            throw new RuntimeException(
                    "OSS: failed to assume role via ECS RAM role. role_arn=" + roleArn
                    + " error=" + e.getMessage(), e);
        }
    }

    private String fetchEcsRoleName() throws Exception {
        String response = httpGet(
                "http://100.100.100.200/latest/meta-data/ram/security-credentials/", 5000);
        String roleName = response.trim();
        int newline = roleName.indexOf('\n');
        if (newline > 0) {
            roleName = roleName.substring(0, newline).trim();
        }
        if (StringUtils.isBlank(roleName)) {
            throw new RuntimeException("No RAM role attached to this ECS instance");
        }
        return roleName;
    }

    private String[] fetchEcsCredentials(String roleName) throws Exception {
        String json = httpGet(
                "http://100.100.100.200/latest/meta-data/ram/security-credentials/" + roleName, 5000);
        String ak = parseJsonField(json, "AccessKeyId");
        String sk = parseJsonField(json, "AccessKeySecret");
        String token = parseJsonField(json, "SecurityToken");
        if (StringUtils.isAnyBlank(ak, sk, token)) {
            throw new RuntimeException("ECS metadata returned empty credentials for role: " + roleName);
        }
        return new String[]{ak, sk, token};
    }

    private Triple<String, String, String> callStsAssumeRole(
            String ecsAk, String ecsSk, String ecsToken,
            String roleArn, String externalId, String region) throws Exception {
        com.aliyuncs.profile.DefaultProfile profile =
                com.aliyuncs.profile.DefaultProfile.getProfile(region);
        com.aliyuncs.auth.BasicSessionCredentials baseCreds =
                new com.aliyuncs.auth.BasicSessionCredentials(ecsAk, ecsSk, ecsToken);
        com.aliyuncs.DefaultAcsClient stsClient = new com.aliyuncs.DefaultAcsClient(
                profile, new com.aliyuncs.auth.StaticCredentialsProvider(baseCreds));

        com.aliyuncs.auth.sts.AssumeRoleRequest req = new com.aliyuncs.auth.sts.AssumeRoleRequest();
        req.setRoleArn(roleArn);
        req.setRoleSessionName("doris-fe-oss-" + (System.currentTimeMillis() % 100000));
        req.setDurationSeconds(3600L);
        // TODO: pass externalId when aliyun-java-sdk-core version on build server
        // supports AssumeRoleRequest.setExternalId(). Currently the bundled version
        // does not expose this method. Track: https://github.com/aliyun/aliyun-openapi-java-sdk

        com.aliyuncs.auth.sts.AssumeRoleResponse resp = stsClient.getAcsResponse(req);
        com.aliyuncs.auth.sts.AssumeRoleResponse.Credentials creds = resp.getCredentials();
        return Triple.of(creds.getAccessKeyId(), creds.getAccessKeySecret(), creds.getSecurityToken());
    }

    private String httpGet(String urlStr, int timeoutMs) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setConnectTimeout(timeoutMs);
        conn.setReadTimeout(timeoutMs);
        conn.setRequestMethod("GET");
        int status = conn.getResponseCode();
        if (status != 200) {
            throw new RuntimeException("HTTP GET " + urlStr + " returned status " + status);
        }
        try (InputStream is = conn.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
            return sb.toString();
        }
    }

    private String parseJsonField(String json, String fieldName) {
        String key = "\"" + fieldName + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) {
            return "";
        }
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) {
            return "";
        }
        int start = json.indexOf('"', colon + 1);
        if (start < 0) {
            return "";
        }
        int end = json.indexOf('"', start + 1);
        if (end < 0) {
            return "";
        }
        return json.substring(start + 1, end);
    }

    // -------------------------------------------------------------------------
    // ObjStorage interface implementation
    // -------------------------------------------------------------------------

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        throw new DdlException("getStsToken() not supported via OSSObjStorage");
    }

    @Override
    public Status headObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().headObject(uri.getBucket(), uri.getKey());
            return Status.OK;
        } catch (OSSException e) {
            if ("NoSuchKey".equals(e.getErrorCode()) || "NoSuchBucket".equals(e.getErrorCode())) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            }
            return new Status(Status.ErrCode.COMMON_ERROR, "headObject failed: " + e.getErrorMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "headObject failed: " + Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status getObject(String remoteFilePath, File localFile) {
        try {
            S3URI uri = S3URI.create(remoteFilePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().getObject(
                    new com.aliyun.oss.model.GetObjectRequest(uri.getBucket(), uri.getKey()),
                    localFile);
            return Status.OK;
        } catch (OSSException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "getObject failed: " + e.getErrorMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "getObject failed: " + Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status putObject(String remotePath, InputStream content, long contentLength) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(contentLength);
            getClient().putObject(uri.getBucket(), uri.getKey(), content, meta);
            return Status.OK;
        } catch (OSSException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "putObject failed: " + e.getErrorMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "putObject failed: " + Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status deleteObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().deleteObject(uri.getBucket(), uri.getKey());
            return Status.OK;
        } catch (OSSException e) {
            if ("NoSuchKey".equals(e.getErrorCode())) {
                return Status.OK;
            }
            return new Status(Status.ErrCode.COMMON_ERROR, "deleteObject failed: " + e.getErrorMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "deleteObject failed: " + Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status deleteObjects(String absolutePath) {
        try {
            S3URI uri = S3URI.create(absolutePath, isUsePathStyle, forceParsingByStandardUri);
            String prefix = uri.getKey();
            String continuationToken = null;
            do {
                ListObjectsV2Request req = new ListObjectsV2Request(uri.getBucket())
                        .withPrefix(prefix).withMaxKeys(1000);
                if (continuationToken != null) {
                    req.setContinuationToken(continuationToken);
                }
                ListObjectsV2Result result = getClient().listObjectsV2(req);
                List<String> keys = new ArrayList<>();
                for (OSSObjectSummary obj : result.getObjectSummaries()) {
                    keys.add(obj.getKey());
                }
                if (!keys.isEmpty()) {
                    getClient().deleteObjects(
                            new com.aliyun.oss.model.DeleteObjectsRequest(uri.getBucket()).withKeys(keys));
                }
                continuationToken = result.isTruncated() ? result.getNextContinuationToken() : null;
            } while (continuationToken != null);
            return Status.OK;
        } catch (OSSException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "deleteObjects failed: " + e.getErrorMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "deleteObjects failed: " + Util.getRootCauseMessage(e));
        }
    }

    @Override
    public Status copyObject(String origFilePath, String destFilePath) {
        try {
            S3URI src = S3URI.create(origFilePath, isUsePathStyle, forceParsingByStandardUri);
            S3URI dst = S3URI.create(destFilePath, isUsePathStyle, forceParsingByStandardUri);
            getClient().copyObject(src.getBucket(), src.getKey(), dst.getBucket(), dst.getKey());
            return Status.OK;
        } catch (OSSException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "copyObject failed: " + e.getErrorMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "copyObject failed: " + Util.getRootCauseMessage(e));
        }
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws DdlException {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            String prefix = uri.getKey();
            ListObjectsV2Request req = new ListObjectsV2Request(uri.getBucket())
                    .withPrefix(prefix).withMaxKeys(1000);
            if (StringUtils.isNotBlank(continuationToken)) {
                req.setContinuationToken(continuationToken);
            }
            ListObjectsV2Result result = getClient().listObjectsV2(req);
            List<RemoteObject> objects = new ArrayList<>();
            for (OSSObjectSummary obj : result.getObjectSummaries()) {
                String relativePath = obj.getKey().length() > prefix.length()
                        ? obj.getKey().substring(prefix.length()) : obj.getKey();
                objects.add(new RemoteObject(obj.getKey(), relativePath, obj.getETag(), obj.getSize()));
            }
            String nextToken = result.isTruncated() ? result.getNextContinuationToken() : "";
            return new RemoteObjects(objects, result.isTruncated(), nextToken);
        } catch (OSSException e) {
            throw new DdlException("OSS listObjects failed: " + e.getErrorMessage());
        } catch (Exception e) {
            throw new DdlException("OSS listObjects failed: " + Util.getRootCauseMessage(e));
        }
    }

    // -------------------------------------------------------------------------
    // Extended operations used by OSSFileSystem
    // -------------------------------------------------------------------------

    /**
     * Completes a multipart upload. Used by OSSFileSystem.completeMultipartUpload().
     * Confirmed API: CompleteMultipartUploadRequest(bucket, key, uploadId, List<PartETag>)
     *               PartETag(int partNumber, String eTag)
     */
    public void completeMultipartUpload(String bucket, String key, String uploadId,
            Map<Integer, String> parts) {
        try {
            List<PartETag> partETags = parts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> new PartETag(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
            getClient().completeMultipartUpload(
                    new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags));
        } catch (Exception e) {
            throw new RuntimeException("OSS completeMultipartUpload failed: " + Util.getRootCauseMessage(e), e);
        }
    }

    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            String bucket = uri.getBucket();
            String key = uri.getKey();
            String schemaAndBucket = remotePath.substring(0, remotePath.length() - key.length());
            String prefix = key.endsWith("/") ? key : key + "/";

            String continuationToken = null;
            do {
                ListObjectsV2Request req = new ListObjectsV2Request(bucket).withPrefix(prefix);
                if (!recursive) {
                    req.setDelimiter("/");
                }
                if (continuationToken != null) {
                    req.setContinuationToken(continuationToken);
                }
                ListObjectsV2Result res = getClient().listObjectsV2(req);
                for (OSSObjectSummary obj : res.getObjectSummaries()) {
                    if (obj.getKey().equals(prefix)) {
                        continue;
                    }
                    result.add(new RemoteFile(toPath(schemaAndBucket, obj.getKey()),
                            false, obj.getSize(), 0L, obj.getLastModified().getTime(), null));
                }
                if (!recursive) {
                    for (String commonPrefix : res.getCommonPrefixes()) {
                        result.add(new RemoteFile(toPath(schemaAndBucket, commonPrefix),
                                true, 0L, 0L, 0L, null));
                    }
                }
                continuationToken = res.isTruncated() ? res.getNextContinuationToken() : null;
            } while (continuationToken != null);
            return Status.OK;
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "OSS listFiles failed: " + Util.getRootCauseMessage(e));
        }
    }

    public Status listDirectories(String remotePath, Set<String> result) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            String bucket = uri.getBucket();
            String key = uri.getKey();
            String prefix = key.endsWith("/") ? key : key + "/";
            String continuationToken = null;
            do {
                ListObjectsV2Request req = new ListObjectsV2Request(bucket)
                        .withPrefix(prefix).withDelimiter("/");
                if (continuationToken != null) {
                    req.setContinuationToken(continuationToken);
                }
                ListObjectsV2Result res = getClient().listObjectsV2(req);
                result.addAll(res.getCommonPrefixes());
                continuationToken = res.isTruncated() ? res.getNextContinuationToken() : null;
            } while (continuationToken != null);
            return Status.OK;
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR,
                    "OSS listDirectories failed: " + Util.getRootCauseMessage(e));
        }
    }

    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        GlobListResult globListResult = globListInternal(remotePath, result, fileNameOnly, null, -1, -1);
        return globListResult.getStatus();
    }

    public GlobListResult globListWithLimit(String remotePath, List<RemoteFile> result,
            String startFile, long fileSizeLimit, long fileNumLimit) {
        return globListInternal(remotePath, result, false, startFile, fileSizeLimit, fileNumLimit);
    }

    private GlobListResult globListInternal(String remotePath, List<RemoteFile> result,
            boolean fileNameOnly, String startFile, long fileSizeLimit, long fileNumLimit) {
        long roundCnt = 0;
        long elementCnt = 0;
        long matchCnt = 0;
        long matchFileSize = 0L;
        long startTime = System.nanoTime();
        String currentMaxFile = "";
        boolean hasLimits = fileSizeLimit > 0 || fileNumLimit > 0;
        String bucket = "";
        String finalPrefix = "";

        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            bucket = uri.getBucket();
            String keyPattern = uri.getKey();

            String globPath = S3Util.extendGlobs(keyPattern);
            if (LOG.isDebugEnabled()) {
                LOG.debug("OSS globList globPath:{}, remotePath:{}", globPath, remotePath);
            }
            java.nio.file.Path pathPattern = Paths.get(globPath);
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern);
            HashSet<String> directorySet = new HashSet<>();

            finalPrefix = S3Util.getLongestPrefix(globPath);

            ListObjectsV2Request request = new ListObjectsV2Request(bucket)
                    .withPrefix(finalPrefix);
            if (startFile != null) {
                request.setStartAfter(startFile);
            }

            boolean isTruncated = false;
            boolean reachLimit = false;
            do {
                roundCnt++;
                ListObjectsV2Result response = getClient().listObjectsV2(request);

                for (OSSObjectSummary obj : response.getObjectSummaries()) {
                    elementCnt++;

                    // Limit already reached: scan for the next matching key so
                    // the caller can detect hasMoreDataToConsume correctly.
                    if (reachLimit) {
                        java.nio.file.Path checkPath = Paths.get(obj.getKey());
                        if (matcher.matches(checkPath)) {
                            currentMaxFile = obj.getKey();
                            break;
                        }
                        continue;
                    }

                    java.nio.file.Path objPath = Paths.get(obj.getKey());
                    boolean isPrefix = false;
                    while (objPath != null && objPath.normalize().toString().startsWith(finalPrefix)) {
                        if (!matcher.matches(objPath)) {
                            isPrefix = true;
                            objPath = objPath.getParent();
                            continue;
                        }
                        if (directorySet.contains(objPath.normalize().toString())) {
                            break;
                        }
                        if (isPrefix) {
                            directorySet.add(objPath.normalize().toString());
                        }
                        matchCnt++;
                        matchFileSize += obj.getSize();
                        String displayPath = fileNameOnly
                                ? objPath.getFileName().toString()
                                : "s3://" + bucket + "/" + objPath.toString();
                        result.add(new RemoteFile(displayPath, !isPrefix,
                                isPrefix ? -1 : obj.getSize(),
                                isPrefix ? -1 : obj.getSize(),
                                isPrefix ? 0 : obj.getLastModified().getTime()));

                        currentMaxFile = obj.getKey();
                        if (hasLimits) {
                            boolean overSize = fileSizeLimit > 0 && matchFileSize > fileSizeLimit;
                            boolean overCount = fileNumLimit > 0 && matchCnt >= fileNumLimit;
                            if (overSize || overCount) {
                                reachLimit = true;
                                break;
                            }
                        }
                        break;
                    }
                }

                isTruncated = response.isTruncated();
                if (isTruncated && !reachLimit) {
                    request = new ListObjectsV2Request(bucket)
                            .withPrefix(finalPrefix)
                            .withContinuationToken(response.getNextContinuationToken());
                    if (startFile != null) {
                        request.setStartAfter(startFile);
                    }
                }
            } while (isTruncated && !reachLimit);

            if (LOG.isDebugEnabled()) {
                LOG.debug("OSS globList remotePath:{}, result:{}", remotePath, result);
            }
            return new GlobListResult(Status.OK, currentMaxFile, bucket, finalPrefix);
        } catch (OSSException e) {
            if ("NoSuchKey".equals(e.getErrorCode()) || "NoSuchBucket".equals(e.getErrorCode())) {
                LOG.info("OSS NoSuchKey/Bucket when listing, treat as empty. bucket={} prefix={}",
                        bucket, finalPrefix);
                return new GlobListResult(Status.OK, "", bucket, finalPrefix);
            }
            LOG.warn("OSS Errors while getting file status bucket={} prefix={}", bucket, finalPrefix, e);
            return new GlobListResult(new Status(Status.ErrCode.COMMON_ERROR,
                    "Errors while getting file status " + e.getErrorMessage()));
        } catch (Exception e) {
            LOG.warn("OSS Errors while getting file status bucket={} prefix={}", bucket, finalPrefix, e);
            return new GlobListResult(new Status(Status.ErrCode.COMMON_ERROR,
                    "Errors while getting file status " + Util.getRootCauseMessage(e)));
        } finally {
            if (LOG.isDebugEnabled()) {
                long duration = System.nanoTime() - startTime;
                LOG.debug("OSS globList: {} elements under prefix {} in {} rounds, "
                        + "{} matched, took {} ms",
                        elementCnt, finalPrefix, roundCnt, matchCnt, duration / 1_000_000);
            }
        }
    }

    protected static Path toPath(String schemaAndBucket, String key) {
        String cleanedBase = (schemaAndBucket == null) ? "" : schemaAndBucket.replaceAll("/+$", "");
        String cleanedKey = (key == null) ? "" : key.replaceAll("^/+", "");
        return new Path(cleanedBase + "/" + cleanedKey);
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.shutdown();
            client = null;
        }
    }
}
