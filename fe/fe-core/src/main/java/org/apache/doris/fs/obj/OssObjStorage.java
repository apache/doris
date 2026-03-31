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

import org.apache.doris.cloud.storage.ObjectInfoAdapter;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.storage.OSSProperties;

import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.HeadObjectRequest;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.BasicCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.profile.DefaultProfile;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * OSS-specific {@link ObjStorage} implementation.
 *
 * <p>Inherits generic CRUD operations from {@link S3ObjStorage} (S3-compatible SDK);
 * overrides list/head/STS/presigned with OSS native SDK for correctness.
 */
public class OssObjStorage extends S3ObjStorage {
    private static final Logger LOG = LogManager.getLogger(OssObjStorage.class);
    private static final long SESSION_EXPIRE_SECONDS = 3600L;

    private final OSSProperties ossProperties;

    public OssObjStorage(OSSProperties properties) {
        super(properties);
        this.ossProperties = properties;
    }

    // ----------------------------------------------------------------
    // STS: Alibaba Cloud RAM AssumeRole
    // ----------------------------------------------------------------

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        String roleArn = ossProperties.getOrigProps().get(ObjectInfoAdapter.STS_ROLE_ARN_KEY);
        if (roleArn == null || roleArn.isEmpty()) {
            throw new DdlException("OSS STS role ARN is not configured");
        }
        AssumeRoleRequest req = new AssumeRoleRequest();
        req.setRoleArn(roleArn);
        req.setRoleSessionName(ObjectInfoAdapter.getNewRoleSessionName());
        req.setDurationSeconds((long) ObjectInfoAdapter.getDurationSeconds());
        try {
            DefaultProfile profile = DefaultProfile.getProfile(ossProperties.getRegion());
            if (Config.enable_sts_vpc) {
                profile.enableUsingVpcEndpoint();
            }
            BasicCredentials basicCred = new BasicCredentials(
                    ossProperties.getAccessKey(), ossProperties.getSecretKey());
            DefaultAcsClient ramClient = new DefaultAcsClient(
                    profile, new StaticCredentialsProvider(basicCred));
            AssumeRoleResponse resp = ramClient.getAcsResponse(req);
            AssumeRoleResponse.Credentials cred = resp.getCredentials();
            return Triple.of(cred.getAccessKeyId(), cred.getAccessKeySecret(), cred.getSecurityToken());
        } catch (Exception e) {
            LOG.warn("Failed to get OSS STS token, roleArn={}", roleArn, e);
            throw new DdlException("Failed to get OSS STS token: " + e.getMessage());
        }
    }

    // ----------------------------------------------------------------
    // Presigned URL: OSS native SDK
    // ----------------------------------------------------------------

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        try (OssNativeClient nativeClient = new OssNativeClient(ossProperties)) {
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(
                    ossProperties.getBucket(), objectKey, HttpMethod.PUT);
            request.setExpiration(new Date(System.currentTimeMillis() + SESSION_EXPIRE_SECONDS * 1000));
            URL signedUrl = nativeClient.get().generatePresignedUrl(request);
            LOG.info("Generated OSS presigned URL for key={}", objectKey);
            return signedUrl.toString();
        } catch (OSSException e) {
            LOG.warn("OSS exception generating presigned URL for key={}", objectKey, e);
            throw new IOException("Failed to generate OSS presigned URL: " + e.getErrorMessage(), e);
        }
    }

    // ----------------------------------------------------------------
    // listObjectsWithPrefix: OSS native listObjectsV2 (continuation token)
    // ----------------------------------------------------------------

    @Override
    public ListObjectsResult listObjectsWithPrefix(
            String prefix, String subPrefix, String continuationToken) throws IOException {
        String fullPrefix = normalizeAndCombinePrefix(prefix, subPrefix);
        try (OssNativeClient nativeClient = new OssNativeClient(ossProperties)) {
            ListObjectsV2Request req = new ListObjectsV2Request()
                    .withBucketName(ossProperties.getBucket())
                    .withPrefix(fullPrefix);
            if (!StringUtils.isEmpty(continuationToken)) {
                req.setContinuationToken(continuationToken);
            }
            ListObjectsV2Result result = nativeClient.get().listObjectsV2(req);
            List<ObjectFile> files = new ArrayList<>();
            for (OSSObjectSummary s : result.getObjectSummaries()) {
                files.add(new ObjectFile(s.getKey(),
                        getRelativePathSafe(prefix, s.getKey()),
                        formatEtag(s.getETag()),
                        s.getSize()));
            }
            return new ListObjectsResult(files, result.isTruncated(),
                    result.isTruncated() ? result.getNextContinuationToken() : null);
        } catch (OSSException e) {
            LOG.warn("Failed to list OSS objects, prefix={}", fullPrefix, e);
            throw new IOException("Failed to list OSS objects, error=" + e.getErrorCode()
                    + ": " + e.getErrorMessage(), e);
        }
    }

    // ----------------------------------------------------------------
    // headObjectWithMeta: OSS native headObject (handles NO_SUCH_KEY)
    // ----------------------------------------------------------------

    @Override
    public ListObjectsResult headObjectWithMeta(String prefix, String subKey) throws IOException {
        String fullKey = normalizeAndCombinePrefix(prefix, subKey);
        try (OssNativeClient nativeClient = new OssNativeClient(ossProperties)) {
            HeadObjectRequest req = new HeadObjectRequest(ossProperties.getBucket(), fullKey);
            ObjectMetadata metadata = nativeClient.get().headObject(req);
            ObjectFile of = new ObjectFile(fullKey,
                    getRelativePathSafe(prefix, fullKey),
                    formatEtag(metadata.getETag()),
                    metadata.getContentLength());
            return new ListObjectsResult(ImmutableList.of(of), false, null);
        } catch (OSSException e) {
            if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
                LOG.warn("Key not found in OSS headObjectWithMeta, key={}", fullKey);
                return new ListObjectsResult(ImmutableList.of(), false, null);
            }
            LOG.warn("Failed to head OSS object, key={}", fullKey, e);
            throw new IOException("Failed to head OSS object: " + e.getErrorMessage(), e);
        }
    }

    // OSS SDK returns ETag without quotes; normalize to match S3 format
    private String formatEtag(String etag) {
        if (etag == null) {
            return null;
        }
        return (etag.startsWith("\"") && etag.endsWith("\"")) ? etag : "\"" + etag + "\"";
    }
}
