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
import org.apache.doris.datasource.property.storage.COSProperties;

import com.google.common.collect.ImmutableList;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import com.tencentcloudapi.sts.v20180813.StsClient;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleRequest;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleResponse;
import com.tencentcloudapi.sts.v20180813.models.Credentials;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Tencent Cloud COS-specific {@link ObjStorage} implementation.
 *
 * <p>Inherits generic CRUD from {@link S3ObjStorage}; overrides list/head/STS/presigned
 * with COS native SDK. COS V1 list uses marker-based pagination (not continuation token).
 */
public class CosObjStorage extends S3ObjStorage {
    private static final Logger LOG = LogManager.getLogger(CosObjStorage.class);
    private static final long SESSION_EXPIRE_SECONDS = 3600L;

    private final COSProperties cosProperties;

    public CosObjStorage(COSProperties properties) {
        super(properties);
        this.cosProperties = properties;
    }

    // ----------------------------------------------------------------
    // STS: Tencent Cloud STS AssumeRole
    // ----------------------------------------------------------------

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        String roleArn    = cosProperties.getOrigProps().get(ObjectInfoAdapter.STS_ROLE_ARN_KEY);
        String externalId = cosProperties.getOrigProps().get(ObjectInfoAdapter.STS_EXTERNAL_ID_KEY);
        if (roleArn == null || roleArn.isEmpty()) {
            throw new DdlException("COS STS role ARN is not configured");
        }
        ClientProfile clientProfile = null;
        if (Config.enable_sts_vpc) {
            HttpProfile httpProfile = new HttpProfile();
            httpProfile.setEndpoint("sts.internal.tencentcloudapi.com");
            clientProfile = new ClientProfile();
            clientProfile.setHttpProfile(httpProfile);
        }
        Credential credential = new Credential(cosProperties.getAccessKey(), cosProperties.getSecretKey());
        StsClient stsClient = (clientProfile != null)
                ? new StsClient(credential, cosProperties.getRegion(), clientProfile)
                : new StsClient(credential, cosProperties.getRegion());
        AssumeRoleRequest req = new AssumeRoleRequest();
        req.setRoleArn(roleArn);
        req.setDurationSeconds((long) ObjectInfoAdapter.getDurationSeconds());
        req.setRoleSessionName(ObjectInfoAdapter.getNewRoleSessionName());
        if (externalId != null && !externalId.isEmpty()) {
            req.setExternalId(externalId);
        }
        try {
            AssumeRoleResponse resp = stsClient.AssumeRole(req);
            Credentials cred = resp.getCredentials();
            return Triple.of(cred.getTmpSecretId(), cred.getTmpSecretKey(), cred.getToken());
        } catch (Exception e) {
            LOG.warn("Failed to get COS STS token, roleArn={}", roleArn, e);
            throw new DdlException("Failed to get COS STS token: " + e.getMessage());
        }
    }

    // ----------------------------------------------------------------
    // Presigned URL: COS native SDK
    // ----------------------------------------------------------------

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        try (CosNativeClient nativeClient = new CosNativeClient(cosProperties)) {
            Date expiry = new Date(System.currentTimeMillis() + SESSION_EXPIRE_SECONDS * 1000L);
            URL url = nativeClient.get().generatePresignedUrl(
                    cosProperties.getBucket(), objectKey, expiry,
                    HttpMethodName.PUT, new HashMap<>(), new HashMap<>());
            LOG.info("Generated COS presigned URL for key={}", objectKey);
            return url.toString();
        } catch (CosClientException e) {
            throw new IOException("Failed to generate COS presigned URL: " + e.getMessage(), e);
        }
    }

    // ----------------------------------------------------------------
    // listObjectsWithPrefix: COS V1 list (marker-based pagination)
    // ----------------------------------------------------------------

    @Override
    public ListObjectsResult listObjectsWithPrefix(
            String prefix, String subPrefix, String continuationToken) throws IOException {
        String fullPrefix = normalizeAndCombinePrefix(prefix, subPrefix);
        try (CosNativeClient nativeClient = new CosNativeClient(cosProperties)) {
            ListObjectsRequest req = new ListObjectsRequest();
            req.setBucketName(cosProperties.getBucket());
            req.setPrefix(fullPrefix);
            req.setMaxKeys(1000);
            if (!StringUtils.isEmpty(continuationToken)) {
                // COS V1 uses marker for pagination, not continuationToken
                req.setMarker(continuationToken);
            }
            ObjectListing listing = nativeClient.get().listObjects(req);
            List<ObjectFile> files = new ArrayList<>();
            for (COSObjectSummary s : listing.getObjectSummaries()) {
                files.add(new ObjectFile(s.getKey(),
                        getRelativePathSafe(prefix, s.getKey()),
                        s.getETag(), s.getSize()));
            }
            // COS V1 next page marker
            String nextToken = listing.isTruncated() ? listing.getNextMarker() : null;
            return new ListObjectsResult(files, listing.isTruncated(), nextToken);
        } catch (CosClientException e) {
            LOG.warn("Failed to list COS objects, prefix={}", fullPrefix, e);
            throw new IOException("Failed to list COS objects: " + e.getMessage(), e);
        }
    }

    // ----------------------------------------------------------------
    // headObjectWithMeta: COS getObjectMetadata (handles 404)
    // ----------------------------------------------------------------

    @Override
    public ListObjectsResult headObjectWithMeta(String prefix, String subKey) throws IOException {
        String fullKey = normalizeAndCombinePrefix(prefix, subKey);
        try (CosNativeClient nativeClient = new CosNativeClient(cosProperties)) {
            ObjectMetadata metadata = nativeClient.get().getObjectMetadata(
                    cosProperties.getBucket(), fullKey);
            ObjectFile of = new ObjectFile(fullKey,
                    getRelativePathSafe(prefix, fullKey),
                    metadata.getETag(), metadata.getContentLength());
            return new ListObjectsResult(ImmutableList.of(of), false, null);
        } catch (CosServiceException e) {
            if (e.getStatusCode() == 404) {
                LOG.warn("Key not found in COS headObjectWithMeta, key={}", fullKey);
                return new ListObjectsResult(ImmutableList.of(), false, null);
            }
            throw new IOException("Failed to head COS object: " + e.getMessage(), e);
        } catch (CosClientException e) {
            throw new IOException("Failed to head COS object: " + e.getMessage(), e);
        }
    }
}
