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

package org.apache.doris.cloud.storage;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.region.Region;
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

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class CosRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(CosRemote.class);
    private COSClient cosClient;

    public CosRemote(ObjectInfo obj) {
        super(obj);
    }

    private void initClient() {
        if (cosClient == null) {
            COSCredentials cred = new BasicCOSCredentials(obj.getAk(), obj.getSk());
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setRegion(new Region(obj.getRegion()));
            clientConfig.setHttpProtocol(HttpProtocol.http);
            cosClient = new COSClient(cred, clientConfig);
        }
    }

    @Override
    public String getPresignedUrl(String fileName) {
        initClient();
        Date expirationDate = new Date(System.currentTimeMillis() + SESSION_EXPIRE_SECOND);
        URL url = cosClient.generatePresignedUrl(obj.getBucket(),
                normalizePrefix(fileName), expirationDate, HttpMethodName.PUT,
                new HashMap<String, String>(), new HashMap<String, String>());
        cosClient.shutdown();
        LOG.info("generate cos presigned url: {}", url);
        return url.toString();
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        ClientProfile clientProfile = null;
        if (Config.enable_sts_vpc) {
            HttpProfile httpProfile = new HttpProfile();
            // https://cloud.tencent.com/document/product/1312/48200
            httpProfile.setEndpoint("sts.internal.tencentcloudapi.com");
            clientProfile = new ClientProfile();
            clientProfile.setHttpProfile(httpProfile);
        }
        Credential credential = new Credential(obj.getAk(), obj.getSk());
        StsClient stsClient = (clientProfile != null) ? new StsClient(credential, obj.getRegion(), clientProfile)
                : new StsClient(credential, obj.getRegion());
        AssumeRoleRequest request = new AssumeRoleRequest();
        request.setRoleArn(obj.getArn());
        request.setDurationSeconds((long) getDurationSeconds());
        request.setRoleSessionName(getNewRoleSessionName());
        request.setExternalId(obj.getExternalId());
        try {
            AssumeRoleResponse assumeRoleResponse = stsClient.AssumeRole(request);
            Credentials credentials = assumeRoleResponse.getCredentials();
            return Triple.of(credentials.getTmpSecretId(), credentials.getTmpSecretKey(),
                    credentials.getToken());
        } catch (Exception e) {
            LOG.warn("Failed get cos sts token", e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public ListObjectsResult listObjects(String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(), continuationToken);
    }

    @Override
    public ListObjectsResult listObjects(String subPrefix, String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(subPrefix), continuationToken);
    }

    @Override
    public ListObjectsResult headObject(String subKey) throws DdlException {
        initClient();
        try {
            String key = normalizePrefix(subKey);
            ObjectMetadata metadata = cosClient.getObjectMetadata(obj.getBucket(), key);
            ObjectFile objectFile = new ObjectFile(key, getRelativePath(key), formatEtag(metadata.getETag()),
                    metadata.getContentLength());
            return new ListObjectsResult(Lists.newArrayList(objectFile), false, null);
        } catch (CosServiceException e) {
            if (e.getStatusCode() == 404) {
                LOG.warn("NoSuchKey when head object for COS, subKey={}", subKey);
                return new ListObjectsResult(Lists.newArrayList(), false, null);
            }
            LOG.warn("Failed to head object for COS, subKey={}", subKey, e);
            throw new DdlException(
                    "Failed to head object for COS, subKey=" + subKey + ", Error code=" + e.getErrorCode()
                            + ", Error message=" + e.getCause().getMessage());
        } catch (CosClientException e) {
            LOG.warn("Failed to head object for COS, subKey={}", subKey, e);
            throw new DdlException(
                    "Failed to head object for COS, subKey=" + subKey + ", Error code=" + e.getErrorCode()
                            + ", Error message=" + e.getCause().getMessage());
        }
    }

    private ListObjectsResult listObjectsInner(String prefix, String continuationToken) throws DdlException {
        initClient();
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
            listObjectsRequest.setBucketName(obj.getBucket());
            listObjectsRequest.setPrefix(prefix);
            listObjectsRequest.setMaxKeys(1000);
            if (!StringUtils.isEmpty(continuationToken)) {
                listObjectsRequest.setMarker(continuationToken);
            }
            ObjectListing objectListing = cosClient.listObjects(listObjectsRequest);
            List<COSObjectSummary> cosObjectSummaries = objectListing.getObjectSummaries();
            List<ObjectFile> objectFiles = new ArrayList<>();
            for (COSObjectSummary s : cosObjectSummaries) {
                objectFiles.add(
                        new ObjectFile(s.getKey(), getRelativePath(s.getKey()), formatEtag(s.getETag()), s.getSize()));
            }
            return new ListObjectsResult(objectFiles, objectListing.isTruncated(), objectListing.getMarker());
        } catch (CosClientException e) {
            LOG.warn("Failed to list objects for COS", e);
            throw new DdlException("Failed to list objects for COS, Error code=" + e.getErrorCode() + ", Error message="
                    + e.getCause().getMessage());
        }
    }

    @Override
    public String toString() {
        return "CosRemote{obj=" + obj + '}';
    }
}
