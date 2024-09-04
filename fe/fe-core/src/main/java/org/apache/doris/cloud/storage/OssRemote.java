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

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
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
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OssRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(OssRemote.class);
    private OSS ossClient;

    public OssRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        String bucketName = obj.getBucket();
        String objectName = normalizePrefix(fileName);
        initClient();
        try {
            GeneratePresignedUrlRequest request
                    = new GeneratePresignedUrlRequest(bucketName, objectName, HttpMethod.PUT);
            Date expiration = new Date(new Date().getTime() + SESSION_EXPIRE_SECOND * 1000);
            request.setExpiration(expiration);
            URL signedUrl = ossClient.generatePresignedUrl(request);
            return signedUrl.toString();
        } catch (OSSException oe) {
            LOG.warn("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason. "
                    + "Error Message: {} , Error Code: {} Request ID: {} Host ID: {}",
                    oe.getErrorMessage(), oe.getErrorCode(), oe.getRequestId(), oe.getHostId());
        } catch (ClientException ce) {
            LOG.warn("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network. Error Message: {}", ce.getMessage());
        } finally {
            close();
        }
        return "";
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
            HeadObjectRequest request = new HeadObjectRequest(obj.getBucket(), key);
            ObjectMetadata metadata = ossClient.headObject(request);
            ObjectFile objectFile = new ObjectFile(key, getRelativePath(key), formatEtag(metadata.getETag()),
                    metadata.getContentLength());
            return new ListObjectsResult(Lists.newArrayList(objectFile), false, null);
        } catch (OSSException e) {
            if (e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                LOG.warn("NoSuchKey when head object for OSS, subKey={}", subKey);
                return new ListObjectsResult(Lists.newArrayList(), false, null);
            }
            LOG.warn("Failed to head object for OSS, subKey={}", subKey, e);
            throw new DdlException(
                    "Failed to head object for OSS, subKey=" + subKey + ", Error code=" + e.getErrorCode()
                            + ", Error message=" + e.getErrorMessage());
        }
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest();
        assumeRoleRequest.setRoleArn(obj.getArn());
        assumeRoleRequest.setRoleSessionName(getNewRoleSessionName());
        assumeRoleRequest.setDurationSeconds((long) getDurationSeconds());
        try {
            DefaultProfile profile = DefaultProfile.getProfile(obj.getRegion());
            if (Config.enable_sts_vpc) {
                profile.enableUsingVpcEndpoint();
            }
            BasicCredentials basicCredentials = new BasicCredentials(obj.getAk(), obj.getSk());
            DefaultAcsClient ramClient = new DefaultAcsClient(profile, new StaticCredentialsProvider(basicCredentials));
            AssumeRoleResponse response = ramClient.getAcsResponse(assumeRoleRequest);
            AssumeRoleResponse.Credentials credentials = response.getCredentials();
            return Triple.of(credentials.getAccessKeyId(), credentials.getAccessKeySecret(),
                    credentials.getSecurityToken());
        } catch (Exception e) {
            LOG.warn("Failed get oss sts token", e);
            throw new DdlException(e.getMessage());
        }
    }

    private ListObjectsResult listObjectsInner(String prefix, String continuationToken) throws DdlException {
        initClient();
        try {
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(obj.getBucket())
                    .withPrefix(prefix);
            if (!StringUtils.isEmpty(continuationToken)) {
                request.setContinuationToken(continuationToken);
            }
            ListObjectsV2Result result = ossClient.listObjectsV2(request);
            List<ObjectFile> objectFiles = new ArrayList<>();
            for (OSSObjectSummary s : result.getObjectSummaries()) {
                objectFiles.add(
                        new ObjectFile(s.getKey(), getRelativePath(s.getKey()), formatEtag(s.getETag()), s.getSize()));
            }
            return new ListObjectsResult(objectFiles, result.isTruncated(), result.getNextContinuationToken());
        } catch (OSSException e) {
            LOG.warn("Failed to list objects for OSS prefix {}", prefix, e);
            throw new DdlException("Failed to list objects for OSS, Error code=" + e.getErrorCode() + ", Error message="
                    + e.getErrorMessage());
        }
    }

    private void initClient() {
        if (ossClient == null) {
            /*
             * There are several timeout configuration, see {@link com.aliyun.oss.ClientConfiguration},
             * please config if needed.
             */
            if (obj.getToken() != null) {
                ossClient = new OSSClientBuilder().build(obj.getEndpoint(), obj.getAk(), obj.getSk(), obj.getToken());
            } else {
                ossClient = new OSSClientBuilder().build(obj.getEndpoint(), obj.getAk(), obj.getSk());
            }
        }
    }

    @Override
    public void close() {
        super.close();
        if (ossClient != null) {
            ossClient.shutdown();
            ossClient = null;
        }
    }

    @Override
    public String toString() {
        return "OssRemote{obj=" + obj + '}';
    }
}
