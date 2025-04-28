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

package org.apache.doris.fsv2.obj;

import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.credentials.CloudCredential;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.fsv2.remote.RemoteFile;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class S3ObjStorage implements ObjStorage<S3Client> {
    private static final Logger LOG = LogManager.getLogger(S3ObjStorage.class);
    private S3Client client;

    protected Map<String, String> properties;

    protected AbstractS3CompatibleProperties s3Properties;

    private boolean isUsePathStyle = false;

    private boolean forceParsingByStandardUri = false;

    public S3ObjStorage(AbstractS3CompatibleProperties properties) {
        this.properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        setProperties(properties);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    protected void setProperties(AbstractS3CompatibleProperties properties) {
        this.s3Properties = properties;
        isUsePathStyle = Boolean.parseBoolean(properties.getUsePathStyle());
        forceParsingByStandardUri = Boolean.parseBoolean(s3Properties.getForceParsingByStandardUrl());
    }

    @Override
    public S3Client getClient() throws UserException {
        if (client == null) {
            String endpointStr = s3Properties.getEndpoint();
            if (!endpointStr.contains("://")) {
                endpointStr = "http://" + endpointStr;
            }
            URI endpoint = URI.create(endpointStr);
            CloudCredential credential = new CloudCredential();
            credential.setAccessKey(s3Properties.getAccessKey());
            credential.setSecretKey(s3Properties.getSecretKey());

            /* if (properties.containsKey(S3Properties.SESSION_TOKEN)) {
                credential.setSessionToken(properties.get(S3Properties.SESSION_TOKEN));
            }*/
            client = S3Util.buildS3Client(endpoint, s3Properties.getRegion(), credential, isUsePathStyle);
        }
        return client;
    }



    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        try {
            remotePath = s3Properties.validateAndNormalizeUri(remotePath);
            URI uri = new URI(remotePath);
            String bucketName = uri.getHost();
            String prefix = uri.getPath().substring(1);
            int wildcardIndex = prefix.indexOf('*');
            String searchPrefix = wildcardIndex > 0 ? prefix.substring(0, wildcardIndex) : prefix;
            try (S3Client s3 = getClient()) {
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(searchPrefix)
                        .build();

                ListObjectsV2Response listResponse = s3.listObjectsV2(listRequest);
                String regex = prefix.replace(".", "\\.")
                        .replace("*", ".*")
                        .replace("?", ".");
                Pattern pattern = Pattern.compile(regex);
                List<RemoteFile> matchedFiles = listResponse.contents().stream()
                        .filter(obj -> pattern.matcher(obj.key()).matches())
                        .map(obj -> {
                            String fullKey = obj.key();
                            String fullPath = "s3://" + bucketName + "/" + fullKey;
                            return new RemoteFile(
                                    fileNameOnly ? fullPath.substring(fullPath.lastIndexOf('/') + 1) : fullPath,
                                    true,
                                    obj.size(),
                                    -1,
                                    obj.lastModified().toEpochMilli()
                            );
                        })
                        .collect(Collectors.toList());

                result.addAll(matchedFiles);
            }
            return Status.OK;
        } catch (Exception e) {
            LOG.warn("Errors while getting file status", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "Errors while getting file status " + e.getMessage());
        }
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        return null;
    }

    @Override
    public Status headObject(String remotePath) {
        try {
            remotePath = s3Properties.validateAndNormalizeUri(remotePath);
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            HeadObjectResponse response = getClient()
                    .headObject(HeadObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build());
            LOG.info("head file " + remotePath + " success: " + response.toString());
            return Status.OK;
        } catch (S3Exception e) {
            if (e.statusCode() == HttpStatus.SC_NOT_FOUND) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            } else {
                LOG.warn("headObject failed:", e);
                return new Status(Status.ErrCode.COMMON_ERROR, "headObject failed: " + e.getMessage());
            }
        } catch (UserException ue) {
            LOG.warn("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status getObject(String remoteFilePath, File localFile) {
        try {
            remoteFilePath = s3Properties.validateAndNormalizeUri(remoteFilePath);
            S3URI uri = S3URI.create(remoteFilePath, isUsePathStyle, forceParsingByStandardUri);
            GetObjectResponse response = getClient().getObject(
                    GetObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build(), localFile.toPath());
            LOG.info("get file " + remoteFilePath + " success: " + response.toString());
            return Status.OK;
        } catch (S3Exception s3Exception) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "get file from s3 error: " + s3Exception.awsErrorDetails().errorMessage());
        } catch (UserException ue) {
            LOG.warn("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.toString());
        }
    }

    @Override
    public Status putObject(String remotePath, @Nullable InputStream content, long contentLength) {
        try {
            remotePath = s3Properties.validateAndNormalizeUri(remotePath);
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            RequestBody body = RequestBody.fromInputStream(content, contentLength);
            PutObjectResponse response =
                    getClient()
                            .putObject(
                                    PutObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build(),
                                    body);
            LOG.info("put object success: " + response.toString());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.error("put object failed:", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "put object failed: " + e.getMessage());
        } catch (Exception ue) {
            LOG.error("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status deleteObject(String remotePath) {
        try {
            remotePath = s3Properties.validateAndNormalizeUri(remotePath);
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            DeleteObjectResponse response =
                    getClient()
                            .deleteObject(
                                    DeleteObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build());
            LOG.info("delete file " + remotePath + " success: " + response.toString());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.warn("delete file failed: ", e);
            if (e.statusCode() == HttpStatus.SC_NOT_FOUND) {
                return Status.OK;
            }
            return new Status(Status.ErrCode.COMMON_ERROR, "delete file failed: " + e.getMessage());
        } catch (UserException ue) {
            LOG.warn("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status deleteObjects(String absolutePath) {
        try {
            absolutePath = s3Properties.validateAndNormalizeUri(absolutePath);
            S3URI baseUri = S3URI.create(absolutePath, isUsePathStyle, forceParsingByStandardUri);
            String continuationToken = "";
            boolean isTruncated = false;
            long totalObjects = 0;
            do {
                RemoteObjects objects = listObjects(absolutePath, continuationToken);
                List<RemoteObject> objectList = objects.getObjectList();
                if (!objectList.isEmpty()) {
                    Delete delete = Delete.builder()
                            .objects(objectList.stream()
                                    .map(RemoteObject::getKey)
                                    .map(k -> ObjectIdentifier.builder().key(k).build())
                                    .collect(Collectors.toList()))
                            .build();
                    DeleteObjectsRequest req = DeleteObjectsRequest.builder()
                            .bucket(baseUri.getBucket())
                            .delete(delete)
                            .build();

                    DeleteObjectsResponse resp = getClient().deleteObjects(req);
                    if (resp.errors().size() > 0) {
                        LOG.warn("{} errors returned while deleting {} objects for dir {}",
                                resp.errors().size(), objectList.size(), absolutePath);
                    }
                    LOG.info("{} of {} objects deleted for dir {}",
                            resp.deleted().size(), objectList.size(), absolutePath);
                    totalObjects += objectList.size();
                }

                isTruncated = objects.isTruncated();
                continuationToken = objects.getContinuationToken();
            } while (isTruncated);
            LOG.info("total delete {} objects for dir {}", totalObjects, absolutePath);
            return Status.OK;
        } catch (DdlException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "list objects for delete objects failed: " + e.getMessage());
        } catch (Exception e) {
            LOG.warn(String.format("delete objects %s failed", absolutePath), e);
            return new Status(Status.ErrCode.COMMON_ERROR, "delete objects failed: " + e.getMessage());
        }
    }

    @Override
    public Status copyObject(String origFilePath, String destFilePath) {
        try {
            origFilePath = s3Properties.validateAndNormalizeUri(origFilePath);
            destFilePath = s3Properties.validateAndNormalizeUri(destFilePath);
            S3URI origUri = S3URI.create(origFilePath, isUsePathStyle, forceParsingByStandardUri);
            S3URI descUri = S3URI.create(destFilePath, isUsePathStyle, forceParsingByStandardUri);
            CopyObjectResponse response = getClient()
                    .copyObject(
                            CopyObjectRequest.builder()
                                    .copySource(origUri.getBucket() + "/" + origUri.getKey())
                                    .destinationBucket(descUri.getBucket())
                                    .destinationKey(descUri.getKey())
                                    .build());
            LOG.info("copy file from " + origFilePath + " to " + destFilePath + " success: " + response.toString());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.error("copy file failed: ", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "copy file failed: " + e.getMessage());
        } catch (UserException ue) {
            LOG.error("copy to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public RemoteObjects listObjects(String absolutePath, String continuationToken) throws DdlException {
        try {
            absolutePath = s3Properties.validateAndNormalizeUri(absolutePath);
            S3URI uri = S3URI.create(absolutePath, isUsePathStyle, forceParsingByStandardUri);
            String bucket = uri.getBucket();
            String prefix = uri.getKey();
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(normalizePrefix(prefix));
            if (!StringUtils.isEmpty(continuationToken)) {
                requestBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = getClient().listObjectsV2(requestBuilder.build());
            List<RemoteObject> remoteObjects = new ArrayList<>();
            for (S3Object c : response.contents()) {
                String relativePath = getRelativePath(prefix, c.key());
                remoteObjects.add(new RemoteObject(c.key(), relativePath, c.eTag(), c.size()));
            }
            return new RemoteObjects(remoteObjects, response.isTruncated(), response.nextContinuationToken());
        } catch (Exception e) {
            LOG.warn(String.format("Failed to list objects for S3: %s", absolutePath), e);
            throw new DdlException("Failed to list objects for S3, Error message: " + e.getMessage(), e);
        }
    }
}
