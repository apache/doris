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
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.fs.remote.RemoteFile;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
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
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class S3ObjStorage implements ObjStorage<S3Client> {
    private static final Logger LOG = LogManager.getLogger(S3ObjStorage.class);
    private S3Client client;

    protected Map<String, String> properties;

    private boolean isUsePathStyle = false;

    private boolean forceParsingByStandardUri = false;

    public S3ObjStorage(Map<String, String> properties) {
        this.properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        setProperties(properties);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    protected void setProperties(Map<String, String> properties) {
        this.properties.putAll(properties);
        try {
            S3Properties.requiredS3Properties(this.properties);
        } catch (DdlException e) {
            throw new IllegalArgumentException(e);
        }
        // Virtual hosted-style is recommended in the s3 protocol.
        // The path-style has been abandoned, but for some unexplainable reasons,
        // the s3 client will determine whether the endpiont starts with `s3`
        // when generating a virtual hosted-sytle request.
        // If not, it will not be converted ( https://github.com/aws/aws-sdk-java-v2/pull/763),
        // but the endpoints of many cloud service providers for object storage do not start with s3,
        // so they cannot be converted to virtual hosted-sytle.
        // Some of them, such as aliyun's oss, only support virtual hosted-style,
        // and some of them(ceph) may only support
        // path-style, so we need to do some additional conversion.
        isUsePathStyle = this.properties.getOrDefault(PropertyConverter.USE_PATH_STYLE, "false")
                .equalsIgnoreCase("true");
        forceParsingByStandardUri = this.properties.getOrDefault(PropertyConverter.FORCE_PARSING_BY_STANDARD_URI,
                "false").equalsIgnoreCase("true");

        String endpoint = this.properties.get(S3Properties.ENDPOINT);
        String region = this.properties.get(S3Properties.REGION);

        this.properties.put(S3Properties.REGION, PropertyConverter.checkRegion(endpoint, region, S3Properties.REGION));
    }

    @Override
    public S3Client getClient() throws UserException {
        if (client == null) {
            String endpointStr = properties.get(S3Properties.ENDPOINT);
            if (!endpointStr.contains("://")) {
                endpointStr = "http://" + endpointStr;
            }
            URI endpoint = URI.create(endpointStr);
            client = S3Util.buildS3Client(endpoint, properties.get(S3Properties.REGION), isUsePathStyle,
                    properties.get(S3Properties.ACCESS_KEY), properties.get(S3Properties.SECRET_KEY),
                    properties.get(S3Properties.SESSION_TOKEN), properties.get(S3Properties.ROLE_ARN),
                    properties.get(S3Properties.EXTERNAL_ID));
        }
        return client;
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        return null;
    }

    @Override
    public Status headObject(String remotePath) {
        try {
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
            LOG.warn("deleteObjects:", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "list objects for delete objects failed: " + e.getMessage());
        } catch (Exception e) {
            LOG.warn(String.format("delete objects %s failed", absolutePath), e);
            return new Status(Status.ErrCode.COMMON_ERROR, "delete objects failed: " + e.getMessage());
        }
    }

    @Override
    public Status copyObject(String origFilePath, String destFilePath) {
        try {
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

    public Status multipartUpload(String remotePath, @Nullable InputStream inputStream, long totalBytes) {
        Status st = Status.OK;
        long uploadedBytes = 0;
        int bytesRead = 0;
        byte[] buffer = new byte[CHUNK_SIZE];
        int partNumber = 1;

        String uploadId = null;
        S3URI uri = null;
        Map<Integer, String> etags = new HashMap<>();

        try {
            uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(uri.getBucket())
                    .key(uri.getKey())
                    .build();
            CreateMultipartUploadResponse createMultipartUploadResponse = getClient()
                    .createMultipartUpload(createMultipartUploadRequest);

            uploadId = createMultipartUploadResponse.uploadId();

            while (uploadedBytes < totalBytes && (bytesRead = inputStream.read(buffer)) != -1) {
                uploadedBytes += bytesRead;
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(uri.getBucket())
                        .key(uri.getKey())
                        .uploadId(uploadId)
                        .partNumber(partNumber).build();
                RequestBody body = RequestBody
                        .fromInputStream(new ByteArrayInputStream(buffer, 0, bytesRead), bytesRead);
                UploadPartResponse uploadPartResponse = getClient().uploadPart(uploadPartRequest, body);

                etags.put(partNumber, uploadPartResponse.eTag());
                partNumber++;
                uploadedBytes += bytesRead;
            }

            List<CompletedPart> completedParts = etags.entrySet().stream()
                    .map(entry -> CompletedPart.builder()
                            .partNumber(entry.getKey())
                            .eTag(entry.getValue())
                            .build())
                    .collect(Collectors.toList());
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();

            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(uri.getBucket())
                    .key(uri.getKey())
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();

            getClient().completeMultipartUpload(completeMultipartUploadRequest);
        } catch (Exception e) {
            LOG.warn("remotePath:{}, ", remotePath, e);
            st = new Status(Status.ErrCode.COMMON_ERROR, "Failed to multipartUpload " + remotePath
                    + " reason: " + e.getMessage());

            if (uri != null && uploadId != null) {
                try {
                    AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                            .bucket(uri.getBucket())
                            .key(uri.getKey())
                            .uploadId(uploadId)
                            .build();
                    getClient().abortMultipartUpload(abortMultipartUploadRequest);
                } catch (Exception e1) {
                    LOG.warn("Failed to abort multipartUpload " + remotePath, e1);
                }
            }
        }
        return st;
    }

    ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) throws UserException {
        return getClient().listObjectsV2(request);
    }

    /**
     * List all files under the given path with glob pattern.
     * For example, if the path is "s3://bucket/path/to/*.csv",
     * it will list all files under "s3://bucket/path/to/" with ".csv" suffix.
     *
     * Copy from `AzureObjStorage.GlobList`
     */
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        long roundCnt = 0;
        long elementCnt = 0;
        long matchCnt = 0;
        long startTime = System.nanoTime();
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            String bucket = uri.getBucket();
            String globPath = uri.getKey(); // eg: path/to/*.csv

            LOG.info("globList globPath:{}, remotePath:{}", globPath, remotePath);

            java.nio.file.Path pathPattern = Paths.get(globPath);
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern);
            HashSet<String> directorySet = new HashSet<>();

            String listPrefix = S3Util.getLongestPrefix(globPath); // similar to Azure
            if (LOG.isDebugEnabled()) {
                LOG.debug("globList listPrefix: '{}' (from globPath: '{}')", listPrefix, globPath);
            }

            // For Directory Buckets, ensure proper prefix handling using standardized approach
            String finalPrefix = listPrefix;

            if (uri.useS3DirectoryBucket()) {
                String adjustedPrefix = S3URI.getDirectoryPrefixForGlob(listPrefix);
                if (LOG.isDebugEnabled() && !adjustedPrefix.equals(listPrefix)) {
                    LOG.debug("Directory bucket detected, adjusting prefix from '{}' to '{}'",
                            listPrefix, adjustedPrefix);
                }
                finalPrefix = adjustedPrefix;
            }

            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(finalPrefix)
                    .build();

            boolean isTruncated = false;
            do {
                roundCnt++;
                ListObjectsV2Response response = listObjectsV2(request);
                for (S3Object obj : response.contents()) {
                    elementCnt++;
                    java.nio.file.Path objPath = Paths.get(obj.key());

                    boolean isPrefix = false;
                    while (objPath != null && objPath.normalize().toString().startsWith(listPrefix)) {
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
                        RemoteFile remoteFile = new RemoteFile(
                                fileNameOnly ? objPath.getFileName().toString() :
                                        "s3://" + bucket + "/" + objPath.toString(),
                                !isPrefix,
                                isPrefix ? -1 : obj.size(),
                                isPrefix ? -1 : obj.size(),
                                isPrefix ? 0 : obj.lastModified().toEpochMilli()
                        );
                        result.add(remoteFile);
                        objPath = objPath.getParent();
                        isPrefix = true;
                    }
                }

                isTruncated = response.isTruncated();
                if (isTruncated) {
                    request = request.toBuilder()
                        .continuationToken(response.nextContinuationToken())
                        .build();
                }
            } while (isTruncated);

            if (LOG.isDebugEnabled()) {
                LOG.debug("remotePath:{}, result:{}", remotePath, result);
            }
            return Status.OK;
        } catch (Exception e) {
            LOG.warn("Errors while getting file status", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "Errors while getting file status " + e.getMessage());
        } finally {
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            LOG.info("process {} elements under prefix {} for {} round, match {} elements, take {} ms",
                    elementCnt, remotePath, roundCnt, matchCnt,
                    duration / 1000 / 1000);
        }
    }
}
