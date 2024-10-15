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
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.fs.remote.RemoteFile;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AzureObjStorage implements ObjStorage<BlobServiceClient> {
    private static final Logger LOG = LogManager.getLogger(AzureObjStorage.class);
    private static final String URI_TEMPLATE = "https://%s.blob.core.windows.net";
    protected Map<String, String> properties;
    private BlobServiceClient client;
    private boolean isUsePathStyle = false;

    private boolean forceParsingByStandardUri = false;

    public AzureObjStorage(Map<String, String> properties) {
        this.properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        setProperties(properties);
    }

    // To ensure compatibility with S3 usage, the path passed by the user still starts with 'S3://${containerName}'.
    // For Azure, we need to remove this part.
    private static String removeUselessSchema(String remotePath) {
        String prefix = "s3://";

        if (remotePath.startsWith(prefix)) {
            remotePath = remotePath.substring(prefix.length());
        }
        // Remove the useless container name
        int firstSlashIndex = remotePath.indexOf('/');
        return remotePath.substring(firstSlashIndex + 1);
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
    }

    @Override
    public BlobServiceClient getClient() throws UserException {
        if (client == null) {
            String uri = String.format(URI_TEMPLATE, properties.get(S3Properties.ACCESS_KEY));
            StorageSharedKeyCredential cred = new StorageSharedKeyCredential(properties.get(S3Properties.ACCESS_KEY),
                    properties.get(S3Properties.SECRET_KEY));
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
            builder.credential(cred);
            builder.endpoint(uri);
            client = builder.buildClient();
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
            BlobClient blobClient = getClient().getBlobContainerClient(uri.getBucket()).getBlobClient(uri.getKey());
            BlobProperties properties = blobClient.getProperties();
            LOG.info("head file {} success: {}", remotePath, properties.toString());
            return Status.OK;
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            } else {
                LOG.warn("headObject {} failed:", remotePath, e);
                return new Status(Status.ErrCode.COMMON_ERROR, "headObject "
                        + remotePath + " failed: " + e.getMessage());
            }
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "headObject "
                    + remotePath + " failed: " + e.getMessage());
        }
    }

    @Override
    public Status getObject(String remoteFilePath, File localFile) {
        try {
            S3URI uri = S3URI.create(remoteFilePath, isUsePathStyle, forceParsingByStandardUri);
            BlobClient blobClient = getClient().getBlobContainerClient(uri.getBucket()).getBlobClient(uri.getKey());
            BlobProperties properties = blobClient.downloadToFile(localFile.getAbsolutePath());
            LOG.info("get file " + remoteFilePath + " success: " + properties.toString());
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "get file from azure error: " + e.getServiceMessage());
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "getObject "
                    + remoteFilePath + " failed: " + e.getMessage());
        }
    }

    @Override
    public Status putObject(String remotePath, @Nullable InputStream content, long contentLength) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            BlobClient blobClient = getClient().getBlobContainerClient(uri.getBucket()).getBlobClient(uri.getKey());
            blobClient.upload(content, contentLength);
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "Error occurred while copying the blob:: " + e.getServiceMessage());
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "putObject "
                    + remotePath + " failed: " + e.getMessage());
        }
    }

    @Override
    public Status deleteObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            BlobClient blobClient = getClient().getBlobContainerClient(uri.getBucket()).getBlobClient(uri.getKey());
            blobClient.delete();
            LOG.info("delete file " + remotePath + " success");
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "get file from azure error: " + e.getServiceMessage());
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "deleteObject "
                    + remotePath + " failed: " + e.getMessage());
        }
    }

    @Override
    public Status deleteObjects(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            BlobContainerClient blobClient = getClient().getBlobContainerClient(uri.getBucket());
            String containerUrl = blobClient.getBlobContainerUrl();
            String continuationToken = "";
            boolean isTruncated = false;
            long totalObjects = 0;
            do {
                RemoteObjects objects = listObjects(remotePath, continuationToken);
                List<RemoteObject> objectList = objects.getObjectList();
                if (!objectList.isEmpty()) {
                    BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(
                            getClient()).buildClient();
                    BlobBatch blobBatch = blobBatchClient.getBlobBatch();

                    for (RemoteObject blob : objectList) {
                        blobBatch.deleteBlob(containerUrl, blob.getKey());
                    }
                    Response<Void> resp = blobBatchClient.submitBatchWithResponse(blobBatch, true, null, Context.NONE);
                    LOG.info("{} objects deleted for dir {} return http code {}",
                            objectList.size(), remotePath, resp.getStatusCode());
                    totalObjects += objectList.size();
                }

                isTruncated = objects.isTruncated();
                continuationToken = objects.getContinuationToken();
            } while (isTruncated);
            LOG.info("total delete {} objects for dir {}", totalObjects, remotePath);
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "list objects for delete objects failed: " + e.getMessage());
        } catch (Exception e) {
            LOG.warn(String.format("delete objects %s failed", remotePath), e);
            return new Status(Status.ErrCode.COMMON_ERROR, "delete objects failed: " + e.getMessage());
        }
    }

    @Override
    public Status copyObject(String origFilePath, String destFilePath) {
        try {
            S3URI origUri = S3URI.create(origFilePath, isUsePathStyle, forceParsingByStandardUri);
            S3URI destUri = S3URI.create(destFilePath, isUsePathStyle, forceParsingByStandardUri);
            BlobClient sourceBlobClient = getClient().getBlobContainerClient(origUri.getBucket())
                    .getBlobClient(origUri.getKey());
            BlobClient destinationBlobClient = getClient().getBlobContainerClient(destUri.getBucket())
                    .getBlobClient(destUri.getKey());
            destinationBlobClient.beginCopy(sourceBlobClient.getBlobUrl(), null);
            LOG.info("Blob copied from " + origFilePath + " to " + destFilePath);
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "Error occurred while copying the blob:: " + e.getServiceMessage());
        } catch (UserException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "copyObject from "
                    + origFilePath + "to " + destFilePath + " failed: " + e.getMessage());
        }
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws DdlException {
        try {
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(remotePath);
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            PagedIterable<BlobItem> pagedBlobs = getClient().getBlobContainerClient(uri.getBucket())
                    .listBlobs(options, continuationToken, null);
            PagedResponse<BlobItem> pagedResponse = pagedBlobs.iterableByPage().iterator().next();
            List<RemoteObject> remoteObjects = new ArrayList<>();

            for (BlobItem blobItem : pagedResponse.getElements()) {
                remoteObjects.add(new RemoteObject(blobItem.getName(), "", blobItem.getProperties().getETag(),
                        blobItem.getProperties().getContentLength()));
            }
            return new RemoteObjects(remoteObjects, pagedResponse.getContinuationToken() != null,
                    pagedResponse.getContinuationToken());
        } catch (BlobStorageException e) {
            LOG.warn(String.format("Failed to list objects for S3: %s", remotePath), e);
            throw new DdlException("Failed to list objects for S3, Error message: " + e.getMessage(), e);
        } catch (UserException e) {
            LOG.warn(String.format("Failed to list objects for S3: %s", remotePath), e);
            throw new DdlException("Failed to list objects for S3, Error message: " + e.getMessage(), e);
        }
    }

    // Due to historical reasons, when the BE parses the object storage path.
    // It assumes the path starts with 'S3://${containerName}'
    // So here the path needs to be constructed in a format that BE can parse.
    private String constructS3Path(String fileName, String bucket) throws UserException {
        LOG.info("the path is {}", String.format("s3://%s/%s", bucket, fileName));
        return String.format("s3://%s/%s", bucket, fileName);
    }

    public static String getLongestPrefix(String globPattern) {
        int length = globPattern.length();
        int earliestSpecialCharIndex = length;

        char[] specialChars = {'*', '?', '[', '{', '\\'};

        for (char specialChar : specialChars) {
            int index = globPattern.indexOf(specialChar);
            if (index != -1 && index < earliestSpecialCharIndex) {
                earliestSpecialCharIndex = index;
            }
        }

        return globPattern.substring(0, earliestSpecialCharIndex);
    }

    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        long roundCnt = 0;
        long elementCnt = 0;
        long matchCnt = 0;
        long startTime = System.nanoTime();
        Status st = Status.OK;
        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            String globPath = uri.getKey();
            String bucket = uri.getBucket();
            LOG.info("try to glob list for azure, remote path {}, orig {}", globPath, remotePath);
            BlobContainerClient client = getClient().getBlobContainerClient(bucket);
            java.nio.file.Path pathPattern = Paths.get(globPath);
            LOG.info("path pattern {}", pathPattern.toString());
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern.toString());

            String listPrefix = getLongestPrefix(globPath);
            LOG.info("azure glob list prefix is {}", listPrefix);
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(listPrefix);
            String newContinuationToken = null;
            do {
                roundCnt++;
                PagedIterable<BlobItem> pagedBlobs = client.listBlobs(options, newContinuationToken, null);
                PagedResponse<BlobItem> pagedResponse = pagedBlobs.iterableByPage().iterator().next();

                for (BlobItem blobItem : pagedResponse.getElements()) {
                    elementCnt++;
                    java.nio.file.Path blobPath = Paths.get(blobItem.getName());

                    if (!matcher.matches(blobPath)) {
                        continue;
                    }
                    matchCnt++;
                    RemoteFile remoteFile = new RemoteFile(
                            fileNameOnly ? blobPath.getFileName().toString() : constructS3Path(blobPath.toString(),
                                    uri.getBucket()),
                            !blobItem.isPrefix(),
                            blobItem.isPrefix() ? -1 : blobItem.getProperties().getContentLength(),
                            blobItem.getProperties().getContentLength(),
                            blobItem.getProperties().getLastModified().getSecond());
                    result.add(remoteFile);
                }
                newContinuationToken = pagedResponse.getContinuationToken();
            } while (newContinuationToken != null);

        } catch (BlobStorageException e) {
            LOG.warn("glob file " + remotePath + " failed because azure error: " + e.getMessage());
            st = new Status(Status.ErrCode.COMMON_ERROR, "glob file " + remotePath
                    + " failed because azure error: " + e.getMessage());
        } catch (Exception e) {
            LOG.warn("errors while glob file " + remotePath, e);
            st = new Status(Status.ErrCode.COMMON_ERROR, "errors while glob file " + remotePath + e.getMessage());
        } finally {
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            LOG.info("process {} elements under prefix {} for {} round, match {} elements, take {} micro second",
                    remotePath, elementCnt, matchCnt, roundCnt,
                    duration / 1000);
        }
        return st;
    }
}
