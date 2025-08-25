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
import org.apache.doris.datasource.property.storage.AzureProperties;
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
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

public class AzureObjStorage implements ObjStorage<BlobServiceClient> {
    private static final Logger LOG = LogManager.getLogger(AzureObjStorage.class);
    private static final String URI_TEMPLATE = "https://%s.blob.core.windows.net";

    protected AzureProperties azureProperties;
    private BlobServiceClient client;
    private boolean isUsePathStyle;

    private boolean forceParsingByStandardUri;

    public AzureObjStorage(AzureProperties azureProperties) {
        this.azureProperties = azureProperties;
        this.isUsePathStyle = Boolean.parseBoolean(azureProperties.getUsePathStyle());
        this.forceParsingByStandardUri = Boolean.parseBoolean(azureProperties.getForceParsingByStandardUrl());
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


    @Override
    public BlobServiceClient getClient() throws UserException {
        if (client == null) {
            StorageSharedKeyCredential cred = new StorageSharedKeyCredential(azureProperties.getAccessKey(),
                    azureProperties.getSecretKey());
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
            builder.credential(cred);
            builder.endpoint(azureProperties.getEndpoint());
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("headObject remotePath:{} bucket:{} key:{} properties:{}",
                        remotePath, uri.getBucket(), uri.getKey(), blobClient.getProperties());
            }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("get file {} success, properties: {}", remoteFilePath, properties);
            }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("delete file {} success", remotePath);
            }
            return Status.OK;
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
                return Status.OK;
            }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("total delete {} objects for dir {}", totalObjects, remotePath);
            }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Blob copy file from {} to {} success", origFilePath, destFilePath);
            }
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
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(uri.getKey());
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
    private String constructS3Path(String fileName, String bucket) {
        String path = String.format("s3://%s/%s", bucket, fileName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("constructS3Path fileName:{}, bucket:{}, the path is {}", fileName, bucket, path);
        }
        return path;
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("try to glob list for azure, remote path {}, orig {}", globPath, remotePath);
            }
            BlobContainerClient client = getClient().getBlobContainerClient(bucket);
            java.nio.file.Path pathPattern = Paths.get(globPath);
            if (LOG.isDebugEnabled()) {
                LOG.debug("azure glob list pathPattern: {}, bucket: {}", pathPattern, bucket);
            }
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern);

            HashSet<String> directorySet = new HashSet<>();
            String listPrefix = S3Util.getLongestPrefix(globPath);
            if (LOG.isDebugEnabled()) {
                LOG.debug("azure glob list prefix is {}", listPrefix);
            }
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(listPrefix);
            String newContinuationToken = null;
            do {
                roundCnt++;
                PagedResponse<BlobItem> pagedResponse = getPagedBlobItems(client, options, newContinuationToken);

                for (BlobItem blobItem : pagedResponse.getElements()) {
                    elementCnt++;
                    java.nio.file.Path blobPath = Paths.get(blobItem.getName());

                    boolean isPrefix = false;
                    while (blobPath.normalize().toString().startsWith(listPrefix)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("get blob {}", blobPath.normalize().toString());
                        }
                        if (!matcher.matches(blobPath)) {
                            isPrefix = true;
                            blobPath = blobPath.getParent();
                            continue;
                        }
                        if (directorySet.contains(blobPath.normalize().toString())) {
                            break;
                        }
                        if (isPrefix) {
                            directorySet.add(blobPath.normalize().toString());
                        }

                        matchCnt++;
                        RemoteFile remoteFile = new RemoteFile(
                                fileNameOnly ? blobPath.getFileName().toString() : constructS3Path(blobPath.toString(),
                                        uri.getBucket()),
                                !isPrefix,
                                isPrefix ? -1 : blobItem.getProperties().getContentLength(),
                                isPrefix ? -1 : blobItem.getProperties().getContentLength(),
                                isPrefix ? 0 : blobItem.getProperties().getLastModified().getSecond());
                        result.add(remoteFile);

                        blobPath = blobPath.getParent();
                        isPrefix = true;
                    }
                }
                newContinuationToken = pagedResponse.getContinuationToken();
            } while (newContinuationToken != null);

        } catch (BlobStorageException e) {
            LOG.warn("glob file " + remotePath + " failed because azure error: " + e.getMessage());
            st = new Status(Status.ErrCode.COMMON_ERROR, "glob file " + remotePath
                    + " failed because azure error: " + e.getMessage());
        } catch (Exception e) {
            LOG.warn("errors while glob file " + remotePath, e);
            st = new Status(Status.ErrCode.COMMON_ERROR,
                    "errors while glob file " + remotePath + ": " + e.getMessage());
        } finally {
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            LOG.info("process {} elements under prefix {} for {} round, match {} elements, take {} micro second",
                    remotePath, elementCnt, roundCnt, matchCnt,
                    duration / 1000);
        }
        return st;
    }

    public PagedResponse<BlobItem> getPagedBlobItems(BlobContainerClient client, ListBlobsOptions options,
                                                     String newContinuationToken) {
        PagedIterable<BlobItem> pagedBlobs = client.listBlobs(options, newContinuationToken, null);
        return pagedBlobs.iterableByPage().iterator().next();
    }


    public Status multipartUpload(String remotePath, @Nullable InputStream inputStream, long totalBytes) {
        Status st = Status.OK;
        long uploadedBytes = 0;
        int bytesRead = 0;
        byte[] buffer = new byte[CHUNK_SIZE];
        List<String> blockIds = new ArrayList<>();
        BlockBlobClient blockBlobClient = null;


        try {
            S3URI uri = S3URI.create(remotePath, isUsePathStyle, forceParsingByStandardUri);
            blockBlobClient = getClient().getBlobContainerClient(uri.getBucket())
                    .getBlobClient(uri.getKey()).getBlockBlobClient();
            while (uploadedBytes < totalBytes && (bytesRead = inputStream.read(buffer)) != -1) {
                String blockId = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes());
                blockIds.add(blockId);
                blockBlobClient.stageBlock(blockId, new ByteArrayInputStream(buffer, 0, bytesRead), bytesRead);
                uploadedBytes += bytesRead;
            }
            blockBlobClient.commitBlockList(blockIds);
        } catch (Exception e) {
            LOG.warn("remotePath:{}, ", remotePath, e);
            st = new Status(Status.ErrCode.COMMON_ERROR, "Failed to multipartUpload " + remotePath
                    + " reason: " + e.getMessage());

            if (blockBlobClient != null) {
                try {
                    blockBlobClient.delete();
                } catch (Exception e1) {
                    LOG.warn("abort multipartUpload failed", e1);
                }
            }
        }
        return st;
    }

    @Override
    public void close() throws Exception {
        // Create a BlobServiceClient instance (thread-safe and reusable).
       // Note: BlobServiceClient does NOT implement Closeable and does not require explicit closing.
    }
}
