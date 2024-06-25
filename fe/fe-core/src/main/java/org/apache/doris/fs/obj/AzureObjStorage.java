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
import org.apache.doris.datasource.property.constants.S3Properties;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AzureObjStorage implements ObjStorage<BlobContainerClient> {
    private static final Logger LOG = LogManager.getLogger(AzureObjStorage.class);
    protected Map<String, String> properties;
    private BlobContainerClient client;

    private static final String URI_TEMPLATE = "https://%s.blob.core.windows.net/%s";

    public AzureObjStorage(Map<String, String> properties) {
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
    }

    @Override
    public BlobContainerClient getClient() throws UserException {
        if (client == null) {
            String containerName = properties.get(S3Properties.BUCKET);
            String uri = String.format(URI_TEMPLATE, properties.get(S3Properties.ACCESS_KEY), containerName);
            StorageSharedKeyCredential cred = new StorageSharedKeyCredential(properties.get(S3Properties.ACCESS_KEY),
                    properties.get(S3Properties.SECRET_KEY));
            BlobContainerClientBuilder builder = new BlobContainerClientBuilder();
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
        BlobClient blobClient = client.getBlobClient(remotePath);
        try {
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
        }
    }

    @Override
    public Status getObject(String remoteFilePath, File localFile) {
        try {
            BlobClient blobClient = client.getBlobClient(remoteFilePath);
            BlobProperties properties = blobClient.downloadToFile(localFile.getAbsolutePath());
            LOG.info("get file " + remoteFilePath + " success: " + properties.toString());
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "get file from azure error: " + e.getServiceMessage());
        }
    }

    @Override
    public Status putObject(String remotePath, @Nullable InputStream content, long contentLength) {
        try {
            BlobClient blobClient = client.getBlobClient(remotePath);
            blobClient.upload(content, contentLength);
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "Error occurred while copying the blob:: " + e.getServiceMessage());
        }
    }

    @Override
    public Status deleteObject(String remotePath) {
        try {
            BlobClient blobClient = client.getBlobClient(remotePath);
            blobClient.delete();
            LOG.info("delete file " + remotePath + " success");
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "get file from azure error: " + e.getServiceMessage());
        }
    }

    @Override
    public Status deleteObjects(String remotePath) {
        try {
            String continuationToken = "";
            boolean isTruncated = false;
            long totalObjects = 0;
            do {
                RemoteObjects objects = listObjects(remotePath, continuationToken);
                List<RemoteObject> objectList = objects.getObjectList();
                if (!objectList.isEmpty()) {
                    BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(
                            client.getServiceClient()).buildClient();
                    BlobBatch blobBatch = blobBatchClient.getBlobBatch();

                    for (RemoteObject blob : objectList) {
                        blobBatch.deleteBlob(client.getBlobContainerUrl(), blob.getKey());
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
            BlobClient sourceBlobClient = client.getBlobClient(origFilePath);
            BlobClient destinationBlobClient = client.getBlobClient(destFilePath);
            destinationBlobClient.beginCopy(sourceBlobClient.getBlobUrl(), null);
            System.out.println("Blob copied from " + origFilePath + " to " + destFilePath);
            return Status.OK;
        } catch (BlobStorageException e) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "Error occurred while copying the blob:: " + e.getServiceMessage());
        }
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws DdlException {
        try {
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(remotePath);
            PagedIterable<BlobItem> pagedBlobs = client.listBlobs(options, continuationToken, null);
            PagedResponse<BlobItem> pagedResponse = pagedBlobs.iterableByPage().iterator().next();
            List<RemoteObject> remoteObjects = new ArrayList<>();

            for (BlobItem blobItem : pagedResponse.getElements()) {
                remoteObjects.add(new RemoteObject(blobItem.getName(), "", blobItem.getProperties().getETag(),
                        blobItem.getProperties().getContentLength()));
            }
            return new RemoteObjects(remoteObjects, pagedResponse.getContinuationToken() == null,
                    pagedResponse.getContinuationToken());
        } catch (BlobStorageException e) {
            LOG.warn(String.format("Failed to list objects for S3: %s", remotePath), e);
            throw new DdlException("Failed to list objects for S3, Error message: " + e.getMessage(), e);
        }
    }
}
