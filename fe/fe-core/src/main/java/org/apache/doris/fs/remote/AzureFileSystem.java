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

package org.apache.doris.fs.remote;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.fs.obj.AzureObjStorage;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class AzureFileSystem extends ObjFileSystem {
    private static final Logger LOG = LogManager.getLogger(AzureFileSystem.class);

    public AzureFileSystem(Map<String, String> properties) {
        super(StorageType.AZURE.name(), StorageType.AZURE, new AzureObjStorage(properties));
        initFsProperties();
    }

    @VisibleForTesting
    public AzureFileSystem(AzureObjStorage storage) {
        super(StorageBackend.StorageType.AZURE.name(), StorageBackend.StorageType.AZURE, storage);
        initFsProperties();
    }

    private void initFsProperties() {
        this.properties.putAll(((AzureObjStorage) objStorage).getProperties());
    }

    @Override
    protected FileSystem nativeFileSystem(String remotePath) throws UserException {
        return null;
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

    // Due to historical reasons, when the BE parses the object storage path.
    // It assumes the path starts with 'S3://${containerName}'
    // So here the path needs to be constructed in a format that BE can parse.
    private String constructS3Path(String fileName) throws UserException {
        BlobContainerClient client = (BlobContainerClient) getObjStorage().getClient();
        String bucket = client.getBlobContainerName();
        LOG.info("the path is {}", String.format("s3://%s/%s", bucket, fileName));
        return String.format("s3://%s/%s", bucket, fileName);
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        String copyPath = new String(remotePath);
        copyPath = removeUselessSchema(copyPath);
        LOG.info("try to glob list for azure, remote path {}, orig {}", copyPath, remotePath);
        try {
            BlobContainerClient client = (BlobContainerClient) getObjStorage().getClient();
            java.nio.file.Path pathPattern = Paths.get(copyPath);
            LOG.info("path pattern {}", pathPattern.toString());
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern.toString());

            ListBlobsOptions options = new ListBlobsOptions().setPrefix(copyPath);
            String newContinuationToken = null;
            do {
                PagedIterable<BlobItem> pagedBlobs = client.listBlobs(options, newContinuationToken, null);
                PagedResponse<BlobItem> pagedResponse = pagedBlobs.iterableByPage().iterator().next();

                for (BlobItem blobItem : pagedResponse.getElements()) {
                    java.nio.file.Path blobPath = Paths.get(blobItem.getName());

                    if (matcher.matches(blobPath)) {
                        RemoteFile remoteFile = new RemoteFile(
                                fileNameOnly ? blobPath.getFileName().toString() : constructS3Path(blobPath.toString()),
                                !blobItem.isPrefix(),
                                blobItem.isPrefix() ? -1 : blobItem.getProperties().getContentLength(),
                                blobItem.getProperties().getContentLength(),
                                blobItem.getProperties().getLastModified().getSecond());
                        result.add(remoteFile);
                    }
                }
                newContinuationToken = pagedResponse.getContinuationToken();
            } while (newContinuationToken != null);

        } catch (BlobStorageException e) {
            LOG.warn("glob file " + remotePath + " failed because azure error: " + e.getMessage());
            return new Status(Status.ErrCode.COMMON_ERROR, "glob file " + remotePath
                    + " failed because azure error: " + e.getMessage());
        } catch (Exception e) {
            LOG.warn("errors while glob file " + remotePath, e);
            return new Status(Status.ErrCode.COMMON_ERROR, "errors while glob file " + remotePath + e.getMessage());
        }
        return Status.OK;
    }
}
