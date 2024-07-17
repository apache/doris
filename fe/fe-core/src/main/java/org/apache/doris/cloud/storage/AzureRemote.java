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

import org.apache.doris.common.DdlException;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.SasProtocol;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public class AzureRemote extends RemoteBase {

    private static final Logger LOG = LogManager.getLogger(AzureRemote.class);

    private static final String URI_TEMPLATE = "https://%s.blob.core.windows.net/%s";

    private BlobContainerClient client;

    public AzureRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        try {
            BlobContainerClientBuilder builder = new BlobContainerClientBuilder();
            builder.credential(new StorageSharedKeyCredential(obj.getAk(), obj.getSk()));
            String containerName = obj.getBucket();
            String uri = String.format(URI_TEMPLATE, obj.getAk(),
                    containerName);
            builder.endpoint(uri);
            BlobContainerClient containerClient = builder.buildClient();

            BlobClient blobClient = containerClient.getBlobClient(normalizePrefix(fileName));

            OffsetDateTime expiryTime = OffsetDateTime.now().plusSeconds(SESSION_EXPIRE_SECOND);
            BlobSasPermission permission = new BlobSasPermission()
                    .setReadPermission(true)
                    .setWritePermission(true)
                    .setDeletePermission(true);

            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(expiryTime, permission)
                    .setProtocol(SasProtocol.HTTPS_ONLY)
                    .setStartTime(OffsetDateTime.now());

            String sasToken = blobClient.generateSas(sasValues);
            return blobClient.getBlobUrl() + "?" + sasToken;
        } catch (Exception e) {
            e.getStackTrace();
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
            BlobClient blobClient = client.getBlobClient(key);
            BlobProperties properties = blobClient.getProperties();
            ObjectFile objectFile = new ObjectFile(key, getRelativePath(key), properties.getETag(),
                    properties.getBlobSize());
            return new ListObjectsResult(Lists.newArrayList(objectFile), false, null);
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                LOG.warn("NoSuchKey when head object for Azure, subKey={}", subKey);
                return new ListObjectsResult(Lists.newArrayList(), false, null);
            }
            LOG.warn("Failed to head object for Azure, subKey={}", subKey, e);
            throw new DdlException(
                    "Failed to head object for Azure, subKey=" + subKey + " Error message=" + e.getMessage());
        }
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        try {
            BlobContainerClientBuilder builder = new BlobContainerClientBuilder();
            builder.credential(new StorageSharedKeyCredential(obj.getAk(), obj.getSk()));
            String containerName = obj.getBucket();
            String uri = String.format(URI_TEMPLATE, obj.getAk(),
                    containerName);
            builder.endpoint(uri);
            BlobContainerClient containerClient = builder.buildClient();
            BlobServiceClient blobServiceClient = containerClient.getServiceClient();

            OffsetDateTime keyStart = OffsetDateTime.now();
            OffsetDateTime keyExpiry = keyStart.plusSeconds(SESSION_EXPIRE_SECOND);
            UserDelegationKey userDelegationKey = blobServiceClient.getUserDelegationKey(keyStart, keyExpiry);

            OffsetDateTime expiryTime = OffsetDateTime.now().plusSeconds(SESSION_EXPIRE_SECOND);
            BlobContainerSasPermission permission = new BlobContainerSasPermission()
                    .setReadPermission(true)
                    .setWritePermission(true)
                    .setListPermission(true);

            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(expiryTime, permission)
                    .setProtocol(SasProtocol.HTTPS_ONLY)
                    .setStartTime(OffsetDateTime.now());

            String sasToken = containerClient.generateUserDelegationSas(sasValues, userDelegationKey);
            return Triple.of(obj.getAk(), obj.getSk(), sasToken);
        } catch (Throwable e) {
            LOG.warn("Failed get Azure sts token", e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public void deleteObjects(List<String> keys) throws DdlException {
        checkDeleteKeys(keys);
        initClient();

        try {
            BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(
                    client.getServiceClient()).buildClient();
            int maxDelete = 1000;
            for (int i = 0; i < keys.size() / maxDelete + 1; i++) {
                int cnt = 0;
                BlobBatch batch = blobBatchClient.getBlobBatch();
                for (int j = maxDelete * i; j < keys.size() && cnt < maxDelete; j++) {
                    batch.deleteBlob(client.getBlobContainerName(), keys.get(j));
                    cnt++;
                }
                Response<Void> resp = blobBatchClient.submitBatchWithResponse(batch, true, null, Context.NONE);
                if (resp.getStatusCode() != HttpStatus.SC_OK) {
                    throw new DdlException(
                            "Failed delete objects, bucket=" + obj.getBucket());
                }
            }
        } catch (BlobStorageException e) {
            LOG.warn("Failed to delete objects for Azure", e);
            throw new DdlException("Failed to delete objects for Azure, Error message=" + e.getMessage());
        }
    }

    @Override
    public void close() {
        client = null;
    }

    @Override
    public String toString() {
        return "AzureRemote{obj=" + obj + '}';
    }

    private ListObjectsResult listObjectsInner(String prefix, String continuationToken) throws DdlException {
        initClient();
        try {
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(prefix);
            PagedIterable<BlobItem> pagedBlobs = client.listBlobs(options, continuationToken, null);
            PagedResponse<BlobItem> pagedResponse = pagedBlobs.iterableByPage().iterator().next();
            List<ObjectFile> objectFiles = new ArrayList<>();

            for (BlobItem blobItem : pagedResponse.getElements()) {
                objectFiles.add(new ObjectFile(blobItem.getName(), getRelativePath(blobItem.getName()),
                        blobItem.getProperties().getETag(), blobItem.getProperties().getContentLength()));
            }
            return new ListObjectsResult(objectFiles, pagedResponse.getContinuationToken() != null,
                    pagedResponse.getContinuationToken());
        } catch (BlobStorageException e) {
            LOG.warn("Failed to list objects for Azure", e);
            throw new DdlException("Failed to list objects for Azure, Error message=" + e.getMessage());
        }
    }

    private void initClient() {
        if (client == null) {
            BlobContainerClientBuilder builder = new BlobContainerClientBuilder();
            if (obj.getToken() != null) {
                builder.credential(new SimpleTokenCredential(obj.getToken()));
            } else {
                builder.credential(new StorageSharedKeyCredential(obj.getAk(), obj.getSk()));
            }
            String containerName = obj.getBucket();
            String uri = String.format(URI_TEMPLATE, obj.getAk(),
                    containerName);
            builder.endpoint(uri);
            client = builder.buildClient();
        }
    }

    // Custom implementation of TokenCredential
    private static class SimpleTokenCredential implements TokenCredential {
        private static final Logger LOG = LogManager.getLogger(SimpleTokenCredential.class);
        private final String token;

        SimpleTokenCredential(String token) {
            this.token = token;
        }

        @Override
        public Mono<AccessToken> getToken(TokenRequestContext request) {
            LOG.info("Getting token for scopes: {}", String.join(", ", request.getScopes()));
            // Assume the token is valid for 1 hour from the current time
            return Mono.just(new AccessToken(token, OffsetDateTime.now().plusSeconds(SESSION_EXPIRE_SECOND)));
        }
    }
}
