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

package org.apache.doris.filesystem.azure;

import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.UploadPartResult;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.SasProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Azure Blob Storage implementation of {@link ObjStorage}.
 *
 * <p>No dependency on fe-core, fe-common, or fe-catalog.
 * Accepts a {@code Map<String, String>} of properties with the following keys:
 * <ul>
 *   <li>{@code AZURE_ACCOUNT_NAME} / {@code azure.account_name} / {@code AWS_ACCESS_KEY}
 *       — storage account name</li>
 *   <li>{@code AZURE_ACCOUNT_KEY} / {@code azure.account_key} / {@code AWS_SECRET_KEY}
 *       — storage account key (shared key auth)</li>
 *   <li>{@code AZURE_ENDPOINT} / {@code AWS_ENDPOINT} — custom endpoint (optional)</li>
 *   <li>{@code AZURE_CLIENT_ID} / {@code azure.oauth2_client_id} — service principal client ID</li>
 *   <li>{@code AZURE_CLIENT_SECRET} / {@code azure.oauth2_client_secret} — service principal secret</li>
 *   <li>{@code AZURE_TENANT_ID} / {@code azure.oauth2_client_tenant_id} — AAD tenant ID</li>
 * </ul>
 */
public class AzureObjStorage implements ObjStorage<BlobServiceClient> {

    private static final Logger LOG = LogManager.getLogger(AzureObjStorage.class);

    static final String PROP_ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    static final String PROP_ACCOUNT_NAME_ALT = "azure.account_name";
    static final String PROP_ACCOUNT_KEY = "AZURE_ACCOUNT_KEY";
    static final String PROP_ACCOUNT_KEY_ALT = "azure.account_key";
    static final String PROP_ENDPOINT = "AZURE_ENDPOINT";
    static final String PROP_ENDPOINT_ALT = "AWS_ENDPOINT";
    static final String PROP_CLIENT_ID = "AZURE_CLIENT_ID";
    static final String PROP_CLIENT_ID_ALT = "azure.oauth2_client_id";
    static final String PROP_CLIENT_SECRET = "AZURE_CLIENT_SECRET";
    static final String PROP_CLIENT_SECRET_ALT = "azure.oauth2_client_secret";
    static final String PROP_TENANT_ID = "AZURE_TENANT_ID";
    static final String PROP_TENANT_ID_ALT = "azure.oauth2_client_tenant_id";

    private static final String DEFAULT_ENDPOINT_TEMPLATE = "https://%s.blob.core.windows.net";
    private static final int HTTP_NOT_FOUND = 404;
    /** Validity period for presigned (SAS) URLs, in seconds. */
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final Map<String, String> properties;
    private volatile BlobServiceClient client;

    public AzureObjStorage(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(properties);
    }

    @Override
    public BlobServiceClient getClient() throws IOException {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = buildClient();
                }
            }
        }
        return client;
    }

    protected BlobServiceClient buildClient() throws IOException {
        String accountName = resolveAccountName();
        String endpoint = resolveEndpoint(accountName);
        String accountKey = resolve(PROP_ACCOUNT_KEY, PROP_ACCOUNT_KEY_ALT, null);
        if (accountKey == null || accountKey.isEmpty()) {
            // Fall back to AWS_SECRET_KEY for S3-compat configurations
            accountKey = properties.get("AWS_SECRET_KEY");
        }
        String clientId = resolve(PROP_CLIENT_ID, PROP_CLIENT_ID_ALT, null);
        String clientSecret = resolve(PROP_CLIENT_SECRET, PROP_CLIENT_SECRET_ALT, null);
        String tenantId = resolve(PROP_TENANT_ID, PROP_TENANT_ID_ALT, null);

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder().endpoint(endpoint);

        if (accountKey != null && !accountKey.isEmpty()) {
            builder.credential(new StorageSharedKeyCredential(accountName, accountKey));
        } else if (clientId != null && !clientId.isEmpty()
                && clientSecret != null && !clientSecret.isEmpty()
                && tenantId != null && !tenantId.isEmpty()) {
            builder.credential(new ClientSecretCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .build());
        } else {
            builder.credential(new DefaultAzureCredentialBuilder().build());
        }
        return builder.buildClient();
    }

    private String resolveAccountName() throws IOException {
        String name = resolve(PROP_ACCOUNT_NAME, PROP_ACCOUNT_NAME_ALT, null);
        if (name == null || name.isEmpty()) {
            // Fall back to AWS_ACCESS_KEY for S3-compat configurations
            name = properties.get("AWS_ACCESS_KEY");
        }
        if (name == null || name.isEmpty()) {
            throw new IOException("Azure account name is required. Set " + PROP_ACCOUNT_NAME);
        }
        return name;
    }

    private String resolveEndpoint(String accountName) {
        String endpoint = resolve(PROP_ENDPOINT, PROP_ENDPOINT_ALT, null);
        if (endpoint != null && !endpoint.isEmpty()) {
            if (!endpoint.startsWith("http")) {
                endpoint = "https://" + endpoint;
            }
            return endpoint;
        }
        return String.format(DEFAULT_ENDPOINT_TEMPLATE, accountName);
    }

    private String resolve(String primaryKey, String altKey, String defaultValue) {
        String value = properties.get(primaryKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        if (altKey != null) {
            value = properties.get(altKey);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return defaultValue;
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(uri.key());
            BlobContainerClient containerClient = getClient().getBlobContainerClient(uri.container());
            PagedIterable<BlobItem> pagedBlobs = containerClient.listBlobs(options, continuationToken, null);
            // The Azure SDK returns an empty PagedIterable (rather than throwing) when no
            // blobs match the prefix. Calling .next() on its page iterator without a hasNext()
            // guard would surface a NoSuchElementException to callers; treat empty pages as
            // an empty result instead.
            Iterator<PagedResponse<BlobItem>> pageIter = pagedBlobs.iterableByPage().iterator();
            if (!pageIter.hasNext()) {
                return new RemoteObjects(Collections.emptyList(), false, null);
            }
            PagedResponse<BlobItem> page = pageIter.next();

            List<RemoteObject> objects = new ArrayList<>();
            for (BlobItem item : page.getElements()) {
                objects.add(new RemoteObject(
                        item.getName(),
                        item.getName().startsWith(uri.key()) ? item.getName().substring(uri.key().length()) : "",
                        item.getProperties().getETag(),
                        item.getProperties().getContentLength() != null
                                ? item.getProperties().getContentLength() : 0L,
                        item.getProperties().getLastModified() != null
                                ? item.getProperties().getLastModified().toInstant().toEpochMilli() : 0L));
            }
            String nextToken = page.getContinuationToken();
            return new RemoteObjects(objects, nextToken != null && !nextToken.isEmpty(), nextToken);
        } catch (NoSuchElementException e) {
            throw new IOException("Failed to list Azure objects at " + remotePath
                    + ": empty paged response", e);
        } catch (BlobStorageException e) {
            throw new IOException("Failed to list Azure objects at " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public RemoteObject headObject(String remotePath) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlobClient blobClient = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key());
            BlobProperties props = blobClient.getProperties();
            return new RemoteObject(uri.key(), "", props.getETag(), props.getBlobSize(),
                    props.getLastModified() != null ? props.getLastModified().toInstant().toEpochMilli() : 0L);
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                throw new FileNotFoundException("404: Object not found: " + remotePath);
            }
            throw new IOException("headObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void putObject(String remotePath, RequestBody requestBody) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlobClient blobClient = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key());
            blobClient.upload(requestBody.content(), requestBody.contentLength(), true);
        } catch (BlobStorageException e) {
            throw new IOException("putObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlobClient blobClient = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key());
            blobClient.delete();
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                return;
            }
            throw new IOException("deleteObject failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void copyObject(String srcPath, String dstPath) throws IOException {
        try {
            AzureUri srcUri = AzureUri.parse(srcPath);
            AzureUri dstUri = AzureUri.parse(dstPath);
            BlobClient srcClient = getClient().getBlobContainerClient(srcUri.container())
                    .getBlobClient(srcUri.key());
            BlobClient dstClient = getClient().getBlobContainerClient(dstUri.container())
                    .getBlobClient(dstUri.key());
            dstClient.beginCopy(srcClient.getBlobUrl(), null).waitForCompletion();
        } catch (BlobStorageException e) {
            throw new IOException("copyObject from " + srcPath + " to " + dstPath
                    + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String initiateMultipartUpload(String remotePath) throws IOException {
        // Azure block blobs don't have an explicit "initiate" API.
        // Return the path itself as the upload ID; block IDs are derived from part numbers.
        return remotePath;
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlockBlobClient blockBlobClient = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key()).getBlockBlobClient();
            String blockId = toBlockId(partNum);
            blockBlobClient.stageBlock(blockId, body.content(), body.contentLength());
            return new UploadPartResult(partNum, blockId);
        } catch (BlobStorageException e) {
            throw new IOException("uploadPart failed for " + remotePath + " part " + partNum
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlockBlobClient blockBlobClient = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key()).getBlockBlobClient();
            List<String> blockIds = new ArrayList<>();
            List<UploadPartResult> sorted = new ArrayList<>(parts);
            sorted.sort((a, b) -> Integer.compare(a.partNumber(), b.partNumber()));
            for (UploadPartResult part : sorted) {
                blockIds.add(toBlockId(part.partNumber()));
            }
            blockBlobClient.commitBlockList(blockIds);
        } catch (BlobStorageException e) {
            throw new IOException("completeMultipartUpload failed for " + remotePath
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
        // Azure has no native "abort multipart upload" API; the closest equivalent is to
        // commit an empty block list (which atomically discards any uncommitted blocks
        // for that blob) and then delete the resulting empty blob so no trace remains.
        //
        // SAFETY: commitBlockList(empty) overwrites whatever is at the target blob, so we
        // MUST refuse to run when a committed blob already exists at this path — otherwise
        // an abort call could destroy real user data. In that case the staged blocks are
        // left to expire on their own (Azure GCs them after the service-side timeout).
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlockBlobClient blockBlobClient = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key()).getBlockBlobClient();
            boolean committedBlobExists;
            try {
                blockBlobClient.getProperties();
                committedBlobExists = true;
            } catch (BlobStorageException e) {
                if (e.getStatusCode() != HTTP_NOT_FOUND) {
                    throw e;
                }
                committedBlobExists = false;
            }
            if (committedBlobExists) {
                LOG.warn("abortMultipartUpload skipped for {}: a committed blob already exists; "
                        + "uncommitted blocks will expire automatically.", remotePath);
                return;
            }
            blockBlobClient.commitBlockList(Collections.emptyList());
            blockBlobClient.delete();
        } catch (BlobStorageException e) {
            // Best-effort: log and swallow rather than mask the original failure that
            // triggered the abort path. Uncommitted blocks will be GC'd by the service.
            LOG.warn("abortMultipartUpload best-effort cleanup failed for {}: {}",
                    remotePath, e.getMessage());
        }
    }

    /**
     * Opens an InputStream to download the blob at the given path.
     * Used by {@link AzureFileSystem} for read operations.
     */
    InputStream openInputStream(String remotePath) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlobClient blobClient = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key());
            return blobClient.openInputStream();
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("openInputStream failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    /**
     * Opens an InputStream starting at {@code fromByte} using an HTTP Range request.
     */
    InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlobClient blobClient = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key());
            com.azure.storage.blob.options.BlobInputStreamOptions opts =
                    new com.azure.storage.blob.options.BlobInputStreamOptions()
                            .setRange(new com.azure.storage.blob.models.BlobRange(fromByte));
            return blobClient.openInputStream(opts);
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("openInputStream failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    /**
     * Returns the last-modified time of the blob in milliseconds since epoch.
     */
    long headObjectLastModified(String remotePath) throws IOException {
        try {
            AzureUri uri = AzureUri.parse(remotePath);
            BlobProperties props = getClient().getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key()).getProperties();
            return props.getLastModified() != null ? props.getLastModified().toInstant().toEpochMilli() : 0L;
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                throw new FileNotFoundException("Object not found: " + remotePath);
            }
            throw new IOException("getProperties failed for " + remotePath + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // -------------------------------------------------------------------------
    // Cloud-specific extension methods
    // -------------------------------------------------------------------------

    /**
     * Generates an Azure SAS presigned URL for PUT access to the given blob key.
     *
     * <p>Requires shared-key authentication ({@code AZURE_ACCOUNT_KEY} /
     * {@code azure.account_key} / {@code AWS_SECRET_KEY}). The returned URL
     * is valid for {@link #SESSION_EXPIRE_SECONDS} seconds.
     *
     * @param objectKey blob key inside the container (no leading slash)
     * @return fully-qualified HTTPS URL with embedded SAS token
     * @throws IOException if the account key is missing or SAS generation fails
     */
    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String accountName = resolveAccountName();
        String accountKey = resolve(PROP_ACCOUNT_KEY, PROP_ACCOUNT_KEY_ALT, null);
        if (accountKey == null || accountKey.isEmpty()) {
            // Fall back to AWS_SECRET_KEY for S3-compat configurations
            accountKey = properties.get("AWS_SECRET_KEY");
        }
        if (accountKey == null || accountKey.isEmpty()) {
            throw new IOException(
                    "getPresignedUrl requires a storage account key (AZURE_ACCOUNT_KEY)");
        }
        AzureUri uri;
        try {
            uri = AzureUri.parse(objectKey);
        } catch (Exception e) {
            throw new IOException("Cannot parse Azure object key: " + objectKey, e);
        }
        String endpoint = resolveEndpoint(accountName);
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        return generateSasUrl(endpoint, uri.container(), uri.key(), credential,
                OffsetDateTime.now().plusSeconds(SESSION_EXPIRE_SECONDS));
    }

    /**
     * Creates a SAS URL for a blob. Protected for testability.
     */
    protected String generateSasUrl(String endpoint, String container, String blobKey,
            StorageSharedKeyCredential credential, OffsetDateTime expiresOn) {
        BlobContainerClient containerClient = new BlobContainerClientBuilder()
                .endpoint(endpoint + "/" + container)
                .credential(credential)
                .buildClient();
        BlobClient blobClient = containerClient.getBlobClient(blobKey);
        BlobSasPermission permission = new BlobSasPermission()
                .setReadPermission(true)
                .setWritePermission(true)
                .setDeletePermission(true);
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(expiresOn, permission)
                .setProtocol(SasProtocol.HTTPS_ONLY)
                .setStartTime(OffsetDateTime.now().minusMinutes(5));
        String sasToken = blobClient.generateSas(sasValues);
        return blobClient.getBlobUrl() + "?" + sasToken;
    }

    /**
     * Lists blobs whose keys start with {@code prefix + subPrefix}.
     *
     * @param prefix      base prefix (e.g., a staging directory path)
     * @param subPrefix   additional sub-prefix to narrow the listing
     * @param token       continuation token from a previous call, or {@code null}
     * @return paged listing result
     * @throws IOException on Azure API errors
     */
    @Override
    public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix, String token)
            throws IOException {
        String fullPrefix = prefix + (subPrefix == null ? "" : subPrefix);
        return listObjects(fullPrefix, token);
    }

    /**
     * Returns metadata for a single blob identified by {@code prefix + subKey}.
     *
     * <p>Returns an empty result instead of throwing if the blob does not exist.
     *
     * @param prefix base directory prefix
     * @param subKey relative key appended to prefix
     * @return {@link RemoteObjects} containing zero or one {@link RemoteObject}
     * @throws IOException on Azure API errors (other than 404)
     */
    @Override
    public RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        String fullKey = prefix + subKey;
        try {
            AzureUri uri = AzureUri.parse(fullKey);
            BlobProperties props = getClient()
                    .getBlobContainerClient(uri.container())
                    .getBlobClient(uri.key())
                    .getProperties();
            long size = props.getBlobSize();
            long lastModifiedMs = props.getLastModified() != null
                    ? props.getLastModified().toInstant().toEpochMilli() : 0L;
            RemoteObject obj = new RemoteObject(uri.key(), uri.key(), null, size, lastModifiedMs);
            return new RemoteObjects(Collections.singletonList(obj), false, null);
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
                return new RemoteObjects(Collections.emptyList(), false, null);
            }
            throw new IOException("headObjectWithMeta failed for " + fullKey + ": " + e.getMessage(), e);
        }
    }

    /**
     * Deletes multiple blobs by their keys within the given container.
     *
     * <p>All deletions are attempted; any failures are collected and thrown
     * as a single {@link IOException} at the end.
     *
     * @param container container (bucket) name
     * @param keys      blob keys to delete
     * @throws IOException if one or more deletions fail
     */
    @Override
    public void deleteObjectsByKeys(String container, List<String> keys) throws IOException {
        BlobContainerClient containerClient = getClient().getBlobContainerClient(container);
        List<String> failures = new ArrayList<>();
        List<Throwable> causes = new ArrayList<>();
        for (String key : keys) {
            try {
                containerClient.getBlobClient(key).delete();
            } catch (BlobStorageException e) {
                if (e.getStatusCode() != HTTP_NOT_FOUND) {
                    failures.add(key + ": " + e.getMessage());
                    causes.add(e);
                }
            } catch (Exception e) {
                failures.add(key + ": " + e.getMessage());
                causes.add(e);
            }
        }
        if (!failures.isEmpty()) {
            IOException composed = new IOException(
                    "deleteObjectsByKeys failed for: " + String.join(", ", failures));
            // Attach the original per-key exceptions as suppressed so the full cause
            // chain (status codes, request IDs, stack traces) survives in debug logs.
            for (Throwable cause : causes) {
                composed.addSuppressed(cause);
            }
            throw composed;
        }
    }

    @Override
    public void close() throws IOException {
        // BlobServiceClient is not Closeable. It internally shares the JVM-wide HTTP
        // client (Netty / OkHttp connection pool) configured by the Azure SDK, which is
        // intentionally process-scoped: tearing it down here would also disrupt every
        // other AzureObjStorage instance that happens to share the pool. The Azure SDK
        // releases the pool on JVM shutdown.
    }

    /**
     * Converts a part number to a Base64-encoded block ID using little-endian byte order,
     * consistent with the existing fe-core Azure multipart upload implementation.
     */
    private static String toBlockId(int partNum) {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(partNum).array();
        return Base64.getEncoder().encodeToString(bytes);
    }
}
