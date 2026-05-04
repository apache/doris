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

import org.apache.doris.filesystem.spi.RemoteObjects;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the cloud extension methods added to {@link AzureObjStorage}:
 * {@code getPresignedUrl}, {@code listObjectsWithPrefix}, {@code headObjectWithMeta},
 * and {@code deleteObjectsByKeys}.
 *
 * <p>Cloud calls are replaced with mocks or overrides; no real Azure credentials are required.
 */
class AzureObjStorageExtensionTest {

    // ------------------------------------------------------------------
    // getPresignedUrl tests
    // ------------------------------------------------------------------

    @Test
    void getPresignedUrl_missingAccountKeyThrowsIOException() {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ACCOUNT_NAME", "myaccount");
        // no account key

        AzureObjStorage storage = new TestableAzureObjStorage(props, null);

        Assertions.assertThrows(IOException.class,
                () -> storage.getPresignedUrl("wasb://mycontainer@myaccount.blob.core.windows.net/blob/key"),
                "Should throw when account key is absent");
    }

    @Test
    void getPresignedUrl_invalidUriThrowsIOException() {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ACCOUNT_NAME", "myaccount");
        props.put("AZURE_ACCOUNT_KEY", "base64-encoded-key==");

        AzureObjStorage storage = new TestableAzureObjStorage(props, null);

        // bare key without scheme is not a valid Azure URI
        Assertions.assertThrows(IOException.class,
                () -> storage.getPresignedUrl("bare/key/without/scheme"),
                "Should throw when URI cannot be parsed");
    }

    @Test
    void getPresignedUrl_happyPath_returnsSasUrl() throws Exception {
        String expectedSasUrl = "https://myaccount.blob.core.windows.net/mycontainer/stage/blob?sv=2021&sig=ABC";
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ACCOUNT_NAME", "myaccount");
        props.put("AZURE_ACCOUNT_KEY", "dGVzdA=="); // base64("test")

        // TestableAzureObjStorage overrides generateSasUrl to return the expected URL
        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, null);
        storage.stubbedSasUrl = expectedSasUrl;

        String result = storage.getPresignedUrl("wasb://mycontainer@myaccount.blob.core.windows.net/stage/blob");

        Assertions.assertEquals(expectedSasUrl, result);
    }

    @Test
    void getPresignedUrl_passesParsedUriToGenerateSasUrl() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ACCOUNT_NAME", "myaccount");
        props.put("AZURE_ACCOUNT_KEY", "dGVzdA==");

        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, null);
        storage.stubbedSasUrl = "https://url?sas=token";

        storage.getPresignedUrl("wasb://mycontainer@myaccount.blob.core.windows.net/stage/blob");

        // Verify the container and blobKey parsed from the URI were passed correctly
        Assertions.assertEquals("mycontainer", storage.lastGenerateSasContainer);
        Assertions.assertEquals("stage/blob", storage.lastGenerateSasBlobKey);
    }

    @Test
    void getPresignedUrl_fallsBackToAwsSecretKey() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ACCOUNT_NAME", "myaccount");
        // No AZURE_ACCOUNT_KEY, but AWS_SECRET_KEY is present (S3-compat config)
        props.put("AWS_SECRET_KEY", "dGVzdA==");

        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, null);
        storage.stubbedSasUrl = "https://sas-with-fallback-key";

        String result = storage.getPresignedUrl(
                "wasb://mycontainer@myaccount.blob.core.windows.net/blob");
        Assertions.assertEquals("https://sas-with-fallback-key", result);
    }

    // ------------------------------------------------------------------
    // listObjectsWithPrefix tests
    // ------------------------------------------------------------------

    @Test
    void listObjectsWithPrefix_mergesPrefixAndSubPrefix() throws Exception {
        Map<String, String> props = buildBasicProps();
        ListObjectsCaptureStorage storage = new ListObjectsCaptureStorage(props);

        storage.listObjectsWithPrefix(
                "wasb://mycontainer@myaccount.blob.core.windows.net/stage/",
                "dir1/",
                null);

        // merged remotePath passed to listObjects should be prefix + subPrefix
        Assertions.assertEquals(
                "wasb://mycontainer@myaccount.blob.core.windows.net/stage/dir1/",
                storage.capturedRemotePath);
        Assertions.assertNull(storage.capturedToken);
    }

    @Test
    void listObjectsWithPrefix_nullSubPrefixTreatedAsEmpty() throws Exception {
        Map<String, String> props = buildBasicProps();
        ListObjectsCaptureStorage storage = new ListObjectsCaptureStorage(props);

        storage.listObjectsWithPrefix(
                "wasb://mycontainer@myaccount.blob.core.windows.net/stage/",
                null,
                "tok123");

        Assertions.assertEquals(
                "wasb://mycontainer@myaccount.blob.core.windows.net/stage/",
                storage.capturedRemotePath);
        Assertions.assertEquals("tok123", storage.capturedToken);
    }

    // ------------------------------------------------------------------
    // headObjectWithMeta tests
    // ------------------------------------------------------------------

    @Test
    void headObjectWithMeta_blobFound_returnsOneElementResult() throws Exception {
        BlobProperties mockProps = Mockito.mock(BlobProperties.class);
        Mockito.when(mockProps.getBlobSize()).thenReturn(1234L);
        Mockito.when(mockProps.getLastModified()).thenReturn(OffsetDateTime.parse("2024-01-01T00:00:00Z"));

        BlobClient mockBlobClient = Mockito.mock(BlobClient.class);
        Mockito.when(mockBlobClient.getProperties()).thenReturn(mockProps);

        BlobContainerClient mockContainerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(mockContainerClient.getBlobClient("stage/myfile.parquet"))
                .thenReturn(mockBlobClient);

        BlobServiceClient mockServiceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(mockServiceClient.getBlobContainerClient("mycontainer"))
                .thenReturn(mockContainerClient);

        Map<String, String> props = buildBasicProps();
        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, mockServiceClient);

        RemoteObjects result = storage.headObjectWithMeta(
                "wasb://mycontainer@myaccount.blob.core.windows.net/stage/",
                "myfile.parquet");

        Assertions.assertFalse(result.isTruncated());
        Assertions.assertEquals(1, result.getObjectList().size());
        Assertions.assertEquals("stage/myfile.parquet", result.getObjectList().get(0).getKey());
        Assertions.assertEquals(1234L, result.getObjectList().get(0).getSize());
    }

    @Test
    void headObjectWithMeta_blobNotFound_returnsEmptyResult() throws Exception {
        BlobClient mockBlobClient = Mockito.mock(BlobClient.class);
        BlobStorageException notFoundEx = Mockito.mock(BlobStorageException.class);
        Mockito.when(notFoundEx.getStatusCode()).thenReturn(404);
        Mockito.when(mockBlobClient.getProperties()).thenThrow(notFoundEx);

        BlobContainerClient mockContainerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(mockContainerClient.getBlobClient(Mockito.anyString()))
                .thenReturn(mockBlobClient);

        BlobServiceClient mockServiceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(mockServiceClient.getBlobContainerClient("mycontainer"))
                .thenReturn(mockContainerClient);

        Map<String, String> props = buildBasicProps();
        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, mockServiceClient);

        RemoteObjects result = storage.headObjectWithMeta(
                "wasb://mycontainer@myaccount.blob.core.windows.net/stage/",
                "nonexistent.parquet");

        Assertions.assertFalse(result.isTruncated());
        Assertions.assertEquals(0, result.getObjectList().size());
    }

    // ------------------------------------------------------------------
    // deleteObjectsByKeys tests
    // ------------------------------------------------------------------

    @Test
    void deleteObjectsByKeys_allSucceed_noException() throws Exception {
        BlobClient mockBlobClient = Mockito.mock(BlobClient.class);
        // delete() does nothing (no exception) by default

        BlobContainerClient mockContainerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(mockContainerClient.getBlobClient(Mockito.anyString()))
                .thenReturn(mockBlobClient);

        BlobServiceClient mockServiceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(mockServiceClient.getBlobContainerClient("mycontainer"))
                .thenReturn(mockContainerClient);

        Map<String, String> props = buildBasicProps();
        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, mockServiceClient);

        List<String> keys = Arrays.asList("stage/f1.parquet", "stage/f2.parquet");
        // Should not throw
        storage.deleteObjectsByKeys("mycontainer", keys);

        Mockito.verify(mockContainerClient).getBlobClient("stage/f1.parquet");
        Mockito.verify(mockContainerClient).getBlobClient("stage/f2.parquet");
        Mockito.verify(mockBlobClient, Mockito.times(2)).delete();
    }

    @Test
    void deleteObjectsByKeys_partialFailure_throwsIOException() throws Exception {
        BlobClient successClient = Mockito.mock(BlobClient.class);
        BlobClient failClient = Mockito.mock(BlobClient.class);
        BlobStorageException blobEx = Mockito.mock(BlobStorageException.class);
        Mockito.when(blobEx.getStatusCode()).thenReturn(500);
        Mockito.when(blobEx.getMessage()).thenReturn("Internal error");
        Mockito.doThrow(blobEx).when(failClient).delete();

        BlobContainerClient mockContainerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(mockContainerClient.getBlobClient("stage/good.parquet")).thenReturn(successClient);
        Mockito.when(mockContainerClient.getBlobClient("stage/bad.parquet")).thenReturn(failClient);

        BlobServiceClient mockServiceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(mockServiceClient.getBlobContainerClient("mycontainer"))
                .thenReturn(mockContainerClient);

        Map<String, String> props = buildBasicProps();
        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, mockServiceClient);

        List<String> keys = Arrays.asList("stage/good.parquet", "stage/bad.parquet");
        IOException ex = Assertions.assertThrows(IOException.class,
                () -> storage.deleteObjectsByKeys("mycontainer", keys));
        Assertions.assertTrue(ex.getMessage().contains("stage/bad.parquet"),
                "Error message should mention the failed key");
    }

    // ------------------------------------------------------------------
    // listObjects empty-iterator tests (F15)
    // ------------------------------------------------------------------

    @Test
    void listObjects_emptyPagedResponseIterator_returnsEmptyResultsAndDoesNotThrow() throws Exception {
        // PagedIterable whose iterableByPage().iterator() reports no elements.
        @SuppressWarnings("unchecked")
        com.azure.core.http.rest.PagedIterable<com.azure.storage.blob.models.BlobItem> emptyPaged =
                Mockito.mock(com.azure.core.http.rest.PagedIterable.class);
        Iterable<com.azure.core.http.rest.PagedResponse<com.azure.storage.blob.models.BlobItem>>
                emptyIterable = Collections::emptyIterator;
        Mockito.when(emptyPaged.iterableByPage()).thenReturn(emptyIterable);

        BlobContainerClient mockContainerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(mockContainerClient.listBlobs(
                Mockito.any(com.azure.storage.blob.models.ListBlobsOptions.class),
                Mockito.any(), Mockito.any())).thenReturn(emptyPaged);

        BlobServiceClient mockServiceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(mockServiceClient.getBlobContainerClient("mycontainer"))
                .thenReturn(mockContainerClient);

        Map<String, String> props = buildBasicProps();
        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, mockServiceClient);

        // Must not throw NoSuchElementException; must return an empty result.
        RemoteObjects result = storage.listObjects(
                "wasb://mycontainer@myaccount.blob.core.windows.net/empty/", null);
        Assertions.assertFalse(result.isTruncated());
        Assertions.assertEquals(0, result.getObjectList().size());
    }

    @Test
    void deleteObjectsByKeys_notFoundIgnored() throws Exception {
        BlobClient mockBlobClient = Mockito.mock(BlobClient.class);
        BlobStorageException notFoundEx = Mockito.mock(BlobStorageException.class);
        Mockito.when(notFoundEx.getStatusCode()).thenReturn(404);
        Mockito.doThrow(notFoundEx).when(mockBlobClient).delete();

        BlobContainerClient mockContainerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(mockContainerClient.getBlobClient(Mockito.anyString()))
                .thenReturn(mockBlobClient);

        BlobServiceClient mockServiceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(mockServiceClient.getBlobContainerClient("mycontainer"))
                .thenReturn(mockContainerClient);

        Map<String, String> props = buildBasicProps();
        TestableAzureObjStorage storage = new TestableAzureObjStorage(props, mockServiceClient);

        // 404 on delete is idempotent and should not throw
        storage.deleteObjectsByKeys("mycontainer", Collections.singletonList("stage/gone.parquet"));
    }

    // ------------------------------------------------------------------
    // F19 — deleteObjectsByKeys attaches per-key exceptions as suppressed
    // ------------------------------------------------------------------

    @Test
    void deleteObjectsByKeys_attachesPerKeyExceptionsAsSuppressed() throws Exception {
        BlobClient failClient1 = Mockito.mock(BlobClient.class);
        BlobStorageException ex1 = Mockito.mock(BlobStorageException.class);
        Mockito.when(ex1.getStatusCode()).thenReturn(500);
        Mockito.when(ex1.getMessage()).thenReturn("server boom");
        Mockito.doThrow(ex1).when(failClient1).delete();

        BlobClient failClient2 = Mockito.mock(BlobClient.class);
        RuntimeException ex2 = new RuntimeException("network timeout");
        Mockito.doThrow(ex2).when(failClient2).delete();

        BlobContainerClient mockContainerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(mockContainerClient.getBlobClient("stage/a.parquet")).thenReturn(failClient1);
        Mockito.when(mockContainerClient.getBlobClient("stage/b.parquet")).thenReturn(failClient2);

        BlobServiceClient mockServiceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(mockServiceClient.getBlobContainerClient("mycontainer"))
                .thenReturn(mockContainerClient);

        TestableAzureObjStorage storage = new TestableAzureObjStorage(buildBasicProps(), mockServiceClient);

        IOException composed = Assertions.assertThrows(IOException.class,
                () -> storage.deleteObjectsByKeys("mycontainer",
                        Arrays.asList("stage/a.parquet", "stage/b.parquet")));

        Throwable[] suppressed = composed.getSuppressed();
        Assertions.assertEquals(2, suppressed.length,
                "both per-key causes must be attached as suppressed exceptions");
        Assertions.assertSame(ex1, suppressed[0]);
        Assertions.assertSame(ex2, suppressed[1]);
    }

    // ------------------------------------------------------------------
    // F20 — abortMultipartUpload safe-noop / commit-empty behaviour
    // ------------------------------------------------------------------

    @Test
    void abortMultipartUpload_safeNoopWhenCommittedBlobExists() throws Exception {
        com.azure.storage.blob.models.BlobProperties props =
                Mockito.mock(com.azure.storage.blob.models.BlobProperties.class);
        Mockito.when(props.getBlobSize()).thenReturn(1024L);

        com.azure.storage.blob.specialized.BlockBlobClient blockClient =
                Mockito.mock(com.azure.storage.blob.specialized.BlockBlobClient.class);
        Mockito.when(blockClient.getProperties()).thenReturn(props);

        BlobClient blobClient = Mockito.mock(BlobClient.class);
        Mockito.when(blobClient.getBlockBlobClient()).thenReturn(blockClient);

        BlobContainerClient containerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(containerClient.getBlobClient("stage/blob")).thenReturn(blobClient);

        BlobServiceClient serviceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(serviceClient.getBlobContainerClient("mycontainer")).thenReturn(containerClient);

        TestableAzureObjStorage storage = new TestableAzureObjStorage(buildBasicProps(), serviceClient);

        storage.abortMultipartUpload(
                "wasb://mycontainer@myaccount.blob.core.windows.net/stage/blob", "uploadId");

        // The committed blob must NOT be touched (no commitBlockList, no delete).
        Mockito.verify(blockClient, Mockito.never()).commitBlockList(Mockito.anyList());
        Mockito.verify(blockClient, Mockito.never()).delete();
    }

    @Test
    void abortMultipartUpload_commitsEmptyAndDeletesWhenNoCommittedBlob() throws Exception {
        com.azure.storage.blob.specialized.BlockBlobClient blockClient =
                Mockito.mock(com.azure.storage.blob.specialized.BlockBlobClient.class);
        BlobStorageException notFoundEx = Mockito.mock(BlobStorageException.class);
        Mockito.when(notFoundEx.getStatusCode()).thenReturn(404);
        Mockito.when(blockClient.getProperties()).thenThrow(notFoundEx);

        BlobClient blobClient = Mockito.mock(BlobClient.class);
        Mockito.when(blobClient.getBlockBlobClient()).thenReturn(blockClient);

        BlobContainerClient containerClient = Mockito.mock(BlobContainerClient.class);
        Mockito.when(containerClient.getBlobClient("stage/blob")).thenReturn(blobClient);

        BlobServiceClient serviceClient = Mockito.mock(BlobServiceClient.class);
        Mockito.when(serviceClient.getBlobContainerClient("mycontainer")).thenReturn(containerClient);

        TestableAzureObjStorage storage = new TestableAzureObjStorage(buildBasicProps(), serviceClient);

        storage.abortMultipartUpload(
                "wasb://mycontainer@myaccount.blob.core.windows.net/stage/blob", "uploadId");

        Mockito.verify(blockClient).commitBlockList(Collections.emptyList());
        Mockito.verify(blockClient).delete();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private Map<String, String> buildBasicProps() {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ACCOUNT_NAME", "myaccount");
        props.put("AZURE_ACCOUNT_KEY", "dGVzdA==");
        return props;
    }

    /**
     * Subclass that injects a mock {@link BlobServiceClient} and overrides {@link #generateSasUrl}
     * so no real Azure SDK connections are needed.
     */
    private static class TestableAzureObjStorage extends AzureObjStorage {
        private final BlobServiceClient mockClient;
        String stubbedSasUrl = "https://stubbed-sas-url";
        String lastGenerateSasContainer;
        String lastGenerateSasBlobKey;

        TestableAzureObjStorage(Map<String, String> props, BlobServiceClient mockClient) {
            super(props);
            this.mockClient = mockClient;
        }

        @Override
        protected BlobServiceClient buildClient() {
            return mockClient;
        }

        @Override
        protected String generateSasUrl(String endpoint, String container, String blobKey,
                StorageSharedKeyCredential credential, OffsetDateTime expiresOn) {
            this.lastGenerateSasContainer = container;
            this.lastGenerateSasBlobKey = blobKey;
            return stubbedSasUrl;
        }
    }

    /**
     * Subclass that captures the arguments passed to {@link #listObjects} by
     * {@link #listObjectsWithPrefix}.
     */
    private static class ListObjectsCaptureStorage extends AzureObjStorage {
        String capturedRemotePath;
        String capturedToken;

        ListObjectsCaptureStorage(Map<String, String> props) {
            super(props);
        }

        @Override
        public RemoteObjects listObjects(String remotePath, String continuationToken) {
            this.capturedRemotePath = remotePath;
            this.capturedToken = continuationToken;
            return new RemoteObjects(Collections.emptyList(), false, null);
        }
    }
}
