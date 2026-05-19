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

package org.apache.doris.filesystem.cos;

import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.DeleteObjectsRequest;
import com.qcloud.cos.model.DeleteObjectsResult;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link CosObjStorage}.
 *
 * <p>Cloud calls are replaced with mocks; no real COS credentials are required.
 */
class CosObjStorageTest {

    // ------------------------------------------------------------------
    // toS3Props() key-translation tests
    // ------------------------------------------------------------------

    @Test
    void toS3Props_cosKeysTranslatedToAwsKeys() {
        Map<String, String> cosProps = new HashMap<>();
        cosProps.put("COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");
        cosProps.put("COS_ACCESS_KEY", "mySecretId");
        cosProps.put("COS_SECRET_KEY", "mySecretKey");
        cosProps.put("COS_BUCKET", "my-bucket-1234567890");
        cosProps.put("COS_REGION", "ap-guangzhou");
        cosProps.put("COS_ROLE_ARN", "qcs::cam::uin/100000:roleName/DorisRole");

        Map<String, String> s3Props = CosObjStorage.toS3Props(cosProps);

        Assertions.assertEquals("https://cos.ap-guangzhou.myqcloud.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("mySecretId", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("mySecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("my-bucket-1234567890", s3Props.get("AWS_BUCKET"));
        Assertions.assertEquals("ap-guangzhou", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("qcs::cam::uin/100000:roleName/DorisRole", s3Props.get("AWS_ROLE_ARN"));
        Assertions.assertEquals("false", s3Props.get("use_path_style"));
    }

    @Test
    void toS3Props_awsKeysPreservedWhenBothPresent() {
        Map<String, String> cosProps = new HashMap<>();
        cosProps.put("COS_ENDPOINT", "https://cos.myqcloud.com");
        cosProps.put("AWS_ENDPOINT", "https://custom.endpoint");
        cosProps.put("COS_ACCESS_KEY", "cosAK");
        cosProps.put("AWS_ACCESS_KEY", "awsAK");

        Map<String, String> s3Props = CosObjStorage.toS3Props(cosProps);

        // AWS_* takes precedence when both exist
        Assertions.assertEquals("https://custom.endpoint", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("awsAK", s3Props.get("AWS_ACCESS_KEY"));
    }

    @Test
    void toS3Props_awsOnlyKeysPassedThrough() {
        Map<String, String> cosProps = new HashMap<>();
        cosProps.put("AWS_ENDPOINT", "https://s3.amazonaws.com");
        cosProps.put("AWS_ACCESS_KEY", "akid");
        cosProps.put("AWS_SECRET_KEY", "sk");
        cosProps.put("AWS_BUCKET", "bucket");

        Map<String, String> s3Props = CosObjStorage.toS3Props(cosProps);

        Assertions.assertEquals("https://s3.amazonaws.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("akid", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("bucket", s3Props.get("AWS_BUCKET"));
    }

    // ------------------------------------------------------------------
    // getPresignedUrl() with mocked COS client
    // ------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void getPresignedUrl_returnsSignedUrlFromCosClient() throws Exception {
        String expectedUrl = "https://my-bucket.cos.ap-guangzhou.myqcloud.com/stage/f1?sign=abc";
        COSClient mockCos = Mockito.mock(COSClient.class);
        Mockito.when(mockCos.generatePresignedUrl(
                        Mockito.anyString(), Mockito.anyString(), Mockito.any(Date.class),
                        Mockito.any(HttpMethodName.class), Mockito.anyMap(), Mockito.anyMap()))
                .thenReturn(new URL(expectedUrl));

        Map<String, String> props = buildBasicProps();
        CosObjStorage storage = new TestableCosObjStorage(props, mockCos);

        String result = storage.getPresignedUrl("stage/f1");

        Assertions.assertEquals(expectedUrl, result);
        ArgumentCaptor<String> bucketCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockCos).generatePresignedUrl(
                bucketCaptor.capture(), keyCaptor.capture(),
                Mockito.any(Date.class), Mockito.any(HttpMethodName.class),
                Mockito.anyMap(), Mockito.anyMap());
        Assertions.assertEquals("my-bucket-1234", bucketCaptor.getValue());
        Assertions.assertEquals("stage/f1", keyCaptor.getValue());
    }

    @Test
    void getPresignedUrl_missingBucketThrowsIOException() {
        COSClient mockCos = Mockito.mock(COSClient.class);
        Map<String, String> props = new HashMap<>();
        props.put("COS_ENDPOINT", "https://cos.myqcloud.com");
        props.put("COS_ACCESS_KEY", "ak");
        props.put("COS_SECRET_KEY", "sk");
        props.put("COS_REGION", "ap-guangzhou");
        // no bucket

        CosObjStorage storage = new TestableCosObjStorage(props, mockCos);

        Assertions.assertThrows(IOException.class, () -> storage.getPresignedUrl("some/key"),
                "Should throw when bucket is missing");
    }

    @Test
    void constructor_missingRegionFailsTypedValidation() {
        COSClient mockCos = Mockito.mock(COSClient.class);
        Map<String, String> props = new HashMap<>();
        props.put("COS_ENDPOINT", "https://cos.myqcloud.com");
        props.put("COS_ACCESS_KEY", "ak");
        props.put("COS_SECRET_KEY", "sk");
        props.put("COS_BUCKET", "my-bucket-1234");
        // no region

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, () -> new TestableCosObjStorage(props, mockCos));

        Assertions.assertTrue(exception.getMessage().contains("Invalid COS filesystem properties"));
        Assertions.assertTrue(exception.getMessage().contains("Region is not set"));
    }

    @Test
    void constructor_extractsRegionFromStandardCosEndpoint() throws Exception {
        COSClient mockCos = Mockito.mock(COSClient.class);
        Map<String, String> props = buildBasicProps();
        props.remove("COS_REGION");

        TestableCosObjStorage storage = new TestableCosObjStorage(props, mockCos);

        Assertions.assertSame(mockCos, storage.getClient());
        Assertions.assertEquals("ap-guangzhou", storage.getBuiltRegion());
        Assertions.assertEquals("ap-guangzhou", storage.getProperties().get("COS_REGION"));
    }

    @Test
    void constructor_acceptsLegacyAwsPropertiesForExistingCosCallers() throws Exception {
        COSClient mockCos = Mockito.mock(COSClient.class);
        TestableCosObjStorage storage = new TestableCosObjStorage(buildBasicAwsProps(), mockCos);

        Assertions.assertSame(mockCos, storage.getClient());
        Assertions.assertEquals("ap-guangzhou", storage.getBuiltRegion());
        Assertions.assertEquals("https://cos.ap-guangzhou.myqcloud.com",
                storage.getProperties().get("COS_ENDPOINT"));
        Assertions.assertEquals("legacy-ak", storage.getProperties().get("COS_ACCESS_KEY"));
        Assertions.assertEquals("legacy-bucket", storage.getProperties().get("COS_BUCKET"));
        Assertions.assertEquals("legacy-ak", storage.getProperties().get("AWS_ACCESS_KEY"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void getPresignedUrl_expiryInFuture() throws Exception {
        long beforeMs = System.currentTimeMillis();
        COSClient mockCos = Mockito.mock(COSClient.class);
        Mockito.when(mockCos.generatePresignedUrl(
                        Mockito.anyString(), Mockito.anyString(), Mockito.any(Date.class),
                        Mockito.any(HttpMethodName.class), Mockito.anyMap(), Mockito.anyMap()))
                .thenAnswer(inv -> {
                    Date expiry = inv.getArgument(2);
                    Assertions.assertTrue(expiry.getTime() > beforeMs, "Expiry must be in the future");
                    long diffSec = (expiry.getTime() - beforeMs) / 1000;
                    Assertions.assertTrue(diffSec >= 3500 && diffSec <= 3700,
                            "Expiry diff should be ~3600s, was " + diffSec);
                    return new URL("https://bucket.cos.ap-guangzhou.myqcloud.com/key?sign=x");
                });

        CosObjStorage storage = new TestableCosObjStorage(buildBasicProps(), mockCos);
        storage.getPresignedUrl("key");
    }

    @Test
    void getClient_returnsCosClient() throws Exception {
        COSClient mockCos = Mockito.mock(COSClient.class);
        CosObjStorage storage = new TestableCosObjStorage(buildBasicProps(), mockCos);

        Assertions.assertSame(mockCos, storage.getClient());
    }

    @Test
    void listObjects_usesCosNativeClientAndMapsResult() throws Exception {
        COSClient mockCos = Mockito.mock(COSClient.class);
        COSObjectSummary summary = new COSObjectSummary();
        summary.setKey("stage/f1.parquet");
        summary.setETag("etag-1");
        summary.setSize(12L);
        summary.setLastModified(new Date(123456789L));
        ObjectListing listing = new ObjectListing();
        listing.getObjectSummaries().add(summary);
        listing.setTruncated(true);
        listing.setNextMarker("next-marker");
        Mockito.when(mockCos.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(listing);

        CosObjStorage storage = new TestableCosObjStorage(buildBasicProps(), mockCos);

        RemoteObjects result = storage.listObjects("cos://my-bucket-1234/stage/", "marker-1");

        Assertions.assertTrue(result.isTruncated());
        Assertions.assertEquals("next-marker", result.getContinuationToken());
        RemoteObject file = result.getObjectList().get(0);
        Assertions.assertEquals("stage/f1.parquet", file.getKey());
        Assertions.assertEquals("f1.parquet", file.getRelativePath());
        Assertions.assertEquals("etag-1", file.getEtag());
        Assertions.assertEquals(12L, file.getSize());
        Assertions.assertEquals(123456789L, file.getModificationTime());
        ArgumentCaptor<ListObjectsRequest> captor = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockCos).listObjects(captor.capture());
        Assertions.assertEquals("my-bucket-1234", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/", captor.getValue().getPrefix());
        Assertions.assertEquals("marker-1", captor.getValue().getMarker());
    }

    @Test
    void headObject_usesCosNativeClientAndMapsMetadata() throws Exception {
        COSClient mockCos = Mockito.mock(COSClient.class);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(34L);
        metadata.setETag("etag-head");
        metadata.setLastModified(new Date(22334455L));
        Mockito.when(mockCos.getObjectMetadata("my-bucket-1234", "stage/f2.parquet"))
                .thenReturn(metadata);

        CosObjStorage storage = new TestableCosObjStorage(buildBasicProps(), mockCos);

        RemoteObject result = storage.headObject("cos://my-bucket-1234/stage/f2.parquet");

        Assertions.assertEquals("stage/f2.parquet", result.getKey());
        Assertions.assertEquals("stage/f2.parquet", result.getRelativePath());
        Assertions.assertEquals("etag-head", result.getEtag());
        Assertions.assertEquals(34L, result.getSize());
        Assertions.assertEquals(22334455L, result.getModificationTime());
    }

    @Test
    void putObject_usesCosNativeClientWithRequestBody() throws Exception {
        COSClient mockCos = Mockito.mock(COSClient.class);
        byte[] content = "hello cos".getBytes(StandardCharsets.UTF_8);
        CosObjStorage storage = new TestableCosObjStorage(buildBasicProps(), mockCos);

        storage.putObject("cos://my-bucket-1234/stage/f3.txt",
                RequestBody.of(new ByteArrayInputStream(content), content.length));

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockCos).putObject(captor.capture());
        Assertions.assertEquals("my-bucket-1234", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/f3.txt", captor.getValue().getKey());
        Assertions.assertEquals(content.length, captor.getValue().getMetadata().getContentLength());
    }

    @Test
    void copyObject_usesCosNativeClient() throws Exception {
        COSClient mockCos = Mockito.mock(COSClient.class);
        CosObjStorage storage = new TestableCosObjStorage(buildBasicProps(), mockCos);

        storage.copyObject("cos://my-bucket-1234/src.txt", "cos://my-bucket-1234/dst.txt");

        ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        Mockito.verify(mockCos).copyObject(captor.capture());
        Assertions.assertEquals("my-bucket-1234", captor.getValue().getSourceBucketName());
        Assertions.assertEquals("src.txt", captor.getValue().getSourceKey());
        Assertions.assertEquals("my-bucket-1234", captor.getValue().getDestinationBucketName());
        Assertions.assertEquals("dst.txt", captor.getValue().getDestinationKey());
    }

    @Test
    void deleteObjectsByKeys_usesCosNativeBatchDelete() throws Exception {
        COSClient mockCos = Mockito.mock(COSClient.class);
        Mockito.when(mockCos.deleteObjects(Mockito.any(DeleteObjectsRequest.class)))
                .thenReturn(new DeleteObjectsResult(Collections.emptyList()));
        CosObjStorage storage = new TestableCosObjStorage(buildBasicProps(), mockCos);

        storage.deleteObjectsByKeys("my-bucket-1234", List.of("a.txt", "b.txt"));

        ArgumentCaptor<DeleteObjectsRequest> captor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        Mockito.verify(mockCos).deleteObjects(captor.capture());
        Assertions.assertEquals("my-bucket-1234", captor.getValue().getBucketName());
        Assertions.assertTrue(captor.getValue().getQuiet());
        Assertions.assertEquals(2, captor.getValue().getKeys().size());
        Assertions.assertEquals("a.txt", captor.getValue().getKeys().get(0).getKey());
        Assertions.assertEquals("b.txt", captor.getValue().getKeys().get(1).getKey());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private Map<String, String> buildBasicProps() {
        Map<String, String> props = new HashMap<>();
        props.put("COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("COS_ACCESS_KEY", "testSecretId");
        props.put("COS_SECRET_KEY", "testSecretKey");
        props.put("COS_BUCKET", "my-bucket-1234");
        props.put("COS_REGION", "ap-guangzhou");
        return props;
    }

    private Map<String, String> buildBasicAwsProps() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("AWS_ACCESS_KEY", "legacy-ak");
        props.put("AWS_SECRET_KEY", "legacy-sk");
        props.put("AWS_BUCKET", "legacy-bucket");
        return props;
    }

    /** Subclass that injects a mock COSClient, bypassing real credential requirements. */
    private static class TestableCosObjStorage extends CosObjStorage {
        private final COSClient mockCos;
        private String builtRegion;

        TestableCosObjStorage(Map<String, String> props, COSClient mockCos) {
            super(props);
            this.mockCos = mockCos;
        }

        @Override
        protected COSClient buildCosClient(String region) {
            this.builtRegion = region;
            return mockCos;
        }

        private String getBuiltRegion() {
            return builtRegion;
        }
    }
}
