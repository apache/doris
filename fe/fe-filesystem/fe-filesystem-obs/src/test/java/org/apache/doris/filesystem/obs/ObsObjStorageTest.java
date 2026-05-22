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

package org.apache.doris.filesystem.obs;

import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import com.obs.services.ObsClient;
import com.obs.services.model.CopyObjectRequest;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.TemporarySignatureRequest;
import com.obs.services.model.TemporarySignatureResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ObsObjStorage}.
 *
 * <p>Cloud calls are replaced with mocks; no real OBS credentials are required.
 */
class ObsObjStorageTest {

    @Test
    void getPresignedUrl_returnsSignedUrlFromObsClient() throws Exception {
        String expectedUrl = "https://my-obs-bucket.obs.cn-north-4.myhuaweicloud.com/stage/f1?sig=abc";
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        TemporarySignatureResponse mockResp = Mockito.mock(TemporarySignatureResponse.class);
        Mockito.when(mockResp.getSignedUrl()).thenReturn(expectedUrl);
        Mockito.when(mockObs.createTemporarySignature(Mockito.any(TemporarySignatureRequest.class)))
                .thenReturn(mockResp);

        Map<String, String> props = buildBasicProps();
        ObsObjStorage storage = new TestableObsObjStorage(props, mockObs);

        String result = storage.getPresignedUrl("stage/f1");

        Assertions.assertEquals(expectedUrl, result);
        ArgumentCaptor<TemporarySignatureRequest> captor =
                ArgumentCaptor.forClass(TemporarySignatureRequest.class);
        Mockito.verify(mockObs).createTemporarySignature(captor.capture());
        Assertions.assertEquals("my-obs-bucket", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/f1", captor.getValue().getObjectKey());
        Assertions.assertEquals(HttpMethodEnum.PUT, captor.getValue().getMethod());
    }

    @Test
    void getPresignedUrl_missingBucketThrowsIOException() {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ENDPOINT", "https://obs.myhuaweicloud.com");
        props.put("OBS_ACCESS_KEY", "ak");
        props.put("OBS_SECRET_KEY", "sk");
        props.put("OBS_REGION", "cn-north-4");
        // no bucket

        ObsObjStorage storage = new TestableObsObjStorage(props, mockObs);

        Assertions.assertThrows(IOException.class, () -> storage.getPresignedUrl("some/key"),
                "Should throw when bucket is missing");
    }

    @Test
    void constructor_missingEndpointFailsTypedValidation() {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ACCESS_KEY", "ak");
        props.put("OBS_SECRET_KEY", "sk");
        props.put("OBS_BUCKET", "my-bucket");
        props.put("OBS_REGION", "cn-north-4");
        // no endpoint

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new TestableObsObjStorage(props, mockObs));

        Assertions.assertTrue(exception.getMessage().contains("Property obs.endpoint is required"));
    }

    @Test
    void getPresignedUrl_expireSecondsIs3600() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        TemporarySignatureResponse mockResp = Mockito.mock(TemporarySignatureResponse.class);
        Mockito.when(mockResp.getSignedUrl())
                .thenReturn("https://bucket.obs.cn-north-4.myhuaweicloud.com/k?sig=x");
        Mockito.when(mockObs.createTemporarySignature(Mockito.any(TemporarySignatureRequest.class)))
                .thenAnswer(inv -> {
                    TemporarySignatureRequest req = inv.getArgument(0);
                    Assertions.assertEquals(3600L, req.getExpires(),
                            "OBS presigned URL must expire in 3600s");
                    return mockResp;
                });

        ObsObjStorage storage = new TestableObsObjStorage(buildBasicProps(), mockObs);
        storage.getPresignedUrl("key");
    }

    @Test
    void getClient_returnsObsClient() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        ObsObjStorage storage = new TestableObsObjStorage(buildBasicProps(), mockObs);

        Assertions.assertSame(mockObs, storage.getClient());
    }

    @Test
    void constructor_acceptsLegacyAwsPropertiesForExistingObsCallers() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        TestableObsObjStorage storage = new TestableObsObjStorage(buildBasicAwsProps(), mockObs);

        Assertions.assertSame(mockObs, storage.getClient());
        Assertions.assertEquals("https://obs.cn-north-4.myhuaweicloud.com",
                storage.getBuiltEndpoint());
        Assertions.assertEquals("legacy-ak", storage.getBuiltAccessKey());
        Assertions.assertEquals("legacy-sk", storage.getBuiltSecretKey());
        Assertions.assertEquals("https://obs.cn-north-4.myhuaweicloud.com",
                storage.getProperties().get("OBS_ENDPOINT"));
        Assertions.assertEquals("legacy-ak", storage.getProperties().get("OBS_ACCESS_KEY"));
        Assertions.assertEquals("legacy-bucket", storage.getProperties().get("OBS_BUCKET"));
        Assertions.assertFalse(storage.getProperties().containsKey("AWS_ACCESS_KEY"));
        Assertions.assertFalse(storage.getProperties().containsKey("AWS_ENDPOINT"));
    }

    @Test
    void listObjects_usesObsNativeClientAndMapsResult() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(12L);
        metadata.setEtag("etag-1");
        metadata.setLastModified(new Date(123456789L));
        ObsObject obsObject = new ObsObject();
        obsObject.setObjectKey("stage/f1.parquet");
        obsObject.setMetadata(metadata);
        ObjectListing listing = new ObjectListing.Builder()
                .objectSummaries(Collections.singletonList(obsObject))
                .truncated(true)
                .nextMarker("next-marker")
                .builder();
        Mockito.when(mockObs.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(listing);

        ObsObjStorage storage = new TestableObsObjStorage(buildBasicProps(), mockObs);

        RemoteObjects result = storage.listObjects("obs://my-obs-bucket/stage/", "marker-1");

        Assertions.assertTrue(result.isTruncated());
        Assertions.assertEquals("next-marker", result.getContinuationToken());
        RemoteObject file = result.getObjectList().get(0);
        Assertions.assertEquals("stage/f1.parquet", file.getKey());
        Assertions.assertEquals("f1.parquet", file.getRelativePath());
        Assertions.assertEquals("etag-1", file.getEtag());
        Assertions.assertEquals(12L, file.getSize());
        Assertions.assertEquals(123456789L, file.getModificationTime());
        ArgumentCaptor<ListObjectsRequest> captor = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockObs).listObjects(captor.capture());
        Assertions.assertEquals("my-obs-bucket", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/", captor.getValue().getPrefix());
        Assertions.assertEquals("marker-1", captor.getValue().getMarker());
    }

    @Test
    void headObject_usesObsNativeClientAndMapsMetadata() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(34L);
        metadata.setEtag("etag-head");
        metadata.setLastModified(new Date(22334455L));
        Mockito.when(mockObs.getObjectMetadata("my-obs-bucket", "stage/f2.parquet"))
                .thenReturn(metadata);

        ObsObjStorage storage = new TestableObsObjStorage(buildBasicProps(), mockObs);

        RemoteObject result = storage.headObject("obs://my-obs-bucket/stage/f2.parquet");

        Assertions.assertEquals("stage/f2.parquet", result.getKey());
        Assertions.assertEquals("stage/f2.parquet", result.getRelativePath());
        Assertions.assertEquals("etag-head", result.getEtag());
        Assertions.assertEquals(34L, result.getSize());
        Assertions.assertEquals(22334455L, result.getModificationTime());
    }

    @Test
    void putObject_usesObsNativeClientWithRequestBody() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        byte[] content = "hello obs".getBytes(StandardCharsets.UTF_8);
        ObsObjStorage storage = new TestableObsObjStorage(buildBasicProps(), mockObs);

        storage.putObject("obs://my-obs-bucket/stage/f3.txt",
                RequestBody.of(new ByteArrayInputStream(content), content.length));

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockObs).putObject(captor.capture());
        Assertions.assertEquals("my-obs-bucket", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/f3.txt", captor.getValue().getObjectKey());
        Assertions.assertEquals(content.length, captor.getValue().getMetadata().getContentLength());
    }

    @Test
    void copyObject_usesObsNativeClient() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        ObsObjStorage storage = new TestableObsObjStorage(buildBasicProps(), mockObs);

        storage.copyObject("obs://my-obs-bucket/src.txt", "obs://my-obs-bucket/dst.txt");

        ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        Mockito.verify(mockObs).copyObject(captor.capture());
        Assertions.assertEquals("my-obs-bucket", captor.getValue().getSourceBucketName());
        Assertions.assertEquals("src.txt", captor.getValue().getSourceObjectKey());
        Assertions.assertEquals("my-obs-bucket", captor.getValue().getDestinationBucketName());
        Assertions.assertEquals("dst.txt", captor.getValue().getDestinationObjectKey());
    }

    @Test
    void deleteObjectsByKeys_usesObsNativeBatchDelete() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        Mockito.when(mockObs.deleteObjects(Mockito.any(DeleteObjectsRequest.class)))
                .thenReturn(new DeleteObjectsResult(Collections.emptyList(), Collections.emptyList()));
        ObsObjStorage storage = new TestableObsObjStorage(buildBasicProps(), mockObs);

        storage.deleteObjectsByKeys("my-obs-bucket", List.of("a.txt", "b.txt"));

        ArgumentCaptor<DeleteObjectsRequest> captor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        Mockito.verify(mockObs).deleteObjects(captor.capture());
        Assertions.assertEquals("my-obs-bucket", captor.getValue().getBucketName());
        Assertions.assertTrue(captor.getValue().isQuiet());
        Assertions.assertEquals(2, captor.getValue().getKeyAndVersionsList().size());
        Assertions.assertEquals("a.txt", captor.getValue().getKeyAndVersionsList().get(0).getKey());
        Assertions.assertEquals("b.txt", captor.getValue().getKeyAndVersionsList().get(1).getKey());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private Map<String, String> buildBasicProps() {
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("OBS_ACCESS_KEY", "testAK");
        props.put("OBS_SECRET_KEY", "testSK");
        props.put("OBS_BUCKET", "my-obs-bucket");
        props.put("OBS_REGION", "cn-north-4");
        return props;
    }

    private Map<String, String> buildBasicAwsProps() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("AWS_ACCESS_KEY", "legacy-ak");
        props.put("AWS_SECRET_KEY", "legacy-sk");
        props.put("AWS_BUCKET", "legacy-bucket");
        return props;
    }

    /** Subclass that injects a mock ObsClient, bypassing real credential requirements. */
    private static class TestableObsObjStorage extends ObsObjStorage {
        private final ObsClient mockObs;
        private String builtEndpoint;
        private String builtAccessKey;
        private String builtSecretKey;

        TestableObsObjStorage(Map<String, String> props, ObsClient mockObs) {
            super(props);
            this.mockObs = mockObs;
        }

        @Override
        protected ObsClient buildObsClient(String endpoint, String accessKey, String secretKey) {
            this.builtEndpoint = endpoint;
            this.builtAccessKey = accessKey;
            this.builtSecretKey = secretKey;
            return mockObs;
        }

        private String getBuiltEndpoint() {
            return builtEndpoint;
        }

        private String getBuiltAccessKey() {
            return builtAccessKey;
        }

        private String getBuiltSecretKey() {
            return builtSecretKey;
        }
    }
}
