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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.common.comm.RequestMessage;
import com.aliyun.oss.common.utils.HttpHeaders;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;
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
 * Unit tests for {@link OssObjStorage}.
 *
 * <p>Cloud calls are replaced with mocks; no real OSS credentials are required.
 */
class OssObjStorageTest {

    @Test
    void getPresignedUrl_returnsSignedUrlFromOssClient() throws Exception {
        String expectedUrl = "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/stage/f1?sig=abc";
        OSS mockOss = Mockito.mock(OSS.class);
        Mockito.when(mockOss.generatePresignedUrl(Mockito.any(GeneratePresignedUrlRequest.class)))
                .thenReturn(new URL(expectedUrl));

        Map<String, String> props = buildBasicProps();
        OssObjStorage storage = new TestableOssObjStorage(props, mockOss);

        String result = storage.getPresignedUrl("stage/f1");

        Assertions.assertEquals(expectedUrl, result);
        ArgumentCaptor<GeneratePresignedUrlRequest> captor =
                ArgumentCaptor.forClass(GeneratePresignedUrlRequest.class);
        Mockito.verify(mockOss).generatePresignedUrl(captor.capture());
        Assertions.assertEquals("my-bucket", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/f1", captor.getValue().getKey());
    }

    @Test
    void isUsePathStyle_reflectsConfiguredProperty() {
        Map<String, String> props = buildBasicProps();
        props.put("use_path_style", "true");
        Assertions.assertTrue(new OssObjStorage(props).isUsePathStyle());
        Assertions.assertFalse(new OssObjStorage(buildBasicProps()).isUsePathStyle());
    }

    @Test
    void getPresignedUrl_missingBucketThrowsIOException() {
        OSS mockOss = Mockito.mock(OSS.class);
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://oss.aliyuncs.com");
        props.put("OSS_ACCESS_KEY", "ak");
        props.put("OSS_SECRET_KEY", "sk");
        props.put("OSS_REGION", "cn-hangzhou");
        // no bucket

        OssObjStorage storage = new TestableOssObjStorage(props, mockOss);

        Assertions.assertThrows(IOException.class, () -> storage.getPresignedUrl("some/key"),
                "Should throw when bucket is missing");
    }

    @Test
    void getPresignedUrl_expiryInFuture() throws Exception {
        long beforeMs = System.currentTimeMillis();
        OSS mockOss = Mockito.mock(OSS.class);
        Mockito.when(mockOss.generatePresignedUrl(Mockito.any(GeneratePresignedUrlRequest.class)))
                .thenAnswer(inv -> {
                    GeneratePresignedUrlRequest req = inv.getArgument(0);
                    Assertions.assertTrue(req.getExpiration().getTime() > beforeMs,
                            "Expiry must be in the future");
                    long diffSec = (req.getExpiration().getTime() - beforeMs) / 1000;
                    Assertions.assertTrue(diffSec >= 3500 && diffSec <= 3700,
                            "Expiry diff should be ~3600s, was " + diffSec);
                    return new URL("https://bucket.oss.aliyuncs.com/key?sig=x");
                });

        OssObjStorage storage = new TestableOssObjStorage(buildBasicProps(), mockOss);
        storage.getPresignedUrl("key");
    }

    @Test
    void getClient_returnsOssClient() throws Exception {
        OSS mockOss = Mockito.mock(OSS.class);
        OssObjStorage storage = new TestableOssObjStorage(buildBasicProps(), mockOss);

        Assertions.assertSame(mockOss, storage.getClient());
    }

    @Test
    void buildOssClient_allowsAnonymousCredentials() throws Exception {
        InspectableOssObjStorage storage = new InspectableOssObjStorage(Map.of(
                "OSS_ENDPOINT", "https://oss-cn-hongkong-internal.aliyuncs.com"));

        OSS client = storage.createClient();

        Assertions.assertNotNull(client);
        client.shutdown();
    }

    @Test
    void anonymousClientConfiguration_removesAuthHeaders() throws Exception {
        ClientBuilderConfiguration config = OssObjStorage.anonymousClientConfiguration();
        RequestMessage request = new RequestMessage("bucket", "key");
        request.getHeaders().put(HttpHeaders.AUTHORIZATION, "OSS anonymous:signature");
        request.getHeaders().put(OSSHeaders.OSS_SECURITY_TOKEN, "token");

        config.getSignerHandlers().get(0).sign(request);

        Assertions.assertFalse(request.getHeaders().containsKey(HttpHeaders.AUTHORIZATION));
        Assertions.assertFalse(request.getHeaders().containsKey(OSSHeaders.OSS_SECURITY_TOKEN));
    }

    @Test
    void listObjects_usesOssNativeClientAndMapsResult() throws Exception {
        OSS mockOss = Mockito.mock(OSS.class);
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setKey("stage/f1.parquet");
        summary.setETag("etag-1");
        summary.setSize(12L);
        summary.setLastModified(new Date(123456789L));
        ObjectListing listing = new ObjectListing();
        listing.setObjectSummaries(Collections.singletonList(summary));
        listing.setTruncated(true);
        listing.setNextMarker("next-marker");
        Mockito.when(mockOss.listObjects(Mockito.any(ListObjectsRequest.class)))
                .thenReturn(listing);

        OssObjStorage storage = new TestableOssObjStorage(buildBasicProps(), mockOss);

        RemoteObjects result = storage.listObjects("oss://my-bucket/stage/", "marker-1");

        Assertions.assertTrue(result.isTruncated());
        Assertions.assertEquals("next-marker", result.getContinuationToken());
        RemoteObject file = result.getObjectList().get(0);
        Assertions.assertEquals("stage/f1.parquet", file.getKey());
        Assertions.assertEquals("f1.parquet", file.getRelativePath());
        Assertions.assertEquals("etag-1", file.getEtag());
        Assertions.assertEquals(12L, file.getSize());
        Assertions.assertEquals(123456789L, file.getModificationTime());
        ArgumentCaptor<ListObjectsRequest> captor = ArgumentCaptor.forClass(ListObjectsRequest.class);
        Mockito.verify(mockOss).listObjects(captor.capture());
        Assertions.assertEquals("my-bucket", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/", captor.getValue().getPrefix());
        Assertions.assertEquals("marker-1", captor.getValue().getMarker());
    }

    @Test
    void headObject_usesOssNativeClientAndMapsMetadata() throws Exception {
        OSS mockOss = Mockito.mock(OSS.class);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(34L);
        metadata.setHeader("ETag", "etag-head");
        metadata.setLastModified(new Date(22334455L));
        Mockito.when(mockOss.getObjectMetadata("my-bucket", "stage/f2.parquet"))
                .thenReturn(metadata);

        OssObjStorage storage = new TestableOssObjStorage(buildBasicProps(), mockOss);

        RemoteObject result = storage.headObject("oss://my-bucket/stage/f2.parquet");

        Assertions.assertEquals("stage/f2.parquet", result.getKey());
        Assertions.assertEquals("stage/f2.parquet", result.getRelativePath());
        Assertions.assertEquals("etag-head", result.getEtag());
        Assertions.assertEquals(34L, result.getSize());
        Assertions.assertEquals(22334455L, result.getModificationTime());
    }

    @Test
    void putObject_usesOssNativeClientWithRequestBody() throws Exception {
        OSS mockOss = Mockito.mock(OSS.class);
        byte[] content = "hello oss".getBytes(StandardCharsets.UTF_8);
        OssObjStorage storage = new TestableOssObjStorage(buildBasicProps(), mockOss);

        storage.putObject("oss://my-bucket/stage/f3.txt",
                RequestBody.of(new ByteArrayInputStream(content), content.length));

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockOss).putObject(captor.capture());
        Assertions.assertEquals("my-bucket", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/f3.txt", captor.getValue().getKey());
        Assertions.assertEquals(content.length, captor.getValue().getMetadata().getContentLength());
    }

    @Test
    void copyObject_usesOssNativeClient() throws Exception {
        OSS mockOss = Mockito.mock(OSS.class);
        OssObjStorage storage = new TestableOssObjStorage(buildBasicProps(), mockOss);

        storage.copyObject("oss://my-bucket/src.txt", "oss://my-bucket/dst.txt");

        ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        Mockito.verify(mockOss).copyObject(captor.capture());
        Assertions.assertEquals("my-bucket", captor.getValue().getSourceBucketName());
        Assertions.assertEquals("src.txt", captor.getValue().getSourceKey());
        Assertions.assertEquals("my-bucket", captor.getValue().getDestinationBucketName());
        Assertions.assertEquals("dst.txt", captor.getValue().getDestinationKey());
    }

    @Test
    void deleteObjectsByKeys_usesOssNativeBatchDelete() throws Exception {
        OSS mockOss = Mockito.mock(OSS.class);
        Mockito.when(mockOss.deleteObjects(Mockito.any(DeleteObjectsRequest.class)))
                .thenReturn(new DeleteObjectsResult(Collections.emptyList()));
        OssObjStorage storage = new TestableOssObjStorage(buildBasicProps(), mockOss);

        storage.deleteObjectsByKeys("my-bucket", List.of("a.txt", "b.txt"));

        ArgumentCaptor<DeleteObjectsRequest> captor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        Mockito.verify(mockOss).deleteObjects(captor.capture());
        Assertions.assertEquals("my-bucket", captor.getValue().getBucketName());
        Assertions.assertTrue(captor.getValue().isQuiet());
        Assertions.assertEquals(List.of("a.txt", "b.txt"), captor.getValue().getKeys());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private Map<String, String> buildBasicProps() {
        Map<String, String> props = new HashMap<>();
        props.put("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com");
        props.put("OSS_ACCESS_KEY", "testAK");
        props.put("OSS_SECRET_KEY", "testSK");
        props.put("OSS_BUCKET", "my-bucket");
        props.put("OSS_REGION", "cn-hangzhou");
        return props;
    }

    /** Subclass that injects a mock OSS client, bypassing real credential requirements. */
    private static class TestableOssObjStorage extends OssObjStorage {
        private final OSS mockOss;

        TestableOssObjStorage(Map<String, String> props, OSS mockOss) {
            super(props);
            this.mockOss = mockOss;
        }

        @Override
        protected OSS buildOssClient() {
            return mockOss;
        }
    }

    private static class InspectableOssObjStorage extends OssObjStorage {
        InspectableOssObjStorage(Map<String, String> props) {
            super(props);
        }

        OSS createClient() throws IOException {
            return buildOssClient();
        }
    }
}
