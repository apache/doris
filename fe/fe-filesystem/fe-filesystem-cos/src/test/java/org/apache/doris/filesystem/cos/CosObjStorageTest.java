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

import com.qcloud.cos.COSClient;
import com.qcloud.cos.http.HttpMethodName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
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
    void getPresignedUrl_missingRegionThrowsIOException() {
        COSClient mockCos = Mockito.mock(COSClient.class);
        Map<String, String> props = new HashMap<>();
        props.put("COS_ENDPOINT", "https://cos.myqcloud.com");
        props.put("COS_ACCESS_KEY", "ak");
        props.put("COS_SECRET_KEY", "sk");
        props.put("COS_BUCKET", "my-bucket-1234");
        // no region

        CosObjStorage storage = new TestableCosObjStorage(props, mockCos);

        Assertions.assertThrows(IOException.class, () -> storage.getPresignedUrl("some/key"),
                "Should throw when region is missing");
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

    /** Subclass that injects a mock COSClient, bypassing real credential requirements. */
    private static class TestableCosObjStorage extends CosObjStorage {
        private final COSClient mockCos;

        TestableCosObjStorage(Map<String, String> props, COSClient mockCos) {
            super(props);
            this.mockCos = mockCos;
        }

        @Override
        protected COSClient buildCosClient(String region) {
            return mockCos;
        }
    }
}
