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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Parity tests for the uri-derived endpoint/region leg of legacy
 * {@code AbstractS3CompatibleProperties.setEndpointIfPossible()} (helper
 * {@code S3PropertyUtils.constructEndpointFromUrl}): with no endpoint/region key set, the
 * endpoint is derived from the raw {@code uri} property and the region is then extracted from
 * that endpoint. The legacy typed classes are the oracle; the SPI providers behind
 * {@link StorageAdapter} must yield the exact same endpoint/region — or throw the exact same
 * error — for the same input map.
 */
public class S3UriDerivationParityTest {

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static S3CompatibleFileSystemProperties spi(String provider, Map<String, String> props) {
        return (S3CompatibleFileSystemProperties) StorageAdapter
                .ofProvider(provider, new HashMap<>(props)).getSpiProperties();
    }

    private static void assertEndpointRegionParity(ObjectStorageProperties legacy,
                                                   String provider, Map<String, String> props) {
        S3CompatibleFileSystemProperties spiProps = spi(provider, props);
        Assertions.assertEquals(legacy.getEndpoint(), spiProps.getEndpoint(), "endpoint");
        Assertions.assertEquals(legacy.getRegion(), spiProps.getRegion(), "region");
    }

    /** Both sides must throw the same exception type with the same message prefix. */
    private static void assertBothThrow(Supplier<?> legacyFactory, String provider,
                                        Map<String, String> props, String messagePrefix) {
        IllegalArgumentException legacyEx = Assertions.assertThrows(IllegalArgumentException.class,
                legacyFactory::get, "legacy oracle should throw");
        IllegalArgumentException spiEx = Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageAdapter.ofProvider(provider, new HashMap<>(props)), "SPI should throw");
        Assertions.assertTrue(legacyEx.getMessage().startsWith(messagePrefix), legacyEx.getMessage());
        Assertions.assertTrue(spiEx.getMessage().startsWith(messagePrefix), spiEx.getMessage());
    }

    // ------------------------------------------------------------------
    // S3
    // ------------------------------------------------------------------

    @Test
    public void testS3VirtualHostedUri() {
        Map<String, String> props = map(
                "uri", "https://mybucket.s3.us-west-2.amazonaws.com/data/file.csv",
                "s3.access_key", "ak",
                "s3.secret_key", "sk");
        S3Properties legacy = S3Properties.of(new HashMap<>(props));
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", legacy.getEndpoint());
        Assertions.assertEquals("us-west-2", legacy.getRegion());
        assertEndpointRegionParity(legacy, "S3", props);
    }

    @Test
    public void testS3PathStyleUri() {
        Map<String, String> props = map(
                "uri", "https://s3.us-west-2.amazonaws.com/mybucket/data/file.csv",
                "use_path_style", "true",
                "s3.access_key", "ak",
                "s3.secret_key", "sk");
        S3Properties legacy = S3Properties.of(new HashMap<>(props));
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", legacy.getEndpoint());
        Assertions.assertEquals("us-west-2", legacy.getRegion());
        assertEndpointRegionParity(legacy, "S3", props);
    }

    @Test
    public void testS3StandardUriParsedVirtualHostedThrowsIdentically() {
        // Without use_path_style=true the legacy parser reads this virtual-hosted: "s3" becomes
        // the bucket, the endpoint is "us-east-1.amazonaws.com" and no region can be extracted
        // from it, so BOTH sides throw "Region is not set...".
        Map<String, String> props = map(
                "uri", "https://s3.us-east-1.amazonaws.com/bucket/x.csv",
                "s3.access_key", "ak",
                "s3.secret_key", "sk");
        assertBothThrow(() -> S3Properties.of(new HashMap<>(props)), "S3", props, "Region is not set");
    }

    @Test
    public void testS3NonStandardHttpsUriThrowsIdentically() {
        Map<String, String> props = map(
                "uri", "https://minio.example.com/bucket/file.csv",
                "s3.access_key", "ak",
                "s3.secret_key", "sk");
        assertBothThrow(() -> S3Properties.of(new HashMap<>(props)), "S3", props, "Region is not set");
    }

    @Test
    public void testS3ExplicitEndpointWinsOverUri() {
        Map<String, String> props = map(
                "uri", "https://mybucket.s3.us-west-2.amazonaws.com/data/file.csv",
                "s3.endpoint", "https://s3.eu-west-1.amazonaws.com",
                "s3.access_key", "ak",
                "s3.secret_key", "sk");
        S3Properties legacy = S3Properties.of(new HashMap<>(props));
        Assertions.assertEquals("https://s3.eu-west-1.amazonaws.com", legacy.getEndpoint());
        Assertions.assertEquals("eu-west-1", legacy.getRegion());
        assertEndpointRegionParity(legacy, "S3", props);
    }

    @Test
    public void testS3RegionDerivedEndpointWinsOverUri() {
        // Legacy setEndpointIfPossible consults getEndpointFromRegion() BEFORE the uri leg.
        Map<String, String> props = map(
                "uri", "https://mybucket.s3.us-west-2.amazonaws.com/data/file.csv",
                "s3.region", "eu-central-1",
                "s3.access_key", "ak",
                "s3.secret_key", "sk");
        S3Properties legacy = S3Properties.of(new HashMap<>(props));
        Assertions.assertEquals("https://s3.eu-central-1.amazonaws.com", legacy.getEndpoint());
        Assertions.assertEquals("eu-central-1", legacy.getRegion());
        assertEndpointRegionParity(legacy, "S3", props);
    }

    // ------------------------------------------------------------------
    // OSS
    // ------------------------------------------------------------------

    @Test
    public void testOssAliyuncsUri() {
        Map<String, String> props = map(
                "uri", "https://mybucket.oss-cn-hangzhou.aliyuncs.com/data/file.csv",
                "oss.access_key", "ak",
                "oss.secret_key", "sk");
        OSSProperties legacy = OSSProperties.of(new HashMap<>(props));
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", legacy.getEndpoint());
        Assertions.assertEquals("cn-hangzhou", legacy.getRegion());
        assertEndpointRegionParity(legacy, "OSS", props);
    }

    @Test
    public void testOssExplicitEndpointWinsOverUri() {
        Map<String, String> props = map(
                "uri", "https://mybucket.oss-cn-hangzhou.aliyuncs.com/data/file.csv",
                "oss.endpoint", "oss-cn-beijing.aliyuncs.com",
                "oss.access_key", "ak",
                "oss.secret_key", "sk");
        OSSProperties legacy = OSSProperties.of(new HashMap<>(props));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", legacy.getEndpoint());
        Assertions.assertEquals("cn-beijing", legacy.getRegion());
        assertEndpointRegionParity(legacy, "OSS", props);
    }

    @Test
    public void testOssHttpUriWinsOverRegionDerivedEndpoint() {
        // Unlike S3, legacy OSS has no getEndpointFromRegion(); its region-based pre-derivation
        // skips http(s) uris, so the uri leg fires and the explicit region is kept as-is.
        Map<String, String> props = map(
                "uri", "https://mybucket.oss-cn-hangzhou.aliyuncs.com/data/file.csv",
                "oss.region", "cn-beijing",
                "oss.access_key", "ak",
                "oss.secret_key", "sk");
        OSSProperties legacy = OSSProperties.of(new HashMap<>(props));
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", legacy.getEndpoint());
        Assertions.assertEquals("cn-beijing", legacy.getRegion());
        assertEndpointRegionParity(legacy, "OSS", props);
    }

    // ------------------------------------------------------------------
    // COS / OBS
    // ------------------------------------------------------------------

    @Test
    public void testCosMyqcloudUri() {
        Map<String, String> props = map(
                "uri", "https://mybucket.cos.ap-guangzhou.myqcloud.com/data/file.csv",
                "cos.access_key", "ak",
                "cos.secret_key", "sk");
        COSProperties legacy = COSProperties.of(new HashMap<>(props));
        Assertions.assertEquals("cos.ap-guangzhou.myqcloud.com", legacy.getEndpoint());
        Assertions.assertEquals("ap-guangzhou", legacy.getRegion());
        assertEndpointRegionParity(legacy, "COS", props);
    }

    @Test
    public void testCosExplicitEndpointWinsOverUri() {
        Map<String, String> props = map(
                "uri", "https://mybucket.cos.ap-guangzhou.myqcloud.com/data/file.csv",
                "cos.endpoint", "cos.ap-beijing.myqcloud.com",
                "cos.access_key", "ak",
                "cos.secret_key", "sk");
        COSProperties legacy = COSProperties.of(new HashMap<>(props));
        Assertions.assertEquals("cos.ap-beijing.myqcloud.com", legacy.getEndpoint());
        Assertions.assertEquals("ap-beijing", legacy.getRegion());
        assertEndpointRegionParity(legacy, "COS", props);
    }

    @Test
    public void testObsMyhuaweicloudUri() {
        Map<String, String> props = map(
                "uri", "https://mybucket.obs.cn-north-4.myhuaweicloud.com/data/file.csv",
                "obs.access_key", "ak",
                "obs.secret_key", "sk");
        OBSProperties legacy = OBSProperties.of(new HashMap<>(props));
        Assertions.assertEquals("obs.cn-north-4.myhuaweicloud.com", legacy.getEndpoint());
        Assertions.assertEquals("cn-north-4", legacy.getRegion());
        assertEndpointRegionParity(legacy, "OBS", props);
    }

    @Test
    public void testObsExplicitEndpointWinsOverUri() {
        Map<String, String> props = map(
                "uri", "https://mybucket.obs.cn-north-4.myhuaweicloud.com/data/file.csv",
                "obs.endpoint", "obs.cn-east-3.myhuaweicloud.com",
                "obs.access_key", "ak",
                "obs.secret_key", "sk");
        OBSProperties legacy = OBSProperties.of(new HashMap<>(props));
        Assertions.assertEquals("obs.cn-east-3.myhuaweicloud.com", legacy.getEndpoint());
        Assertions.assertEquals("cn-east-3", legacy.getRegion());
        assertEndpointRegionParity(legacy, "OBS", props);
    }

    // ------------------------------------------------------------------
    // MinIO / Ozone (legacy inherited the uri leg through the shared base;
    // both classes then require a non-blank endpoint)
    // ------------------------------------------------------------------

    @Test
    public void testMinioPathStyleUri() {
        Map<String, String> props = map(
                "uri", "http://127.0.0.1:9000/warehouse/data.orc",
                "minio.use_path_style", "true",
                "minio.access_key", "ak",
                "minio.secret_key", "sk");
        MinioProperties legacy = new MinioProperties(new HashMap<>(props));
        ConnectorPropertiesUtils.bindConnectorProperties(legacy, new HashMap<>(props));
        legacy.initNormalizeAndCheckProps();
        Assertions.assertEquals("127.0.0.1:9000", legacy.getEndpoint());
        Assertions.assertEquals("us-east-1", legacy.getRegion());
        assertEndpointRegionParity(legacy, "MINIO", props);
    }

    @Test
    public void testOzoneDefaultPathStyleUri() {
        Map<String, String> props = map(
                "uri", "http://ozone-s3g.local:9878/vol-bucket/key1",
                "ozone.access_key", "ak",
                "ozone.secret_key", "sk");
        OzoneProperties legacy = new OzoneProperties(new HashMap<>(props));
        ConnectorPropertiesUtils.bindConnectorProperties(legacy, new HashMap<>(props));
        legacy.initNormalizeAndCheckProps();
        Assertions.assertEquals("ozone-s3g.local:9878", legacy.getEndpoint());
        Assertions.assertEquals("us-east-1", legacy.getRegion());
        assertEndpointRegionParity(legacy, "OZONE", props);
    }
}
