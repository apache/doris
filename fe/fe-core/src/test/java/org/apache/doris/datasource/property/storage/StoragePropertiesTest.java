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

import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link StorageProperties}, focusing on the explicit {@code fs.xx.support}
 * flag behavior that controls whether heuristic-based {@code guessIsMe} detection is used.
 *
 * <p>Background: Each storage provider uses an OR logic of
 * {@code isFsSupport(props, FS_XX_SUPPORT) || XXProperties.guessIsMe(props)} to detect
 * whether it should be activated. The {@code guessIsMe} heuristic inspects endpoint strings
 * to infer the provider type. However, some endpoints are ambiguous — for example,
 * {@code aliyuncs.com} can trigger both OSS and S3 guessIsMe, and {@code amazonaws.com}
 * can trigger S3 even when the user only intended OSS.
 *
 * <p>The fix: when any {@code fs.xx.support=true} is explicitly set, all providers skip
 * {@code guessIsMe} and rely solely on their respective {@code fs.xx.support} flags.
 * This prevents cross-provider false-positive matches.
 */
public class StoragePropertiesTest {

    // ========================================================================================
    // 1. Backward compatibility: no explicit fs.xx.support → guessIsMe still works
    // ========================================================================================

    /**
     * When no {@code fs.xx.support} flag is set, providers should fall back to
     * {@code guessIsMe} heuristic. An OSS endpoint containing "aliyuncs.com"
     * should be detected as OSS via guessIsMe.
     */
    @Test
    public void testNoExplicitSupport_guessIsMeStillWorks_OSS() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        // guessIsMe should detect OSS; default HDFS is always prepended
        Assertions.assertTrue(types.contains(HdfsProperties.class),
                "Default HDFS should always be present");
        Assertions.assertTrue(types.contains(OSSProperties.class),
                "OSS should be detected via guessIsMe when no explicit fs.xx.support is set");
    }

    /**
     * When no {@code fs.xx.support} flag is set, an S3 endpoint containing
     * "amazonaws.com" should be detected as S3 via guessIsMe.
     */
    @Test
    public void testNoExplicitSupport_guessIsMeStillWorks_S3() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        props.put("s3.region", "us-east-1");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        Assertions.assertTrue(types.contains(S3Properties.class),
                "S3 should be detected via guessIsMe when no explicit fs.xx.support is set");
    }

    // ========================================================================================
    // 2. Core scenario: explicit fs.oss.support=true → S3 guessIsMe is skipped
    // ========================================================================================

    /**
     * When {@code fs.oss.support=true} is explicitly set, even if the properties also
     * contain an S3 endpoint ({@code s3.amazonaws.com}) that would normally trigger
     * {@code S3Properties.guessIsMe}, S3 should NOT be matched.
     * Only OSS should appear — the default HDFS fallback is also skipped in explicit mode.
     *
     * <p>This is the primary scenario that motivated the fix: a user configuring an
     * Aliyun DLF Iceberg catalog with OSS storage, where the endpoint string
     * accidentally matches S3's guessIsMe heuristic.
     */
    @Test
    public void testExplicitOssSupport_skipsS3GuessIsMe() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_OSS_SUPPORT, "true");
        props.put("oss.endpoint", "oss-cn-beijing-internal.aliyuncs.com");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");
        // This endpoint would normally trigger S3Properties.guessIsMe
        props.put("s3.endpoint", "s3.amazonaws.com");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        Assertions.assertEquals(1, all.size(),
                "Should only contain OSS — no default HDFS in explicit mode");
        Assertions.assertTrue(types.contains(OSSProperties.class));
        Assertions.assertFalse(types.contains(S3Properties.class),
                "S3 should NOT be matched when fs.oss.support is explicitly set");
        Assertions.assertFalse(types.contains(HdfsProperties.class),
                "Default HDFS should NOT be added in explicit mode");
    }

    // ========================================================================================
    // 3. Symmetric scenario: explicit fs.s3.support=true → OSS guessIsMe is skipped
    // ========================================================================================

    /**
     * When {@code fs.s3.support=true} is explicitly set, even if the properties also
     * contain an OSS endpoint ({@code aliyuncs.com}) that would normally trigger
     * {@code OSSProperties.guessIsMe}, OSS should NOT be matched.
     * Default HDFS fallback is also skipped in explicit mode.
     */
    @Test
    public void testExplicitS3Support_skipsOssGuessIsMe() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_S3_SUPPORT, "true");
        props.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        props.put("s3.region", "us-east-1");
        // This endpoint would normally trigger OSSProperties.guessIsMe
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        Assertions.assertEquals(1, all.size(),
                "Should only contain S3 — no default HDFS in explicit mode");
        Assertions.assertTrue(types.contains(S3Properties.class),
                "S3 should be matched via explicit fs.s3.support");
        Assertions.assertFalse(types.contains(OSSProperties.class),
                "OSS should NOT be matched when fs.s3.support is explicitly set");
        Assertions.assertFalse(types.contains(HdfsProperties.class),
                "Default HDFS should NOT be added in explicit mode");
    }

    // ========================================================================================
    // 4. Multiple explicit flags: only flagged providers are matched
    // ========================================================================================

    /**
     * When multiple {@code fs.xx.support=true} flags are set (e.g., OSS + S3),
     * both should be matched, but other providers whose guessIsMe might fire
     * (e.g., COS via myqcloud.com endpoint) should NOT be matched.
     * Default HDFS fallback is also skipped in explicit mode.
     */
    @Test
    public void testMultipleExplicitSupport_onlyFlaggedProvidersMatched() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_OSS_SUPPORT, "true");
        props.put(StorageProperties.FS_S3_SUPPORT, "true");
        props.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");
        props.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        props.put("s3.region", "us-east-1");
        // COS endpoint that would trigger COSProperties.guessIsMe
        props.put("cos.endpoint", "cos.ap-guangzhou.myqcloud.com");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        Assertions.assertEquals(2, all.size(),
                "Should only contain OSS + S3, no default HDFS and no COS");
        Assertions.assertTrue(types.contains(OSSProperties.class),
                "OSS should be matched via explicit flag");
        Assertions.assertTrue(types.contains(S3Properties.class),
                "S3 should be matched via explicit flag");
        Assertions.assertFalse(types.contains(COSProperties.class),
                "COS should NOT be matched — no fs.cos.support=true, and guessIsMe is disabled");
        Assertions.assertFalse(types.contains(HdfsProperties.class),
                "Default HDFS should NOT be added in explicit mode");
    }

    // ========================================================================================
    // 5. createPrimary also respects the explicit support logic
    // ========================================================================================

    /**
     * {@code createPrimary} should also skip guessIsMe when an explicit flag is set.
     * With {@code fs.s3.support=true}, even if OSS endpoint is present and would
     * match via guessIsMe, createPrimary should not return OSS.
     */
    @Test
    public void testCreatePrimary_explicitSupport_skipsGuessIsMe() {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_S3_SUPPORT, "true");
        props.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        props.put("s3.region", "us-east-1");
        // OSS endpoint that would normally trigger OSSProperties.guessIsMe
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");

        // HDFS is first in PROVIDERS list, but without fs.hdfs.support and guessIsMe disabled,
        // it won't match. S3 should be the first match via its explicit flag.
        StorageProperties primary = StorageProperties.createPrimary(props);
        Assertions.assertInstanceOf(S3Properties.class, primary,
                "createPrimary should return S3 (first explicit match), not OSS via guessIsMe");
    }

    // ========================================================================================
    // 6. Default HDFS fallback is skipped when explicit support is set
    // ========================================================================================

    /**
     * When {@code fs.oss.support=true} is set and no HDFS-specific properties exist,
     * the default HDFS fallback should NOT be added. In explicit mode, only the
     * providers with matching {@code fs.xx.support=true} flags are included.
     */
    @Test
    public void testExplicitSupport_noDefaultHdfsFallback() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_OSS_SUPPORT, "true");
        props.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");

        List<StorageProperties> all = StorageProperties.createAll(props);

        Assertions.assertEquals(1, all.size(),
                "Should only contain OSS, no default HDFS in explicit mode");
        Assertions.assertInstanceOf(OSSProperties.class, all.get(0));
    }

    /**
     * When {@code fs.hdfs.support=true} is explicitly set alongside another provider,
     * HDFS should appear because it was explicitly requested, not as a default fallback.
     */
    @Test
    public void testExplicitHdfsSupport_hdfsIncluded() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_HDFS_SUPPORT, "true");
        props.put(StorageProperties.FS_OSS_SUPPORT, "true");
        props.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        Assertions.assertEquals(2, all.size(),
                "Should contain HDFS (explicit) + OSS");
        Assertions.assertTrue(types.contains(HdfsProperties.class),
                "HDFS should be present because fs.hdfs.support=true was explicitly set");
        Assertions.assertTrue(types.contains(OSSProperties.class));
    }

    // ========================================================================================
    // 7. Edge case: fs.xx.support=false does NOT count as explicit
    // ========================================================================================

    /**
     * Setting {@code fs.oss.support=false} should NOT be treated as an explicit flag.
     * guessIsMe should still be active in this case.
     */
    @Test
    public void testFsSupportFalse_doesNotDisableGuessIsMe() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_OSS_SUPPORT, "false");
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        // fs.oss.support=false means no explicit flag is truly set,
        // so guessIsMe should still work and detect OSS via endpoint
        Assertions.assertTrue(types.contains(OSSProperties.class),
                "OSS should still be detected via guessIsMe when fs.oss.support=false");
    }

    // ========================================================================================
    // 8. Real-world DLF scenario: OSS explicit + aliyuncs.com endpoint
    // ========================================================================================

    /**
     * Simulates a real-world Aliyun DLF Iceberg catalog scenario:
     * the user sets {@code fs.oss.support=true} and provides an OSS endpoint
     * containing "aliyuncs.com". Without the fix, "aliyuncs.com" could also
     * match other providers' guessIsMe (since some providers check s3.endpoint
     * which might be set to an aliyuncs.com URL). With the fix, only OSS is matched,
     * and the default HDFS fallback is also skipped.
     */
    @Test
    public void testDlfIcebergScenario_explicitOss_noFalsePositives() throws UserException {
        Map<String, String> props = new HashMap<>();
        // User explicitly declares OSS storage
        props.put(StorageProperties.FS_OSS_SUPPORT, "true");
        props.put("oss.endpoint", "oss-cn-beijing-internal.aliyuncs.com");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");
        props.put("oss.region", "cn-beijing");
        // DLF catalog properties that might contain ambiguous endpoints
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        // Only OSS should be present — no default HDFS, no false positives
        Assertions.assertEquals(1, all.size(),
                "DLF scenario: should only have OSS, no default HDFS and no false positives");
        Assertions.assertTrue(types.contains(OSSProperties.class));
        Assertions.assertFalse(types.contains(HdfsProperties.class),
                "Default HDFS should not appear in explicit mode");
        Assertions.assertFalse(types.contains(S3Properties.class),
                "S3 should not appear in DLF OSS scenario");
    }

    /**
     * Setting {@code fs.oss.support=false} should NOT be treated as an explicit flag.
     * guessIsMe should still be active in this case.
     */
    @Test
    public void testMinioFixed_explicitMinio() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_MINIO_SUPPORT, "true");
        props.put("minio.endpoint", "htpp://minio.br88.9000");
        props.put("minio.access_key", "ak");
        props.put("minio.secret_key", "sk");
        props.put("s3.region", "minio");

        List<StorageProperties> all = StorageProperties.createAll(props);
        List<Class<?>> types = toTypeList(all);

        // fs.oss.support=false means no explicit flag is truly set,
        // so guessIsMe should still work and detect OSS via endpoint
        Assertions.assertTrue(types.contains(MinioProperties.class),
                "Minio should be created when fs.minio.support=true");
    }

    // ========================================================================================
    // Helper
    // ========================================================================================

    private static List<Class<?>> toTypeList(List<StorageProperties> all) {
        return all.stream()
                .map(Object::getClass)
                .collect(Collectors.toList());
    }

    // ========================================================================================
    // 10. getRegionFromProperties tests
    // ========================================================================================

    @Test
    public void testGetRegionFromProperties_s3Region() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.region", "us-west-2");
        String region = AbstractS3CompatibleProperties.getRegionFromProperties(props);
        Assertions.assertEquals("us-west-2", region);
    }

    @Test
    public void testGetRegionFromProperties_ossRegion() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.region", "cn-hangzhou");
        String region = AbstractS3CompatibleProperties.getRegionFromProperties(props);
        Assertions.assertEquals("cn-hangzhou", region);
    }

    @Test
    public void testGetRegionFromProperties_awsRegion() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_REGION", "eu-west-1");
        String region = AbstractS3CompatibleProperties.getRegionFromProperties(props);
        Assertions.assertEquals("eu-west-1", region);
    }

    @Test
    public void testGetRegionFromProperties_cosRegion() {
        Map<String, String> props = new HashMap<>();
        props.put("cos.region", "ap-guangzhou");
        String region = AbstractS3CompatibleProperties.getRegionFromProperties(props);
        Assertions.assertEquals("ap-guangzhou", region);
    }

    @Test
    public void testGetRegionFromProperties_noRegion() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        String region = AbstractS3CompatibleProperties.getRegionFromProperties(props);
        Assertions.assertNull(region);
    }

    @Test
    public void testGetRegionFromProperties_emptyProps() {
        Map<String, String> props = new HashMap<>();
        String region = AbstractS3CompatibleProperties.getRegionFromProperties(props);
        Assertions.assertNull(region);
    }

    // ========================================================================================
    // equals() / hashCode() tests — ConnectionProperties value-based equality
    // ========================================================================================

    @Test
    public void testEqualsAndHashCode_sameOrigProps() throws UserException {
        Map<String, String> props1 = new HashMap<>();
        props1.put("type", "hms");
        props1.put("hive.metastore.uris", "thrift://localhost:9083");

        Map<String, String> props2 = new HashMap<>(props1);

        List<StorageProperties> list1 = StorageProperties.createAll(props1);
        List<StorageProperties> list2 = StorageProperties.createAll(props2);

        Assertions.assertFalse(list1.isEmpty());
        Assertions.assertEquals(list1.size(), list2.size());

        for (int i = 0; i < list1.size(); i++) {
            StorageProperties sp1 = list1.get(i);
            StorageProperties sp2 = list2.get(i);
            Assertions.assertNotSame(sp1, sp2, "Different instances expected");
            Assertions.assertEquals(sp1, sp2, "Same origProps should be equal");
            Assertions.assertEquals(sp1.hashCode(), sp2.hashCode(),
                    "Equal objects must have equal hashCodes");
        }
    }

    @Test
    public void testNotEquals_differentOrigProps() throws UserException {
        Map<String, String> hdfsProps = new HashMap<>();
        hdfsProps.put("type", "hms");
        hdfsProps.put("hive.metastore.uris", "thrift://host1:9083");

        Map<String, String> hdfsProps2 = new HashMap<>();
        hdfsProps2.put("type", "hms");
        hdfsProps2.put("hive.metastore.uris", "thrift://host2:9083");

        List<StorageProperties> list1 = StorageProperties.createAll(hdfsProps);
        List<StorageProperties> list2 = StorageProperties.createAll(hdfsProps2);

        Assertions.assertFalse(list1.isEmpty());
        Assertions.assertEquals(list1.size(), list2.size());

        for (int i = 0; i < list1.size(); i++) {
            Assertions.assertNotEquals(list1.get(i), list2.get(i),
                    "Different origProps should not be equal");
        }
    }

    @Test
    public void testNotEquals_differentTypes() throws UserException {
        Map<String, String> hdfsProps = new HashMap<>();
        hdfsProps.put("fs.hdfs.support", "true");

        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("fs.s3.support", "true");
        s3Props.put("s3.endpoint", "http://s3.us-east-1.amazonaws.com");
        s3Props.put("s3.region", "us-east-1");
        s3Props.put("s3.access_key", "ak");
        s3Props.put("s3.secret_key", "sk");

        List<StorageProperties> hdfsList = StorageProperties.createAll(hdfsProps);
        List<StorageProperties> s3List = StorageProperties.createAll(s3Props);

        Assertions.assertFalse(hdfsList.isEmpty());
        Assertions.assertFalse(s3List.isEmpty());

        // HDFS and S3 properties should never be equal
        for (StorageProperties hdfs : hdfsList) {
            for (StorageProperties s3 : s3List) {
                Assertions.assertNotEquals(hdfs, s3,
                        "Properties of different storage types should not be equal");
            }
        }
    }

    @Test
    public void testEquals_nullAndSelf() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("type", "hms");
        List<StorageProperties> list = StorageProperties.createAll(props);
        Assertions.assertFalse(list.isEmpty());
        StorageProperties sp = list.get(0);

        Assertions.assertEquals(sp, sp, "Same instance should be equal to itself");
        Assertions.assertNotEquals(null, sp, "Should not be equal to null");
        Assertions.assertNotEquals(sp, "not a StorageProperties",
                "Should not be equal to different type");
    }
}
