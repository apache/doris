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

package org.apache.doris.fs;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Phase A exit gate: registry-level bindPrimary/bindAll selection over real providers
 * (test-scope impl modules on the classpath, discovered via ServiceLoader) must mirror
 * fe-core's StorageProperties.createPrimary/createAll — same priority, same explicit
 * fs.xx.support kill-switch for guesses, same default-HDFS fallback. HDFS-family outputs
 * are additionally compared key-by-key against the fe-core oracle (A1 acceptance);
 * S3-family key-set alignment is tracked separately by the 2.4 parity ledger.
 */
public class SpiBindRoutingParityTest {

    private static FileSystemPluginManager manager;

    @BeforeAll
    static void setUp() {
        manager = new FileSystemPluginManager();
        manager.loadBuiltins();
    }

    private static void assertExactMap(Map<String, String> expected, Map<String, String> actual) {
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }

    @Test
    public void testJfsDefaultFsWithHdfsHintsBindsExactlyOnce() throws UserException {
        // external_table_p0 jfs catalog shape: fs.defaultFS carries a jfs:// uri AND is itself
        // an HDFS key-hint, so JFS and HDFS guesses both fire. Legacy had ONE class for both
        // (jfs rode HdfsProperties) and produced a single instance; the plugin split must not
        // double-bind or the TypeId-keyed catalog map throws "Duplicate storage type: HDFS".
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "jfs://volume-name");
        props.put("fs.jfs.impl", "io.juicefs.JuiceFileSystem");
        props.put("hadoop.username", "hadoop");
        List<StorageProperties> legacy = StorageProperties.createAll(new HashMap<>(props));
        List<FileSystemProperties> spi = manager.bindAll(new HashMap<>(props));
        Assertions.assertEquals(legacy.size(), spi.size(), "bind count must match legacy");
        Assertions.assertEquals(1, spi.size());
        Assertions.assertEquals("JFS", spi.get(0).providerName());
        Assertions.assertEquals("HDFS", legacy.get(0).getType().name());
    }

    @Test
    public void testNonCloudEndpointWithRegionFallsToMinioLikeFeCore() throws UserException {
        // NereidsParserDigestTest regression shape: EXPORT ... WITH S3 uses placeholder
        // endpoint/region values. Legacy S3.guessIsMe short-circuits on the present endpoint
        // (not amazonaws -> no claim) WITHOUT consulting its region fallback, and MinIO — the
        // "any other S3-compatible" fallback — claims the map. The first SPI port excluded
        // MinIO whenever an s3.region key existed, so NOTHING claimed this map.
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "xxxxx");
        props.put("s3.region", "xxxxx");
        StorageProperties feCore = StorageProperties.createPrimary(new HashMap<>(props));
        FileSystemProperties spi = manager.bindPrimary(new HashMap<>(props));
        Assertions.assertEquals("MINIO", feCore.getType().name());
        Assertions.assertEquals("MINIO", spi.providerName().toUpperCase());
        // Region present WITHOUT any endpoint alias still routes S3 in both worlds.
        Map<String, String> regionOnly = new HashMap<>();
        regionOnly.put("s3.region", "us-east-1");
        Assertions.assertEquals(StorageProperties.createPrimary(new HashMap<>(regionOnly)).getType().name(),
                manager.bindPrimary(new HashMap<>(regionOnly)).providerName().toUpperCase());
    }

    @Test
    public void testAllFourteenProvidersLoaded() {
        List<String> names = manager.getProviders().stream()
                .map(p -> p.name().toUpperCase())
                .collect(Collectors.toList());
        for (String expected : new String[] {"HDFS", "JFS", "OSS_HDFS", "OSS", "S3", "OBS", "COS",
                "GCS", "AZURE", "MINIO", "OZONE", "BROKER", "LOCAL", "HTTP"}) {
            Assertions.assertTrue(names.contains(expected), "missing provider: " + expected + " in " + names);
        }
    }

    @Test
    public void testBareHdfsUriBindMatchesFeCore() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "hdfs://ns1/warehouse/t");
        FileSystemProperties spi = manager.bindPrimary(props);
        Assertions.assertEquals("HDFS", spi.providerName());

        StorageProperties feCore = StorageProperties.createPrimary(new HashMap<>(props));
        assertExactMap(feCore.getBackendConfigProperties(),
                spi.toBackendProperties().orElseThrow().toMap());
    }

    @Test
    public void testHdfsKerberosBindMatchesFeCore() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        props.put("hdfs.authentication.type", "kerberos");
        props.put("hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM");
        props.put("hadoop.kerberos.keytab", "/etc/doris/doris.keytab");

        FileSystemProperties spi = manager.bindPrimary(props);
        Assertions.assertEquals("HDFS", spi.providerName());
        Assertions.assertTrue(spi.toHadoopProperties().orElseThrow().isKerberos());

        StorageProperties feCore = StorageProperties.createPrimary(new HashMap<>(props));
        assertExactMap(feCore.getBackendConfigProperties(),
                spi.toBackendProperties().orElseThrow().toMap());
    }

    @Test
    public void testExplicitOssSupportSuppressesGuessAndFallback() throws UserException {
        // 2.3-④ acceptance: with fs.oss.support=true, the ambiguous s3.region key must NOT
        // pull in S3, and no default HDFS is added — exactly one OSS binding, matching fe-core.
        Map<String, String> props = new HashMap<>();
        props.put("fs.oss.support", "true");
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        props.put("s3.region", "cn-hangzhou");

        List<FileSystemProperties> spiAll = manager.bindAll(props);
        Assertions.assertEquals(1, spiAll.size());
        Assertions.assertEquals("OSS", spiAll.get(0).providerName());

        List<StorageProperties> feCoreAll = StorageProperties.createAll(new HashMap<>(props));
        Assertions.assertEquals(feCoreAll.size(), spiAll.size());
    }

    @Test
    public void testAmbiguousDoubleHitMatchesFeCore() throws UserException {
        // Without the explicit flag the same map double-hits OSS (aliyuncs endpoint) and S3
        // (s3.region), plus the default HDFS fallback at index 0 — mirroring fe-core exactly.
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        props.put("s3.region", "cn-hangzhou");

        List<FileSystemProperties> spiAll = manager.bindAll(props);
        Assertions.assertEquals(3, spiAll.size());
        Assertions.assertEquals("HDFS", spiAll.get(0).providerName());
        Assertions.assertEquals("OSS", spiAll.get(1).providerName());
        Assertions.assertEquals("S3", spiAll.get(2).providerName());

        List<StorageProperties> feCoreAll = StorageProperties.createAll(new HashMap<>(props));
        Assertions.assertEquals(3, feCoreAll.size());
    }

    @Test
    public void testOssHdfsWinsOverOssAndStaysExclusive() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "cn-hangzhou.oss-dls.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        FileSystemProperties spi = manager.bindPrimary(props);
        Assertions.assertEquals("OSS_HDFS", spi.providerName());

        List<FileSystemProperties> all = manager.bindAll(props);
        Assertions.assertTrue(all.stream().noneMatch(p -> "OSS".equals(p.providerName())),
                "OSS must stay mutually exclusive with OSS_HDFS");
    }

    @Test
    public void testJfsUriRoutesToJfsPluginWithFeCoreDefaultFs() {
        // fe-core rides jfs:// on HdfsProperties; the plugin split routes it to the JFS
        // provider. The load-bearing derivation (fs.defaultFS) must match fe-core's.
        Map<String, String> props = new HashMap<>();
        props.put("uri", "jfs://myjfs/warehouse/t");
        FileSystemProperties spi = manager.bindPrimary(props);
        Assertions.assertEquals("JFS", spi.providerName());

        StorageProperties feCore = StorageProperties.createPrimary(new HashMap<>(props));
        Assertions.assertEquals(
                feCore.getBackendConfigProperties().get("fs.defaultFS"),
                spi.toBackendProperties().orElseThrow().toMap().get("fs.defaultFS"));
    }

    @Test
    public void testOzoneExplicitOnly() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.ozone.support", "true");
        props.put("ozone.endpoint", "http://127.0.0.1:9878");
        props.put("ozone.access_key", "myAk");
        props.put("ozone.secret_key", "mySk");
        Assertions.assertEquals("OZONE", manager.bindPrimary(props).providerName());
    }

    @Test
    public void testEmptyPropsPrimaryThrowsAndAllYieldsHdfsFallback() throws UserException {
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> manager.bindPrimary(new HashMap<>()));

        List<FileSystemProperties> spiAll = manager.bindAll(new HashMap<>());
        Assertions.assertEquals(1, spiAll.size());
        Assertions.assertEquals("HDFS", spiAll.get(0).providerName());

        List<StorageProperties> feCoreAll = StorageProperties.createAll(new HashMap<>());
        Assertions.assertEquals(1, feCoreAll.size());
    }

    @Test
    public void testEveryTypeBindableViaPrimary() {
        // Phase A exit condition: all 14 fe-core storage types constructible through the
        // registry from raw user props (Broker/Local/HTTP included).
        Assertions.assertEquals("BROKER", manager.bindPrimary(
                ParityMaps.of("broker.name", "b1")).providerName());
        Assertions.assertEquals("LOCAL", manager.bindPrimary(
                ParityMaps.of("file_path", "/tmp/x.csv")).providerName());
        Assertions.assertEquals("HTTP", manager.bindPrimary(
                ParityMaps.of("fs.http.support", "true", "uri", "https://example.com/f.csv")).providerName());
        Assertions.assertEquals("GCS", manager.bindPrimary(
                ParityMaps.of("gs.endpoint", "https://storage.googleapis.com",
                        "gs.access_key", "ak", "gs.secret_key", "sk")).providerName());
        Assertions.assertEquals("MINIO", manager.bindPrimary(
                ParityMaps.of("minio.endpoint", "http://127.0.0.1:9000",
                        "minio.access_key", "ak", "minio.secret_key", "sk")).providerName());
        Assertions.assertEquals("OBS", manager.bindPrimary(
                ParityMaps.of("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com",
                        "obs.access_key", "ak", "obs.secret_key", "sk")).providerName());
        Assertions.assertEquals("COS", manager.bindPrimary(
                ParityMaps.of("cos.endpoint", "cos.ap-guangzhou.myqcloud.com",
                        "cos.access_key", "ak", "cos.secret_key", "sk")).providerName());
        Assertions.assertEquals("AZURE", manager.bindPrimary(
                ParityMaps.of("provider", "azure", "azure.account_name", "acc",
                        "azure.account_key", "key", "container", "c")).providerName());
        Assertions.assertEquals("S3", manager.bindPrimary(
                ParityMaps.of("s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                        "s3.access_key", "ak", "s3.secret_key", "sk")).providerName());
    }

    /** Tiny varargs map builder to keep the exit-gate cases readable. */
    private static final class ParityMaps {
        static Map<String, String> of(String... kv) {
            Map<String, String> m = new HashMap<>();
            for (int i = 0; i < kv.length; i += 2) {
                m.put(kv[i], kv[i + 1]);
            }
            return m;
        }
    }
}
