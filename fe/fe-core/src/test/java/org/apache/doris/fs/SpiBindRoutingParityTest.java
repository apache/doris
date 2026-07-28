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

import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Frozen routing contract for bindPrimary/bindAll over the real providers (test-scope impl
 * modules on the classpath). Every expectation below was originally verified against the
 * legacy fe-core factory (deleted in Phase D): priority order, explicit fs.xx.support
 * kill-switch for guesses, default-HDFS fallback, and the load-bearing routing quirks.
 */
public class SpiBindRoutingParityTest {

    private static FileSystemPluginManager manager;

    @BeforeAll
    static void setUp() {
        manager = new FileSystemPluginManager();
        manager.loadBuiltins();
    }

    @Test
    public void testJfsDefaultFsWithHdfsHintsBindsExactlyOnce() {
        // external_table_p0 jfs catalog shape: fs.defaultFS carries a jfs:// uri AND is itself
        // an HDFS key-hint, so JFS and HDFS guesses both fire. Legacy had ONE class for both
        // (jfs rode HdfsProperties) and produced a single instance; the plugin split must not
        // double-bind or the TypeId-keyed catalog map throws "Duplicate storage type: HDFS".
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "jfs://volume-name");
        props.put("fs.jfs.impl", "io.juicefs.JuiceFileSystem");
        props.put("hadoop.username", "hadoop");
        List<FileSystemProperties> spi = manager.bindAll(new HashMap<>(props));
        Assertions.assertEquals(1, spi.size());
        Assertions.assertEquals("JFS", spi.get(0).providerName());
    }

    @Test
    public void testNonCloudEndpointWithRegionFallsToMinioLikeFeCore() {
        // NereidsParserDigestTest regression shape: EXPORT ... WITH S3 uses placeholder
        // endpoint/region values. Legacy S3.guessIsMe short-circuits on the present endpoint
        // (not amazonaws -> no claim) WITHOUT consulting its region fallback, and MinIO — the
        // "any other S3-compatible" fallback — claims the map. The first SPI port excluded
        // MinIO whenever an s3.region key existed, so NOTHING claimed this map.
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "xxxxx");
        props.put("s3.region", "xxxxx");
        FileSystemProperties spi = manager.bindPrimary(new HashMap<>(props));
        Assertions.assertEquals("MINIO", spi.providerName().toUpperCase());
        // Region present WITHOUT any endpoint alias routes S3 (legacy region fallback).
        Map<String, String> regionOnly = new HashMap<>();
        regionOnly.put("s3.region", "us-east-1");
        Assertions.assertEquals("S3", manager.bindPrimary(new HashMap<>(regionOnly)).providerName().toUpperCase());
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

        // frozen legacy-shaped backend keys (formerly asserted against the fe-core oracle)
        Map<String, String> backend = spi.toBackendProperties().orElseThrow().toMap();
        Assertions.assertEquals("hdfs://ns1", backend.get("fs.defaultFS"));
        Assertions.assertEquals("true", backend.get("ipc.client.fallback-to-simple-auth-allowed"));
        Assertions.assertEquals("simple", backend.get("hdfs.security.authentication"));
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

        // frozen legacy-shaped kerberos backend keys
        Map<String, String> backend = spi.toBackendProperties().orElseThrow().toMap();
        Assertions.assertEquals("kerberos", backend.get("hdfs.security.authentication"));
        Assertions.assertEquals("kerberos", backend.get("hadoop.security.authentication"));
        Assertions.assertEquals("doris/_HOST@EXAMPLE.COM", backend.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/doris/doris.keytab", backend.get("hadoop.kerberos.keytab"));
    }

    @Test
    public void testExplicitOssSupportSuppressesGuessAndFallback() {
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
    }

    @Test
    public void testAmbiguousDoubleHitMatchesFeCore() {
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
        // load-bearing derivation frozen: fs.defaultFS comes from the jfs uri authority
        Assertions.assertEquals("jfs://myjfs",
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
    public void testEmptyPropsPrimaryThrowsAndAllYieldsHdfsFallback() {
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> manager.bindPrimary(new HashMap<>()));

        List<FileSystemProperties> spiAll = manager.bindAll(new HashMap<>());
        Assertions.assertEquals(1, spiAll.size());
        Assertions.assertEquals("HDFS", spiAll.get(0).providerName());
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

    @Test
    public void testAzureGovCloudDfsEndpointBindsAzureNotMinio() {
        // The Azure claim is host-suffix based over the LIVE Config.azure_blob_host_suffixes
        // list (injected into the guess probe view); MinIO — the "any other S3-compatible"
        // fallback — must consult the same predicate. A US-Government dfs endpoint therefore
        // yields exactly one AZURE binding (plus the default-HDFS fallback at index 0) and
        // never a parallel MINIO binding.
        Map<String, String> props = ParityMaps.of(
                "s3.endpoint", "https://acct.dfs.core.usgovcloudapi.net",
                "s3.access_key", "ak",
                "s3.secret_key", "sk");

        List<FileSystemProperties> all = manager.bindAll(new HashMap<>(props));
        Assertions.assertEquals(1,
                all.stream().filter(p -> "AZURE".equalsIgnoreCase(p.providerName())).count(),
                "exactly one AZURE binding expected: " + all);
        Assertions.assertTrue(
                all.stream().noneMatch(p -> "MINIO".equalsIgnoreCase(p.providerName())),
                "MinIO must yield to the Azure host-suffix claim: " + all);

        Assertions.assertEquals("AZURE", manager.bindPrimary(new HashMap<>(props)).providerName());
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
