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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Golden parity tests for {@link HdfsFileSystemProperties}. Each test pins {@code toMap()} (the typed BE map)
 * to the exact key set legacy fe-core {@code HdfsProperties.getBackendConfigProperties()} would produce for
 * the same input. This is the UT-level equivalence gate for the P1-T04 HDFS regression fix (DV-004 / R-007):
 * the typed pipeline {@code getStorageProperties().toBackendProperties().toMap()} must re-produce the HDFS
 * backend keys that flow into {@code THdfsParams}.
 */
class HdfsFileSystemPropertiesTest {

    private static Map<String, String> beMap(Map<String, String> input) {
        Optional<BackendStorageProperties> be = HdfsFileSystemProperties.of(input).toBackendProperties();
        Assertions.assertTrue(be.isPresent(), "HDFS must expose backend storage properties");
        Assertions.assertEquals(BackendStorageKind.HDFS, be.get().backendKind());
        return be.get().toMap();
    }

    @Test
    void simpleAuthBackendMapMatchesLegacy() {
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");

        Map<String, String> expected = new HashMap<>();
        expected.put("fs.defaultFS", "hdfs://nn:8020");
        expected.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        expected.put("hdfs.security.authentication", "simple");

        Assertions.assertEquals(expected, beMap(in));
    }

    @Test
    void kerberosBackendMapMatchesLegacy() {
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        in.put("hadoop.security.authentication", "kerberos");
        in.put("hadoop.kerberos.principal", "doris/_HOST@REALM");
        in.put("hadoop.kerberos.keytab", "/etc/security/doris.keytab");

        Map<String, String> expected = new HashMap<>();
        expected.put("fs.defaultFS", "hdfs://nn:8020");
        expected.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        expected.put("hdfs.security.authentication", "kerberos");
        expected.put("hadoop.security.authentication", "kerberos");
        expected.put("hadoop.kerberos.principal", "doris/_HOST@REALM");
        expected.put("hadoop.kerberos.keytab", "/etc/security/doris.keytab");

        Assertions.assertEquals(expected, beMap(in));
    }

    @Test
    void kerberosViaDorisAliasSynthesizesHadoopKeys() {
        // The Doris-flavored aliases (hdfs.authentication.*) drive the same emission as the hadoop.* keys.
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        in.put("hdfs.authentication.type", "kerberos");
        in.put("hdfs.authentication.kerberos.principal", "doris@REALM");
        in.put("hdfs.authentication.kerberos.keytab", "/etc/security/doris.keytab");

        Map<String, String> out = beMap(in);
        Assertions.assertEquals("kerberos", out.get("hdfs.security.authentication"));
        Assertions.assertEquals("kerberos", out.get("hadoop.security.authentication"));
        Assertions.assertEquals("doris@REALM", out.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/security/doris.keytab", out.get("hadoop.kerberos.keytab"));
    }

    @Test
    void haConfigPassesThroughAndValidates() {
        Map<String, String> in = new HashMap<>();
        in.put("dfs.nameservices", "ns1");
        in.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        in.put("dfs.namenode.rpc-address.ns1.nn1", "host1:8020");
        in.put("dfs.namenode.rpc-address.ns1.nn2", "host2:8020");
        in.put("dfs.client.failover.proxy.provider.ns1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        Map<String, String> out = beMap(in);
        // Every dfs.* HA key passes through verbatim.
        in.forEach((k, v) -> Assertions.assertEquals(v, out.get(k), "HA key should pass through: " + k));
        // And the always-on synthesized keys are present.
        Assertions.assertEquals("true", out.get("ipc.client.fallback-to-simple-auth-allowed"));
        Assertions.assertEquals("simple", out.get("hdfs.security.authentication"));
    }

    @Test
    void haConfigMissingFailoverProviderThrows() {
        Map<String, String> in = new HashMap<>();
        in.put("dfs.nameservices", "ns1");
        in.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        in.put("dfs.namenode.rpc-address.ns1.nn1", "host1:8020");
        in.put("dfs.namenode.rpc-address.ns1.nn2", "host2:8020");
        // missing dfs.client.failover.proxy.provider.ns1

        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> HdfsFileSystemProperties.of(in));
        Assertions.assertTrue(ex.getMessage().contains("dfs.client.failover.proxy.provider.ns1"), ex.getMessage());
    }

    @Test
    void haConfigSingleNamenodeThrows() {
        Map<String, String> in = new HashMap<>();
        in.put("dfs.nameservices", "ns1");
        in.put("dfs.ha.namenodes.ns1", "nn1");
        in.put("dfs.namenode.rpc-address.ns1.nn1", "host1:8020");

        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> HdfsFileSystemProperties.of(in));
        Assertions.assertTrue(ex.getMessage().contains("at least 2 namenodes"), ex.getMessage());
    }

    @Test
    void hadoopUsernameEmitted() {
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        in.put("hadoop.username", "doris");
        Assertions.assertEquals("doris", beMap(in).get("hadoop.username"));
    }

    @Test
    void allowFallbackOverridden() {
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        in.put("ipc.client.fallback-to-simple-auth-allowed", "false");
        Assertions.assertEquals("false", beMap(in).get("ipc.client.fallback-to-simple-auth-allowed"));
    }

    @Test
    void defaultFsDerivedFromUri() {
        Map<String, String> in = new HashMap<>();
        in.put("uri", "hdfs://nn:8020/warehouse/db");

        Map<String, String> expected = new HashMap<>();
        expected.put("fs.defaultFS", "hdfs://nn:8020");
        expected.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        expected.put("hdfs.security.authentication", "simple");

        // 'uri' is not a hadoop./dfs./fs./juicefs. key, so it is NOT passed through; only fs.defaultFS is derived.
        Assertions.assertEquals(expected, beMap(in));
    }

    @Test
    void juicefsKeysPassThrough() {
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "jfs://vol/");
        in.put("juicefs.meta", "redis://localhost:6379/0");
        Assertions.assertEquals("redis://localhost:6379/0", beMap(in).get("juicefs.meta"));
    }

    @Test
    void kerberosMissingKeytabThrows() {
        Map<String, String> in = new HashMap<>();
        in.put("hadoop.security.authentication", "kerberos");
        in.put("hadoop.kerberos.principal", "doris@REALM");
        // no keytab
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> HdfsFileSystemProperties.of(in));
        Assertions.assertTrue(ex.getMessage().contains("principal or keytab is not set"), ex.getMessage());
    }

    @Test
    void classifiersMatchHdfs() {
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        HdfsFileSystemProperties p = HdfsFileSystemProperties.of(in);
        Assertions.assertEquals("HDFS", p.providerName());
        Assertions.assertEquals(StorageKind.HDFS_COMPATIBLE, p.kind());
        Assertions.assertEquals(FileSystemType.HDFS, p.type());
        Assertions.assertEquals(BackendStorageKind.HDFS, p.backendKind());
        // C2: HDFS now surfaces a Hadoop-config map so the paimon FE catalog-create Configuration picks up
        // the hadoop.config.resources XML / HA / auth keys (was Optional.empty before the fix).
        Assertions.assertTrue(p.toHadoopProperties().isPresent());
    }

    @Test
    void xmlResourcesAreLoadedIntoBackendMap() throws IOException {
        Path dir = Files.createTempDirectory("hadoop_conf");
        Path xml = dir.resolve("hdfs-site.xml");
        Files.write(xml,
                ("<?xml version=\"1.0\"?><configuration>"
                        + "<property><name>dfs.custom.key</name><value>custom-value</value></property>"
                        + "<property><name>fs.defaultFS</name><value>hdfs://from-xml:9000</value></property>"
                        + "</configuration>").getBytes(StandardCharsets.UTF_8));
        String prev = HdfsConfigFileLoader.hadoopConfigDirOverride;
        HdfsConfigFileLoader.hadoopConfigDirOverride = dir.toString() + "/";
        try {
            Map<String, String> in = new HashMap<>();
            in.put("fs.defaultFS", "hdfs://nn:8020");
            in.put("hadoop.config.resources", "hdfs-site.xml");

            Map<String, String> out = beMap(in);
            Assertions.assertEquals("custom-value", out.get("dfs.custom.key"));
            // user-provided fs.defaultFS overrides the FILE-provided fs.defaultFS (overlay: XML < passthrough).
            Assertions.assertEquals("hdfs://nn:8020", out.get("fs.defaultFS"));
            Assertions.assertEquals("simple", out.get("hdfs.security.authentication"));
        } finally {
            HdfsConfigFileLoader.hadoopConfigDirOverride = prev;
        }
    }

    /** Returns the FE catalog-create Hadoop config map (C2). */
    private static Map<String, String> hadoopMap(Map<String, String> input) {
        Optional<HadoopStorageProperties> h = HdfsFileSystemProperties.of(input).toHadoopProperties();
        Assertions.assertTrue(h.isPresent(), "HDFS must expose a Hadoop config map (C2)");
        return h.get().toHadoopConfigurationMap();
    }

    @Test
    void xmlKeysReachHadoopConfigMap() throws IOException {
        // C2 regression pin: a key that lives ONLY in the referenced XML (not a raw catalog prop, so it
        // cannot ride the connector's raw fs./dfs./hadoop. passthrough) must reach the FE Hadoop config map.
        // Pre-fix toHadoopProperties() was empty -> .get() throws -> RED.
        Path dir = Files.createTempDirectory("hadoop_conf");
        Path xml = dir.resolve("hdfs-site.xml");
        Files.write(xml,
                ("<?xml version=\"1.0\"?><configuration>"
                        + "<property><name>dfs.custom.key</name><value>custom-value</value></property>"
                        + "</configuration>").getBytes(StandardCharsets.UTF_8));
        String prev = HdfsConfigFileLoader.hadoopConfigDirOverride;
        HdfsConfigFileLoader.hadoopConfigDirOverride = dir.toString() + "/";
        try {
            Map<String, String> in = new HashMap<>();
            in.put("fs.defaultFS", "hdfs://nn:8020");
            in.put("hadoop.config.resources", "hdfs-site.xml");

            Map<String, String> out = hadoopMap(in);
            Assertions.assertEquals("custom-value", out.get("dfs.custom.key"));
        } finally {
            HdfsConfigFileLoader.hadoopConfigDirOverride = prev;
        }
    }

    @Test
    void hadoopConfigMapExcludesFrameworkDefaultsButBeMapKeepsThem() throws IOException {
        // Clobber guard (encodes WHY): the FE Hadoop map must NOT carry Hadoop's built-in fs.s3a.* defaults
        // (which would overwrite a co-bound object-store provider's tuned fs.s3a.path.style.access=true in a
        // multi-backend merge). The BE map (toMap) DOES keep them, for byte-parity with the legacy backend
        // set. Asserting both sides pins the deliberate FE/BE asymmetry.
        Path dir = Files.createTempDirectory("hadoop_conf");
        Path xml = dir.resolve("hdfs-site.xml");
        Files.write(xml,
                ("<?xml version=\"1.0\"?><configuration>"
                        + "<property><name>dfs.custom.key</name><value>custom-value</value></property>"
                        + "</configuration>").getBytes(StandardCharsets.UTF_8));
        String prev = HdfsConfigFileLoader.hadoopConfigDirOverride;
        HdfsConfigFileLoader.hadoopConfigDirOverride = dir.toString() + "/";
        try {
            Map<String, String> in = new HashMap<>();
            in.put("fs.defaultFS", "hdfs://nn:8020");
            in.put("hadoop.config.resources", "hdfs-site.xml");

            Map<String, String> feMap = hadoopMap(in);
            Map<String, String> beMapWithDefaults = beMap(in);
            // FE map: defaults-free -> the framework fs.s3a.* defaults are absent.
            Assertions.assertNull(feMap.get("fs.s3a.path.style.access"),
                    "FE Hadoop map must not carry the core-default.xml fs.s3a.* defaults (clobber guard)");
            Assertions.assertNull(feMap.get("fs.s3a.connection.maximum"));
            // BE map: defaults-laden -> those same framework defaults are present (legacy byte-parity).
            // Assert presence, not the exact default value (it is hadoop-version-dependent, e.g. the
            // fs.s3a.connection.maximum default is 96 on hadoop 3.3.x but 500 on 3.4.x).
            Assertions.assertNotNull(beMapWithDefaults.get("fs.s3a.path.style.access"));
            Assertions.assertNotNull(beMapWithDefaults.get("fs.s3a.connection.maximum"));
            Assertions.assertTrue(beMapWithDefaults.size() > feMap.size(),
                    "BE map (defaults-laden) must carry more keys than the defaults-free FE map");
        } finally {
            HdfsConfigFileLoader.hadoopConfigDirOverride = prev;
        }
    }

    @Test
    void hadoopConfigMapKeepsMeaningfulKeys() {
        // Defaults-free does NOT mean empty: the synthesized HDFS keys + fs.defaultFS must survive in the
        // FE Hadoop map even with no hadoop.config.resources (blank => loadConfigMap returns empty, then the
        // synthesized keys are added; no framework defaults either way).
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        Map<String, String> out = hadoopMap(in);
        Assertions.assertEquals("hdfs://nn:8020", out.get("fs.defaultFS"));
        Assertions.assertEquals("simple", out.get("hdfs.security.authentication"));
        Assertions.assertEquals("true", out.get("ipc.client.fallback-to-simple-auth-allowed"));
        Assertions.assertNull(out.get("fs.s3a.path.style.access"));
    }

    @Test
    void provNameViaProvider() {
        // bind() routes through HdfsFileSystemProperties.of and yields the typed model.
        HdfsFileSystemProvider provider = new HdfsFileSystemProvider();
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        HdfsFileSystemProperties bound = provider.bind(in);
        Assertions.assertEquals("hdfs://nn:8020", bound.toMap().get("fs.defaultFS"));
    }

    @Test
    void emptyInputFallbackMatchesLegacy() {
        // The framework auto-fallback HDFS storage is built with NO explicit keys; the BE map is just the
        // two always-on synthesized keys (legacy initBackendConfigProperties produces the same).
        Map<String, String> expected = new HashMap<>();
        expected.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        expected.put("hdfs.security.authentication", "simple");
        Assertions.assertEquals(expected, beMap(new HashMap<>()));
    }

    @Test
    void kerberosCredsPresentButSimpleTypeDoesNotSynthesize() {
        // principal/keytab present but NO auth-type key => stays "simple": the hadoop.security.authentication
        // synthesis block must NOT fire. The discriminator is hdfsAuthenticationType only, never the presence
        // of principal/keytab (which still pass through via the hadoop.* prefix).
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        in.put("hadoop.kerberos.principal", "doris@REALM");
        in.put("hadoop.kerberos.keytab", "/etc/security/doris.keytab");

        Map<String, String> out = beMap(in);
        Assertions.assertEquals("simple", out.get("hdfs.security.authentication"));
        Assertions.assertNull(out.get("hadoop.security.authentication"));
        // creds still flow to BE via passthrough.
        Assertions.assertEquals("doris@REALM", out.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/security/doris.keytab", out.get("hadoop.kerberos.keytab"));
    }

    @Test
    void viewfsAndJfsUrisDeriveDefaultFs() {
        Map<String, String> viewfs = new HashMap<>();
        viewfs.put("uri", "viewfs://cluster/warehouse");
        Assertions.assertEquals("viewfs://cluster", beMap(viewfs).get("fs.defaultFS"));

        Map<String, String> jfs = new HashMap<>();
        jfs.put("uri", "jfs://vol/warehouse");
        Assertions.assertEquals("jfs://vol", beMap(jfs).get("fs.defaultFS"));
    }

    @Test
    void ofsAndOssUrisDoNotDeriveDefaultFs() {
        // ofs/oss are bound by the provider but are NOT in URI_SCHEMES, so no fs.defaultFS is derived (legacy
        // parity: legacy supportSchema = {hdfs, viewfs, jfs}).
        Map<String, String> ofs = new HashMap<>();
        ofs.put("uri", "ofs://cluster/warehouse");
        Assertions.assertNull(beMap(ofs).get("fs.defaultFS"));

        Map<String, String> oss = new HashMap<>();
        oss.put("uri", "oss://bucket/warehouse");
        Assertions.assertNull(beMap(oss).get("fs.defaultFS"));
    }

    @Test
    void allowFallbackBlankUsesDefault() {
        // An explicit blank value is filtered by binding (isNotBlank gate) so the field stays default ""
        // and the else-branch emits "true" — matching legacy.
        Map<String, String> in = new HashMap<>();
        in.put("fs.defaultFS", "hdfs://nn:8020");
        in.put("ipc.client.fallback-to-simple-auth-allowed", "");
        Assertions.assertEquals("true", beMap(in).get("ipc.client.fallback-to-simple-auth-allowed"));
    }

    @Test
    void multiUriDoesNotDeriveDefaultFs() {
        // A comma-separated uri list is not a usable single fs.defaultFS (legacy getUri returns null).
        Map<String, String> in = new HashMap<>();
        in.put("uri", "hdfs://nn1:8020,hdfs://nn2:8020");
        Assertions.assertNull(beMap(in).get("fs.defaultFS"));
    }

    @Test
    void haConfigMissingNamenodesKeyThrows() {
        Map<String, String> in = new HashMap<>();
        in.put("dfs.nameservices", "ns1");
        // missing dfs.ha.namenodes.ns1
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> HdfsFileSystemProperties.of(in));
        Assertions.assertTrue(ex.getMessage().contains("dfs.ha.namenodes.ns1"), ex.getMessage());
    }

    @Test
    void haConfigMissingRpcAddressThrows() {
        Map<String, String> in = new HashMap<>();
        in.put("dfs.nameservices", "ns1");
        in.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        in.put("dfs.namenode.rpc-address.ns1.nn1", "host1:8020");
        // missing rpc-address for nn2
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class, () -> HdfsFileSystemProperties.of(in));
        Assertions.assertTrue(ex.getMessage().contains("dfs.namenode.rpc-address.ns1.nn2"), ex.getMessage());
    }

    @Test
    void haConfigMultiNameserviceWithWhitespaceValidates() {
        // Comma + whitespace nameservices are trimmed and each is validated independently.
        Map<String, String> in = new HashMap<>();
        in.put("dfs.nameservices", "ns1, ns2");
        in.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        in.put("dfs.namenode.rpc-address.ns1.nn1", "h1:8020");
        in.put("dfs.namenode.rpc-address.ns1.nn2", "h2:8020");
        in.put("dfs.client.failover.proxy.provider.ns1", "x.Provider");
        in.put("dfs.ha.namenodes.ns2", "nn3,nn4");
        in.put("dfs.namenode.rpc-address.ns2.nn3", "h3:8020");
        in.put("dfs.namenode.rpc-address.ns2.nn4", "h4:8020");
        in.put("dfs.client.failover.proxy.provider.ns2", "x.Provider");

        Assertions.assertEquals("h3:8020", beMap(in).get("dfs.namenode.rpc-address.ns2.nn3"));
    }

    @Test
    void malformedUriFailsLoud() {
        // Parity with legacy: a malformed uri (no explicit fs.defaultFS) fails loud at bind, not silently.
        Map<String, String> in = new HashMap<>();
        in.put("uri", "hdfs://nn:8020/a path{bad}");
        Assertions.assertThrows(IllegalArgumentException.class, () -> HdfsFileSystemProperties.of(in));
    }

    @Test
    void configDirResolvedFromEngineSystemProperty() throws IOException {
        // F1 wiring: with no explicit override, the loader resolves the config dir from the engine-set system
        // property (fe-core FileSystemFactory sets it from Config.hadoop_config_dir for non-default installs).
        Path dir = Files.createTempDirectory("hadoop_conf_sysprop");
        Path xml = dir.resolve("core-site.xml");
        Files.write(xml,
                ("<?xml version=\"1.0\"?><configuration>"
                        + "<property><name>dfs.sysprop.key</name><value>v</value></property>"
                        + "</configuration>").getBytes(StandardCharsets.UTF_8));
        String prevOverride = HdfsConfigFileLoader.hadoopConfigDirOverride;
        String prevProp = System.getProperty(HdfsConfigFileLoader.CONFIG_DIR_PROPERTY);
        HdfsConfigFileLoader.hadoopConfigDirOverride = null; // force the system-property path
        System.setProperty(HdfsConfigFileLoader.CONFIG_DIR_PROPERTY, dir.toString() + "/");
        try {
            Map<String, String> in = new HashMap<>();
            in.put("fs.defaultFS", "hdfs://nn:8020");
            in.put("hadoop.config.resources", "core-site.xml");
            Assertions.assertEquals("v", beMap(in).get("dfs.sysprop.key"));
        } finally {
            HdfsConfigFileLoader.hadoopConfigDirOverride = prevOverride;
            if (prevProp == null) {
                System.clearProperty(HdfsConfigFileLoader.CONFIG_DIR_PROPERTY);
            } else {
                System.setProperty(HdfsConfigFileLoader.CONFIG_DIR_PROPERTY, prevProp);
            }
        }
    }
}
