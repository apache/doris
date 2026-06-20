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

package org.apache.doris.connector.paimon;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link PaimonCatalogFactory}, the pure flavor-assembly core.
 *
 * <p>These tests are entirely offline: {@code buildCatalogOptions} is a pure transform
 * (Map in, Paimon {@link Options} out), {@code validate} is fail-fast pre-flight, and the Hadoop
 * config builders are pure (Maps in, conf out), so no live catalog or env is touched. No Mockito —
 * props are plain maps.
 *
 * <p>This is the parity baseline for B1: the per-flavor option keys MUST mirror the legacy
 * fe-core {@code AbstractPaimonProperties} + each {@code Paimon*MetaStoreProperties}.
 *
 * <p>P1-T03: the canonical object-store translation ({@code s3.*}/{@code oss.*}/... -&gt; {@code fs.s3a.*})
 * moved OUT of this factory to fe-filesystem; the builders now receive it pre-computed as a
 * {@code storageHadoopConfig} map (what {@code PaimonConnector} assembles from
 * {@code ConnectorContext.getStorageProperties().toHadoopConfigurationMap()}). These tests therefore
 * pin the connector-LOCAL contract — storage-map overlay, {@code paimon.*} re-key, raw
 * {@code fs./dfs./hadoop.} passthrough, last-write-wins, kerberos-after-storage — NOT the canonical
 * translation, which is owned and tested by fe-filesystem's {@code *FileSystemPropertiesTest}. The
 * end-to-end new/old equivalence is gated by the docker 5-flavor run (P1-T06; see DV-003).
 */
public class PaimonCatalogFactoryTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /**
     * A pre-computed object-store storage Hadoop-config map — the fe-filesystem
     * {@code toHadoopConfigurationMap()} output the connector now overlays. {@code storage()} with no
     * args is the no-static-object-store case (HDFS-only / REST), where the map is empty.
     */
    private static Map<String, String> storage(String... kv) {
        return props(kv);
    }

    // ---------------------------------------------------------------------
    // buildCatalogOptions — per-flavor metastore identifier + warehouse
    // ---------------------------------------------------------------------

    @Test
    public void filesystemSetsMetastoreFilesystemAndWarehouse() {
        Options opts = PaimonCatalogFactory.buildCatalogOptions(
                props("paimon.catalog.type", "filesystem", "warehouse", "/wh"));

        // WHY: filesystem is the default flavor; its metastore identifier selects
        // FileSystemCatalogFactory and the warehouse is the on-disk root. Both are load-bearing
        // for catalog creation. MUTATION: emitting "hive"/"jdbc" or dropping warehouse -> red.
        Assertions.assertEquals("filesystem", opts.get("metastore"));
        Assertions.assertEquals("/wh", opts.get("warehouse"));
    }

    @Test
    public void hmsSetsHiveMetastoreUriPoolAndLocation() {
        Options opts = PaimonCatalogFactory.buildCatalogOptions(props(
                "paimon.catalog.type", "hms",
                "warehouse", "/wh",
                "hive.metastore.uris", "thrift://nn:9083"));

        // WHY: hms maps to paimon's "hive" metastore; the legacy HMS flavor always emits the
        // metastore uri plus the pool-eviction + location-in-properties defaults. Dropping any
        // would change how the (B1-d2) HiveCatalog connects/caches. MUTATION: metastore!="hive",
        // missing uri, or wrong defaults -> red.
        Assertions.assertEquals("hive", opts.get("metastore"));
        Assertions.assertEquals("thrift://nn:9083", opts.get("uri"));
        Assertions.assertEquals("300000", opts.get("client-pool-cache.eviction-interval-ms"));
        Assertions.assertEquals("false", opts.get("location-in-properties"));
    }

    @Test
    public void hmsAcceptsUriAliasAndOverrides() {
        Options opts = PaimonCatalogFactory.buildCatalogOptions(props(
                "paimon.catalog.type", "hms",
                "warehouse", "/wh",
                "uri", "thrift://alias:9083",
                "client-pool-cache.eviction-interval-ms", "60000",
                "location-in-properties", "true"));

        // WHY: the legacy HMS flavor accepts the bare "uri" alias for the metastore URI and lets
        // the user override the pool/location defaults. MUTATION: ignoring the alias or hardcoding
        // the defaults instead of reading the user value -> red.
        Assertions.assertEquals("thrift://alias:9083", opts.get("uri"));
        Assertions.assertEquals("60000", opts.get("client-pool-cache.eviction-interval-ms"));
        Assertions.assertEquals("true", opts.get("location-in-properties"));
    }

    @Test
    public void restSetsMetastoreRestUriAndStripsRestPrefix() {
        Options opts = PaimonCatalogFactory.buildCatalogOptions(props(
                "paimon.catalog.type", "rest",
                "paimon.rest.uri", "http://rest:8080",
                "paimon.rest.token.provider", "bear"));

        // WHY: rest maps to the "rest" metastore; the legacy rest flavor sets "uri" from
        // paimon.rest.uri and re-keys every paimon.rest.* prop by stripping the prefix (so
        // token.provider becomes a paimon option). MUTATION: metastore!="rest", missing uri, or
        // not stripping the paimon.rest. prefix -> red.
        Assertions.assertEquals("rest", opts.get("metastore"));
        Assertions.assertEquals("http://rest:8080", opts.get("uri"));
        Assertions.assertEquals("bear", opts.get("token.provider"));
    }

    @Test
    public void jdbcSetsMetastoreUriUserAndRawJdbcKeys() {
        Options opts = PaimonCatalogFactory.buildCatalogOptions(props(
                "paimon.catalog.type", "jdbc",
                "warehouse", "/wh",
                "uri", "jdbc:mysql://db:3306/meta",
                "paimon.jdbc.user", "alice",
                "jdbc.password", "secret",
                "jdbc.foo", "bar"));

        // WHY: jdbc maps to JdbcCatalogFactory; the legacy jdbc flavor sets the CatalogOptions URI,
        // the jdbc.user/jdbc.password (read from either alias), and passes through any raw jdbc.*
        // key. These are exactly the options the JdbcCatalog reads. MUTATION: metastore!="jdbc",
        // missing uri/user/password, or dropping the raw jdbc.foo passthrough -> red.
        Assertions.assertEquals("jdbc", opts.get("metastore"));
        Assertions.assertEquals("jdbc:mysql://db:3306/meta", opts.get("uri"));
        Assertions.assertEquals("alice", opts.get("jdbc.user"));
        Assertions.assertEquals("secret", opts.get("jdbc.password"));
        Assertions.assertEquals("bar", opts.get("jdbc.foo"));
    }

    @Test
    public void dlfSetsHiveMetastoreClientClassAndPoolKeys() {
        Options opts = PaimonCatalogFactory.buildCatalogOptions(props(
                "paimon.catalog.type", "dlf",
                "warehouse", "/wh",
                "dlf.access_key", "ak",
                "dlf.secret_key", "sk",
                "dlf.endpoint", "dlf.cn.aliyuncs.com"));

        // WHY: dlf is adapted onto paimon's "hive" metastore via the Aliyun ProxyMetaStoreClient;
        // the legacy DLF flavor always emits that client class plus the conf:dlf.catalog.id pool
        // key. These two are what make a HiveCatalog talk to DLF. MUTATION: metastore!="hive",
        // wrong client class, or missing pool key -> red.
        Assertions.assertEquals("hive", opts.get("metastore"));
        Assertions.assertEquals("com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient",
                opts.get("metastore.client.class"));
        Assertions.assertEquals("conf:dlf.catalog.id", opts.get("client-pool-cache.keys"));
    }

    @Test
    public void paimonPrefixPassthroughExcludesStoragePrefixes() {
        Options opts = PaimonCatalogFactory.buildCatalogOptions(props(
                "paimon.catalog.type", "filesystem",
                "warehouse", "/wh",
                "paimon.read.batch-size", "4096",
                "paimon.s3.access-key", "should-not-leak"));

        // WHY: the legacy appendCatalogOptions re-keys generic paimon.* props by stripping the
        // prefix, BUT deliberately excludes storage prefixes (paimon.s3./s3a./fs.s3./fs.oss.)
        // because those belong in the Hadoop Configuration (B1 dispatch 2), not the catalog
        // Options. MUTATION: dropping the passthrough (read.batch-size missing) or leaking the
        // storage key (s3.access-key present) -> red.
        Assertions.assertEquals("4096", opts.get("read.batch-size"));
        Assertions.assertNull(opts.get("access-key"),
                "storage-prefixed paimon.s3.* keys must NOT be promoted into catalog options");
    }

    @Test
    public void restBuildOptionsOmitsBlankWarehouse() {
        Options opts = PaimonCatalogFactory.buildCatalogOptions(props(
                "paimon.catalog.type", "rest",
                "paimon.rest.uri", "http://rest:8080"));

        // WHY: this pins option ASSEMBLY only (independent of validate, which now requires a
        // warehouse for rest too): the common appender sets the warehouse option only when the
        // warehouse value is non-blank, so a blank/absent warehouse produces no warehouse key
        // rather than a blank one. MUTATION: emitting a (blank) warehouse key when none was given,
        // or unconditionally setting warehouse -> red.
        Assertions.assertNull(opts.get("warehouse"),
                "buildCatalogOptions must not emit a warehouse option when the warehouse is blank");
    }

    // ---------------------------------------------------------------------
    // buildHadoopConfiguration — storage-config overlay + paimon.* re-key + raw passthrough
    // (P1-T03: the canonical object-store translation now arrives pre-computed in storageHadoopConfig
    //  from ConnectorContext.getStorageProperties(); the connector-local overlay/last-write-wins stays)
    // ---------------------------------------------------------------------

    @Test
    public void buildHadoopConfigurationNormalizesS3PrefixesAndCopiesRawKeys() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "paimon.s3.access-key", "ak",
                "paimon.s3a.secret-key", "sk",
                "paimon.fs.s3.endpoint", "s3.amazonaws.com",
                "paimon.fs.oss.endpoint.region", "oss-cn.aliyuncs.com",
                "fs.defaultFS", "hdfs://nn:8020",
                "dfs.nameservices", "nn",
                "hadoop.security.authentication", "kerberos",
                "paimon.read.batch-size", "4096"), storage());

        // WHY: the live FileIO/S3FileIO only recognizes Hadoop-prefixed keys; the connector strips each
        // of the four user storage prefixes (paimon.s3./s3a./fs.s3./fs.oss.) and re-keys them under
        // fs.s3a., while genuine fs.*/dfs./hadoop.* keys are passed through verbatim so HDFS/auth config
        // reaches the catalog. This connector-local overlay is UNCHANGED by P1-T03 (only the canonical
        // object-store translation moved out to storageHadoopConfig). MUTATION: not normalizing to
        // fs.s3a. (key still under the old prefix), or dropping the raw fs./dfs./hadoop. passthrough -> red.
        Assertions.assertEquals("ak", conf.get("fs.s3a.access-key"));
        Assertions.assertEquals("sk", conf.get("fs.s3a.secret-key"));
        Assertions.assertEquals("s3.amazonaws.com", conf.get("fs.s3a.endpoint"));
        // paimon.fs.oss.* also normalizes onto the fs.s3a. prefix (all four userStoragePrefixes map to
        // FS_S3A_PREFIX). Distinct suffix to avoid colliding with paimon.fs.s3.endpoint above.
        Assertions.assertEquals("oss-cn.aliyuncs.com", conf.get("fs.s3a.endpoint.region"));
        Assertions.assertEquals("hdfs://nn:8020", conf.get("fs.defaultFS"));
        Assertions.assertEquals("nn", conf.get("dfs.nameservices"));
        Assertions.assertEquals("kerberos", conf.get("hadoop.security.authentication"));
        // A non-storage paimon.* key (a catalog Option) must NOT leak into the Hadoop Configuration.
        Assertions.assertNull(conf.get("paimon.read.batch-size"));
        Assertions.assertNull(conf.get("read.batch-size"));
    }

    @Test
    public void buildHadoopConfigurationAppliesStorageHadoopConfig() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(
                props("fs.defaultFS", "hdfs://nn:8020"),
                storage("fs.s3a.access.key", "ak",
                        "fs.s3a.endpoint", "s3.amazonaws.com",
                        "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"));

        // WHY (P1-T03): the canonical object-store config (fs.s3a.* etc.) now arrives PRE-COMPUTED in
        // storageHadoopConfig — assembled by PaimonConnector from ConnectorContext.getStorageProperties()
        // via fe-filesystem's toHadoopConfigurationMap() — and the connector overlays it verbatim. Before
        // P1-T03 the connector recomputed it from props via the legacy buildObjectStorageHadoopConfig.
        // MUTATION: not applying storageHadoopConfig (fs.s3a.access.key null) -> red.
        Assertions.assertEquals("ak", conf.get("fs.s3a.access.key"));
        Assertions.assertEquals("s3.amazonaws.com", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"));
        // the raw fs./dfs./hadoop. passthrough still applies alongside the pre-computed storage map.
        Assertions.assertEquals("hdfs://nn:8020", conf.get("fs.defaultFS"));
    }

    @Test
    public void buildHadoopConfigurationExplicitFsS3aKeyOverridesStorageConfig() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(
                props("fs.s3a.access.key", "explicit"),
                storage("fs.s3a.access.key", "from-storage"));

        // WHY: the raw fs.* passthrough runs AFTER the storageHadoopConfig overlay (last-write-wins =
        // legacy addResource(getHadoopStorageConfig) THEN appendUserHadoopConfig ordering), so a power
        // user who explicitly set fs.s3a.access.key in the catalog props still wins over the
        // fe-filesystem-derived value. MUTATION: reversing precedence (storage overlays raw) -> "from-storage" -> red.
        Assertions.assertEquals("explicit", conf.get("fs.s3a.access.key"));
    }

    @Test
    public void buildHadoopConfigurationPaimonPrefixOverridesStorageConfig() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(
                props("paimon.s3.endpoint", "from-paimon"),
                storage("fs.s3a.endpoint", "from-storage"));

        // WHY: the paimon.* prefix re-key (paimon.s3.endpoint -> fs.s3a.endpoint) is part of the
        // connector-specific overlay that runs LAST, so an explicit paimon.s3.* key wins over the
        // fe-filesystem storage map (last-write-wins). MUTATION: storage overlaying the paimon.* re-key
        // (fs.s3a.endpoint == "from-storage") -> red.
        Assertions.assertEquals("from-paimon", conf.get("fs.s3a.endpoint"));
    }

    @Test
    public void buildStorageHadoopConfigFoldsInHdfsHadoopMap() {
        // C2 end-to-end seam: a storage property exposing a Hadoop-config key that is NOT a raw catalog
        // prop (so it cannot ride the connector's fs./dfs./hadoop. passthrough) must reach the FE catalog
        // Configuration via ctx.getStorageProperties().toHadoopProperties() -> buildStorageHadoopConfig ->
        // buildHadoopConfiguration. This is exactly the leg the HDFS C2 fix relies on: after the fix
        // HdfsFileSystemProperties.toHadoopProperties() is non-empty and carries its hadoop.config.resources
        // XML keys. MUTATION: dropping the toHadoopProperties() merge in buildStorageHadoopConfig -> red.
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.storageProperties = Collections.singletonList(
                new StubHadoopStorageProperties(Collections.singletonMap("dfs.custom.key", "custom-value")));
        PaimonConnector connector = new PaimonConnector(props(), ctx);

        Map<String, String> merged = connector.buildStorageHadoopConfig();
        Assertions.assertEquals("custom-value", merged.get("dfs.custom.key"),
                "buildStorageHadoopConfig must fold in each storage prop's toHadoopConfigurationMap()");

        // ...and that merged map flows into the actual catalog Configuration (the key is absent from props,
        // so the only path by which it can land in conf is the storageHadoopConfig overlay).
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(), merged);
        Assertions.assertEquals("custom-value", conf.get("dfs.custom.key"));
    }

    /** Minimal {@link StorageProperties} exposing a fixed Hadoop config map (C2 seam test double). */
    private static final class StubHadoopStorageProperties implements StorageProperties, HadoopStorageProperties {
        private final Map<String, String> hadoopConfig;

        StubHadoopStorageProperties(Map<String, String> hadoopConfig) {
            this.hadoopConfig = hadoopConfig;
        }

        @Override
        public Optional<HadoopStorageProperties> toHadoopProperties() {
            return Optional.of(this);
        }

        @Override
        public Map<String, String> toHadoopConfigurationMap() {
            return hadoopConfig;
        }

        @Override
        public String providerName() {
            return "STUB";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.HDFS_COMPATIBLE;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.HDFS;
        }

        @Override
        public Map<String, String> rawProperties() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, String> matchedProperties() {
            return Collections.emptyMap();
        }
    }

    // ---------------------------------------------------------------------
    // assembleHiveConf — seed optional base (hive.conf.resources) THEN overlay shared-parser overrides
    // (the HiveConf key CONTENT for hms/dlf is produced + parity-tested by fe-connector-metastore-spi's
    //  HmsMetaStorePropertiesImplTest / DlfMetaStorePropertiesImplTest; here we pin only the F2 layering)
    // ---------------------------------------------------------------------

    @Test
    public void assembleHiveConfSeedsBaseThenOverridesWin() {
        // WHY (F2): an external hive-site.xml (hive.conf.resources, loaded FE-side) is the BASE; the
        // connection/user overrides from the shared parser must be applied ON TOP so they win, while
        // base-only keys survive. MUTATION: applying overrides FIRST (base clobbers them) -> red.
        Map<String, String> base = new HashMap<>();
        base.put("hive.metastore.sasl.qop", "auth-conf"); // base-only key, must survive
        base.put("hive.metastore.uris", "thrift://from-file:9083"); // overridden below
        Map<String, String> overrides = new HashMap<>();
        overrides.put("hive.metastore.uris", "thrift://from-override:9083"); // wins over base
        overrides.put("hadoop.username", "doris"); // override-only key

        HiveConf hc = PaimonCatalogFactory.assembleHiveConf(base, overrides);

        Assertions.assertEquals("auth-conf", hc.get("hive.metastore.sasl.qop"));
        Assertions.assertEquals("thrift://from-override:9083", hc.get("hive.metastore.uris"));
        Assertions.assertEquals("doris", hc.get("hadoop.username"));
    }

    @Test
    public void assembleHiveConfPinsPluginClassLoaderNotTccl() {
        // WHY (FIX-1, CI 973411): HiveMetaStoreClient.loadFilterHooks resolves metastore.filter.hook via
        // Configuration.getClass, which uses the HiveConf's OWN classLoader field. new HiveConf() captures
        // the thread-context CL active AT CONSTRUCTION into that field. At runtime assembleHiveConf runs
        // before the plugin TCCL pin, so the conf would capture the parent 'app' loader; under child-first
        // plugin loading that resolves DefaultMetaStoreFilterHookImpl from the parent while
        // MetaStoreFilterHook is child-loaded -> "class ... not ...". The conf MUST be pinned to the plugin
        // loader (the one that loaded HiveMetaStoreClient/MetaStoreFilterHook), exactly as
        // buildHadoopConfiguration already does. MUTATION: drop the setClassLoader -> the conf keeps the
        // foreign TCCL below -> red. (A flat-classpath assertion alone cannot repro the real cross-loader
        // cast, so we install a distinct TCCL to make the captured-loader bug observable offline.)
        ClassLoader foreign = new URLClassLoader(new URL[0], null);
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(foreign);
            HiveConf hc = PaimonCatalogFactory.assembleHiveConf(null, Collections.emptyMap());
            Assertions.assertSame(PaimonCatalogFactory.class.getClassLoader(), hc.getClassLoader());
            Assertions.assertNotSame(foreign, hc.getClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    @Test
    public void assembleHiveConfAcceptsNullBase() {
        // WHY: the dlf flavor has no hive.conf.resources base, so it passes null; assembleHiveConf must
        // not NPE and must still apply the overrides. MUTATION: removing the null guard -> NPE -> red.
        Map<String, String> overrides = new HashMap<>();
        overrides.put("dlf.catalog.endpoint", "dlf-vpc.cn-hangzhou.aliyuncs.com");

        HiveConf hc = PaimonCatalogFactory.assembleHiveConf(null, overrides);

        Assertions.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", hc.get("dlf.catalog.endpoint"));
    }

}
