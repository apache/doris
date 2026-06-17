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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    // validate — fail-fast
    // ---------------------------------------------------------------------

    @Test
    public void validateRejectsUnknownFlavor() {
        // WHY: an unknown paimon.catalog.type must fail at CREATE CATALOG, not silently fall back
        // to filesystem (the pre-B1 stub bug). MUTATION: removing the flavor whitelist check -> red.
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props("paimon.catalog.type", "bogus", "warehouse", "/wh")));
        Assertions.assertTrue(ex.getMessage().contains("bogus"));
    }

    @Test
    public void validateRequiresWarehouseForFilesystem() {
        // WHY: filesystem/hms/jdbc/dlf all need a warehouse; missing it must fail fast.
        // MUTATION: dropping the warehouse-required check for filesystem -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props("paimon.catalog.type", "filesystem")));
    }

    @Test
    public void validateRequiresWarehouseForRest() {
        // WHY (legacy parity): the base AbstractPaimonProperties declares warehouse as a required
        // @ConnectorProperty and PaimonRestMetaStoreProperties does NOT override it, so legacy
        // REJECTS a REST catalog without warehouse. validate must require warehouse for rest too,
        // not exempt it. MUTATION: re-adding a REST exemption to the warehouse-required check
        // (rest-without-warehouse passing) -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props(
                        "paimon.catalog.type", "rest",
                        "paimon.rest.uri", "http://rest:8080")));
    }

    @Test
    public void validateRestDlfTokenProviderRequiresAkSk() {
        // WHY: legacy ParamRules.requireIf — when the REST token provider is "dlf", the dlf
        // access-key-id AND access-key-secret are mandatory. MUTATION: removing the requireIf -> red.
        // NOTE: warehouse is supplied so the throw exercises the Ak/Sk requireIf, not the
        // warehouse-required check.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props(
                        "paimon.catalog.type", "rest",
                        "warehouse", "/wh",
                        "paimon.rest.uri", "http://rest:8080",
                        "paimon.rest.token.provider", "dlf")));
    }

    @Test
    public void validateJdbcDriverUrlWithoutDriverClassFails() {
        // WHY: legacy getBackendPaimonOptions/registerJdbcDriver require driver_class whenever a
        // driver_url is given (otherwise the driver cannot be loaded). MUTATION: removing that
        // coupling check -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props(
                        "paimon.catalog.type", "jdbc",
                        "warehouse", "/wh",
                        "uri", "jdbc:mysql://db:3306/meta",
                        "paimon.jdbc.driver_url", "mysql.jar")));
    }

    @Test
    public void validateDlfRequiresAccessKey() {
        // WHY: legacy AliyunDLFBaseProperties.buildRules requires dlf.access_key (and secret_key).
        // MUTATION: removing the access-key required check -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props(
                        "paimon.catalog.type", "dlf",
                        "warehouse", "/wh",
                        "dlf.secret_key", "sk",
                        "dlf.endpoint", "dlf.cn.aliyuncs.com")));
    }

    @Test
    public void validateDlfRequiresEndpointOrRegion() {
        // WHY: legacy DLF derives the endpoint from the region; if BOTH endpoint and region are
        // blank it throws "dlf.endpoint is required." MUTATION: removing the endpoint-or-region
        // check -> red.
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props(
                        "paimon.catalog.type", "dlf",
                        "warehouse", "/wh",
                        "dlf.access_key", "ak",
                        "dlf.secret_key", "sk")));
        Assertions.assertTrue(ex.getMessage().contains("dlf.endpoint"));
    }

    @Test
    public void validateHmsRequiresUri() {
        // WHY: the hms flavor cannot connect without a metastore uri; legacy HMSBaseProperties
        // requires hive.metastore.uris (or the uri alias). MUTATION: removing the hms uri check -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props(
                        "paimon.catalog.type", "hms",
                        "warehouse", "/wh")));
    }

    @Test
    public void validateAcceptsEachWellFormedFlavor() {
        // WHY: the happy path for every flavor must pass cleanly — a validator that rejects valid
        // configs is as broken as one that accepts invalid ones. MUTATION: an over-eager required
        // check on any flavor -> red.
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.validate(
                props("paimon.catalog.type", "filesystem", "warehouse", "/wh")));
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.validate(props(
                "paimon.catalog.type", "hms", "warehouse", "/wh", "hive.metastore.uris", "thrift://nn:9083")));
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.validate(props(
                "paimon.catalog.type", "rest", "warehouse", "/wh", "paimon.rest.uri", "http://rest:8080")));
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.validate(props(
                "paimon.catalog.type", "jdbc", "warehouse", "/wh", "uri", "jdbc:mysql://db:3306/meta")));
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.validate(props(
                "paimon.catalog.type", "dlf", "warehouse", "/wh",
                "dlf.access_key", "ak", "dlf.secret_key", "sk", "dlf.region", "cn-hangzhou")));
    }

    @Test
    public void validateDefaultsToFilesystemWhenTypeAbsent() {
        // WHY: an absent paimon.catalog.type defaults to filesystem (DEFAULT_CATALOG_TYPE), which
        // then requires a warehouse. MUTATION: defaulting to something else, or not requiring
        // warehouse on the implicit-filesystem path -> red.
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.validate(props("warehouse", "/wh")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonCatalogFactory.validate(props("not-a-type", "x")));
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
        // P1-T03 the connector recomputed it from props via fe-property buildObjectStorageHadoopConfig.
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

    // ---------------------------------------------------------------------
    // buildHmsHiveConf — metastore uri + hive.* verbatim + auth key + storage overlay
    // ---------------------------------------------------------------------

    @Test
    public void buildHmsHiveConfSetsUriHiveKeysAuthAndStorage() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.sasl.enabled", "true",
                "hive.metastore.client.principal", "doris@REALM",
                "hive.metastore.client.keytab", "/etc/doris.keytab",
                "hadoop.security.authentication", "kerberos",
                "paimon.s3.access-key", "ak"), Collections.emptyMap(), Collections.emptyMap());

        // WHY: a live HiveCatalog reads the metastore uri from the HiveConf, honors any user hive.*
        // override, and needs the auth keys (alongside the FE-injected UGI). The "uri" alias must
        // resolve to hive.metastore.uris, and the paimon.s3.* key must re-key onto fs.s3a. via the
        // connector overlay. MUTATION: missing metastore uri, dropping a hive.* override, dropping an
        // auth key, or not applying the connector storage overlay -> red.
        Assertions.assertEquals("thrift://nn:9083", hc.get("hive.metastore.uris"));
        Assertions.assertEquals("true", hc.get("hive.metastore.sasl.enabled"));
        Assertions.assertEquals("doris@REALM", hc.get("hive.metastore.client.principal"));
        Assertions.assertEquals("/etc/doris.keytab", hc.get("hive.metastore.client.keytab"));
        Assertions.assertEquals("kerberos", hc.get("hadoop.security.authentication"));
        Assertions.assertEquals("ak", hc.get("fs.s3a.access-key"));
    }

    @Test
    public void buildHmsHiveConfOverlaysStorageHadoopConfig() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(
                props("uri", "thrift://nn:9083"),
                Collections.emptyMap(),
                storage("fs.s3a.access.key", "ak", "fs.s3a.endpoint", "s3.amazonaws.com"));

        // WHY (P1-T03): the HMS HiveConf must carry the pre-computed object-store storage config so the
        // live HiveCatalog can read warehouse data files over S3. MUTATION: not overlaying
        // storageHadoopConfig (fs.s3a.access.key null) -> red.
        Assertions.assertEquals("ak", hc.get("fs.s3a.access.key"));
        Assertions.assertEquals("s3.amazonaws.com", hc.get("fs.s3a.endpoint"));
        Assertions.assertEquals("thrift://nn:9083", hc.get("hive.metastore.uris"));
    }

    @Test
    public void buildHmsHiveConfKerberosSetsSaslServicePrincipalAndAuthToLocal() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.authentication.type", "kerberos",
                "hive.metastore.client.principal", "doris@REALM",
                "hive.metastore.client.keytab", "/etc/doris.keytab",
                "hive.metastore.service.principal", "hive/_HOST@REALM",
                "hadoop.security.auth_to_local", "RULE:[1:$1@$0](.*@REALM)s/@.*//"),
                Collections.emptyMap(), Collections.emptyMap());

        // WHY (I-2 parity gap): legacy HMSBaseProperties.initHadoopAuthenticator, when the metastore
        // auth type is kerberos, sets hive.metastore.sasl.enabled=true +
        // hadoop.security.authentication=kerberos (lines 160-167), promotes the SERVICE principal to
        // hive.metastore.kerberos.principal (sourced from hive.metastore.service.principal, lines
        // 153-155), and carries hadoop.security.auth_to_local (lines 156-159). Without SASL + the
        // service principal a live HiveMetaStoreClient cannot complete the GSSAPI handshake against a
        // kerberized HMS. MUTATION: dropping sasl.enabled, the service principal, auth_to_local, or
        // not forcing hadoop.security.authentication=kerberos -> red.
        Assertions.assertEquals("true", hc.get("hive.metastore.sasl.enabled"));
        Assertions.assertEquals("kerberos", hc.get("hadoop.security.authentication"));
        Assertions.assertEquals("hive/_HOST@REALM", hc.get("hive.metastore.kerberos.principal"));
        Assertions.assertEquals("RULE:[1:$1@$0](.*@REALM)s/@.*//", hc.get("hadoop.security.auth_to_local"));
    }

    @Test
    public void buildHmsHiveConfKerberosAcceptsServicePrincipalAlias() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.authentication.type", "kerberos",
                "hive.metastore.client.principal", "doris@REALM",
                "hive.metastore.client.keytab", "/etc/doris.keytab",
                // alias: legacy @ConnectorProperty(names={"hive.metastore.service.principal",
                // "hive.metastore.kerberos.principal"}) — the bare kerberos.principal key is the
                // service-principal alias when service.principal is absent.
                "hive.metastore.kerberos.principal", "hive/_HOST@REALM"),
                Collections.emptyMap(), Collections.emptyMap());

        // WHY (I-2 alias parity): the service principal can arrive under either alias; the
        // hive.* verbatim copy already lands hive.metastore.kerberos.principal, but the alias
        // resolution must still treat it as the service principal source (and not get clobbered by a
        // blank service.principal). MUTATION: not reading the kerberos.principal alias as the service
        // principal -> red.
        Assertions.assertEquals("hive/_HOST@REALM", hc.get("hive.metastore.kerberos.principal"));
        Assertions.assertEquals("true", hc.get("hive.metastore.sasl.enabled"));
    }

    @Test
    public void buildHmsHiveConfKerberosSurvivesSimpleHdfsAuthPassthrough() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.authentication.type", "kerberos",
                "hive.metastore.client.principal", "doris@REALM",
                "hive.metastore.client.keytab", "/etc/doris.keytab",
                "hadoop.security.authentication", "simple"),
                Collections.emptyMap(), Collections.emptyMap());

        // WHY (pre-existing MAJOR, found by the FIX-FECONF impl review): legacy runs initHadoopAuthenticator
        // LAST, so a kerberized HMS forces hadoop.security.authentication=kerberos authoritatively even when
        // the HDFS namenode uses simple auth (a real kerberized-HMS + simple-HDFS deployment). The connector's
        // raw hadoop.* passthrough in applyStorageConfig re-copies the literal hadoop.security.authentication=
        // simple, so if the kerberos block runs BEFORE the overlay the forced "kerberos" is clobbered back to
        // "simple" while sasl.enabled stays "true" -> an inconsistent HiveConf that breaks the live GSSAPI
        // handshake. The kerberos block must therefore run AFTER applyStorageConfig. MUTATION: kerberos block
        // before the storage overlay -> hadoop.security.authentication clobbered to "simple" -> red.
        Assertions.assertEquals("kerberos", hc.get("hadoop.security.authentication"));
        Assertions.assertEquals("true", hc.get("hive.metastore.sasl.enabled"));
    }

    @Test
    public void buildHmsHiveConfKerberosSurvivesStorageOverlayAuthPassthrough() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.authentication.type", "kerberos",
                "hive.metastore.client.principal", "doris@REALM",
                "hive.metastore.client.keytab", "/etc/doris.keytab"),
                Collections.emptyMap(),
                // a storage map that carries a hadoop.security.authentication must NOT clobber the
                // forced kerberos auth.
                storage("hadoop.security.authentication", "simple"));

        // WHY (P1-T03 ordering invariant, sibling to ...SurvivesSimpleHdfsAuthPassthrough): P1-T03 moved
        // the object-store config source to the storageHadoopConfig map, which applyStorageConfig applies
        // BEFORE the kerberos-conditional block (the same position the old fe-property canonical map held).
        // So a hadoop.security.authentication arriving via the STORAGE MAP (not just the raw props
        // passthrough) must still be overridden to kerberos — proving the kerberos block runs after the
        // storage overlay regardless of which source set the key. MUTATION: applying storageHadoopConfig
        // AFTER the kerberos block (or dropping the force) -> "simple" wins -> red.
        Assertions.assertEquals("kerberos", hc.get("hadoop.security.authentication"));
        Assertions.assertEquals("true", hc.get("hive.metastore.sasl.enabled"));
    }

    @Test
    public void buildHmsHiveConfSimpleDoesNotEnableSasl() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.authentication.type", "simple"),
                Collections.emptyMap(), Collections.emptyMap());

        // WHY (I-2 negative parity): legacy only enables SASL on the kerberos branch; a simple
        // (non-kerberized) HMS must NOT advertise sasl.enabled=true or it would attempt a GSSAPI
        // handshake against a plaintext metastore and fail. (HiveConf carries a baked-in default of
        // "false", so the invariant is "not true", not "absent" — legacy likewise never sets it to
        // true on the simple path.) MUTATION: unconditionally setting sasl.enabled=true regardless of
        // auth type -> red.
        Assertions.assertNotEquals("true", hc.get("hive.metastore.sasl.enabled"),
                "simple-auth HMS must not enable metastore SASL");
    }

    @Test
    public void buildHmsHiveConfSetsClientSocketTimeoutDefault() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(
                props("uri", "thrift://nn:9083"), Collections.emptyMap(), Collections.emptyMap());

        // WHY (I-2): legacy checkAndInit defaults the metastore client socket timeout to
        // Config.hive_metastore_client_timeout_second (=10) when the user has not overridden it
        // (lines 204-208), so a hung metastore does not block CREATE CATALOG forever. MUTATION:
        // dropping the default timeout -> red.
        Assertions.assertEquals("10", hc.get("hive.metastore.client.socket.timeout"));
    }

    // ---------------------------------------------------------------------
    // requireOssStorageForDlf — OSS-only gate (legacy OSS||OSS_HDFS, NOT generic S3)
    // ---------------------------------------------------------------------

    @Test
    public void requireOssStorageForDlfRejectsS3OnlyConfig() {
        // WHY (I-1 parity): legacy PaimonAliyunDLFMetaStoreProperties.initializeCatalog required a
        // StorageProperties of Type.OSS || OSS_HDFS specifically — a generic S3 backend is NOT
        // accepted. A DLF catalog configured with only s3.* keys (no oss) must be rejected as
        // misconfigured, with the exact legacy message. MUTATION: loosening the gate to also accept
        // s3 prefixes (so an s3-only DLF catalog passes) -> red.
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> PaimonCatalogFactory.requireOssStorageForDlf(props(
                        "s3.access-key", "ak",
                        "fs.s3a.endpoint", "s3.amazonaws.com",
                        "paimon.s3.secret-key", "sk")));
        Assertions.assertEquals("Paimon DLF metastore requires OSS storage properties.", ex.getMessage());
    }

    @Test
    public void requireOssStorageForDlfAcceptsOssConfig() {
        // WHY (I-1 parity): an OSS-backed DLF catalog is the supported case; the gate must pass when
        // any oss./fs.oss./paimon.fs.oss. key is present. MUTATION: a gate that rejects a valid
        // OSS-backed DLF catalog -> red.
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.requireOssStorageForDlf(props(
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com")));
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.requireOssStorageForDlf(props(
                "fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com")));
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.requireOssStorageForDlf(props(
                "paimon.fs.oss.access-key", "oss-ak")));
    }

    // ---------------------------------------------------------------------
    // buildDlfHiveConf — 8 dlf.catalog.* keys + endpoint-from-region + uid fallback + throw + storage
    // ---------------------------------------------------------------------

    @Test
    public void buildDlfHiveConfSetsAllEightDlfKeysAndOverlaysStorage() {
        HiveConf hc = PaimonCatalogFactory.buildDlfHiveConf(props(
                "dlf.access_key", "ak",
                "dlf.secret_key", "sk",
                "dlf.session_token", "tok",
                "dlf.region", "cn-hangzhou",
                "dlf.endpoint", "dlf.cn-hangzhou.aliyuncs.com",
                "dlf.catalog.uid", "uid-1",
                "dlf.catalog.id", "cat-1",
                "dlf.catalog.proxyMode", "DLF_ONLY",
                "paimon.fs.oss.access-key", "oss-ak"), Collections.emptyMap());

        // WHY: DLF is adapted onto a HiveCatalog via the ProxyMetaStoreClient, which reads the eight
        // DataLakeConfig.CATALOG_* keys from the HiveConf; all eight must be present with the
        // verified literal key names, plus the connector storage overlay (here the paimon.fs.oss.*
        // re-key onto fs.s3a.). MUTATION: a wrong/missing dlf.catalog.* key name, or not applying the
        // connector storage overlay -> red.
        Assertions.assertEquals("ak", hc.get("dlf.catalog.accessKeyId"));
        Assertions.assertEquals("sk", hc.get("dlf.catalog.accessKeySecret"));
        Assertions.assertEquals("tok", hc.get("dlf.catalog.securityToken"));
        Assertions.assertEquals("cn-hangzhou", hc.get("dlf.catalog.region"));
        Assertions.assertEquals("dlf.cn-hangzhou.aliyuncs.com", hc.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("uid-1", hc.get("dlf.catalog.uid"));
        Assertions.assertEquals("cat-1", hc.get("dlf.catalog.id"));
        Assertions.assertEquals("DLF_ONLY", hc.get("dlf.catalog.proxyMode"));
        Assertions.assertEquals("oss-ak", hc.get("fs.s3a.access-key"));
    }

    @Test
    public void buildDlfHiveConfOverlaysStorageHadoopConfig() {
        HiveConf hc = PaimonCatalogFactory.buildDlfHiveConf(
                props("dlf.access_key", "ak",
                        "dlf.secret_key", "sk",
                        "dlf.endpoint", "dlf.cn-hangzhou.aliyuncs.com"),
                storage("fs.oss.accessKeyId", "oak",
                        "fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem"));

        // WHY (P1-T03): the DLF HiveConf must carry the pre-computed OSS storage config (Jindo fs.oss.*)
        // from fe-filesystem so the ProxyMetaStoreClient/FileIO can read OSS data files, while the
        // dlf.catalog.* metastore keys stay present. MUTATION: not overlaying storageHadoopConfig
        // (fs.oss.accessKeyId null) -> red.
        Assertions.assertEquals("oak", hc.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem", hc.get("fs.oss.impl"));
        Assertions.assertEquals("ak", hc.get("dlf.catalog.accessKeyId"));
    }

    @Test
    public void buildDlfHiveConfDerivesVpcEndpointFromRegionByDefault() {
        HiveConf hc = PaimonCatalogFactory.buildDlfHiveConf(props(
                "dlf.access_key", "ak",
                "dlf.secret_key", "sk",
                "dlf.region", "cn-beijing",
                "dlf.catalog.uid", "uid-1"), Collections.emptyMap());

        // WHY: legacy checkAndInit derives the endpoint from the region when the endpoint is blank;
        // the DEFAULT (accessPublic=false) is the VPC endpoint. MUTATION: deriving the public
        // endpoint by default, or not deriving at all -> red.
        Assertions.assertEquals("dlf-vpc.cn-beijing.aliyuncs.com", hc.get("dlf.catalog.endpoint"));
    }

    @Test
    public void buildDlfHiveConfDerivesPublicEndpointWhenAccessPublic() {
        HiveConf hc = PaimonCatalogFactory.buildDlfHiveConf(props(
                "dlf.access_key", "ak",
                "dlf.secret_key", "sk",
                "dlf.region", "cn-beijing",
                "dlf.access.public", "true",
                "dlf.catalog.uid", "uid-1"), Collections.emptyMap());

        // WHY: when dlf.access.public is truthy the public endpoint (dlf.<region>...) is used instead
        // of the VPC one. MUTATION: ignoring accessPublic (still deriving the vpc endpoint) -> red.
        Assertions.assertEquals("dlf.cn-beijing.aliyuncs.com", hc.get("dlf.catalog.endpoint"));
    }

    @Test
    public void buildDlfHiveConfFallsBackCatalogIdToUid() {
        HiveConf hc = PaimonCatalogFactory.buildDlfHiveConf(props(
                "dlf.access_key", "ak",
                "dlf.secret_key", "sk",
                "dlf.endpoint", "dlf.cn-hangzhou.aliyuncs.com",
                "dlf.catalog.uid", "uid-42"), Collections.emptyMap());

        // WHY: legacy checkAndInit defaults the catalog id to the uid when no explicit catalog id is
        // given (the DLF account's default catalog is keyed by uid). MUTATION: leaving the catalog
        // id blank instead of falling back to uid -> red.
        Assertions.assertEquals("uid-42", hc.get("dlf.catalog.id"));
    }

    @Test
    public void buildDlfHiveConfThrowsWhenEndpointAndRegionBlank() {
        // WHY: legacy checkAndInit throws "dlf.endpoint is required." when neither an endpoint nor a
        // region (to derive one) is given — the DLF client cannot connect without it. MUTATION:
        // removing the throw (returning a HiveConf with a blank endpoint) -> red.
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> PaimonCatalogFactory.buildDlfHiveConf(props(
                        "dlf.access_key", "ak",
                        "dlf.secret_key", "sk",
                        "dlf.catalog.uid", "uid-1"), Collections.emptyMap()));
        Assertions.assertTrue(ex.getMessage().contains("dlf.endpoint"));
    }

    // ---------------------------------------------------------------------
    // FIX-HMS-CONFRES — buildHmsHiveConf(props, hiveConfResources, storage) base-merge
    // ---------------------------------------------------------------------

    @Test
    public void buildHmsHiveConfOverlaysResolvedHiveConfResourcesAsBase() {
        Map<String, String> fileKeys = new HashMap<>();
        fileKeys.put("hive.metastore.sasl.qop", "auth-conf");
        fileKeys.put("hive.metastore.thrift.transport", "custom");
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(
                props("uri", "thrift://nn:9083"), fileKeys, Collections.emptyMap());

        // WHY (MAJOR, Finding §8): connection-critical keys present ONLY in the external hive-site.xml
        // (hive.conf.resources) must reach the catalog HiveConf — before the fix buildHmsHiveConf
        // built the conf from the raw prop map only and dropped the file entirely. MUTATION: dropping
        // the file-keys base merge (today's behavior) -> these keys absent -> red.
        Assertions.assertEquals("auth-conf", hc.get("hive.metastore.sasl.qop"));
        Assertions.assertEquals("custom", hc.get("hive.metastore.thrift.transport"));
        Assertions.assertEquals("thrift://nn:9083", hc.get("hive.metastore.uris"));
    }

    @Test
    public void buildHmsHiveConfUserHivePropOverridesFileResource() {
        // A non-uri hive.* key avoids the separate uri-alias resolution (HMS_URI), isolating the
        // file-base vs user-hive.* precedence under test.
        Map<String, String> fileKeys = new HashMap<>();
        fileKeys.put("hive.metastore.sasl.qop", "FILE-qop");
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.sasl.qop", "USER-qop"), fileKeys, Collections.emptyMap());

        // WHY: legacy precedence is file=base, user hive.* WINS. This can only pass if the file map is
        // applied FIRST (as the base), then overridden by the verbatim user hive.* copy. MUTATION:
        // applying the file map AFTER the user keys -> the file value "FILE-qop" wins -> red.
        Assertions.assertEquals("USER-qop", hc.get("hive.metastore.sasl.qop"),
                "a user hive.* prop must override the same key from the file base");
    }

    // ---------------------------------------------------------------------
    // FIX-FECONF-STORAGE-PARITY — HMS username alias (P8-4)
    // ---------------------------------------------------------------------

    @Test
    public void buildHmsHiveConfResolvesUsernameFromHiveMetastoreUsernameAlias() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.username", "hms-user"), Collections.emptyMap(), Collections.emptyMap());

        // WHY (P8-4): legacy HMSBaseProperties binds the username from {hive.metastore.username, hadoop.username}
        // and sets HADOOP_USER_NAME (= "hadoop.username"). Before the fix the connector only copied the literal
        // hadoop.username, so a user who set ONLY hive.metastore.username had it land as an inert verbatim hive.*
        // key and never reach hadoop.username (the UGI key). MUTATION: dropping the alias resolution -> null -> red.
        Assertions.assertEquals("hms-user", hc.get("hadoop.username"));
    }

    @Test
    public void buildHmsHiveConfUsernameAliasPriorityHiveMetastoreWins() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.username", "primary",
                "hadoop.username", "secondary"), Collections.emptyMap(), Collections.emptyMap());

        // WHY: legacy alias order lists hive.metastore.username FIRST, so it wins when both are set.
        // MUTATION: reversing the priority (hadoop.username wins) -> red.
        Assertions.assertEquals("primary", hc.get("hadoop.username"));
    }
}
