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

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link PaimonCatalogFactory}, the pure flavor-assembly core.
 *
 * <p>These tests are entirely offline: {@code buildCatalogOptions} is a pure transform
 * (Map in, Paimon {@link Options} out) and {@code validate} is fail-fast pre-flight, so no
 * live catalog, hadoop config, or env is touched. No Mockito — props are plain maps.
 *
 * <p>This is the parity baseline for B1: the per-flavor option keys MUST mirror the legacy
 * fe-core {@code AbstractPaimonProperties} + each {@code Paimon*MetaStoreProperties}.
 */
public class PaimonCatalogFactoryTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
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
    // buildHadoopConfiguration — S3 prefix normalization + raw fs./dfs. passthrough
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
                "paimon.read.batch-size", "4096"));

        // WHY: the live FileIO/S3FileIO only recognizes Hadoop-prefixed keys; the legacy
        // normalizeS3Config strips each of the four user storage prefixes and re-keys them under
        // fs.s3a., while genuine fs.*/dfs./hadoop.* keys are passed through verbatim so HDFS/auth
        // config reaches the catalog. MUTATION: not normalizing to fs.s3a. (key still under the old
        // prefix), or dropping the raw fs./dfs./hadoop. passthrough -> red.
        Assertions.assertEquals("ak", conf.get("fs.s3a.access-key"));
        Assertions.assertEquals("sk", conf.get("fs.s3a.secret-key"));
        Assertions.assertEquals("s3.amazonaws.com", conf.get("fs.s3a.endpoint"));
        // paimon.fs.oss.* also normalizes onto the fs.s3a. prefix (legacy behavior: all four
        // userStoragePrefixes map to FS_S3A_PREFIX). Distinct suffix to avoid colliding with the
        // paimon.fs.s3.endpoint above (HashMap iteration order is not guaranteed).
        Assertions.assertEquals("oss-cn.aliyuncs.com", conf.get("fs.s3a.endpoint.region"));
        Assertions.assertEquals("hdfs://nn:8020", conf.get("fs.defaultFS"));
        Assertions.assertEquals("nn", conf.get("dfs.nameservices"));
        Assertions.assertEquals("kerberos", conf.get("hadoop.security.authentication"));
        // A non-storage paimon.* key (a catalog Option) must NOT leak into the Hadoop Configuration.
        Assertions.assertNull(conf.get("paimon.read.batch-size"));
        Assertions.assertNull(conf.get("read.batch-size"));
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
                "paimon.s3.access-key", "ak"));

        // WHY: a live HiveCatalog reads the metastore uri from the HiveConf, honors any user hive.*
        // override, and needs the auth keys (alongside the FE-injected UGI) plus the storage config
        // to reach the warehouse. The "uri" alias must resolve to hive.metastore.uris. MUTATION:
        // missing metastore uri, dropping a hive.* override, dropping an auth key, or not overlaying
        // the normalized storage config -> red.
        Assertions.assertEquals("thrift://nn:9083", hc.get("hive.metastore.uris"));
        Assertions.assertEquals("true", hc.get("hive.metastore.sasl.enabled"));
        Assertions.assertEquals("doris@REALM", hc.get("hive.metastore.client.principal"));
        Assertions.assertEquals("/etc/doris.keytab", hc.get("hive.metastore.client.keytab"));
        Assertions.assertEquals("kerberos", hc.get("hadoop.security.authentication"));
        Assertions.assertEquals("ak", hc.get("fs.s3a.access-key"));
    }

    @Test
    public void buildHmsHiveConfKerberosSetsSaslServicePrincipalAndAuthToLocal() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.authentication.type", "kerberos",
                "hive.metastore.client.principal", "doris@REALM",
                "hive.metastore.client.keytab", "/etc/doris.keytab",
                "hive.metastore.service.principal", "hive/_HOST@REALM",
                "hadoop.security.auth_to_local", "RULE:[1:$1@$0](.*@REALM)s/@.*//"));

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
                "hive.metastore.kerberos.principal", "hive/_HOST@REALM"));

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
                "hadoop.security.authentication", "simple"));

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
    public void buildHmsHiveConfSimpleDoesNotEnableSasl() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.authentication.type", "simple"));

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
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props("uri", "thrift://nn:9083"));

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
    // buildDlfHiveConf — 8 dlf.catalog.* keys + endpoint-from-region + uid fallback + throw
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
                "paimon.fs.oss.access-key", "oss-ak"));

        // WHY: DLF is adapted onto a HiveCatalog via the ProxyMetaStoreClient, which reads the eight
        // DataLakeConfig.CATALOG_* keys from the HiveConf; all eight must be present with the
        // verified literal key names, plus the OSS storage overlay. MUTATION: a wrong/missing
        // dlf.catalog.* key name, or not overlaying the storage config -> red.
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
    public void buildDlfHiveConfDerivesVpcEndpointFromRegionByDefault() {
        HiveConf hc = PaimonCatalogFactory.buildDlfHiveConf(props(
                "dlf.access_key", "ak",
                "dlf.secret_key", "sk",
                "dlf.region", "cn-beijing",
                "dlf.catalog.uid", "uid-1"));

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
                "dlf.catalog.uid", "uid-1"));

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
                "dlf.catalog.uid", "uid-42"));

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
                        "dlf.catalog.uid", "uid-1")));
        Assertions.assertTrue(ex.getMessage().contains("dlf.endpoint"));
    }

    // ---------------------------------------------------------------------
    // FIX-STORAGE-CREDS — canonical s3.*/oss.*/AWS_* alias translation
    // (ported legacy appendS3HdfsProperties + OSSProperties.initializeHadoopStorageConfig)
    // ---------------------------------------------------------------------

    @Test
    public void buildHadoopConfigurationTranslatesCanonicalS3Credentials() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "s3.access_key", "ak",
                "s3.secret_key", "sk",
                "s3.endpoint", "s3.ap-east-1.amazonaws.com"));

        // WHY (BLOCKER, Finding 9.1): a filesystem catalog created with the DOCUMENTED canonical
        // keys (the same ones test_paimon_s3.groovy passes) must reach the S3 FileIO with real
        // credentials. Before the fix applyStorageConfig recognized only paimon.*/raw fs.* keys, so
        // s3.access_key/s3.secret_key/s3.endpoint were SILENTLY DROPPED and the Paimon FileSystem
        // catalog hit S3 anonymously -> access-denied at plan time. MUTATION: dropping the canonical
        // s3.* translation (today's behavior) leaves fs.s3a.access.key null -> red.
        Assertions.assertEquals("ak", conf.get("fs.s3a.access.key"));
        Assertions.assertEquals("sk", conf.get("fs.s3a.secret.key"));
        Assertions.assertEquals("s3.ap-east-1.amazonaws.com", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                conf.get("fs.s3a.aws.credentials.provider"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"));
        Assertions.assertEquals("true", conf.get("fs.s3.impl.disable.cache"));
    }

    @Test
    public void buildHadoopConfigurationTranslatesAwsEnvAliases() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "AWS_ACCESS_KEY", "ak",
                "AWS_SECRET_KEY", "sk",
                "AWS_TOKEN", "tok",
                "AWS_ENDPOINT", "s3.amazonaws.com",
                "AWS_REGION", "us-east-1"));

        // WHY: legacy accepted the AWS_* alias family (S3Properties @ConnectorProperty names). This
        // verifies the alias priority list resolves them (not just the primary s3.* key), including
        // the session token and endpoint region. MUTATION: dropping any AWS_* alias -> red.
        Assertions.assertEquals("ak", conf.get("fs.s3a.access.key"));
        Assertions.assertEquals("sk", conf.get("fs.s3a.secret.key"));
        Assertions.assertEquals("tok", conf.get("fs.s3a.session.token"));
        Assertions.assertEquals("s3.amazonaws.com", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("us-east-1", conf.get("fs.s3a.endpoint.region"));
    }

    @Test
    public void buildHadoopConfigurationDoesNotEmitCredsProviderForAnonymousBucket() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "s3.endpoint", "s3.amazonaws.com",
                "s3.region", "us-east-1"));

        // WHY (parity): legacy guards the credentials provider + access/secret keys behind
        // isNotBlank(accessKey), so a public/anonymous bucket (endpoint/region but no keys) still
        // gets fs.s3.impl + endpoint but is NOT forced onto our single SimpleAWSCredentialsProvider
        // override (which would break the env/IAM fallback chain). access.key has no Hadoop default
        // so it stays null; the provider key DOES have a Hadoop default chain, so the meaningful
        // check is that we did not override it to Simple-only. MUTATION: emitting the provider or a
        // blank access key unconditionally -> red (would force credentialed auth on a public bucket).
        Assertions.assertEquals("s3.amazonaws.com", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("us-east-1", conf.get("fs.s3a.endpoint.region"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"));
        Assertions.assertNull(conf.get("fs.s3a.access.key"));
        Assertions.assertNotEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                conf.get("fs.s3a.aws.credentials.provider"),
                "anonymous bucket must not be forced onto our Simple-only credentials provider");
    }

    @Test
    public void buildHadoopConfigurationExplicitFsS3aKeyOverridesCanonical() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "s3.access_key", "canon",
                "fs.s3a.access.key", "explicit"));

        // WHY: the raw fs.* passthrough runs AFTER the canonical translation (last-write-wins =
        // legacy addResource(getHadoopStorageConfig) THEN appendUserHadoopConfig ordering), so a
        // power user who explicitly set fs.s3a.access.key still wins over the canonical alias.
        // MUTATION: a refactor that reverses precedence (canonical overlays raw) -> "canon" -> red.
        Assertions.assertEquals("explicit", conf.get("fs.s3a.access.key"));
    }

    @Test
    public void buildDlfHiveConfTranslatesCanonicalOssCredentials() {
        HiveConf hc = PaimonCatalogFactory.buildDlfHiveConf(props(
                "dlf.access_key", "dak",
                "dlf.secret_key", "dsk",
                "dlf.endpoint", "dlf.cn-hangzhou.aliyuncs.com",
                "dlf.region", "cn-hangzhou",
                "oss.access_key", "oak",
                "oss.secret_key", "osk",
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "oss.region", "cn-hangzhou"));

        // WHY (BLOCKER, Finding 9.2): the DLF gate passes when an oss.* key is present, but before
        // the fix buildDlfHiveConf overlaid storage only through the old applyStorageConfig, which
        // dropped the canonical oss.access_key/oss.secret_key/oss.endpoint/oss.region -> the HiveConf
        // carried NO usable OSS FileIO config -> DLF/HMS catalog could not read OSS data files. The
        // dlf.catalog.* metastore keys must still be present AND the OSS (Jindo) storage keys set.
        // MUTATION: dropping the canonical OSS translation leaves fs.oss.accessKeyId null -> red.
        Assertions.assertEquals("dak", hc.get("dlf.catalog.accessKeyId"));
        Assertions.assertEquals("dlf.cn-hangzhou.aliyuncs.com", hc.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("oak", hc.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("osk", hc.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", hc.get("fs.oss.endpoint"));
        Assertions.assertEquals("cn-hangzhou", hc.get("fs.oss.region"));
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem", hc.get("fs.oss.impl"));
    }

    @Test
    public void requireOssStorageForDlfThenBuildDlfHiveConfYieldsOssCreds() {
        Map<String, String> p = props(
                "dlf.access_key", "dak",
                "dlf.secret_key", "dsk",
                "dlf.endpoint", "dlf.cn-hangzhou.aliyuncs.com",
                "oss.access_key", "oak",
                "oss.secret_key", "osk",
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");

        // WHY (BLOCKER end-to-end): the gate and the storage translation must agree on the SAME key
        // set. With canonical oss.* only (no paimon.fs.oss.*), the gate must pass AND the resulting
        // HiveConf must carry usable OSS credentials. Before the fix the gate passed but the conf had
        // no creds. MUTATION: gate/translation disagreeing on the oss.* key set -> red.
        Assertions.assertDoesNotThrow(() -> PaimonCatalogFactory.requireOssStorageForDlf(p));
        Assertions.assertEquals("oak", PaimonCatalogFactory.buildDlfHiveConf(p).get("fs.oss.accessKeyId"));
    }

    @Test
    public void buildDlfHiveConfDerivesOssEndpointFromRegion() {
        HiveConf vpc = PaimonCatalogFactory.buildDlfHiveConf(props(
                "dlf.access_key", "dak",
                "dlf.secret_key", "dsk",
                "dlf.endpoint", "dlf.cn-hangzhou.aliyuncs.com",
                "oss.region", "cn-hangzhou"));

        // WHY (DLF parity, Finding 9.2 completeness): DLF users typically pass a region, not an
        // explicit oss.endpoint. Legacy derived the OSS endpoint from the region via
        // OSSProperties.getOssEndpoint(region, accessPublic); the DEFAULT (non-public) is the
        // -internal endpoint. MUTATION: not deriving (fs.oss.endpoint null) or using the public form
        // by default -> red.
        Assertions.assertEquals("oss-cn-hangzhou-internal.aliyuncs.com", vpc.get("fs.oss.endpoint"));

        HiveConf pub = PaimonCatalogFactory.buildDlfHiveConf(props(
                "dlf.access_key", "dak",
                "dlf.secret_key", "dsk",
                "dlf.endpoint", "dlf.cn-hangzhou.aliyuncs.com",
                "oss.region", "cn-hangzhou",
                "dlf.access.public", "true"));

        // WHY: when access is public the endpoint has no -internal suffix. MUTATION: ignoring
        // accessPublic -> red.
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", pub.get("fs.oss.endpoint"));
    }

    // ---------------------------------------------------------------------
    // FIX-HMS-CONFRES — buildHmsHiveConf(props, hiveConfResources) base-merge
    // ---------------------------------------------------------------------

    @Test
    public void buildHmsHiveConfOverlaysResolvedHiveConfResourcesAsBase() {
        Map<String, String> fileKeys = new HashMap<>();
        fileKeys.put("hive.metastore.sasl.qop", "auth-conf");
        fileKeys.put("hive.metastore.thrift.transport", "custom");
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(
                props("uri", "thrift://nn:9083"), fileKeys);

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
                "hive.metastore.sasl.qop", "USER-qop"), fileKeys);

        // WHY: legacy precedence is file=base, user hive.* WINS. This can only pass if the file map is
        // applied FIRST (as the base), then overridden by the verbatim user hive.* copy. MUTATION:
        // applying the file map AFTER the user keys -> the file value "FILE-qop" wins -> red.
        Assertions.assertEquals("USER-qop", hc.get("hive.metastore.sasl.qop"),
                "a user hive.* prop must override the same key from the file base");
    }

    @Test
    public void buildHmsHiveConfSingleArgUsesEmptyResources() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props("uri", "thrift://nn:9083"));

        // WHY: the back-compat 1-arg overload must behave exactly as before (empty file resources),
        // so all existing callers/tests are unaffected. MUTATION: the 1-arg overload diverging from
        // the 2-arg-with-empty-map -> red.
        Assertions.assertEquals("thrift://nn:9083", hc.get("hive.metastore.uris"));
        Assertions.assertEquals("10", hc.get("hive.metastore.client.socket.timeout"));
    }

    // ---------------------------------------------------------------------
    // FIX-FECONF-STORAGE-PARITY — S3 endpoint-from-region + divergent tuning defaults + path-style
    // ---------------------------------------------------------------------

    @Test
    public void buildHadoopConfigurationDerivesS3EndpointFromRegion() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "s3.access_key", "ak",
                "s3.secret_key", "sk",
                "s3.region", "us-west-2"));

        // WHY (user-approved parity, same defect class as the OSS P8-1 fix): a region-only AWS S3 catalog
        // (no explicit endpoint) must still derive an endpoint so the FE Paimon FileIO can resolve it; legacy
        // S3Properties.getEndpointFromRegion returns https://s3.<region>.amazonaws.com. MUTATION: dropping the
        // derivation leaves fs.s3a.endpoint null while fs.s3a.endpoint.region is set -> red.
        Assertions.assertEquals("https://s3.us-west-2.amazonaws.com", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("us-west-2", conf.get("fs.s3a.endpoint.region"));
    }

    @Test
    public void buildHadoopConfigurationEmitsS3TuningDefaults() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "s3.endpoint", "s3.amazonaws.com",
                "s3.region", "us-east-1"));

        // WHY (BLOCKER-class parity, caught only by the completeness critic): legacy appendS3HdfsProperties
        // ALWAYS emits the 4 tuning keys, and the AWS-S3 field DEFAULTS are 50/3000/1000 (S3Properties),
        // NOT the 100/10000/10000 the object stores use. A single shared default would silently mis-tune
        // every AWS S3 paimon catalog. MUTATION: emitting no tuning keys (today) or the 100/10000/10000
        // object-store values for the S3 path -> red.
        Assertions.assertEquals("50", conf.get("fs.s3a.connection.maximum"));
        Assertions.assertEquals("3000", conf.get("fs.s3a.connection.request.timeout"));
        Assertions.assertEquals("1000", conf.get("fs.s3a.connection.timeout"));
        Assertions.assertEquals("false", conf.get("fs.s3a.path.style.access"));
    }

    @Test
    public void buildHadoopConfigurationEmitsS3PathStyleFromAlias() {
        Configuration pathStyle = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "s3.endpoint", "minio:9000",
                "s3.region", "us-east-1",
                "use_path_style", "true"));
        Configuration s3Alias = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "s3.endpoint", "minio:9000",
                "s3.region", "us-east-1",
                "s3.path-style-access", "true"));

        // WHY (P8-2/P9-3, MinIO/path-style): fs.s3a.path.style.access must be derived from either the
        // use_path_style or s3.path-style-access alias; before the fix it was never emitted, so a MinIO /
        // path-style bucket was hit virtual-hosted-style and failed. MUTATION: not reading the alias (always
        // false) -> red.
        Assertions.assertEquals("true", pathStyle.get("fs.s3a.path.style.access"));
        Assertions.assertEquals("true", s3Alias.get("fs.s3a.path.style.access"));
    }

    // ---------------------------------------------------------------------
    // FIX-FECONF-STORAGE-PARITY — OSS endpoint-from-region (filesystem + hms) + S3A base
    // ---------------------------------------------------------------------

    @Test
    public void buildHadoopConfigurationDerivesOssEndpointFromRegion() {
        Configuration internal = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "oss.access_key", "ak",
                "oss.secret_key", "sk",
                "oss.region", "cn-hangzhou"));

        // WHY (P8-1/P8-3): a filesystem-flavor OSS catalog with only a region (no explicit oss.endpoint) must
        // derive the OSS endpoint, mirroring legacy OSSProperties.getOssEndpoint(region, dlfAccessPublic). The
        // DEFAULT (dlfAccessPublic=false) is the -internal endpoint. Before the fix the derivation lived only
        // in the DLF flavor, so a filesystem OSS catalog got fs.oss.endpoint=null -> FileIO could not resolve.
        // MUTATION: no derivation for the filesystem path, or deriving the public form by default -> red.
        Assertions.assertEquals("oss-cn-hangzhou-internal.aliyuncs.com", internal.get("fs.oss.endpoint"));

        Configuration pub = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "oss.access_key", "ak",
                "oss.secret_key", "sk",
                "oss.region", "cn-hangzhou",
                "dlf.access.public", "true"));
        // WHY: dlf.access.public=true selects the public (no -internal) form, even for a filesystem OSS
        // catalog. MUTATION: ignoring the public flag on the shared OSS path -> red.
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", pub.get("fs.oss.endpoint"));
    }

    @Test
    public void buildHmsHiveConfDerivesOssEndpointFromRegion() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "oss.access_key", "ak",
                "oss.secret_key", "sk",
                "oss.region", "cn-shanghai"));

        // WHY (parity completeness, RT-skeptic-3): moving the OSS endpoint-from-region derivation into the
        // shared applyCanonicalOssConfig also grants the HMS flavor the same legacy OSSProperties.of()
        // derivation it always had via fe-core. MUTATION: deriving only on the filesystem/dlf paths and not
        // when applyStorageConfig is overlaid onto a HiveConf -> red.
        Assertions.assertEquals("oss-cn-shanghai-internal.aliyuncs.com", hc.get("fs.oss.endpoint"));
    }

    @Test
    public void buildHadoopConfigurationEmitsS3aBaseForOssCatalog() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "oss.access_key", "oss-ak",
                "oss.secret_key", "oss-sk",
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com"));

        // WHY (P8-1/P8-3, RT-skeptic-1/4): legacy OSS inherits the full S3A base via super.appendS3HdfsProperties
        // (for s3://-over-OSS back-compat); before the fix applyCanonicalOssConfig emitted ONLY Jindo fs.oss.*
        // keys. The S3A base must carry the OSS-resolved endpoint/creds (NOT re-resolved from s3.* aliases) and
        // the OSS tuning default (100, NOT the S3 50). MUTATION: OSS block skipping the S3A base (fs.s3a.impl
        // null), or emitting the S3 tuning default -> red.
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"));
        Assertions.assertEquals("oss-ak", conf.get("fs.s3a.access.key"));
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("100", conf.get("fs.s3a.connection.maximum"));
        // The Jindo OSS keys remain (unchanged behavior).
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem", conf.get("fs.oss.impl"));
        Assertions.assertEquals("oss-ak", conf.get("fs.oss.accessKeyId"));
    }

    // ---------------------------------------------------------------------
    // FIX-FECONF-STORAGE-PARITY — COS / OBS (P9-2)
    // ---------------------------------------------------------------------

    @Test
    public void buildHadoopConfigurationEmitsCosKeysForCosCatalog() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "cosn://bucket/wh",
                "cos.access_key", "cak",
                "cos.secret_key", "csk",
                "cos.endpoint", "cos.ap-beijing.myqcloud.com"));

        // WHY (P9-2): a cosn:// paimon catalog needs the Tencent COS FileSystem impl + cosn credentials; before
        // the fix there was NO COS handling at all. fs.cosn.impl=S3AFileSystem makes cosn:// an S3A instance, so
        // the S3A base (endpoint/creds, resolved from the cos.* aliases) is ALSO load-bearing. MUTATION: no COS
        // block (fs.cosn.impl null), or not threading the cos.* creds into the S3A base -> red.
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.cosn.impl"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.cos.impl"));
        Assertions.assertEquals("cak", conf.get("fs.cosn.userinfo.secretId"));
        Assertions.assertEquals("csk", conf.get("fs.cosn.userinfo.secretKey"));
        // S3A base carries the cos endpoint + creds + the object-store tuning default.
        Assertions.assertEquals("cos.ap-beijing.myqcloud.com", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("cak", conf.get("fs.s3a.access.key"));
        Assertions.assertEquals("100", conf.get("fs.s3a.connection.maximum"));
    }

    @Test
    public void buildHadoopConfigurationDetectsCosByEndpointPattern() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "cosn://bucket/wh",
                "s3.access_key", "ak",
                "s3.secret_key", "sk",
                "s3.endpoint", "cos.ap-beijing.myqcloud.com"));

        // WHY (RT-skeptic-2, the framing fix): legacy detects COS by ENDPOINT PATTERN (myqcloud.com), NOT by a
        // cos.* key. A cosn:// catalog configured with only s3.* keys + an s3.endpoint pointing at a myqcloud
        // endpoint must STILL get fs.cosn.impl (a cos.*-key-only gate would miss it and the cosn:// warehouse
        // would have no COS FileSystem impl). MUTATION: gating COS only on cos.* keys -> fs.cosn.impl null -> red.
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.cosn.impl"));
        Assertions.assertEquals("ak", conf.get("fs.cosn.userinfo.secretId"));
    }

    @Test
    public void buildHadoopConfigurationEmitsCosRegionUnconditionally() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "cosn://bucket/wh",
                "cos.access_key", "cak",
                "cos.secret_key", "csk",
                "cos.endpoint", "cos.ap-beijing.myqcloud.com"));

        // WHY: COSProperties writes fs.cosn.bucket.region UNCONDITIONALLY (always emitted, never absent). After the
        // migration to the shared fe-property COSProperties, the region is DERIVED from the
        // cos.<region>.myqcloud.com endpoint (faithful to legacy COSProperties.endpointPatterns) — so a cosn
        // catalog with an endpoint but no explicit cos.region now gets the endpoint-derived region instead of the
        // old hand-port's blank value. MUTATION: not emitting fs.cosn.bucket.region -> red.
        Assertions.assertEquals("ap-beijing", conf.get("fs.cosn.bucket.region"));
    }

    @Test
    public void buildHadoopConfigurationEmitsObsKeysForObsCatalog() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "obs://bucket/wh",
                "obs.access_key", "oak",
                "obs.secret_key", "osk",
                "obs.endpoint", "obs.cn-north-4.myhuaweicloud.com"));

        // WHY (P9-2): an obs:// paimon catalog needs the Huawei OBS FileSystem impl + obs credentials; before
        // the fix there was NO OBS handling. The impl is native OBSFileSystem when classpath-available, else the
        // S3A fallback (classpath-dependent, so accept either), but the creds/endpoint are load-bearing.
        // MUTATION: no OBS block (fs.obs.access.key null) -> red.
        String obsImpl = conf.get("fs.obs.impl");
        Assertions.assertTrue("org.apache.hadoop.fs.obs.OBSFileSystem".equals(obsImpl)
                        || "org.apache.hadoop.fs.s3a.S3AFileSystem".equals(obsImpl),
                "fs.obs.impl must be the native OBS impl or the S3A fallback, got: " + obsImpl);
        Assertions.assertEquals("oak", conf.get("fs.obs.access.key"));
        Assertions.assertEquals("osk", conf.get("fs.obs.secret.key"));
        Assertions.assertEquals("obs.cn-north-4.myhuaweicloud.com", conf.get("fs.obs.endpoint"));
    }

    @Test
    public void buildHadoopConfigurationDetectsObsByEndpointPattern() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "obs://bucket/wh",
                "s3.access_key", "ak",
                "s3.secret_key", "sk",
                "s3.endpoint", "obs.cn-north-4.myhuaweicloud.com"));

        // WHY (RT-skeptic-2): legacy detects OBS by the myhuaweicloud.com endpoint pattern, not an obs.* key.
        // An obs:// catalog with only s3.* keys must still get fs.obs.*. MUTATION: obs.*-key-only gate -> red.
        Assertions.assertNotNull(conf.get("fs.obs.impl"));
        Assertions.assertEquals("ak", conf.get("fs.obs.access.key"));
    }

    @Test
    public void buildHadoopConfigurationDoesNotEmitCosOrObsForPlainS3() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "s3://bucket/wh",
                "s3.access_key", "ak",
                "s3.secret_key", "sk",
                "s3.endpoint", "s3.us-east-1.amazonaws.com"));

        // WHY (RT-skeptic-2, negative parity): a plain AWS S3 catalog (no cos./obs. key, no myqcloud/
        // myhuaweicloud endpoint) must NOT trigger the COS or OBS blocks. MUTATION: a detection gate that
        // fires COS/OBS on shared s3.* keys -> fs.cosn.impl/fs.obs.impl emitted for a pure-S3 catalog -> red.
        Assertions.assertNull(conf.get("fs.cosn.impl"));
        Assertions.assertNull(conf.get("fs.obs.impl"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"));
    }

    // ---------------------------------------------------------------------
    // FIX-PAIMON-MINIO-STORAGE — canonical minio.* alias translation
    // (ported legacy MinioProperties: S3A-compatible, schema "s3")
    // ---------------------------------------------------------------------

    @Test
    public void buildHadoopConfigurationTranslatesCanonicalMinioCredentials() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "s3://warehouse/wh",
                "minio.endpoint", "http://10.0.0.1:9000",
                "minio.access_key", "admin",
                "minio.secret_key", "password"));

        // WHY (the reported bug): a filesystem paimon catalog created with the documented minio.* keys
        // (test_paimon_minio.groovy) must reach the S3A FileIO over s3://. Before this fix applyStorageConfig
        // recognized only s3.*/oss.*/cos.*/obs.*/raw fs.* keys, so EVERY minio.* alias resolved null,
        // applyCanonicalS3Config early-returned, and fs.s3.impl was never set -> Paimon FileIO.get threw
        // "Could not find a file io implementation for scheme 's3'". The load-bearing assertion is fs.s3.impl
        // (the missing registration); the credentials/endpoint follow from the same S3A base. MUTATION:
        // dropping the minio block leaves fs.s3.impl / fs.s3a.access.key null -> red.
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3.impl"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"));
        Assertions.assertEquals("http://10.0.0.1:9000", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("admin", conf.get("fs.s3a.access.key"));
        Assertions.assertEquals("password", conf.get("fs.s3a.secret.key"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                conf.get("fs.s3a.aws.credentials.provider"));
    }

    @Test
    public void buildHadoopConfigurationMinioDefaultsRegionAndObjectStoreTuning() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "s3://warehouse/wh",
                "minio.endpoint", "http://10.0.0.1:9000",
                "minio.access_key", "admin",
                "minio.secret_key", "password"));

        // WHY (parity with legacy MinioProperties defaults): MinioProperties defaults region to us-east-1 and
        // the connection tuning to 100/10000/10000 (NOT the S3Properties 50/3000/1000). A dedicated MinIO block
        // is required precisely so these defaults are not silently taken from the S3 block. MUTATION: routing
        // minio.* through the S3 block's defaults -> region absent + maxConn 50 -> red.
        Assertions.assertEquals("us-east-1", conf.get("fs.s3a.endpoint.region"));
        Assertions.assertEquals("100", conf.get("fs.s3a.connection.maximum"));
        Assertions.assertEquals("10000", conf.get("fs.s3a.connection.request.timeout"));
        Assertions.assertEquals("10000", conf.get("fs.s3a.connection.timeout"));
    }

    @Test
    public void buildHadoopConfigurationMinioExplicitRegionWins() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "s3://warehouse/wh",
                "minio.endpoint", "http://10.0.0.1:9000",
                "minio.access_key", "admin",
                "minio.secret_key", "password",
                "minio.region", "us-west-2"));

        // WHY: an explicit minio.region must override the us-east-1 default (the test_paimon_minio
        // *_with_region catalogs depend on this). MUTATION: hardcoding the default region -> red.
        Assertions.assertEquals("us-west-2", conf.get("fs.s3a.endpoint.region"));
    }

    @Test
    public void buildHadoopConfigurationPlainS3DoesNotTriggerMinioDefaults() {
        Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(props(
                "warehouse", "s3://bucket/wh",
                "s3.access_key", "ak",
                "s3.secret_key", "sk",
                "s3.endpoint", "s3.us-east-1.amazonaws.com"));

        // WHY (negative parity): a pure s3.* catalog (no minio. key) must NOT trip the minio block, which would
        // clobber the S3 tuning defaults (50/3000/1000) with the object-store ones (100/10000/10000). MUTATION:
        // a minio gate that fires on shared s3.* keys -> fs.s3a.connection.maximum 100 -> red.
        Assertions.assertEquals("50", conf.get("fs.s3a.connection.maximum"));
        Assertions.assertEquals("3000", conf.get("fs.s3a.connection.request.timeout"));
        Assertions.assertEquals("1000", conf.get("fs.s3a.connection.timeout"));
    }

    // ---------------------------------------------------------------------
    // FIX-FECONF-STORAGE-PARITY — HMS username alias (P8-4)
    // ---------------------------------------------------------------------

    @Test
    public void buildHmsHiveConfResolvesUsernameFromHiveMetastoreUsernameAlias() {
        HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(props(
                "uri", "thrift://nn:9083",
                "hive.metastore.username", "hms-user"));

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
                "hadoop.username", "secondary"));

        // WHY: legacy alias order lists hive.metastore.username FIRST, so it wins when both are set.
        // MUTATION: reversing the priority (hadoop.username wins) -> red.
        Assertions.assertEquals("primary", hc.get("hadoop.username"));
    }
}
