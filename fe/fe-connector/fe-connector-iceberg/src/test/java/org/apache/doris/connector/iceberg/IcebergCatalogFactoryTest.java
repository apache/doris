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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.aws.AwsClientProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link IcebergCatalogFactory}, the pure flavor-resolution core. Mirrors the role
 * of {@code PaimonCatalogFactoryTest}: every method is a pure transform over plain Maps / Strings
 * (no env, no clock, no live catalog), so the tests are entirely offline. No Mockito.
 *
 * <p>P6.1 baseline: the per-flavor catalog-impl class names MUST mirror the legacy fe-core
 * {@code IcebergConnector.resolveCatalogImpl} switch. Here we pin the impl-name routing the current
 * production code performs.
 */
public class IcebergCatalogFactoryTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    // ---------------------------------------------------------------------
    // resolveFlavor — lower-cased iceberg.catalog.type, null when absent/blank
    // ---------------------------------------------------------------------

    @Test
    public void resolveFlavorLowerCasesTheCatalogType() {
        // WHY: the second-level dispatch keys off the flavor; the legacy code lower-cases the raw
        // iceberg.catalog.type so a user who writes "REST"/"Hms" still routes correctly. MUTATION:
        // returning the raw value (no toLowerCase) -> "REST" != "rest" -> red.
        Assertions.assertEquals("rest",
                IcebergCatalogFactory.resolveFlavor(props("iceberg.catalog.type", "REST")));
        Assertions.assertEquals("hms",
                IcebergCatalogFactory.resolveFlavor(props("iceberg.catalog.type", "Hms")));
        Assertions.assertEquals("glue",
                IcebergCatalogFactory.resolveFlavor(props("iceberg.catalog.type", "glue")));
    }

    @Test
    public void resolveFlavorReturnsNullWhenAbsent() {
        // WHY: an absent iceberg.catalog.type must resolve to null (the "no second-level flavor"
        // signal), NOT throw or invent a default. MUTATION: defaulting to a flavor string -> red.
        Assertions.assertNull(IcebergCatalogFactory.resolveFlavor(Collections.emptyMap()));
    }

    @Test
    public void resolveFlavorReturnsNullWhenBlank() {
        // WHY: a present-but-empty value is treated as absent (the production guard is
        // null-or-isEmpty), so it must also fold to null. MUTATION: dropping the isEmpty() guard ->
        // returns "" -> red.
        Assertions.assertNull(IcebergCatalogFactory.resolveFlavor(props("iceberg.catalog.type", "")));
    }

    // ---------------------------------------------------------------------
    // resolveCatalogImpl — per-flavor catalog-impl class name
    // ---------------------------------------------------------------------

    @Test
    public void resolveCatalogImplMapsRestToRestCatalog() {
        // WHY: each flavor must resolve to the EXACT catalog-impl class CatalogUtil/the bespoke path
        // instantiates; a wrong class name would build the wrong catalog backend. These names are the
        // parity contract with legacy IcebergConnector.resolveCatalogImpl. MUTATION: any wrong/empty
        // class name -> red.
        Assertions.assertEquals("org.apache.iceberg.rest.RESTCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("rest"));
    }

    @Test
    public void resolveCatalogImplMapsHmsToHiveCatalog() {
        Assertions.assertEquals("org.apache.iceberg.hive.HiveCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("hms"));
    }

    @Test
    public void resolveCatalogImplMapsGlueToGlueCatalog() {
        Assertions.assertEquals("org.apache.iceberg.aws.glue.GlueCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("glue"));
    }

    @Test
    public void resolveCatalogImplMapsHadoopToHadoopCatalog() {
        Assertions.assertEquals("org.apache.iceberg.hadoop.HadoopCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("hadoop"));
    }

    @Test
    public void resolveCatalogImplMapsJdbcToJdbcCatalog() {
        Assertions.assertEquals("org.apache.iceberg.jdbc.JdbcCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("jdbc"));
    }

    @Test
    public void resolveCatalogImplMapsS3TablesToS3TablesCatalog() {
        Assertions.assertEquals("software.amazon.s3tables.iceberg.S3TablesCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("s3tables"));
    }

    @Test
    public void resolveCatalogImplRejectsRemovedDlfFlavor() {
        // WHY: iceberg.catalog.type=dlf (DLF 1.0 over the vendored thrift ProxyMetaStoreClient) was removed, so
        // it must now hit the default arm and fail loud like any unknown flavor — never resolve to a class that
        // no longer ships. MUTATION: re-adding a dlf arm -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergCatalogFactory.resolveCatalogImpl("dlf"));
        // Assert on the supported-types LIST only: the message also echoes the rejected input, so a naive
        // contains("dlf") over the whole message would match the echo and never fail.
        String supported = ex.getMessage().substring(ex.getMessage().indexOf("Supported types:"));
        Assertions.assertFalse(supported.contains("dlf"),
                "the supported-types list must no longer advertise dlf: " + supported);
        Assertions.assertTrue(supported.contains("glue"),
                "glue is the iceberg-native backend and must stay supported: " + supported);
    }

    @Test
    public void resolveCatalogImplIsCaseInsensitive() {
        // WHY: the switch lower-cases its input, so mixed/upper-case flavors (e.g. from a user who
        // typed "REST"/"Hms") must still resolve. MUTATION: removing the toLowerCase in the switch ->
        // the default branch throws on "REST" -> red.
        Assertions.assertEquals("org.apache.iceberg.rest.RESTCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("REST"));
        Assertions.assertEquals("org.apache.iceberg.hive.HiveCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("Hms"));
    }

    @Test
    public void resolveCatalogImplThrowsOnNull() {
        // WHY: a null catalogType means the required iceberg.catalog.type property is missing; the
        // factory must fail fast with a DorisConnectorException rather than NPE or return null.
        // MUTATION: removing the null guard -> NPE on toLowerCase -> red (wrong exception type).
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergCatalogFactory.resolveCatalogImpl(null));
        Assertions.assertTrue(ex.getMessage().contains("iceberg.catalog.type"),
                "the missing-property error must name the iceberg.catalog.type key");
    }

    @Test
    public void resolveCatalogImplThrowsOnUnknownType() {
        // WHY: an unrecognized flavor must be rejected loudly (not silently mapped to a default
        // backend), so a typo surfaces at catalog creation. MUTATION: a default branch that returns
        // some impl instead of throwing -> no exception -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergCatalogFactory.resolveCatalogImpl("nosuchcatalog"));
        Assertions.assertTrue(ex.getMessage().contains("nosuchcatalog"),
                "the unknown-type error must echo the offending flavor value");
    }

    // ---------------------------------------------------------------------
    // buildBaseCatalogProperties — copy-all + warehouse + manifest cache (common base)
    // ---------------------------------------------------------------------

    @Test
    public void buildBaseCopiesAllPropsAndMapsWarehouse() {
        // WHY: legacy AbstractIcebergProperties.initializeCatalog seeds catalogProps from getOrigProps()
        // (copy-all) so arbitrary user/iceberg keys pass through to the SDK, then maps warehouse to
        // CatalogProperties.WAREHOUSE_LOCATION ("warehouse"). MUTATION: dropping the copy-all (selective
        // re-key) loses "foo"; a wrong warehouse key loses "warehouse" -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildBaseCatalogProperties(
                props("iceberg.catalog.type", "hadoop", "warehouse", "s3://b/wh", "foo", "bar"));
        Assertions.assertEquals("bar", opts.get("foo"));
        Assertions.assertEquals("s3://b/wh", opts.get("warehouse"));
    }

    @Test
    public void buildBaseDoesNotEnableManifestCacheByDefault() {
        // WHY: legacy DEFAULT_ICEBERG_MANIFEST_CACHE_ENABLE=false — with no explicit
        // io.manifest.cache-enabled and no meta.cache.iceberg.manifest.enable, the key must stay ABSENT
        // (not default-on). MUTATION: unconditionally putting "true" -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildBaseCatalogProperties(
                props("iceberg.catalog.type", "rest"));
        Assertions.assertNull(opts.get("io.manifest.cache-enabled"));
    }

    @Test
    public void buildBaseDerivesManifestCacheEnabledFromMetaCache() {
        // WHY: when the FE meta-cache is enabled (meta.cache.iceberg.manifest.enable=true) and
        // io.manifest.cache-enabled is not set directly, legacy derives io.manifest.cache-enabled=true.
        // The key is DOTTED ("io.manifest.cache-enabled"); the recon agent guessed a hyphenated spelling.
        // MUTATION: wrong key spelling OR skipping the derivation -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildBaseCatalogProperties(
                props("iceberg.catalog.type", "rest", "meta.cache.iceberg.manifest.enable", "true"));
        Assertions.assertEquals("true", opts.get("io.manifest.cache-enabled"));
    }

    @Test
    public void buildBaseExplicitManifestCacheDisabledWinsOverMetaCache() {
        // WHY: an explicit io.manifest.cache-enabled=false must short-circuit the meta-cache derivation
        // (legacy hasIoManifestCacheEnabled guard), so the user's false is preserved. MUTATION: letting
        // the derivation overwrite it to "true" -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildBaseCatalogProperties(
                props("iceberg.catalog.type", "rest", "io.manifest.cache-enabled", "false",
                        "meta.cache.iceberg.manifest.enable", "true"));
        Assertions.assertEquals("false", opts.get("io.manifest.cache-enabled"));
    }

    @Test
    public void buildBaseMetaCacheEnabledButZeroTtlDoesNotEnable() {
        // WHY: legacy isCacheEnabled = enable && ttl != 0 && capacity != 0; an explicit ttl-second=0
        // disables the cache even when enable=true, so io.manifest.cache-enabled must NOT be derived.
        // MUTATION: deriving on enable alone (ignoring ttl/capacity==0) -> "true" -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildBaseCatalogProperties(
                props("iceberg.catalog.type", "rest", "meta.cache.iceberg.manifest.enable", "true",
                        "meta.cache.iceberg.manifest.ttl-second", "0"));
        Assertions.assertNull(opts.get("io.manifest.cache-enabled"));
    }

    // ---------------------------------------------------------------------
    // appendS3FileIOProperties — storage creds -> iceberg S3FileIO dialect (D-061)
    // ---------------------------------------------------------------------

    @Test
    public void appendS3FileIoMapsAllCredentialFields() {
        // WHY: mirror legacy toS3FileIOProperties — the connector reads the fe-filesystem typed
        // S3CompatibleFileSystemProperties getters and emits the iceberg S3FileIO dialect with the
        // VERIFIED SDK constants (region key is client.region, NOT aws.region). MUTATION: any wrong key
        // spelling -> the asserted key is absent -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendS3FileIOProperties(opts,
                new FakeS3CompatibleStorageProperties("S3")
                        .endpoint("https://s3.us-east-1.amazonaws.com").region("us-east-1")
                        .accessKey("AK").secretKey("SK").sessionToken("TK").usePathStyle("true"));
        Assertions.assertEquals("https://s3.us-east-1.amazonaws.com", opts.get("s3.endpoint"));
        Assertions.assertEquals("true", opts.get("s3.path-style-access"));
        Assertions.assertEquals("us-east-1", opts.get("client.region"));
        Assertions.assertEquals("AK", opts.get("s3.access-key-id"));
        Assertions.assertEquals("SK", opts.get("s3.secret-access-key"));
        Assertions.assertEquals("TK", opts.get("s3.session-token"));
    }

    @Test
    public void appendS3FileIoOmitsBlankFields() {
        // WHY: legacy guards every put with isNotBlank, so an unset credential must NOT be emitted as an
        // empty value (which would override the real chain). MUTATION: unconditional put -> "" present -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendS3FileIOProperties(opts,
                new FakeS3CompatibleStorageProperties("S3").region("us-east-1"));
        Assertions.assertEquals("us-east-1", opts.get("client.region"));
        Assertions.assertNull(opts.get("s3.access-key-id"));
        Assertions.assertNull(opts.get("s3.endpoint"));
    }

    @Test
    public void appendS3FileIoEmitsAssumeRoleForGenericS3() {
        // WHY: legacy putAssumeRoleProperties fires only for the generic S3 type (instanceof S3Properties)
        // and a non-blank role ARN; keys = client.factory + aws.region + client.assume-role.{region,arn,
        // external-id}. MUTATION: wrong keys / missing external-id -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendS3FileIOProperties(opts,
                new FakeS3CompatibleStorageProperties("S3").region("us-west-2")
                        .roleArn("arn:aws:iam::1:role/r").externalId("eid"));
        Assertions.assertEquals("org.apache.iceberg.aws.AssumeRoleAwsClientFactory", opts.get("client.factory"));
        Assertions.assertEquals("us-west-2", opts.get("aws.region"));
        Assertions.assertEquals("us-west-2", opts.get("client.assume-role.region"));
        Assertions.assertEquals("arn:aws:iam::1:role/r", opts.get("client.assume-role.arn"));
        Assertions.assertEquals("eid", opts.get("client.assume-role.external-id"));
    }

    @Test
    public void appendS3FileIoSkipsAssumeRoleForNonGenericS3() {
        // WHY: legacy gates assume-role on instanceof S3Properties (generic AWS). OSS/COS/OBS
        // (providerName != "S3") must NOT get the assume-role block even with a role ARN. MUTATION:
        // gating on roleArn alone (ignoring provider) -> client.factory present -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendS3FileIOProperties(opts,
                new FakeS3CompatibleStorageProperties("OSS").region("oss-cn").roleArn("arn:aws:iam::1:role/r"));
        Assertions.assertNull(opts.get("client.factory"));
        Assertions.assertNull(opts.get("client.assume-role.arn"));
    }

    // ---------------------------------------------------------------------
    // chooseS3Compatible — prefer explicit non-S3 subtype (mirror legacy)
    // ---------------------------------------------------------------------

    @Test
    public void chooseS3CompatiblePrefersNonS3Subtype() {
        // WHY: legacy toFileIOProperties prefers the first NON-S3Properties S3-compatible storage (an
        // explicit OSS/COS/OBS choice trumps the generic S3 fallback). MUTATION: returning the S3
        // fallback when an OSS is present -> red.
        List<StorageProperties> storages = Arrays.asList(
                new FakeS3CompatibleStorageProperties("S3"),
                new FakeS3CompatibleStorageProperties("OSS"));
        Optional<S3CompatibleFileSystemProperties> chosen = IcebergCatalogFactory.chooseS3Compatible(storages);
        Assertions.assertTrue(chosen.isPresent());
        Assertions.assertEquals("OSS", chosen.get().providerName());
    }

    @Test
    public void chooseS3CompatibleFallsBackToGenericS3() {
        // WHY: when only the generic S3 type is present it is the chosen one (fallback). MUTATION:
        // returning empty when an S3 is present -> red.
        Optional<S3CompatibleFileSystemProperties> chosen = IcebergCatalogFactory.chooseS3Compatible(
                Collections.singletonList(new FakeS3CompatibleStorageProperties("S3")));
        Assertions.assertTrue(chosen.isPresent());
        Assertions.assertEquals("S3", chosen.get().providerName());
    }

    @Test
    public void chooseS3CompatibleEmptyWhenNoS3Storage() {
        // WHY: a credential-less / HDFS-only catalog has no S3-compatible storage, so no S3FileIO props
        // are emitted (empty Optional). MUTATION: returning a present value -> red.
        Assertions.assertFalse(IcebergCatalogFactory.chooseS3Compatible(Collections.emptyList()).isPresent());
    }

    // ---------------------------------------------------------------------
    // appendRestProperties — mirror IcebergRestProperties
    // ---------------------------------------------------------------------

    @Test
    public void appendRestEmitsUriAlwaysWithAliasPriority() {
        // WHY: legacy puts CatalogProperties.URI UNCONDITIONALLY (field default ""), alias priority
        // iceberg.rest.uri > uri. MUTATION: only-if-nonblank put OR wrong alias order -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.uri", "https://rest", "uri", "https://other"), Optional.empty());
        Assertions.assertEquals("https://rest", opts.get("uri"));

        Map<String, String> empty = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(empty, props(), Optional.empty());
        Assertions.assertEquals("", empty.get("uri"), "uri must be emitted as empty string when no alias is set");
    }

    @Test
    public void appendRestEmitsPrefixOnlyWhenSet() {
        // WHY: legacy emits "prefix" only if iceberg.rest.prefix is non-blank. MUTATION: unconditional put -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts, props("iceberg.rest.prefix", "p1"), Optional.empty());
        Assertions.assertEquals("p1", opts.get("prefix"));
        Map<String, String> none = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(none, props(), Optional.empty());
        Assertions.assertNull(none.get("prefix"));
    }

    @Test
    public void appendRestEmitsVendedCredentialsHeaderWhenEnabled() {
        // WHY: legacy puts header.X-Iceberg-Access-Delegation=vended-credentials iff
        // Boolean.parseBoolean(iceberg.rest.vended-credentials-enabled). MUTATION: wrong header key/value or
        // emitting when disabled -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.vended-credentials-enabled", "true"), Optional.empty());
        Assertions.assertEquals("vended-credentials", opts.get("header.X-Iceberg-Access-Delegation"));
        Map<String, String> off = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(off, props(), Optional.empty());
        Assertions.assertNull(off.get("header.X-Iceberg-Access-Delegation"));
    }

    @Test
    public void appendRestEmitsTimeoutsWithDefaults() {
        // WHY: legacy fields default non-blank (10000 / 60000) and are put effectively always under the literal
        // keys rest.client.connection-timeout-ms / rest.client.socket-timeout-ms. MUTATION: wrong defaults or
        // wrong literal keys -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts, props(), Optional.empty());
        Assertions.assertEquals("10000", opts.get("rest.client.connection-timeout-ms"));
        Assertions.assertEquals("60000", opts.get("rest.client.socket-timeout-ms"));
        Map<String, String> over = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(over,
                props("iceberg.rest.connection-timeout-ms", "5000", "iceberg.rest.socket-timeout-ms", "7000"),
                Optional.empty());
        Assertions.assertEquals("5000", over.get("rest.client.connection-timeout-ms"));
        Assertions.assertEquals("7000", over.get("rest.client.socket-timeout-ms"));
    }

    @Test
    public void appendRestOAuth2CredentialBranchEmitsCredentialAndTokenRefreshDefault() {
        // WHY: when security.type=oauth2 and a credential is present, legacy emits credential + optional
        // server-uri/scope + token-refresh-enabled (default "true" from OAuth2Properties default).
        // MUTATION: emitting token instead, wrong keys, or dropping token-refresh-enabled -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.security.type", "oauth2",
                        "iceberg.rest.oauth2.credential", "id:secret",
                        "iceberg.rest.oauth2.server-uri", "https://auth",
                        "iceberg.rest.oauth2.scope", "catalog"),
                Optional.empty());
        Assertions.assertEquals("id:secret", opts.get("credential"));
        Assertions.assertEquals("https://auth", opts.get("oauth2-server-uri"));
        Assertions.assertEquals("catalog", opts.get("scope"));
        Assertions.assertEquals("true", opts.get("token-refresh-enabled"));
        Assertions.assertNull(opts.get("token"), "credential branch must NOT emit a token");
    }

    @Test
    public void appendRestOAuth2TokenBranchWhenNoCredential() {
        // WHY: oauth2 with no credential uses the pre-configured token flow: emit OAuth2Properties.TOKEN.
        // MUTATION: emitting credential / token-refresh-enabled here -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.security.type", "oauth2", "iceberg.rest.oauth2.token", "tok"), Optional.empty());
        Assertions.assertEquals("tok", opts.get("token"));
        Assertions.assertNull(opts.get("credential"));
        Assertions.assertNull(opts.get("token-refresh-enabled"));
    }

    @Test
    public void appendRestOAuth2NotAppliedWhenSecurityNotOauth2() {
        // WHY: the oauth2 block is gated on security.type==oauth2 (default none). MUTATION: applying it
        // unconditionally would leak credential/token even for a non-oauth2 catalog -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.oauth2.credential", "id:secret"), Optional.empty());
        Assertions.assertNull(opts.get("credential"));
    }

    @Test
    public void appendRestSigningBlockEmitsSigningKeysAndS3ExplicitCredentials() {
        // WHY: when signing-name is set, legacy emits rest.signing-name/sigv4-enabled/signing-region; for
        // glue/s3tables the credentials come from the chosen S3 store, EXPLICIT (static AK/SK) -> rest.* creds
        // (AwsProperties.REST_*). MUTATION: wrong signing keys, or sourcing creds from the wrong place -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.signing-name", "glue", "iceberg.rest.sigv4-enabled", "true",
                        "iceberg.rest.signing-region", "us-east-1"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").accessKey("AK").secretKey("SK")
                        .sessionToken("TK")));
        Assertions.assertEquals("glue", opts.get("rest.signing-name"));
        Assertions.assertEquals("true", opts.get("rest.sigv4-enabled"));
        Assertions.assertEquals("us-east-1", opts.get("rest.signing-region"));
        Assertions.assertEquals("AK", opts.get("rest.access-key-id"));
        Assertions.assertEquals("SK", opts.get("rest.secret-access-key"));
        Assertions.assertEquals("TK", opts.get("rest.session-token"));
    }

    @Test
    public void appendRestSigningGlueAssumeRoleWhenNoStaticCreds() {
        // WHY: legacy getCredentialType precedence is EXPLICIT then ASSUME_ROLE; with no static AK/SK but a role
        // ARN the glue/s3tables signing path emits the assume-role block (client.factory + client.assume-role.*).
        // MUTATION: emitting rest.access-key-id from a blank AK, or skipping assume-role -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.signing-name", "s3tables", "iceberg.rest.sigv4-enabled", "true",
                        "iceberg.rest.signing-region", "us-west-2"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").region("us-west-2")
                        .roleArn("arn:aws:iam::1:role/r")));
        Assertions.assertEquals("org.apache.iceberg.aws.AssumeRoleAwsClientFactory", opts.get("client.factory"));
        Assertions.assertEquals("arn:aws:iam::1:role/r", opts.get("client.assume-role.arn"));
        Assertions.assertNull(opts.get("rest.access-key-id"), "no static creds -> no explicit rest creds");
    }

    @Test
    public void appendRestSigningOtherNameUsesIcebergRestCredentials() {
        // WHY: a signing-name NOT in {glue,s3tables} uses the iceberg.rest.* explicit creds (not the S3 store).
        // MUTATION: reading the S3 store here -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.signing-name", "custom",
                        "iceberg.rest.access-key-id", "RAK", "iceberg.rest.secret-access-key", "RSK"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").accessKey("SHOULD_NOT_USE")
                        .secretKey("SHOULD_NOT_USE")));
        Assertions.assertEquals("RAK", opts.get("rest.access-key-id"));
        Assertions.assertEquals("RSK", opts.get("rest.secret-access-key"));
    }

    @Test
    public void appendRestSigningGlueProviderChainPinsNonDefaultProvider() {
        // F14: glue/s3tables signing with NO static creds and NO role -> PROVIDER_CHAIN. A non-DEFAULT
        // s3.credentials_provider_type must pin client.credentials-provider to that provider class (was silently
        // dropped). MUTATION: dropping the else branch -> the key is absent -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.signing-name", "glue", "s3.credentials_provider_type", "anonymous"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").region("us-east-1")));
        Assertions.assertEquals(AnonymousCredentialsProvider.class.getName(),
                opts.get("client.credentials-provider"));

        // DEFAULT / absent -> nothing emitted (SDK default chain, the common case).
        Map<String, String> defaultOpts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(defaultOpts,
                props("iceberg.rest.signing-name", "glue"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").region("us-east-1")));
        Assertions.assertNull(defaultOpts.get("client.credentials-provider"),
                "DEFAULT/absent provider mode must emit no client.credentials-provider");
    }

    @Test
    public void appendRestSigningOtherNameProviderChainPinsNonDefaultProvider() {
        // F14: a non-glue/s3tables signing-name with NO explicit iceberg.rest.* creds falls to PROVIDER_CHAIN;
        // iceberg.rest.credentials_provider_type pins the provider class. MUTATION: dropping the else -> absent.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts,
                props("iceberg.rest.signing-name", "custom",
                        "iceberg.rest.credentials_provider_type", "web-identity"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3")));
        Assertions.assertEquals(WebIdentityTokenFileCredentialsProvider.class.getName(),
                opts.get("client.credentials-provider"));
        Assertions.assertNull(opts.get("rest.access-key-id"), "no explicit rest creds were supplied");
    }

    @Test
    public void appendRestNoSigningBlockWhenSigningNameAbsent() {
        // WHY: the entire signing block is gated on a non-blank signing-name. MUTATION: emitting rest.signing-name
        // (even empty) without the gate -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendRestProperties(opts, props(), Optional.empty());
        Assertions.assertNull(opts.get("rest.signing-name"));
        Assertions.assertNull(opts.get("rest.sigv4-enabled"));
    }

    // ---------------------------------------------------------------------
    // appendGlueProperties — mirror IcebergGlueMetaStoreProperties
    // ---------------------------------------------------------------------

    @Test
    public void appendGlueEmitsS3KeysUnconditionallyFromChosenStore() {
        // WHY: legacy appendS3Props uses PLAIN puts (no isNotBlank guard) from S3Properties.of(origProps), so an
        // unset session token is written as "". MUTATION: blank-guarding these puts (like the base S3FileIO path)
        // -> the empty s3.session-token would be absent -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(opts, props("glue.access_key", "a", "glue.secret_key", "b",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").accessKey("S3AK").secretKey("S3SK")
                        .endpoint("https://s3").usePathStyle("true")));
        Assertions.assertEquals("S3AK", opts.get("s3.access-key-id"));
        Assertions.assertEquals("S3SK", opts.get("s3.secret-access-key"));
        Assertions.assertEquals("https://s3", opts.get("s3.endpoint"));
        Assertions.assertEquals("true", opts.get("s3.path-style-access"));
        Assertions.assertEquals("", opts.get("s3.session-token"), "blank session token must be emitted as empty");
    }

    @Test
    public void appendGlueAccessKeyBranchEmitsProviderKeysAndWins() {
        // WHY: when glue access_key & secret_key are both set, emit the ConfigurationAWSCredentialsProvider2x
        // provider keys and RETURN (mutually exclusive with the IAM-role branch). MUTATION: wrong key/value, or
        // also emitting client.factory (IAM branch) -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(opts,
                props("glue.access_key", "GAK", "glue.secret_key", "GSK", "aws.glue.session-token", "GST",
                        "glue.role_arn", "arn:aws:iam::1:role/should-not-fire",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com"),
                Optional.empty());
        Assertions.assertEquals("org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x",
                opts.get("client.credentials-provider"));
        Assertions.assertEquals("GAK", opts.get("client.credentials-provider.glue.access_key"));
        Assertions.assertEquals("GSK", opts.get("client.credentials-provider.glue.secret_key"));
        Assertions.assertEquals("GST", opts.get("client.credentials-provider.glue.session_token"));
        Assertions.assertNull(opts.get("aws.catalog.credentials.provider.factory.class"),
                "the factory key was only ever read by the removed thrift-generation Glue client");
        Assertions.assertNull(opts.get("client.factory"), "AK/SK branch must short-circuit the IAM-role branch");
    }

    @Test
    public void glueSessionTokenSurvivesToTheResolvedCredential() {
        // Drives the REAL iceberg plumbing end to end -- emit -> AwsClientProperties strips the
        // "client.credentials-provider." prefix -> DynMethods reflects into create(Map) -> we read the token.
        // Asserting the emission alone (see the test above) cannot catch the two halves drifting apart: the
        // provider used to read only ak/sk, so a supplied token was dropped here and AWS then rejected the
        // temporary credentials. MUTATION: dropping the token read in create() -> AwsBasicCredentials -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(opts,
                props("glue.access_key", "GAK", "glue.secret_key", "GSK", "aws.glue.session-token", "GST",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com"),
                Optional.empty());

        // null ak/sk so iceberg falls back to the named client.credentials-provider (the glue client's path).
        AwsCredentials resolved = new AwsClientProperties(opts)
                .credentialsProvider(null, null, null)
                .resolveCredentials();

        Assertions.assertInstanceOf(AwsSessionCredentials.class, resolved,
                "a supplied glue session token must yield session credentials, not basic ones");
        Assertions.assertEquals("GST", ((AwsSessionCredentials) resolved).sessionToken());
        Assertions.assertEquals("GAK", resolved.accessKeyId());
        Assertions.assertEquals("GSK", resolved.secretAccessKey());
    }

    @Test
    public void glueWithoutSessionTokenStaysBasicCredentials() {
        // The no-token path must keep today's behaviour. MUTATION: building session credentials unconditionally
        // -> AwsSessionCredentials.create accepts a blank token silently -> this turns red instead of AWS
        // rejecting it much later with a confusing 4xx.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(opts,
                props("glue.access_key", "GAK", "glue.secret_key", "GSK",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com"),
                Optional.empty());

        AwsCredentials resolved = new AwsClientProperties(opts)
                .credentialsProvider(null, null, null)
                .resolveCredentials();

        Assertions.assertInstanceOf(AwsBasicCredentials.class, resolved);
        Assertions.assertEquals("GAK", resolved.accessKeyId());
        Assertions.assertEquals("GSK", resolved.secretAccessKey());
    }

    @Test
    public void appendGlueIamRoleBranchWhenNoAccessKey() {
        // WHY: with no glue AK/SK but a glue.role_arn, legacy emits the assume-role block (client.factory +
        // aws.region + client.assume-role.arn/region + optional external-id). MUTATION: wrong keys or skipping
        // external-id -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(opts,
                props("glue.role_arn", "arn:aws:iam::1:role/r", "glue.external_id", "eid", "glue.region", "eu-west-1",
                        "glue.endpoint", "https://glue.eu-west-1.amazonaws.com"),
                Optional.empty());
        Assertions.assertEquals("org.apache.iceberg.aws.AssumeRoleAwsClientFactory", opts.get("client.factory"));
        Assertions.assertEquals("eu-west-1", opts.get("aws.region"));
        Assertions.assertEquals("arn:aws:iam::1:role/r", opts.get("client.assume-role.arn"));
        Assertions.assertEquals("eu-west-1", opts.get("client.assume-role.region"));
        Assertions.assertEquals("eid", opts.get("client.assume-role.external-id"));
    }

    @Test
    public void appendGlueEmitsEndpointAndClientRegionAlways() {
        // WHY: legacy always puts glue.endpoint (AwsProperties.GLUE_CATALOG_ENDPOINT) and client.region. MUTATION:
        // dropping either -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(opts,
                props("glue.access_key", "a", "glue.secret_key", "b", "glue.region", "ap-south-1",
                        "glue.endpoint", "https://glue.ap-south-1.amazonaws.com"),
                Optional.empty());
        Assertions.assertEquals("https://glue.ap-south-1.amazonaws.com", opts.get("glue.endpoint"));
        Assertions.assertEquals("ap-south-1", opts.get("client.region"));
    }

    @Test
    public void appendGlueRegionFallsBackToEndpointRegexThenUsEast1() {
        // WHY: legacy resolves the region from glue.region first, else extracts it from the endpoint host via
        // ENDPOINT_PATTERN, else us-east-1. MUTATION: not extracting from the endpoint, or wrong fallback -> red.
        Map<String, String> fromEndpoint = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(fromEndpoint,
                props("glue.access_key", "a", "glue.secret_key", "b",
                        "glue.endpoint", "https://glue-fips.ca-central-1.api.aws"),
                Optional.empty());
        Assertions.assertEquals("ca-central-1", fromEndpoint.get("client.region"));

        Map<String, String> defaulted = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(defaulted,
                props("glue.access_key", "a", "glue.secret_key", "b", "glue.endpoint", "https://not-a-glue-host"),
                Optional.empty());
        Assertions.assertEquals("us-east-1", defaulted.get("client.region"));
    }

    @Test
    public void appendGluePutsWarehousePlaceholderOnlyWhenAbsent() {
        // WHY: legacy putIfAbsent(WAREHOUSE_LOCATION, "s3://doris") — fills the placeholder only when the user
        // did not supply a warehouse. MUTATION: an unconditional put would clobber the user's warehouse -> red.
        Map<String, String> defaulted = new HashMap<>();
        IcebergCatalogFactory.appendGlueProperties(defaulted,
                props("glue.access_key", "a", "glue.secret_key", "b", "glue.endpoint", "https://glue.x.amazonaws.com"),
                Optional.empty());
        Assertions.assertEquals("s3://doris", defaulted.get("warehouse"));

        Map<String, String> userWh = new HashMap<>();
        userWh.put("warehouse", "s3://mybucket/wh");
        IcebergCatalogFactory.appendGlueProperties(userWh,
                props("glue.access_key", "a", "glue.secret_key", "b", "glue.endpoint", "https://glue.x.amazonaws.com"),
                Optional.empty());
        Assertions.assertEquals("s3://mybucket/wh", userWh.get("warehouse"));
    }

    // ---------------------------------------------------------------------
    // appendJdbcProperties — mirror IcebergJdbcMetaStoreProperties
    // ---------------------------------------------------------------------

    @Test
    public void appendJdbcEmitsUriWithAliasPriority() {
        // WHY: legacy uri alias priority is {uri, iceberg.jdbc.uri} (uri wins). MUTATION: wrong order -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendJdbcProperties(opts,
                props("uri", "jdbc:mysql://h/db", "iceberg.jdbc.uri", "jdbc:other"));
        Assertions.assertEquals("jdbc:mysql://h/db", opts.get("uri"));
    }

    @Test
    public void appendJdbcAddsDottedKeysOnlyWhenSet() {
        // WHY: legacy addIfNotBlank maps each iceberg.jdbc.<x> to the dotted jdbc.<x> only when non-blank.
        // MUTATION: wrong emitted key spelling or emitting a blank value -> red.
        Map<String, String> opts = new HashMap<>();
        IcebergCatalogFactory.appendJdbcProperties(opts,
                props("uri", "jdbc:mysql://h/db", "iceberg.jdbc.user", "u", "iceberg.jdbc.password", "p",
                        "iceberg.jdbc.init-catalog-tables", "true", "iceberg.jdbc.schema-version", "V1",
                        "iceberg.jdbc.strict-mode", "false"));
        Assertions.assertEquals("u", opts.get("jdbc.user"));
        Assertions.assertEquals("p", opts.get("jdbc.password"));
        Assertions.assertEquals("true", opts.get("jdbc.init-catalog-tables"));
        Assertions.assertEquals("V1", opts.get("jdbc.schema-version"));
        Assertions.assertEquals("false", opts.get("jdbc.strict-mode"));

        Map<String, String> bare = new HashMap<>();
        IcebergCatalogFactory.appendJdbcProperties(bare, props("uri", "jdbc:mysql://h/db"));
        Assertions.assertNull(bare.get("jdbc.user"), "an unset jdbc.user must NOT be emitted");
    }

    // ---------------------------------------------------------------------
    // buildCatalogProperties — orchestrator (impl per flavor, type removed, jdbc catalog_name removed)
    // ---------------------------------------------------------------------

    @Test
    public void buildCatalogPropertiesSetsImplAndRemovesType() {
        // WHY: every flavor must set the correct catalog-impl and the iceberg SDK forbids both "type" and
        // "catalog-impl", so "type" (= iceberg.catalog.type's SDK alias) must be removed. The Doris-side
        // iceberg.catalog.type key is a separate raw key carried by copy-all and is harmless. MUTATION: not
        // setting impl, or leaving "type" -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildCatalogProperties(
                props("iceberg.catalog.type", "hadoop", "warehouse", "s3://b/wh", "type", "hadoop"),
                "hadoop", Optional.empty());
        Assertions.assertEquals("org.apache.iceberg.hadoop.HadoopCatalog", opts.get("catalog-impl"));
        Assertions.assertNull(opts.get("type"), "the SDK 'type' key must be removed before building");
    }

    @Test
    public void buildCatalogPropertiesRemovesJdbcCatalogNameFromMap() {
        // WHY: iceberg.jdbc.catalog_name is the positional catalog NAME, removed from the options map by legacy
        // initCatalog. MUTATION: leaving it in the map -> red (iceberg would treat it as an unknown option).
        Map<String, String> opts = IcebergCatalogFactory.buildCatalogProperties(
                props("iceberg.catalog.type", "jdbc", "uri", "jdbc:mysql://h/db", "warehouse", "s3://b/wh",
                        "iceberg.jdbc.catalog_name", "mycat"),
                "jdbc", Optional.empty());
        Assertions.assertEquals("org.apache.iceberg.jdbc.JdbcCatalog", opts.get("catalog-impl"));
        Assertions.assertNull(opts.get("iceberg.jdbc.catalog_name"),
                "the jdbc catalog_name must be consumed positionally, not left in the options map");
    }

    @Test
    public void buildCatalogPropertiesHmsEmitsNoS3FileIoKeys() {
        // WHY: legacy iceberg HMS does NOT call toFileIOProperties — object-store access rides the HiveConf, not
        // the s3.* options. MUTATION: appending the base S3FileIO for HMS -> s3.endpoint present -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildCatalogProperties(
                props("iceberg.catalog.type", "hms"), "hms",
                Optional.of(new FakeS3CompatibleStorageProperties("S3").endpoint("https://s3").accessKey("AK")));
        Assertions.assertEquals("org.apache.iceberg.hive.HiveCatalog", opts.get("catalog-impl"));
        Assertions.assertNull(opts.get("s3.endpoint"), "HMS must not emit S3FileIO options");
        Assertions.assertNull(opts.get("s3.access-key-id"));
    }

    @Test
    public void buildCatalogPropertiesRestVendedPropagatesClientRegionWithoutBoundS3() {
        // WHY: a REST catalog with vended credentials binds NO fe-filesystem S3 storage (no static AK/SK/role ->
        // chosenS3 empty), yet iceberg S3FileIO still needs client.region or it falls through to the AWS SDK
        // DefaultAwsRegionProviderChain and the write commit fails with "Unable to load region". The raw s3.region
        // carried by copy-all is inert (iceberg reads client.region). Legacy toFileIOProperties supplied this in
        // its chosen==null branch. MUTATION: dropping the empty-chosenS3 region fallback -> client.region absent -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildCatalogProperties(
                props("iceberg.catalog.type", "rest", "uri", "https://rest",
                        "iceberg.rest.vended-credentials-enabled", "true", "s3.endpoint", "https://minio:9000",
                        "s3.region", "us-east-1"),
                "rest", Optional.empty());
        Assertions.assertEquals("us-east-1", opts.get("client.region"),
                "vended REST (no bound S3) must still translate s3.region -> client.region");
    }

    @Test
    public void buildCatalogPropertiesRestVendedResolvesRegionFromWidenedAliases() {
        // WHY: the empty-chosenS3 region fallback must scan the SAME region aliases legacy getRegionFromProperties
        // did (the fe-core S3Properties isRegionField set), not just {s3.region, aws.region, region, client.region}.
        // A vended REST catalog whose region arrives only via AWS_REGION or iceberg.rest.signing-region would
        // otherwise yield no client.region -> AWS SDK DefaultAwsRegionProviderChain -> "Unable to load region".
        // RED before widening: AWS_REGION (uppercase) does not match the narrow lowercase aws.region and
        // iceberg.rest.signing-region is absent from the narrow 4-alias set -> client.region null.
        Map<String, String> viaAwsRegion = IcebergCatalogFactory.buildCatalogProperties(
                props("iceberg.catalog.type", "rest", "uri", "https://rest",
                        "iceberg.rest.vended-credentials-enabled", "true", "AWS_REGION", "us-east-1"),
                "rest", Optional.empty());
        Assertions.assertEquals("us-east-1", viaAwsRegion.get("client.region"),
                "region supplied only via AWS_REGION must translate to client.region");

        Map<String, String> viaSigningRegion = IcebergCatalogFactory.buildCatalogProperties(
                props("iceberg.catalog.type", "rest", "uri", "https://rest",
                        "iceberg.rest.vended-credentials-enabled", "true",
                        "iceberg.rest.signing-region", "eu-west-1"),
                "rest", Optional.empty());
        Assertions.assertEquals("eu-west-1", viaSigningRegion.get("client.region"),
                "region supplied only via iceberg.rest.signing-region must translate to client.region");
    }

    // ---------------------------------------------------------------------
    // buildS3TablesCatalogProperties — bespoke s3tables options (NO catalog-impl, NO type removal)
    // ---------------------------------------------------------------------

    @Test
    public void buildS3TablesCatalogPropertiesEmitsS3FileIoAndWarehouseNoImpl() {
        // WHY: s3tables is NOT built via CatalogUtil — legacy IcebergS3TablesMetaStoreProperties hands the
        // 3-arg S3TablesCatalog.initialize a props map = getOrigProps + warehouse(=table-bucket ARN) +
        // manifest-cache + S3FileIO creds, and adds NEITHER "catalog-impl" NOR removes "type" (those are the
        // CatalogUtil path's concern). MUTATION: adding catalog-impl, removing type, or dropping S3FileIO -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildS3TablesCatalogProperties(
                props("iceberg.catalog.type", "s3tables", "type", "iceberg",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3")
                        .endpoint("https://s3.us-east-1.amazonaws.com").region("us-east-1")
                        .accessKey("AK").secretKey("SK").sessionToken("TK").usePathStyle("true")));
        Assertions.assertEquals("arn:aws:s3tables:us-east-1:1:bucket/b", opts.get("warehouse"),
                "the table-bucket ARN warehouse must be carried through for the 3-arg initialize");
        Assertions.assertEquals("AK", opts.get("s3.access-key-id"));
        Assertions.assertEquals("SK", opts.get("s3.secret-access-key"));
        Assertions.assertEquals("TK", opts.get("s3.session-token"));
        Assertions.assertEquals("https://s3.us-east-1.amazonaws.com", opts.get("s3.endpoint"));
        Assertions.assertEquals("us-east-1", opts.get("client.region"));
        Assertions.assertEquals("true", opts.get("s3.path-style-access"));
        Assertions.assertNull(opts.get("catalog-impl"),
                "bespoke s3tables initialize must not receive a catalog-impl");
        Assertions.assertEquals("iceberg", opts.get("type"),
                "bespoke s3tables path does not perform the CatalogUtil 'type' removal");
    }

    @Test
    public void buildS3TablesCatalogPropertiesEmitsAssumeRoleWhenNoStaticCreds() {
        // WHY: with no static AK/SK but a role ARN, the FileIO credential block is the generic-S3 assume-role
        // keys (client.factory + aws.region + client.assume-role.{region,arn,external-id}) — the same path the
        // legacy putS3FileIOCredentialProperties ASSUME_ROLE branch emits. MUTATION: missing assume-role keys
        // OR leaking static AK/SK -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildS3TablesCatalogProperties(
                props("iceberg.catalog.type", "s3tables", "warehouse", "arn:aws:s3tables:us-west-2:1:bucket/b"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").region("us-west-2")
                        .roleArn("arn:aws:iam::1:role/r").externalId("eid")));
        Assertions.assertEquals("org.apache.iceberg.aws.AssumeRoleAwsClientFactory", opts.get("client.factory"));
        Assertions.assertEquals("arn:aws:iam::1:role/r", opts.get("client.assume-role.arn"));
        Assertions.assertEquals("us-west-2", opts.get("client.assume-role.region"));
        Assertions.assertEquals("eid", opts.get("client.assume-role.external-id"));
        Assertions.assertEquals("us-west-2", opts.get("client.region"));
        Assertions.assertNull(opts.get("s3.access-key-id"), "no static creds were supplied");
    }

    @Test
    public void buildS3TablesCatalogPropertiesEmitsProviderChainWhenNoStaticNoRole() {
        // F14: s3tables FileIO with NO static creds and NO role -> PROVIDER_CHAIN. A non-DEFAULT
        // s3.credentials_provider_type pins client.credentials-provider (mirrors legacy putCredentialsProvider).
        // MUTATION: dropping the else branch in appendS3TablesFileIOProperties -> the key is absent -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildS3TablesCatalogProperties(
                props("iceberg.catalog.type", "s3tables", "warehouse", "arn:aws:s3tables:us-west-2:1:bucket/b",
                        "s3.credentials_provider_type", "ENV"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").region("us-west-2")));
        Assertions.assertEquals(EnvironmentVariableCredentialsProvider.class.getName(),
                opts.get("client.credentials-provider"));
        Assertions.assertNull(opts.get("client.factory"), "no role -> no assume-role block");
    }

    @Test
    public void buildS3TablesCatalogPropertiesExplicitStaticCredsSuppressAssumeRole() {
        // WHY: s3tables uses legacy putS3FileIOCredentialProperties, whose getCredentialType is EXPLICIT-wins —
        // static AK/SK present returns BEFORE any assume-role keys, EVEN when a role ARN is ALSO configured. This
        // differs from the generic toS3FileIOProperties (rest/hadoop/jdbc) which always emits assume-role-if-role.
        // The s3tables path must NOT reuse the always-emit appendS3FileIOProperties helper. MUTATION: emitting the
        // assume-role block (client.factory / client.assume-role.*) when static creds are present -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildS3TablesCatalogProperties(
                props("iceberg.catalog.type", "s3tables", "warehouse", "arn:aws:s3tables:us-west-2:1:bucket/b"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").region("us-west-2")
                        .accessKey("AK").secretKey("SK").roleArn("arn:aws:iam::1:role/r")));
        Assertions.assertEquals("AK", opts.get("s3.access-key-id"));
        Assertions.assertEquals("SK", opts.get("s3.secret-access-key"));
        Assertions.assertNull(opts.get("client.factory"),
                "EXPLICIT static creds must suppress the assume-role block on the s3tables path");
        Assertions.assertNull(opts.get("client.assume-role.arn"));
    }

    @Test
    public void buildS3TablesCatalogPropertiesWithoutStorageOmitsS3FileIo() {
        // WHY: with no bound S3-compatible storage AND no region alias in the props, only the base keys are present
        // (warehouse + manifest-cache) — no s3.* credential keys are fabricated, and client.region stays absent
        // because there is no region to propagate. (When a region IS present it is now emitted; see
        // buildS3TablesCatalogPropertiesPropagatesClientRegionWithoutBoundS3.) MUTATION: fabricating any s3.* -> red.
        Map<String, String> opts = IcebergCatalogFactory.buildS3TablesCatalogProperties(
                props("iceberg.catalog.type", "s3tables", "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"),
                Optional.empty());
        Assertions.assertEquals("arn:aws:s3tables:us-east-1:1:bucket/b", opts.get("warehouse"));
        Assertions.assertNull(opts.get("s3.access-key-id"));
        Assertions.assertNull(opts.get("client.region"));
        Assertions.assertNull(opts.get("catalog-impl"));
    }

    @Test
    public void buildS3TablesCatalogPropertiesPropagatesClientRegionWithoutBoundS3() {
        // WHY: an EC2 instance-profile s3tables catalog (no bound storage) must still propagate an explicit
        // s3.region to the data-plane S3FileIO as client.region, or S3FileIO falls to IMDS /
        // DefaultAwsRegionProviderChain. Parallels the REST vended-cred region test. RED at HEAD (the old
        // ifPresent-only path emitted nothing without a storage). No s3.* credential keys (none are bound).
        Map<String, String> opts = IcebergCatalogFactory.buildS3TablesCatalogProperties(
                props("iceberg.catalog.type", "s3tables", "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b",
                        "s3.region", "us-east-1"),
                Optional.empty());
        Assertions.assertEquals("us-east-1", opts.get("client.region"),
                "no-storage s3tables must propagate s3.region -> client.region for the data-plane S3FileIO");
        Assertions.assertNull(opts.get("s3.access-key-id"), "no credentials are bound");
    }

    // ---------------------------------------------------------------------
    // resolveCatalogName — jdbc positional vs default
    // ---------------------------------------------------------------------

    @Test
    public void resolveCatalogNameUsesJdbcCatalogNameForJdbc() {
        // WHY: legacy passes iceberg.jdbc.catalog_name as the catalog NAME (overriding the Doris catalog name).
        // MUTATION: returning the default name for jdbc -> red.
        Assertions.assertEquals("mycat", IcebergCatalogFactory.resolveCatalogName(
                props("iceberg.jdbc.catalog_name", "mycat"), "jdbc", "doris_cat"));
    }

    @Test
    public void resolveCatalogNameThrowsWhenJdbcCatalogNameMissing() {
        // WHY: iceberg.jdbc.catalog_name is required (legacy @ConnectorProperty required=true); a missing value
        // must fail loud rather than silently fall back to the Doris catalog name. MUTATION: returning the default
        // -> no exception -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergCatalogFactory.resolveCatalogName(props(), "jdbc", "doris_cat"));
        Assertions.assertTrue(ex.getMessage().contains("iceberg.jdbc.catalog_name"));
    }

    @Test
    public void resolveCatalogNameUsesDefaultForNonJdbc() {
        // WHY: every non-jdbc flavor uses the Doris catalog name. MUTATION: reading catalog_name for hms -> red.
        Assertions.assertEquals("doris_cat", IcebergCatalogFactory.resolveCatalogName(
                props("iceberg.jdbc.catalog_name", "ignored"), "hms", "doris_cat"));
    }

    // ---------------------------------------------------------------------
    // buildHadoopConfiguration / assembleHiveConf — storage + HiveConf sinks
    // ---------------------------------------------------------------------

    @Test
    public void buildHadoopConfigurationAppliesStorageThenRawPassthrough() {
        // WHY: the conf carries the pre-computed storage config plus the raw fs./dfs./hadoop. passthrough for
        // inline keys. MUTATION: dropping the passthrough or the storage map -> red.
        Map<String, String> storage = new HashMap<>();
        storage.put("fs.s3a.endpoint", "https://s3");
        Configuration conf = IcebergCatalogFactory.buildHadoopConfiguration(
                props("dfs.nameservices", "ns1", "unrelated.key", "x"), storage);
        Assertions.assertEquals("https://s3", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("ns1", conf.get("dfs.nameservices"));
        Assertions.assertNull(conf.get("unrelated.key"), "non fs./dfs./hadoop. keys must not be copied into the conf");
    }

    @Test
    public void assembleHiveConfLayersOverridesOverBase(@TempDir java.nio.file.Path tmp) throws Exception {
        // WHY: the external hive-site.xml named by hive.conf.resources is seeded first, then the
        // metastore-spi overrides win, so a connection key in the overrides correctly overrides the file.
        // MUTATION: reversing the order -> the base value would win -> red.
        // The connector resolves the file itself, so this drives the REAL file->HiveConf path: it writes a
        // real XML and reads the values back off the HiveConf.
        java.nio.file.Files.write(tmp.resolve("hive-site.xml"),
                ("<?xml version=\"1.0\"?><configuration>"
                        + "<property><name>hive.metastore.uris</name><value>thrift://from-file:9083</value></property>"
                        + "<property><name>base.only</name><value>kept</value></property>"
                        + "</configuration>").getBytes(java.nio.charset.StandardCharsets.UTF_8));

        String prev = System.getProperty("doris.hadoop.config.dir");
        System.setProperty("doris.hadoop.config.dir", tmp.toString() + java.io.File.separator);
        try {
            Map<String, String> overrides = new HashMap<>();
            overrides.put("hive.metastore.uris", "thrift://override:9083");
            HiveConf conf = IcebergCatalogFactory.assembleHiveConf("hive-site.xml", overrides);
            Assertions.assertEquals("thrift://override:9083", conf.get("hive.metastore.uris"));
            Assertions.assertEquals("kept", conf.get("base.only"));
        } finally {
            if (prev == null) {
                System.clearProperty("doris.hadoop.config.dir");
            } else {
                System.setProperty("doris.hadoop.config.dir", prev);
            }
        }
    }

    @Test
    public void assembleHiveConfFailsLoudOnMissingFile(@TempDir java.nio.file.Path tmp) {
        // WHY: the operator named a file carrying connection-critical settings; a missing file must fail
        // loud, never degrade to "connect with defaults". MUTATION: swallowing the miss -> red.
        String prev = System.getProperty("doris.hadoop.config.dir");
        System.setProperty("doris.hadoop.config.dir", tmp.toString() + java.io.File.separator);
        try {
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> IcebergCatalogFactory.assembleHiveConf("absent.xml", Collections.emptyMap()));
            Assertions.assertTrue(e.getMessage().contains("Config resource file does not exist"),
                    "message must name the unresolvable file; was: " + e.getMessage());
        } finally {
            if (prev == null) {
                System.clearProperty("doris.hadoop.config.dir");
            } else {
                System.setProperty("doris.hadoop.config.dir", prev);
            }
        }
    }

}
