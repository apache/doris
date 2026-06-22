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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
 * {@code IcebergConnector.resolveCatalogImpl} switch. The s3tables/dlf bespoke instantiation fixes
 * are later tasks; here we pin the impl-name routing the current production code performs.
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
    public void resolveCatalogImplMapsDlfToDorisDlfCatalog() {
        Assertions.assertEquals("org.apache.doris.connector.iceberg.dlf.DLFCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("dlf"));
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
}
