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

import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * WHOLE-MAP snapshots of {@link IcebergCatalogFactory#buildCatalogProperties} (and the bespoke
 * {@link IcebergCatalogFactory#buildS3TablesCatalogProperties}), one per flavor and per emission branch.
 *
 * <p><b>Why a snapshot and not more per-key assertions.</b> {@link IcebergCatalogFactoryTest} asserts that
 * the keys it names are right; nothing there notices a key that appears, disappears, or is spelled
 * differently. These options ARE the catalog connection: the iceberg SDK ignores an option it does not
 * recognize, so a dropped or misspelled key does not throw — it produces a catalog that connects with
 * different settings than the operator asked for. Asserting the ENTIRE map turns any drift into a diff.
 *
 * <p>They exist because the per-flavor assembly is being folded onto the bound
 * {@code Iceberg*MetaStoreProperties} holders, retiring the parallel raw-map alias scan. Once the old path
 * is gone there is nothing left to compare against, so the reference is captured HERE, before the change,
 * and kept afterwards as the permanent guard that holder and assembly stay in agreement.
 *
 * <p>Every input map deliberately carries the awkward shapes as well as the ordinary ones: the alias form
 * that must win over the plain one, a raw {@code jdbc.*} passthrough, and keys the base copy-all carries
 * through untouched.
 */
public class IcebergCatalogOptionsSnapshotTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Sorted so an assertion failure renders as a readable diff of the whole option map. */
    private static void assertOptions(Map<String, String> expected, Map<String, String> input, String flavor,
            Optional<S3CompatibleFileSystemProperties> chosenS3) {
        Assertions.assertEquals(new TreeMap<>(expected),
                new TreeMap<>(IcebergCatalogFactory.buildCatalogProperties(input, flavor, chosenS3)));
    }

    private static Optional<S3CompatibleFileSystemProperties> s3(FakeS3CompatibleStorageProperties fake) {
        return Optional.of(fake);
    }

    private static Optional<S3CompatibleFileSystemProperties> noS3() {
        return Optional.empty();
    }

    // ---------------------------------------------------------------------
    // REST
    // ---------------------------------------------------------------------

    /**
     * The floor: a REST catalog that sets nothing optional still emits both client timeouts, because the
     * legacy fields defaulted non-blank and the emission is unconditional.
     */
    @Test
    public void restMinimalSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest.example/api",
                        "warehouse", "s3://bucket/wh",
                        "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                        "rest.client.connection-timeout-ms", "10000",
                        "rest.client.socket-timeout-ms", "60000"),
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest.example/api",
                        "warehouse", "s3://bucket/wh"),
                "rest", noS3());
    }

    /**
     * OAuth2 client-credentials flow with every optional REST knob set. Pins two things per-key tests miss:
     * {@code iceberg.rest.uri} OVERWRITES the plain {@code uri} the copy-all base already carried, and the
     * vended-credentials flag emits a header key rather than travelling as itself.
     */
    @Test
    public void restOAuth2CredentialFlowSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "rest",
                        "iceberg.rest.uri", "https://rest/api",
                        "uri", "https://rest/api",
                        "iceberg.rest.prefix", "main",
                        "iceberg.rest.vended-credentials-enabled", "true",
                        "iceberg.rest.connection-timeout-ms", "5000",
                        "iceberg.rest.socket-timeout-ms", "15000",
                        "iceberg.rest.security.type", "oauth2",
                        "iceberg.rest.oauth2.credential", "cid:csecret",
                        "iceberg.rest.oauth2.server-uri", "https://auth/token",
                        "iceberg.rest.oauth2.scope", "catalog",
                        "iceberg.rest.oauth2.token-refresh-enabled", "false",
                        "s3.region", "us-west-2",
                        "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                        "prefix", "main",
                        "header.X-Iceberg-Access-Delegation", "vended-credentials",
                        "rest.client.connection-timeout-ms", "5000",
                        "rest.client.socket-timeout-ms", "15000",
                        "credential", "cid:csecret",
                        "oauth2-server-uri", "https://auth/token",
                        "scope", "catalog",
                        "token-refresh-enabled", "false",
                        "client.region", "us-west-2"),
                props("iceberg.catalog.type", "rest",
                        "iceberg.rest.uri", "https://rest/api",
                        "uri", "https://plain-uri-loses-to-the-alias",
                        "iceberg.rest.prefix", "main",
                        "iceberg.rest.vended-credentials-enabled", "true",
                        "iceberg.rest.connection-timeout-ms", "5000",
                        "iceberg.rest.socket-timeout-ms", "15000",
                        "iceberg.rest.security.type", "oauth2",
                        "iceberg.rest.oauth2.credential", "cid:csecret",
                        "iceberg.rest.oauth2.server-uri", "https://auth/token",
                        "iceberg.rest.oauth2.scope", "catalog",
                        "iceberg.rest.oauth2.token-refresh-enabled", "false",
                        "s3.region", "us-west-2"),
                "rest", noS3());
    }

    /** Pre-configured token flow: no credential, so the token branch fires and no scope/server-uri appear. */
    @Test
    public void restOAuth2TokenFlowSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.security.type", "oauth2",
                        "iceberg.rest.oauth2.token", "tkn-123",
                        "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                        "rest.client.connection-timeout-ms", "10000",
                        "rest.client.socket-timeout-ms", "60000",
                        "token", "tkn-123"),
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.security.type", "oauth2",
                        "iceberg.rest.oauth2.token", "tkn-123"),
                "rest", noS3());
    }

    /**
     * Surrounding whitespace is stripped from every {@code iceberg.rest.*} value.
     *
     * <p>This CHANGED when the assembly moved onto the bound holder, and the change is the point: the
     * property binder has always trimmed, so a uri written with a trailing space was already validated
     * trimmed at CREATE while the catalog was built from the untrimmed string. The two now agree. Note the
     * raw keys themselves ride through the copy-all base untrimmed — only the values the assembly derives
     * are normalized.
     */
    @Test
    public void restTrimsSurroundingWhitespaceSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "rest",
                        "iceberg.rest.uri", "  https://rest/api  ",
                        "iceberg.rest.security.type", "oauth2",
                        "iceberg.rest.oauth2.token", " tkn-123 ",
                        "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                        "rest.client.connection-timeout-ms", "10000",
                        "rest.client.socket-timeout-ms", "60000",
                        "uri", "https://rest/api",
                        "token", "tkn-123"),
                props("iceberg.catalog.type", "rest",
                        "iceberg.rest.uri", "  https://rest/api  ",
                        "iceberg.rest.security.type", "oauth2",
                        "iceberg.rest.oauth2.token", " tkn-123 "),
                "rest", noS3());
    }

    /**
     * signing-name=glue with a bound generic-S3 store: the signing block takes its credentials from the
     * STORE (not the {@code iceberg.rest.*} keys), and the S3FileIO dialect is emitted on top.
     */
    @Test
    public void restSigningGlueWithBoundS3Snapshot() {
        assertOptions(
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.signing-name", "glue",
                        "iceberg.rest.signing-region", "us-east-1",
                        "iceberg.rest.sigv4-enabled", "true",
                        "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                        "rest.client.connection-timeout-ms", "10000",
                        "rest.client.socket-timeout-ms", "60000",
                        "rest.signing-name", "glue",
                        "rest.sigv4-enabled", "true",
                        "rest.signing-region", "us-east-1",
                        "rest.access-key-id", "AK",
                        "rest.secret-access-key", "SK",
                        "rest.session-token", "ST",
                        "s3.endpoint", "https://s3",
                        "s3.path-style-access", "true",
                        "client.region", "us-east-1",
                        "s3.access-key-id", "AK",
                        "s3.secret-access-key", "SK",
                        "s3.session-token", "ST"),
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.signing-name", "glue",
                        "iceberg.rest.signing-region", "us-east-1",
                        "iceberg.rest.sigv4-enabled", "true"),
                "rest", s3(new FakeS3CompatibleStorageProperties("S3").endpoint("https://s3")
                        .region("us-east-1").accessKey("AK").secretKey("SK").sessionToken("ST")
                        .usePathStyle("true")));
    }

    /**
     * signing-name=glue with a role-only store: the signing block emits the assume-role keys, and the
     * S3FileIO pass emits them a second time (same values) plus the dialect. Pinned because the two
     * emitters are separate code paths that happen to agree.
     */
    @Test
    public void restSigningGlueAssumeRoleSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.signing-name", "glue",
                        "iceberg.rest.signing-region", "us-east-1",
                        "iceberg.rest.sigv4-enabled", "true",
                        "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                        "rest.client.connection-timeout-ms", "10000",
                        "rest.client.socket-timeout-ms", "60000",
                        "rest.signing-name", "glue",
                        "rest.sigv4-enabled", "true",
                        "rest.signing-region", "us-east-1",
                        "client.factory", "org.apache.iceberg.aws.AssumeRoleAwsClientFactory",
                        "aws.region", "us-east-1",
                        "client.assume-role.region", "us-east-1",
                        "client.assume-role.arn", "arn:aws:iam::1:role/r",
                        "client.assume-role.external-id", "eid",
                        "client.region", "us-east-1"),
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.signing-name", "glue",
                        "iceberg.rest.signing-region", "us-east-1",
                        "iceberg.rest.sigv4-enabled", "true"),
                "rest", s3(new FakeS3CompatibleStorageProperties("S3").region("us-east-1")
                        .roleArn("arn:aws:iam::1:role/r").externalId("eid")));
    }

    /**
     * A non-glue signing name takes the explicit {@code iceberg.rest.*} credentials instead. Also pins that
     * the empty-storage region fallback reaches {@code iceberg.rest.signing-region} — it is one of the
     * region aliases, so a catalog that names no S3 region at all still gets a client.region.
     */
    @Test
    public void restSigningOtherNameWithRestCredentialsSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.signing-name", "custom",
                        "iceberg.rest.signing-region", "cn-north-1",
                        "iceberg.rest.sigv4-enabled", "true",
                        "iceberg.rest.access-key-id", "RAK",
                        "iceberg.rest.secret-access-key", "RSK",
                        "iceberg.rest.session-token", "RST",
                        "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                        "rest.client.connection-timeout-ms", "10000",
                        "rest.client.socket-timeout-ms", "60000",
                        "rest.signing-name", "custom",
                        "rest.sigv4-enabled", "true",
                        "rest.signing-region", "cn-north-1",
                        "rest.access-key-id", "RAK",
                        "rest.secret-access-key", "RSK",
                        "rest.session-token", "RST",
                        "client.region", "cn-north-1"),
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.signing-name", "custom",
                        "iceberg.rest.signing-region", "cn-north-1",
                        "iceberg.rest.sigv4-enabled", "true",
                        "iceberg.rest.access-key-id", "RAK",
                        "iceberg.rest.secret-access-key", "RSK",
                        "iceberg.rest.session-token", "RST"),
                "rest", noS3());
    }

    /**
     * PROVIDER_CHAIN: no explicit REST credentials and a non-DEFAULT provider mode pins the provider class.
     * The mode is read from the three-key alias set that spans the s3/glue/rest namespaces.
     */
    @Test
    public void restSigningProviderChainSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.signing-name", "custom",
                        "iceberg.rest.credentials_provider_type", "instance-profile",
                        "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                        "rest.client.connection-timeout-ms", "10000",
                        "rest.client.socket-timeout-ms", "60000",
                        "rest.signing-name", "custom",
                        "rest.sigv4-enabled", "",
                        "rest.signing-region", "",
                        "client.credentials-provider",
                        "software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider"),
                props("iceberg.catalog.type", "rest",
                        "uri", "https://rest/api",
                        "iceberg.rest.signing-name", "custom",
                        "iceberg.rest.credentials_provider_type", "instance-profile"),
                "rest", noS3());
    }

    // ---------------------------------------------------------------------
    // GLUE
    // ---------------------------------------------------------------------

    /**
     * Glue with explicit AK/SK: the five s3.* FileIO keys come unconditionally from the bound store (empty
     * strings included — legacy plain puts), the region is extracted from the endpoint host, and the
     * credentials go through the Doris configuration provider rather than the SDK chain.
     */
    @Test
    public void glueStaticCredentialsSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "glue",
                        "warehouse", "s3://bucket/wh",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com",
                        "glue.access_key", "GAK",
                        "glue.secret_key", "GSK",
                        "aws.glue.session-token", "GST",
                        "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog",
                        "s3.access-key-id", "AK",
                        "s3.secret-access-key", "SK",
                        "s3.endpoint", "https://s3",
                        "s3.path-style-access", "false",
                        "s3.session-token", "ST",
                        "client.credentials-provider",
                        "org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x",
                        "client.credentials-provider.glue.access_key", "GAK",
                        "client.credentials-provider.glue.secret_key", "GSK",
                        "client.credentials-provider.glue.session_token", "GST",
                        "client.region", "us-east-1"),
                props("iceberg.catalog.type", "glue",
                        "warehouse", "s3://bucket/wh",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com",
                        "glue.access_key", "GAK",
                        "glue.secret_key", "GSK",
                        "aws.glue.session-token", "GST"),
                "glue", s3(new FakeS3CompatibleStorageProperties("S3").endpoint("https://s3")
                        .accessKey("AK").secretKey("SK").sessionToken("ST").usePathStyle("false")));
    }

    /**
     * Glue with an IAM role and no bound storage: the assume-role block replaces the credentials provider,
     * and the warehouse placeholder appears because the catalog set none (glue does not use a warehouse but
     * the SDK requires the key to be present).
     */
    @Test
    public void glueAssumeRoleWithoutWarehouseSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "glue",
                        "glue.endpoint", "https://glue.eu-west-1.amazonaws.com",
                        "glue.role_arn", "arn:aws:iam::1:role/r",
                        "glue.external_id", "eid",
                        "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog",
                        "client.factory", "org.apache.iceberg.aws.AssumeRoleAwsClientFactory",
                        "aws.region", "eu-west-1",
                        "client.assume-role.arn", "arn:aws:iam::1:role/r",
                        "client.assume-role.region", "eu-west-1",
                        "client.assume-role.external-id", "eid",
                        "client.region", "eu-west-1",
                        "warehouse", "s3://doris"),
                props("iceberg.catalog.type", "glue",
                        "glue.endpoint", "https://glue.eu-west-1.amazonaws.com",
                        "glue.role_arn", "arn:aws:iam::1:role/r",
                        "glue.external_id", "eid"),
                "glue", noS3());
    }

    /** An explicit region alias beats the endpoint-derived one; {@code aws.glue.region} is the last alias. */
    @Test
    public void glueExplicitRegionAliasSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "glue",
                        "warehouse", "s3://bucket/wh",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com",
                        "aws.glue.region", "ap-southeast-1",
                        "glue.access_key", "GAK",
                        "glue.secret_key", "GSK",
                        "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog",
                        "client.credentials-provider",
                        "org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x",
                        "client.credentials-provider.glue.access_key", "GAK",
                        "client.credentials-provider.glue.secret_key", "GSK",
                        "client.region", "ap-southeast-1"),
                props("iceberg.catalog.type", "glue",
                        "warehouse", "s3://bucket/wh",
                        "glue.endpoint", "https://glue.us-east-1.amazonaws.com",
                        "aws.glue.region", "ap-southeast-1",
                        "glue.access_key", "GAK",
                        "glue.secret_key", "GSK"),
                "glue", noS3());
    }

    /**
     * Neither an explicit region alias nor a parseable endpoint host: the region falls back to the legacy
     * {@code us-east-1} literal rather than being omitted, which is what keeps the glue client buildable.
     */
    @Test
    public void glueRegionFallsBackToUsEast1Snapshot() {
        assertOptions(
                props("iceberg.catalog.type", "glue",
                        "warehouse", "s3://bucket/wh",
                        "glue.endpoint", "https://vpce-1234.glue.privatelink.aws",
                        "glue.role_arn", "arn:aws:iam::1:role/r",
                        "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog",
                        "client.factory", "org.apache.iceberg.aws.AssumeRoleAwsClientFactory",
                        "aws.region", "us-east-1",
                        "client.assume-role.arn", "arn:aws:iam::1:role/r",
                        "client.assume-role.region", "us-east-1",
                        "client.region", "us-east-1"),
                props("iceberg.catalog.type", "glue",
                        "warehouse", "s3://bucket/wh",
                        "glue.endpoint", "https://vpce-1234.glue.privatelink.aws",
                        "glue.role_arn", "arn:aws:iam::1:role/r"),
                "glue", noS3());
    }

    // ---------------------------------------------------------------------
    // JDBC
    // ---------------------------------------------------------------------

    /**
     * A jdbc catalog that names no uri at all still gets the key, as an EMPTY string — the legacy field
     * defaulted to "" and the put is unconditional. Pinned because "absent" and "present but empty" are
     * different inputs to the SDK.
     */
    @Test
    public void jdbcWithoutUriEmitsEmptyUriSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "jdbc",
                        "warehouse", "s3://bucket/wh",
                        "uri", "",
                        "catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog"),
                props("iceberg.catalog.type", "jdbc",
                        "warehouse", "s3://bucket/wh",
                        "iceberg.jdbc.catalog_name", "mycat"),
                "jdbc", noS3());
    }

    /**
     * Every jdbc knob set: the dotted {@code jdbc.*} options are derived from the {@code iceberg.jdbc.*}
     * aliases, a raw {@code jdbc.*} key rides through on the base copy-all, and the positional catalog_name
     * is REMOVED from the map (it is passed as the catalog name argument instead).
     */
    @Test
    public void jdbcFullSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "jdbc",
                        "uri", "jdbc:postgresql://h/db",
                        "warehouse", "s3://bucket/wh",
                        "iceberg.jdbc.user", "u",
                        "iceberg.jdbc.password", "p",
                        "iceberg.jdbc.init-catalog-tables", "true",
                        "iceberg.jdbc.schema-version", "V1",
                        "iceberg.jdbc.strict-mode", "false",
                        "jdbc.useSSL", "false",
                        "catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog",
                        "jdbc.user", "u",
                        "jdbc.password", "p",
                        "jdbc.init-catalog-tables", "true",
                        "jdbc.schema-version", "V1",
                        "jdbc.strict-mode", "false"),
                props("iceberg.catalog.type", "jdbc",
                        "uri", "jdbc:postgresql://h/db",
                        "warehouse", "s3://bucket/wh",
                        "iceberg.jdbc.catalog_name", "mycat",
                        "iceberg.jdbc.user", "u",
                        "iceberg.jdbc.password", "p",
                        "iceberg.jdbc.init-catalog-tables", "true",
                        "iceberg.jdbc.schema-version", "V1",
                        "iceberg.jdbc.strict-mode", "false",
                        "jdbc.useSSL", "false"),
                "jdbc", noS3());
    }

    /**
     * The jdbc uri alias order is the REVERSE of the rest one: plain {@code uri} wins over
     * {@code iceberg.jdbc.uri}. Getting this backwards would silently connect a live catalog to the other
     * database, so it is pinned as a whole-map fact.
     */
    @Test
    public void jdbcUriAliasOrderSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "jdbc",
                        "uri", "jdbc:postgresql://plain/db",
                        "iceberg.jdbc.uri", "jdbc:postgresql://prefixed/db",
                        "warehouse", "s3://bucket/wh",
                        "catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog"),
                props("iceberg.catalog.type", "jdbc",
                        "uri", "jdbc:postgresql://plain/db",
                        "iceberg.jdbc.uri", "jdbc:postgresql://prefixed/db",
                        "warehouse", "s3://bucket/wh",
                        "iceberg.jdbc.catalog_name", "mycat"),
                "jdbc", noS3());
    }

    // ---------------------------------------------------------------------
    // HMS / HADOOP / S3TABLES
    // ---------------------------------------------------------------------

    /**
     * HMS emits NO S3FileIO options even with a fully-credentialed store bound: object-store access rides
     * the HiveConf, which is a separate sink. A bound store leaking s3.* here would change what the live
     * catalog authenticates with.
     */
    @Test
    public void hmsSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://h:9083",
                        "warehouse", "s3://bucket/wh",
                        "catalog-impl", "org.apache.iceberg.hive.HiveCatalog"),
                props("iceberg.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://h:9083",
                        "warehouse", "s3://bucket/wh"),
                "hms", s3(new FakeS3CompatibleStorageProperties("S3").endpoint("https://s3")
                        .accessKey("AK").secretKey("SK")));
    }

    /**
     * Hadoop over a NON-generic store (OSS): the s3.* dialect is emitted but the assume-role block is not,
     * because that block is reserved for the generic S3 provider. Blank getters emit nothing.
     */
    @Test
    public void hadoopWithNonGenericStoreSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "hadoop",
                        "warehouse", "oss://bucket/wh",
                        "catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog",
                        "s3.endpoint", "https://oss",
                        "client.region", "cn-hangzhou",
                        "s3.access-key-id", "AK",
                        "s3.secret-access-key", "SK"),
                props("iceberg.catalog.type", "hadoop",
                        "warehouse", "oss://bucket/wh"),
                "hadoop", s3(new FakeS3CompatibleStorageProperties("OSS").endpoint("https://oss")
                        .region("cn-hangzhou").accessKey("AK").secretKey("SK")));
    }

    /**
     * The manifest cache is derived, not copied: the FE-side {@code meta.cache.iceberg.manifest.*} spec
     * turns into the iceberg {@code io.manifest.cache-enabled} option when enabled with a non-zero ttl and
     * capacity. Only the derived key is added — the meta.cache keys themselves ride through on copy-all.
     */
    @Test
    public void manifestCacheDerivationSnapshot() {
        assertOptions(
                props("iceberg.catalog.type", "hadoop",
                        "warehouse", "s3://bucket/wh",
                        "meta.cache.iceberg.manifest.enable", "true",
                        "meta.cache.iceberg.manifest.ttl-second", "3600",
                        "meta.cache.iceberg.manifest.capacity", "512",
                        "catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog",
                        "io.manifest.cache-enabled", "true"),
                props("iceberg.catalog.type", "hadoop",
                        "warehouse", "s3://bucket/wh",
                        "meta.cache.iceberg.manifest.enable", "true",
                        "meta.cache.iceberg.manifest.ttl-second", "3600",
                        "meta.cache.iceberg.manifest.capacity", "512"),
                "hadoop", noS3());
    }

    /**
     * s3tables is built through the 3-arg initialize, so it gets NEITHER a catalog-impl NOR the {@code type}
     * removal, and static credentials suppress the assume-role block entirely (EXPLICIT wins).
     */
    @Test
    public void s3TablesWithStaticCredentialsSnapshot() {
        Map<String, String> expected = props("iceberg.catalog.type", "s3tables",
                "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b",
                "s3.role_arn", "arn:aws:iam::1:role/ignored",
                "client.region", "us-east-1",
                "s3.access-key-id", "AK",
                "s3.secret-access-key", "SK");
        Map<String, String> actual = IcebergCatalogFactory.buildS3TablesCatalogProperties(
                props("iceberg.catalog.type", "s3tables",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b",
                        "s3.role_arn", "arn:aws:iam::1:role/ignored"),
                Optional.of(new FakeS3CompatibleStorageProperties("S3").region("us-east-1")
                        .accessKey("AK").secretKey("SK").roleArn("arn:aws:iam::1:role/ignored")));
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }

    /** No bound storage: the region still reaches client.region through the raw alias scan. */
    @Test
    public void s3TablesWithoutBoundStorageSnapshot() {
        Map<String, String> expected = props("iceberg.catalog.type", "s3tables",
                "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b",
                "s3.region", "us-east-1",
                "client.region", "us-east-1");
        Map<String, String> actual = IcebergCatalogFactory.buildS3TablesCatalogProperties(
                props("iceberg.catalog.type", "s3tables",
                        "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b",
                        "s3.region", "us-east-1"),
                Optional.empty());
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }
}
