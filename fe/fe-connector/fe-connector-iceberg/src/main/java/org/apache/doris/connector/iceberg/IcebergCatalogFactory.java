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
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pure, testable assembly core for the Iceberg connector flavor switch — the iceberg-SDK-specific bits
 * that stay in the connector. Mirrors the role of {@code PaimonCatalogFactory}: a stateless static
 * holder whose methods are PURE (they read only the supplied props — no env, no clock, no live
 * catalog), which is what makes them unit-testable offline.
 *
 * <p>P6.1 (this task) holds only the flavor resolution: {@link #resolveFlavor(Map)} (the lower-cased
 * {@code iceberg.catalog.type}) and {@link #resolveCatalogImpl(String)} (the catalog-impl class name
 * for the five {@code CatalogUtil}-built flavors plus the two bespoke ones). The full per-flavor
 * property / Hadoop-{@code Configuration} / {@code HiveConf} assembly currently dropped by the
 * skeleton — ported from the fe-core {@code AbstractIcebergProperties} + each
 * {@code Iceberg*MetaStoreProperties#initCatalog} — lands in a later task (P6-T05/T06/T07); this task
 * is a structural inversion only, with no behavior change.
 *
 * <p>Note: {@code s3tables} and {@code dlf} are listed here for completeness, but legacy does NOT build
 * them via {@code CatalogUtil.buildIcebergCatalog} (s3tables hand-builds an {@code S3TablesClient};
 * dlf uses {@code new DLFCatalog().setConf(..).initialize(..)}). Routing them through the impl-name
 * path is the existing skeleton behavior, preserved verbatim here; the bespoke instantiation fixes are
 * P6-T06 / P6-T07.
 */
public final class IcebergCatalogFactory {

    // Manifest-cache derivation defaults — mirror fe-core IcebergExternalCatalog so the
    // meta.cache.iceberg.manifest.* -> io.manifest.cache-enabled derivation matches legacy exactly.
    private static final boolean DEFAULT_MANIFEST_CACHE_ENABLE = false;
    private static final long DEFAULT_MANIFEST_CACHE_TTL_SECOND = 48L * 60 * 60;
    private static final long DEFAULT_MANIFEST_CACHE_CAPACITY = 1024L;

    // Mirror of legacy AWSGlueMetaStoreBaseProperties.ENDPOINT_PATTERN: extracts the region from a glue
    // endpoint host (e.g. glue.us-east-1.amazonaws.com / glue-fips.us-east-1.api.aws) when no explicit
    // glue.region is set, before falling back to us-east-1.
    private static final Pattern GLUE_ENDPOINT_PATTERN = Pattern.compile(
            "^(?:https?://)?(?:glue|glue-fips)\\.([a-z0-9-]+)\\.(?:api\\.aws|amazonaws\\.com)$");

    // Region-field aliases scanned to propagate client.region when NO fe-filesystem S3 storage is bound
    // (e.g. REST vended credentials: no static AK/SK/role, so S3FileSystemProvider.supports is false and
    // chosenS3 is empty). Mirrors the legacy AbstractIcebergProperties.toFileIOProperties chosen==null
    // fallback (getRegionFromProperties). The raw s3.region copied by buildBaseCatalogProperties is inert
    // because iceberg S3FileIO reads client.region, not s3.region.
    private static final String[] S3_REGION_ALIASES = {"s3.region", "aws.region", "region", "client.region"};

    private IcebergCatalogFactory() {
    }

    /**
     * Builds the COMMON iceberg catalog-property map shared by all five {@code CatalogUtil} flavors,
     * mirroring the legacy {@code AbstractIcebergProperties.initializeCatalog} base: (1) seed from ALL
     * raw props (legacy {@code getOrigProps()} copy-all — arbitrary user/iceberg keys pass through to
     * the SDK; the per-flavor appenders + the connector add the derived keys on top), (2) map
     * {@code warehouse} to {@link CatalogProperties#WAREHOUSE_LOCATION}, (3) add manifest-cache keys.
     * PURE: depends only on {@code props}. The flavor's {@code catalog-impl} and the {@code type}
     * removal are applied by the caller (the connector / per-flavor path).
     */
    public static Map<String, String> buildBaseCatalogProperties(Map<String, String> props) {
        Map<String, String> opts = new HashMap<>(props);
        String warehouse = props.get(CatalogProperties.WAREHOUSE_LOCATION);
        if (StringUtils.isNotBlank(warehouse)) {
            opts.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }
        appendManifestCacheProperties(props, opts);
        return opts;
    }

    /**
     * Mirrors legacy {@code AbstractIcebergProperties.addManifestCacheProperties}: pass through any
     * explicitly-set {@code io.manifest.cache.*} keys, then — only when the user did NOT set
     * {@code io.manifest.cache-enabled} directly — derive it to {@code "true"} from the FE meta-cache
     * spec ({@code meta.cache.iceberg.manifest.*}) using the same {@code enable && ttl != 0 &&
     * capacity != 0} rule. Default-disabled (legacy {@code DEFAULT_ICEBERG_MANIFEST_CACHE_ENABLE}).
     */
    private static void appendManifestCacheProperties(Map<String, String> props, Map<String, String> opts) {
        boolean hasExplicitEnabled = StringUtils.isNotBlank(props.get(CatalogProperties.IO_MANIFEST_CACHE_ENABLED));
        copyIfPresent(props, opts, CatalogProperties.IO_MANIFEST_CACHE_ENABLED);
        copyIfPresent(props, opts, CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS);
        copyIfPresent(props, opts, CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES);
        copyIfPresent(props, opts, CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH);
        if (!hasExplicitEnabled) {
            CacheSpec spec = CacheSpec.fromProperties(props,
                    IcebergConnectorProperties.MANIFEST_CACHE_ENABLE, DEFAULT_MANIFEST_CACHE_ENABLE,
                    IcebergConnectorProperties.MANIFEST_CACHE_TTL, DEFAULT_MANIFEST_CACHE_TTL_SECOND,
                    IcebergConnectorProperties.MANIFEST_CACHE_CAPACITY, DEFAULT_MANIFEST_CACHE_CAPACITY);
            if (CacheSpec.isCacheEnabled(spec.isEnable(), spec.getTtlSecond(), spec.getCapacity())) {
                opts.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
            }
        }
    }

    private static void copyIfPresent(Map<String, String> props, Map<String, String> opts, String key) {
        String value = props.get(key);
        if (StringUtils.isNotBlank(value)) {
            opts.put(key, value);
        }
    }

    /**
     * Selects the S3-compatible storage whose iceberg S3FileIO config should be emitted, mirroring
     * legacy {@code AbstractIcebergProperties.toFileIOProperties}: prefer the first NON-generic-S3
     * provider (an explicit {@code OSS}/{@code COS}/{@code OBS} choice trumps the generic {@code S3}
     * fallback), else the first S3-compatible storage. The generic-S3 analog of legacy {@code
     * S3Properties} is identified by {@code providerName().equals("S3")} — note OSS/COS/OBS all report
     * {@code FileSystemType.S3}, so the provider NAME (not {@code type()}) is the discriminator. PURE.
     */
    public static Optional<S3CompatibleFileSystemProperties> chooseS3Compatible(
            List<? extends StorageProperties> storages) {
        S3CompatibleFileSystemProperties fallback = null;
        S3CompatibleFileSystemProperties target = null;
        for (StorageProperties sp : storages) {
            if (sp instanceof S3CompatibleFileSystemProperties) {
                S3CompatibleFileSystemProperties s3 = (S3CompatibleFileSystemProperties) sp;
                if (fallback == null) {
                    fallback = s3;
                }
                if (target == null && !"S3".equals(s3.providerName())) {
                    target = s3;
                }
            }
        }
        return Optional.ofNullable(target != null ? target : fallback);
    }

    /**
     * Emits the iceberg {@code S3FileIO} catalog properties from the chosen fe-filesystem S3-compatible
     * storage, mirroring legacy {@code AbstractIcebergProperties.toS3FileIOProperties} (D-061): the
     * connector reads the typed {@link S3CompatibleFileSystemProperties} getters and writes the iceberg
     * S3FileIO dialect ({@code s3.*} + {@link AwsClientProperties#CLIENT_REGION}); the assume-role block
     * (the legacy {@code IcebergAwsAssumeRoleProperties} analog) is emitted only for the generic
     * {@code S3} provider (legacy {@code instanceof S3Properties}). Every put is blank-guarded. PURE.
     */
    public static void appendS3FileIOProperties(Map<String, String> opts, S3CompatibleFileSystemProperties s3) {
        putS3FileIODialect(opts, s3);
        if ("S3".equals(s3.providerName())) {
            appendAssumeRoleProperties(opts, s3);
        }
    }

    /**
     * Emits the S3FileIO dialect from the bound fe-filesystem S3 storage when present; otherwise (no bound
     * S3 storage, e.g. a REST catalog with vended credentials and no static AK/SK) still propagates
     * {@code client.region} from the raw catalog properties so S3FileIO does not fall through to the AWS
     * SDK {@code DefaultAwsRegionProviderChain} and fail the write commit with "Unable to load region".
     * Legacy parity with {@code AbstractIcebergProperties.toFileIOProperties}, whose {@code chosen == null}
     * branch supplied the region for exactly the rest/hadoop/jdbc flavors.
     */
    private static void appendS3FileIO(Map<String, String> opts, Map<String, String> props,
            Optional<S3CompatibleFileSystemProperties> chosenS3) {
        if (chosenS3.isPresent()) {
            appendS3FileIOProperties(opts, chosenS3.get());
        } else {
            putIfNotBlank(opts, AwsClientProperties.CLIENT_REGION, firstNonBlank(props, S3_REGION_ALIASES));
        }
    }

    /**
     * Emits ONLY the {@code s3.*} + {@link AwsClientProperties#CLIENT_REGION} S3FileIO dialect keys (no
     * credential-type block), shared by {@link #appendS3FileIOProperties} (rest/hadoop/jdbc/glue) and the
     * s3tables emitter. Every put is blank-guarded.
     */
    private static void putS3FileIODialect(Map<String, String> opts, S3CompatibleFileSystemProperties s3) {
        putIfNotBlank(opts, S3FileIOProperties.ENDPOINT, s3.getEndpoint());
        putIfNotBlank(opts, S3FileIOProperties.PATH_STYLE_ACCESS, s3.getUsePathStyle());
        putIfNotBlank(opts, AwsClientProperties.CLIENT_REGION, s3.getRegion());
        putIfNotBlank(opts, S3FileIOProperties.ACCESS_KEY_ID, s3.getAccessKey());
        putIfNotBlank(opts, S3FileIOProperties.SECRET_ACCESS_KEY, s3.getSecretKey());
        putIfNotBlank(opts, S3FileIOProperties.SESSION_TOKEN, s3.getSessionToken());
    }

    /**
     * Emits the s3tables S3FileIO credential block mirroring legacy
     * {@code IcebergAwsClientCredentialsProperties.putS3FileIOCredentialProperties} — the EXPLICIT-wins ladder,
     * which is DISTINCT from the generic {@link #appendS3FileIOProperties} ({@code toS3FileIOProperties}) used by
     * rest/hadoop/jdbc/glue: the {@code s3.*} dialect always, then ONLY the credential-type addition.
     * {@code EXPLICIT} (static AK/SK present) adds NOTHING — the static keys suffice and, per legacy
     * {@code getCredentialType}, EXPLICIT precedes ASSUME_ROLE so a role ARN is ignored when static creds are set.
     * {@code ASSUME_ROLE} (role ARN, no static) adds the assume-role block. {@code PROVIDER_CHAIN} sets
     * {@code client.credentials-provider} to the non-DEFAULT provider class the user selected (F14; DEFAULT ->
     * nothing, SDK default chain). PURE.
     */
    private static void appendS3TablesFileIOProperties(Map<String, String> opts, S3CompatibleFileSystemProperties s3,
            Map<String, String> props) {
        putS3FileIODialect(opts, s3);
        if (s3.hasStaticCredentials()) {
            return;
        }
        if (s3.hasAssumeRole()) {
            appendAssumeRoleProperties(opts, s3);
        } else {
            // F14: PROVIDER_CHAIN — pin the non-DEFAULT provider class (mirrors legacy putCredentialsProvider).
            putIfNotBlank(opts, AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                    AwsCredentialsProviderModes.classNameFor(props, AwsCredentialsProviderModes.S3_MODE_KEYS));
        }
    }

    /**
     * Mirrors legacy {@code IcebergAwsAssumeRoleProperties.putAssumeRoleProperties}: no-op unless the
     * role ARN is set; otherwise wires {@link AssumeRoleAwsClientFactory} + the {@code aws.region} alias
     * and {@code client.assume-role.*} keys (external-id only when present).
     */
    private static void appendAssumeRoleProperties(Map<String, String> opts, S3CompatibleFileSystemProperties s3) {
        if (StringUtils.isBlank(s3.getRoleArn())) {
            return;
        }
        opts.put(AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName());
        opts.put("aws.region", s3.getRegion());
        opts.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, s3.getRegion());
        opts.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, s3.getRoleArn());
        if (StringUtils.isNotBlank(s3.getExternalId())) {
            opts.put(AwsProperties.CLIENT_ASSUME_ROLE_EXTERNAL_ID, s3.getExternalId());
        }
    }

    private static void putIfNotBlank(Map<String, String> opts, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            opts.put(key, value);
        }
    }

    /** Resolves the lower-cased flavor from {@code iceberg.catalog.type}; null/blank stays null. */
    public static String resolveFlavor(Map<String, String> props) {
        String catalogType = props.get(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE);
        if (catalogType == null || catalogType.isEmpty()) {
            return null;
        }
        return catalogType.toLowerCase(Locale.ROOT);
    }

    /**
     * Resolve the Iceberg catalog implementation class name from the catalog type string. PURE:
     * depends only on {@code catalogType}. Lifted verbatim from the former
     * {@code IcebergConnector.resolveCatalogImpl}.
     */
    public static String resolveCatalogImpl(String catalogType) {
        if (catalogType == null) {
            throw new DorisConnectorException(
                    "Missing '" + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + "' property");
        }
        switch (catalogType.toLowerCase(Locale.ROOT)) {
            case IcebergConnectorProperties.TYPE_REST:
                return "org.apache.iceberg.rest.RESTCatalog";
            case IcebergConnectorProperties.TYPE_HMS:
                return "org.apache.iceberg.hive.HiveCatalog";
            case IcebergConnectorProperties.TYPE_GLUE:
                return "org.apache.iceberg.aws.glue.GlueCatalog";
            case IcebergConnectorProperties.TYPE_HADOOP:
                return "org.apache.iceberg.hadoop.HadoopCatalog";
            case IcebergConnectorProperties.TYPE_JDBC:
                return "org.apache.iceberg.jdbc.JdbcCatalog";
            case IcebergConnectorProperties.TYPE_S3_TABLES:
                return "software.amazon.s3tables.iceberg.S3TablesCatalog";
            case IcebergConnectorProperties.TYPE_DLF:
                return "org.apache.doris.connector.iceberg.dlf.DLFCatalog";
            default:
                throw new DorisConnectorException(
                        "Unknown " + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + ": " + catalogType
                                + ". Supported types: rest, hms, glue, hadoop, jdbc, s3tables, dlf");
        }
    }

    /**
     * Assembles the full iceberg catalog OPTIONS map for one of the five {@code CatalogUtil}-built flavors
     * (rest / hms / glue / hadoop / jdbc), mirroring the legacy fe-core
     * {@code AbstractIcebergProperties.initializeCatalog} base + each {@code Iceberg*MetaStoreProperties#initCatalog}:
     * the common base (copy-all + warehouse + manifest cache), the flavor's {@code catalog-impl}, the
     * per-flavor derivations, the S3FileIO dialect (for rest/hadoop/jdbc; glue emits its own), the jdbc
     * {@code catalog_name} positional removal, and finally the removal of the {@code type} key (the iceberg SDK
     * forbids both {@code type} and {@code catalog-impl}). PURE: a function of {@code props} + {@code chosenS3}.
     *
     * <p>The metastore connection (HMS {@code HiveConf}) and storage {@code Configuration} are SEPARATE sinks
     * built by the connector ({@link #assembleHiveConf} / {@link #buildHadoopConfiguration}); they are not part
     * of this options map. {@code s3tables}/{@code dlf} are bespoke (T06/T07) and fall through to the base +
     * impl only here (the existing skeleton behavior), so this method covers exactly the five SDK-built flavors.
     */
    public static Map<String, String> buildCatalogProperties(Map<String, String> props, String flavor,
            Optional<S3CompatibleFileSystemProperties> chosenS3) {
        Map<String, String> opts = buildBaseCatalogProperties(props);
        opts.put(CatalogProperties.CATALOG_IMPL, resolveCatalogImpl(flavor));
        switch (flavor) {
            case IcebergConnectorProperties.TYPE_REST:
                appendRestProperties(opts, props, chosenS3);
                appendS3FileIO(opts, props, chosenS3);
                break;
            case IcebergConnectorProperties.TYPE_GLUE:
                // glue emits its OWN s3.* (unconditional) + glue-client creds; it does NOT use the base
                // S3FileIO path (legacy IcebergGlueMetaStoreProperties ignores storagePropertiesList).
                appendGlueProperties(opts, props, chosenS3);
                break;
            case IcebergConnectorProperties.TYPE_JDBC:
                appendJdbcProperties(opts, props);
                appendS3FileIO(opts, props, chosenS3);
                // iceberg.jdbc.catalog_name is the positional catalog NAME (see resolveCatalogName); legacy
                // removes it from the options map before building.
                opts.remove(IcebergConnectorProperties.JDBC_CATALOG_NAME);
                break;
            case IcebergConnectorProperties.TYPE_HMS:
                // No S3FileIO options: legacy iceberg HMS does not call toFileIOProperties; object-store access
                // rides the HiveConf (fs.s3a.* from storage), built by the connector via assembleHiveConf.
                break;
            case IcebergConnectorProperties.TYPE_HADOOP:
                appendS3FileIO(opts, props, chosenS3);
                break;
            default:
                // s3tables / dlf: bespoke instantiation is T06/T07. Preserve the skeleton's base+impl routing.
                break;
        }
        // The iceberg SDK forbids both "type" and "catalog-impl"; legacy buildIcebergCatalog removes "type".
        opts.remove(CatalogUtil.ICEBERG_CATALOG_TYPE);
        return opts;
    }

    /**
     * Assembles the iceberg catalog OPTIONS map for the BESPOKE {@code s3tables} flavor, mirroring the legacy
     * fe-core {@code AbstractIcebergProperties.initializeCatalog} base + {@code IcebergS3TablesMetaStoreProperties}
     * {@code buildS3CatalogProperties}: the common base (copy-all + warehouse=table-bucket ARN + manifest cache)
     * plus the {@code S3FileIO} dialect ({@code client.region} + {@code s3.*}) and the EXPLICIT-wins credential
     * block ({@link #appendS3TablesFileIOProperties}) — which, unlike the generic rest/hadoop/jdbc FileIO path,
     * suppresses the assume-role keys when static AK/SK are present (legacy {@code putS3FileIOCredentialProperties}
     * returns early for EXPLICIT). PURE: a function of {@code props} + {@code chosenS3}.
     *
     * <p>Unlike {@link #buildCatalogProperties}, this does NOT add a {@code catalog-impl} and does NOT remove the
     * {@code type} key: s3tables is built by the connector via {@code new S3TablesCatalog().initialize(name, opts,
     * client)} (the 3-arg path), NOT by {@code CatalogUtil.buildIcebergCatalog}, so neither the catalog-impl nor
     * the type-exclusion the SDK demands of the {@code CatalogUtil} path applies. The only hard requirement of the
     * 3-arg initialize is a non-blank {@code warehouse} (the table-bucket ARN), carried here by the base copy-all.
     * The control-plane {@code S3TablesClient} (region + credentials + endpoint + http) is built LIVE by the
     * connector ({@code IcebergConnector.buildS3TablesClient}); it is not part of this pure options map.
     */
    public static Map<String, String> buildS3TablesCatalogProperties(Map<String, String> props,
            Optional<S3CompatibleFileSystemProperties> chosenS3) {
        Map<String, String> opts = buildBaseCatalogProperties(props);
        chosenS3.ifPresent(s3 -> appendS3TablesFileIOProperties(opts, s3, props));
        return opts;
    }

    /**
     * Resolves the catalog NAME to pass to {@code CatalogUtil.buildIcebergCatalog}. For the jdbc flavor this is
     * the required {@code iceberg.jdbc.catalog_name} (legacy passes it as the positional {@code catalogName} arg,
     * overriding the Doris catalog name); every other flavor uses {@code defaultName} (the Doris catalog name).
     */
    public static String resolveCatalogName(Map<String, String> props, String flavor, String defaultName) {
        if (IcebergConnectorProperties.TYPE_JDBC.equals(flavor)) {
            String name = firstNonBlank(props, IcebergConnectorProperties.JDBC_CATALOG_NAME);
            if (StringUtils.isBlank(name)) {
                throw new DorisConnectorException(
                        IcebergConnectorProperties.JDBC_CATALOG_NAME + " is required for an iceberg jdbc catalog");
            }
            return name;
        }
        return defaultName;
    }

    // ---------------------------------------------------------------------
    // REST appender (mirror IcebergRestProperties.initIcebergRestCatalogProperties)
    // ---------------------------------------------------------------------

    /**
     * Mirrors legacy {@code IcebergRestProperties}: core ({@code uri} always, default empty), optional
     * ({@code prefix} / vended-credentials header / the two effectively-always timeouts), oauth2, and the glue
     * sigv4 signing block (with credentials sourced from the chosen S3 store for glue/s3tables, else from the
     * {@code iceberg.rest.*} aliases). PURE.
     */
    public static void appendRestProperties(Map<String, String> opts, Map<String, String> props,
            Optional<S3CompatibleFileSystemProperties> chosenS3) {
        // Core: uri is put UNCONDITIONALLY (legacy field default ""), alias priority iceberg.rest.uri > uri.
        opts.put(CatalogProperties.URI,
                firstNonBlankOrEmpty(props, IcebergConnectorProperties.REST_URI, IcebergConnectorProperties.URI));
        // Optional.
        putIfNotBlank(opts, IcebergConnectorProperties.REST_PREFIX_KEY,
                firstNonBlank(props, IcebergConnectorProperties.REST_PREFIX));
        String vendedEnabled =
                firstNonBlankOrEmpty(props, IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED);
        if (Boolean.parseBoolean(vendedEnabled)) {
            opts.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_HEADER,
                    IcebergConnectorProperties.REST_VENDED_CREDENTIALS_VALUE);
        }
        // Timeouts: legacy fields default non-blank, so they are effectively always emitted.
        opts.put(IcebergConnectorProperties.REST_CONNECTION_TIMEOUT_MS_KEY,
                firstNonBlankOr(props, IcebergConnectorProperties.DEFAULT_REST_CONNECTION_TIMEOUT_MS,
                        IcebergConnectorProperties.REST_CONNECTION_TIMEOUT_MS));
        opts.put(IcebergConnectorProperties.REST_SOCKET_TIMEOUT_MS_KEY,
                firstNonBlankOr(props, IcebergConnectorProperties.DEFAULT_REST_SOCKET_TIMEOUT_MS,
                        IcebergConnectorProperties.REST_SOCKET_TIMEOUT_MS));
        appendRestOAuth2Properties(opts, props);
        appendRestSigningProperties(opts, props, chosenS3);
    }

    private static void appendRestOAuth2Properties(Map<String, String> opts, Map<String, String> props) {
        String securityType = firstNonBlank(props, IcebergConnectorProperties.REST_SECURITY_TYPE);
        if (!IcebergConnectorProperties.SECURITY_TYPE_OAUTH2.equalsIgnoreCase(securityType)) {
            return;
        }
        String credential = firstNonBlank(props, IcebergConnectorProperties.REST_OAUTH2_CREDENTIAL);
        if (StringUtils.isNotBlank(credential)) {
            // Client Credentials Flow.
            opts.put(OAuth2Properties.CREDENTIAL, credential);
            putIfNotBlank(opts, OAuth2Properties.OAUTH2_SERVER_URI,
                    firstNonBlank(props, IcebergConnectorProperties.REST_OAUTH2_SERVER_URI));
            putIfNotBlank(opts, OAuth2Properties.SCOPE,
                    firstNonBlank(props, IcebergConnectorProperties.REST_OAUTH2_SCOPE));
            opts.put(OAuth2Properties.TOKEN_REFRESH_ENABLED,
                    firstNonBlankOr(props, String.valueOf(OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT),
                            IcebergConnectorProperties.REST_OAUTH2_TOKEN_REFRESH_ENABLED));
        } else {
            // Pre-configured Token Flow (validation guarantees a token here when credential is absent).
            opts.put(OAuth2Properties.TOKEN,
                    firstNonBlankOrEmpty(props, IcebergConnectorProperties.REST_OAUTH2_TOKEN));
        }
    }

    private static void appendRestSigningProperties(Map<String, String> opts, Map<String, String> props,
            Optional<S3CompatibleFileSystemProperties> chosenS3) {
        String signingName = firstNonBlank(props, IcebergConnectorProperties.REST_SIGNING_NAME);
        if (StringUtils.isBlank(signingName)) {
            return;
        }
        // signing-name is case-sensitive; do not lower-case it.
        opts.put(IcebergConnectorProperties.REST_SIGNING_NAME_KEY, signingName);
        opts.put(IcebergConnectorProperties.REST_SIGV4_ENABLED_KEY,
                firstNonBlankOrEmpty(props, IcebergConnectorProperties.REST_SIGV4_ENABLED));
        opts.put(IcebergConnectorProperties.REST_SIGNING_REGION_KEY,
                firstNonBlankOrEmpty(props, IcebergConnectorProperties.REST_SIGNING_REGION));
        if (IcebergConnectorProperties.SIGNING_NAME_GLUE.equals(signingName)
                || IcebergConnectorProperties.SIGNING_NAME_S3TABLES.equals(signingName)) {
            // glue/s3tables: credentials come from the chosen S3 store, switching on its credential type
            // (legacy getCredentialType precedence: EXPLICIT before ASSUME_ROLE before PROVIDER_CHAIN).
            if (chosenS3.isPresent()) {
                S3CompatibleFileSystemProperties s3 = chosenS3.get();
                if (s3.hasStaticCredentials()) {
                    putRestExplicitCredentials(opts, s3.getAccessKey(), s3.getSecretKey(), s3.getSessionToken());
                } else if (s3.hasAssumeRole()) {
                    appendAssumeRoleProperties(opts, s3);
                } else {
                    // F14: PROVIDER_CHAIN — pin the non-DEFAULT provider class the user selected via
                    // s3.credentials_provider_type (mirrors legacy putCredentialsProvider). DEFAULT/blank ->
                    // null -> nothing emitted (SDK default chain, the common case).
                    putIfNotBlank(opts, AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                            AwsCredentialsProviderModes.classNameFor(props, AwsCredentialsProviderModes.S3_MODE_KEYS));
                }
            }
        } else {
            // other signing-name: explicit iceberg.rest.* credentials, else the non-DEFAULT provider chain (F14).
            String restAccessKey = firstNonBlank(props, IcebergConnectorProperties.REST_ACCESS_KEY_ID);
            String restSecretKey = firstNonBlank(props, IcebergConnectorProperties.REST_SECRET_ACCESS_KEY);
            if (StringUtils.isNotBlank(restAccessKey) && StringUtils.isNotBlank(restSecretKey)) {
                putRestExplicitCredentials(opts, restAccessKey, restSecretKey,
                        firstNonBlank(props, IcebergConnectorProperties.REST_SESSION_TOKEN));
            } else {
                putIfNotBlank(opts, AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                        AwsCredentialsProviderModes.classNameFor(
                                props, IcebergConnectorProperties.REST_CREDENTIALS_PROVIDER_TYPE));
            }
        }
    }

    /** Mirrors legacy {@code putExplicitRestCredentials}: emit rest.* creds only when AK and SK are both set. */
    private static void putRestExplicitCredentials(Map<String, String> opts, String accessKey, String secretKey,
            String sessionToken) {
        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
            return;
        }
        opts.put(AwsProperties.REST_ACCESS_KEY_ID, accessKey);
        opts.put(AwsProperties.REST_SECRET_ACCESS_KEY, secretKey);
        putIfNotBlank(opts, AwsProperties.REST_SESSION_TOKEN, sessionToken);
    }

    // ---------------------------------------------------------------------
    // GLUE appender (mirror IcebergGlueMetaStoreProperties.initCatalog)
    // ---------------------------------------------------------------------

    /**
     * Mirrors legacy {@code IcebergGlueMetaStoreProperties}: the 5 {@code s3.*} FileIO keys emitted
     * UNCONDITIONALLY from the chosen S3 store (legacy plain puts allow empty strings), {@code glue.endpoint},
     * exactly one credential branch (AK/SK provider OR assume-role), {@code client.region} (always, with the
     * endpoint-regex / us-east-1 fallback), and a {@code putIfAbsent} warehouse placeholder. {@code conf=null}.
     * PURE. NOTE (D-061): the s3.* values come from the fe-filesystem typed store, not legacy's
     * {@code S3Properties.of(origProps)}; for a glue catalog whose creds were supplied ONLY via {@code glue.*}
     * aliases (which fe-filesystem does not read into the S3 store) the s3.* FileIO creds may be absent — a
     * UT-invisible edge handled at the P6.6 docker gate (the glue-client creds below still come from glue.*).
     */
    public static void appendGlueProperties(Map<String, String> opts, Map<String, String> props,
            Optional<S3CompatibleFileSystemProperties> chosenS3) {
        chosenS3.ifPresent(s3 -> {
            opts.put(S3FileIOProperties.ACCESS_KEY_ID, nullToEmpty(s3.getAccessKey()));
            opts.put(S3FileIOProperties.SECRET_ACCESS_KEY, nullToEmpty(s3.getSecretKey()));
            opts.put(S3FileIOProperties.ENDPOINT, nullToEmpty(s3.getEndpoint()));
            opts.put(S3FileIOProperties.PATH_STYLE_ACCESS, nullToEmpty(s3.getUsePathStyle()));
            opts.put(S3FileIOProperties.SESSION_TOKEN, nullToEmpty(s3.getSessionToken()));
        });
        String glueEndpoint = firstNonBlank(props, IcebergConnectorProperties.GLUE_ENDPOINT);
        putIfNotBlank(opts, AwsProperties.GLUE_CATALOG_ENDPOINT, glueEndpoint);
        String glueRegion = resolveGlueRegion(props, glueEndpoint);
        String glueAccessKey = firstNonBlank(props, IcebergConnectorProperties.GLUE_ACCESS_KEY);
        String glueSecretKey = firstNonBlank(props, IcebergConnectorProperties.GLUE_SECRET_KEY);
        if (StringUtils.isNotBlank(glueAccessKey) && StringUtils.isNotBlank(glueSecretKey)) {
            opts.put(IcebergConnectorProperties.GLUE_CREDENTIALS_PROVIDER_KEY,
                    IcebergConnectorProperties.GLUE_CREDENTIALS_PROVIDER_2X);
            opts.put(IcebergConnectorProperties.GLUE_CREDENTIALS_PROVIDER_ACCESS_KEY, glueAccessKey);
            opts.put(IcebergConnectorProperties.GLUE_CREDENTIALS_PROVIDER_SECRET_KEY, glueSecretKey);
            opts.put(IcebergConnectorProperties.GLUE_CREDENTIALS_PROVIDER_FACTORY_KEY,
                    IcebergConnectorProperties.GLUE_CREDENTIALS_PROVIDER_FACTORY);
            putIfNotBlank(opts, IcebergConnectorProperties.GLUE_CREDENTIALS_PROVIDER_SESSION_TOKEN,
                    firstNonBlank(props, IcebergConnectorProperties.GLUE_SESSION_TOKEN));
        } else {
            String glueIamRole = firstNonBlank(props, IcebergConnectorProperties.GLUE_IAM_ROLE);
            if (StringUtils.isNotBlank(glueIamRole)) {
                opts.put(AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName());
                opts.put(IcebergConnectorProperties.AWS_REGION_KEY, glueRegion);
                opts.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, glueIamRole);
                opts.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, glueRegion);
                putIfNotBlank(opts, AwsProperties.CLIENT_ASSUME_ROLE_EXTERNAL_ID,
                        firstNonBlank(props, IcebergConnectorProperties.GLUE_EXTERNAL_ID));
            }
        }
        opts.put(AwsClientProperties.CLIENT_REGION, glueRegion);
        opts.putIfAbsent(CatalogProperties.WAREHOUSE_LOCATION, IcebergConnectorProperties.GLUE_CHECKED_WAREHOUSE);
    }

    /**
     * Mirrors legacy {@code AWSGlueMetaStoreBaseProperties.checkAndInit} region resolution: an explicit
     * glue.region / aws.region / aws.glue.region wins; else extract from the endpoint host; else us-east-1.
     */
    private static String resolveGlueRegion(Map<String, String> props, String glueEndpoint) {
        String region = firstNonBlank(props, IcebergConnectorProperties.GLUE_REGION);
        if (StringUtils.isNotBlank(region)) {
            return region;
        }
        if (StringUtils.isNotBlank(glueEndpoint)) {
            Matcher matcher = GLUE_ENDPOINT_PATTERN.matcher(glueEndpoint.toLowerCase(Locale.ROOT));
            if (matcher.matches() && StringUtils.isNotBlank(matcher.group(1))) {
                return matcher.group(1);
            }
        }
        return IcebergConnectorProperties.GLUE_DEFAULT_REGION;
    }

    // ---------------------------------------------------------------------
    // JDBC appender (mirror IcebergJdbcMetaStoreProperties.initIcebergJdbcCatalogProperties)
    // ---------------------------------------------------------------------

    /**
     * Mirrors legacy {@code IcebergJdbcMetaStoreProperties}: {@code uri} (alias priority {@code uri} >
     * {@code iceberg.jdbc.uri}, required), the five dotted {@code jdbc.*} keys added only-if-non-blank from
     * their {@code iceberg.jdbc.*} aliases. The raw {@code jdbc.*} passthrough legacy performs is already
     * covered by the base copy-all ({@link #buildBaseCatalogProperties} seeds the map from all props), so it is
     * not repeated here. The {@code catalog_name} positional removal + driver registration are handled by the
     * connector. PURE.
     */
    public static void appendJdbcProperties(Map<String, String> opts, Map<String, String> props) {
        opts.put(CatalogProperties.URI, firstNonBlankOrEmpty(props, IcebergConnectorProperties.JDBC_URI));
        putIfNotBlank(opts, IcebergConnectorProperties.JDBC_USER_KEY,
                firstNonBlank(props, IcebergConnectorProperties.JDBC_USER));
        putIfNotBlank(opts, IcebergConnectorProperties.JDBC_PASSWORD_KEY,
                firstNonBlank(props, IcebergConnectorProperties.JDBC_PASSWORD));
        putIfNotBlank(opts, IcebergConnectorProperties.JDBC_INIT_CATALOG_TABLES_KEY,
                firstNonBlank(props, IcebergConnectorProperties.JDBC_INIT_CATALOG_TABLES));
        putIfNotBlank(opts, IcebergConnectorProperties.JDBC_SCHEMA_VERSION_KEY,
                firstNonBlank(props, IcebergConnectorProperties.JDBC_SCHEMA_VERSION));
        putIfNotBlank(opts, IcebergConnectorProperties.JDBC_STRICT_MODE_KEY,
                firstNonBlank(props, IcebergConnectorProperties.JDBC_STRICT_MODE));
    }

    // ---------------------------------------------------------------------
    // Storage Configuration / HiveConf builders (mirror PaimonCatalogFactory)
    // ---------------------------------------------------------------------

    /**
     * Builds the storage Hadoop {@link Configuration} for the rest/hadoop/jdbc flavors, mirroring legacy
     * {@code new Configuration()} + each storage {@code getHadoopStorageConfig()}: the pre-computed canonical
     * object-store/HDFS config ({@code storageHadoopConfig}, from fe-filesystem's
     * {@code toHadoopConfigurationMap()}) plus the raw {@code fs.}/{@code dfs.}/{@code hadoop.} passthrough for
     * inline user keys. The conf classloader is pinned to the plugin loader so Hadoop's
     * {@code fs.<scheme>.impl} resolution stays in one loader (FIX-PAIMON-HADOOP-CLASSLOADER parity). PURE.
     */
    public static Configuration buildHadoopConfiguration(Map<String, String> props,
            Map<String, String> storageHadoopConfig) {
        Configuration conf = new Configuration();
        conf.setClassLoader(IcebergCatalogFactory.class.getClassLoader());
        storageHadoopConfig.forEach(conf::set);
        props.forEach((key, value) -> {
            if (key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")) {
                conf.set(key, value);
            }
        });
        return conf;
    }

    /**
     * Assembles the {@link HiveConf} for the hms flavor, mirroring {@code PaimonCatalogFactory.assembleHiveConf}:
     * seed the optional external hive-site.xml {@code base} first, then layer the metastore-spi
     * {@code toHiveConfOverrides} on top (last-write-wins). The conf classloader is pinned to the plugin loader
     * (HiveMetaStoreClient filter-hook resolution parity). PURE (a function of the two maps).
     */
    public static HiveConf assembleHiveConf(Map<String, String> base, Map<String, String> overrides) {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setClassLoader(IcebergCatalogFactory.class.getClassLoader());
        if (base != null) {
            base.forEach(hiveConf::set);
        }
        overrides.forEach(hiveConf::set);
        return hiveConf;
    }

    /**
     * Builds the Hadoop {@link Configuration} for the bespoke {@code dlf} flavor, mirroring legacy
     * {@code IcebergAliyunDLFMetaStoreProperties.initCatalog}: the {@code dlf.catalog.*} keys from the
     * metastore-spi {@code toDlfCatalogConf()} (= the {@code DataLakeConfig.CATALOG_*} constant values), plus
     * the two fixed hive keys {@code hive.metastore.type=dlf} and {@code type=hms} that legacy sets on the DLF
     * {@code Configuration}. The conf classloader is pinned to the plugin loader (metastore client + filter-hook
     * resolution parity). PURE (a function of {@code dlfCatalogConf}).
     */
    public static Configuration buildDlfConfiguration(Map<String, String> dlfCatalogConf) {
        Configuration conf = new Configuration();
        conf.setClassLoader(IcebergCatalogFactory.class.getClassLoader());
        dlfCatalogConf.forEach(conf::set);
        conf.set("hive.metastore.type", "dlf");
        conf.set("type", "hms");
        return conf;
    }

    // ---------------------------------------------------------------------
    // Pure helpers
    // ---------------------------------------------------------------------

    /** Returns the first non-blank value among the given keys (alias priority), or {@code null} if none set. */
    public static String firstNonBlank(Map<String, String> props, String... keys) {
        for (String key : keys) {
            String value = props.get(key);
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private static String firstNonBlankOrEmpty(Map<String, String> props, String... keys) {
        String value = firstNonBlank(props, keys);
        return value == null ? "" : value;
    }

    private static String firstNonBlankOr(Map<String, String> props, String defaultValue, String... keys) {
        String value = firstNonBlank(props, keys);
        return value == null ? defaultValue : value;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
