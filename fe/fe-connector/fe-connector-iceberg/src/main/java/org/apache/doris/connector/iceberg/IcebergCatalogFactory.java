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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

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
            boolean enable = getBoolean(props, IcebergConnectorProperties.MANIFEST_CACHE_ENABLE,
                    DEFAULT_MANIFEST_CACHE_ENABLE);
            long ttl = getLong(props, IcebergConnectorProperties.MANIFEST_CACHE_TTL,
                    DEFAULT_MANIFEST_CACHE_TTL_SECOND);
            long capacity = getLong(props, IcebergConnectorProperties.MANIFEST_CACHE_CAPACITY,
                    DEFAULT_MANIFEST_CACHE_CAPACITY);
            if (enable && ttl != 0 && capacity != 0) {
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
        putIfNotBlank(opts, S3FileIOProperties.ENDPOINT, s3.getEndpoint());
        putIfNotBlank(opts, S3FileIOProperties.PATH_STYLE_ACCESS, s3.getUsePathStyle());
        putIfNotBlank(opts, AwsClientProperties.CLIENT_REGION, s3.getRegion());
        putIfNotBlank(opts, S3FileIOProperties.ACCESS_KEY_ID, s3.getAccessKey());
        putIfNotBlank(opts, S3FileIOProperties.SECRET_ACCESS_KEY, s3.getSecretKey());
        putIfNotBlank(opts, S3FileIOProperties.SESSION_TOKEN, s3.getSessionToken());
        if ("S3".equals(s3.providerName())) {
            appendAssumeRoleProperties(opts, s3);
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

    private static boolean getBoolean(Map<String, String> props, String key, boolean defaultValue) {
        String value = props.get(key);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private static long getLong(Map<String, String> props, String key, long defaultValue) {
        String value = props.get(key);
        return value == null ? defaultValue : NumberUtils.toLong(value, defaultValue);
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
}
