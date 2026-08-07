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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Everything a user writes in {@code CREATE CATALOG} for an iceberg catalog that is not specific to one
 * metastore backend (the "A class" of the connector property convention; deployment-level settings live
 * in {@link IcebergConf}). The per-flavor keys — uri, credentials, warehouse — belong to the bound
 * {@code *MetaStoreProperties} instead, which is why only four keys are declared here.
 *
 * <p><b>The two entry points are not interchangeable.</b> {@link #of(Map)} runs at CREATE, at ALTER
 * validation, AND on every connector build — including the lazy rebuild after an FE restart, which
 * happens at least twice for an interactively created catalog. So it binds and derives, and it does not
 * enforce anything the connector can run without: a rule added after a catalog was created must not stop
 * that catalog from coming back. {@link #checkCreateTimeOnlyRules()} carries the rules that only ever ran
 * against a statement, and only the provider calls it.
 *
 * <p>The flavor is bound as a plain String, not an enum. Backends are discovered through
 * {@code ServiceLoader}, so the set is open by design and a central enum would have to be edited every
 * time one is added.
 */
public final class IcebergCatalogProperties {

    /** Iceberg catalog backend type: rest, hms, glue, jdbc, hadoop, s3tables. */
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";

    // ---- Flavor literals (the accepted iceberg.catalog.type values) ----
    public static final String TYPE_REST = "rest";
    public static final String TYPE_HMS = "hms";
    public static final String TYPE_GLUE = "glue";
    public static final String TYPE_JDBC = "jdbc";
    public static final String TYPE_HADOOP = "hadoop";
    public static final String TYPE_S3_TABLES = "s3tables";

    /**
     * Whether to map iceberg BINARY/FIXED to Doris VARBINARY instead of STRING.
     *
     * <p>Canonical (dotted) CREATE-CATALOG key, mirroring fe-core
     * {@code CatalogProperty.ENABLE_MAPPING_VARBINARY}. The connector receives the raw catalog property
     * map, which only ever carries this dotted key, so the read MUST use the dotted spelling — an
     * underscore variant is never present and would silently read false.
     */
    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";

    /** Whether to map iceberg TIMESTAMPTZ to Doris TIMESTAMPTZ. Dotted key, same rationale as above. */
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    /**
     * The catalog level of a 3-level REST namespace ({@code <catalog>.<db>.<table>}), mirroring legacy
     * {@code IcebergExternalCatalog.EXTERNAL_CATALOG_NAME}: when present it is appended to every namespace
     * and roots database listing. Despite reading like a REST-only knob it is honored for EVERY flavor
     * ({@code IcebergCatalogOps} applies it without a flavor gate), which is why it lives here rather than
     * on the REST backend properties.
     */
    public static final String EXTERNAL_CATALOG_NAME = "external_catalog.name";

    @ConnectorProperty(names = {ICEBERG_CATALOG_TYPE}, required = false,
            description = "The metastore backend: rest, hms, glue, jdbc, hadoop or s3tables.")
    private String catalogType = "";

    @ConnectorProperty(names = {ENABLE_MAPPING_VARBINARY}, required = false,
            description = "Map iceberg BINARY/FIXED to Doris VARBINARY instead of STRING.")
    private boolean enableMappingVarbinary;

    @ConnectorProperty(names = {ENABLE_MAPPING_TIMESTAMP_TZ}, required = false,
            description = "Map iceberg TIMESTAMPTZ to Doris TIMESTAMPTZ.")
    private boolean enableMappingTimestampTz;

    // Null, NOT "": the consumers ask for an Optional, and a blank default would turn every catalog that
    // never set the key into one carrying an empty namespace level.
    @ConnectorProperty(names = {EXTERNAL_CATALOG_NAME}, required = false,
            description = "The catalog level of a 3-level REST namespace.")
    private String externalCatalogName;

    private final Map<String, String> raw;
    private String flavor;

    private IcebergCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    /**
     * Binds and derives. Pure, idempotent, and free of I/O — it runs on every connector build.
     *
     * <p>Unknown keys are never rejected: the same map carries engine keys ({@code type},
     * {@code meta.cache.*}, ...) and storage keys ({@code s3.*}, ...), and ALTER CATALOG can only
     * overwrite a key, never remove one, so a rejected unknown key could not be repaired.
     */
    public static IcebergCatalogProperties of(Map<String, String> properties) {
        IcebergCatalogProperties p = new IcebergCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        // Null rather than a default flavor: "the catalog names no backend" is a distinct state that the
        // build and the CREATE-time dispatch both report on, and inventing a default here would silently
        // route such a catalog somewhere.
        p.flavor = p.catalogType.isEmpty() ? null : p.catalogType.toLowerCase(Locale.ROOT);
        return p;
    }

    /**
     * The checks that only ever ran against a CREATE/ALTER CATALOG statement, kept out of {@link #of(Map)}
     * so they cannot break the rebuild of a catalog that predates them.
     *
     * <p>Order is load-bearing and matches the provider method this replaces: the meta-cache values are
     * rejected first (restoring the legacy {@code IcebergExternalCatalog.checkProperties} fail-fast that
     * the SPI cutover dropped, so a bad {@code meta.cache.iceberg.*} is rejected rather than silently
     * coerced to a cache-disabling default), then the backend's own rules.
     *
     * @return this, so the provider can call it in one expression
     */
    public IcebergCatalogProperties checkCreateTimeOnlyRules() {
        checkMetaCacheProperties(raw);
        // Selects the backend by flavor and enforces its fail-fast rules -- REST (security/creds enums,
        // OAuth2, signing, AK/SK), Glue (AK/SK-together, endpoint https, at-least-one-credential), JDBC
        // (uri/catalog_name/warehouse), the shared HMS connection checks; hadoop/s3tables are no-op (their
        // storage is validated upstream at fe-filesystem bind). A blank or unknown flavor makes bindForType
        // throw (no provider supports it). Storage plays no part in validation, so an empty storage map is
        // passed, and the bound instance is deliberately not kept -- it was built without storage and must
        // not be mistaken for the one the HMS catalog build needs.
        MetaStoreProviders.bindForType(flavor, raw, Collections.emptyMap()).validate();
        return this;
    }

    /**
     * Byte-for-byte parity with the legacy {@code IcebergExternalCatalog.checkProperties}: table/manifest
     * {@code enable} must be boolean, {@code ttl-second} must be a long &ge; -1 (the "no expiration"
     * sentinel), {@code capacity} must be a long &ge; 0. Absent keys are skipped.
     */
    private static void checkMetaCacheProperties(Map<String, String> properties) {
        CacheSpec.checkBooleanProperty(properties.get(IcebergConnector.TABLE_CACHE_ENABLE),
                IcebergConnector.TABLE_CACHE_ENABLE);
        CacheSpec.checkLongProperty(properties.get(IcebergConnector.TABLE_CACHE_TTL_SECOND),
                -1L, IcebergConnector.TABLE_CACHE_TTL_SECOND);
        CacheSpec.checkLongProperty(properties.get(IcebergConnector.TABLE_CACHE_CAPACITY),
                0L, IcebergConnector.TABLE_CACHE_CAPACITY);

        CacheSpec.checkBooleanProperty(properties.get(IcebergConnector.MANIFEST_CACHE_ENABLE),
                IcebergConnector.MANIFEST_CACHE_ENABLE);
        CacheSpec.checkLongProperty(properties.get(IcebergConnector.MANIFEST_CACHE_TTL_SECOND),
                -1L, IcebergConnector.MANIFEST_CACHE_TTL_SECOND);
        CacheSpec.checkLongProperty(properties.get(IcebergConnector.MANIFEST_CACHE_CAPACITY),
                0L, IcebergConnector.MANIFEST_CACHE_CAPACITY);
    }

    /** The metastore backend, lower-cased; {@code null} when the catalog does not name one. */
    public String getFlavor() {
        return flavor;
    }

    public boolean isEnableMappingVarbinary() {
        return enableMappingVarbinary;
    }

    public boolean isEnableMappingTimestampTz() {
        return enableMappingTimestampTz;
    }

    /** Empty when the catalog names no extra namespace level. */
    public Optional<String> getExternalCatalogName() {
        return Optional.ofNullable(externalCatalogName);
    }

    /**
     * The raw catalog properties, unmodifiable. Kept because several consumers forward whole key
     * NAMESPACES the connector does not model — the copy-all seed of the catalog options,
     * {@code fs./dfs./hadoop.} into the Hadoop config, the S3 region aliases. Those are passthroughs, not
     * properties this class owns.
     */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
