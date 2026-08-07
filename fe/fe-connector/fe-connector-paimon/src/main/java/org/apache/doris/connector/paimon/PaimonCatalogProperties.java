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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Everything a user writes in {@code CREATE CATALOG} for a paimon catalog that is not specific to one
 * metastore backend (the "A class" of the connector property convention; deployment-level settings
 * live in {@link PaimonConf}). The per-flavor keys — uri, credentials, warehouse — belong to the
 * bound {@code *MetaStoreProperties} instead, which is why only three keys are declared here.
 *
 * <p><b>The two entry points are not interchangeable.</b> {@link #of(Map)} runs at CREATE, at ALTER
 * validation, AND on every connector build — including the lazy rebuild after an FE restart, which
 * happens at least twice for an interactively created catalog. So it binds and derives, and it does
 * not enforce anything the connector can run without: a rule added after a catalog was created must
 * not stop that catalog from coming back. {@link #checkCreateTimeOnlyRules()} carries the rules that
 * only ever ran against a statement, and only the provider calls it.
 *
 * <p>The flavor is bound as a plain String, not an enum. Backends are discovered through
 * {@code ServiceLoader}, so the set is open by design and a central enum would have to be edited
 * every time one is added.
 */
public final class PaimonCatalogProperties {

    private static final Logger LOG = LogManager.getLogger(PaimonCatalogProperties.class);

    /** Paimon catalog backend type: filesystem, hms, rest, jdbc. */
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";

    /**
     * Whether to map Paimon BINARY/VARBINARY to Doris VARBINARY instead of STRING.
     *
     * <p>Canonical (dotted) CREATE-CATALOG key, mirroring fe-core
     * {@code CatalogProperty.ENABLE_MAPPING_VARBINARY} and the legacy paimon path. The connector
     * receives the raw catalog property map ({@code catalogProperty.getProperties()}), which only
     * ever carries this dotted key (fe-core {@code setDefaultPropsIfMissing} writes only it), so the
     * read MUST use the dotted spelling — an underscore variant is never present and would read false.
     */
    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";

    /**
     * Whether to map Paimon TIMESTAMP_WITH_LOCAL_TIME_ZONE to TIMESTAMPTZ.
     *
     * <p>Canonical (dotted) CREATE-CATALOG key, mirroring fe-core
     * {@code CatalogProperty.ENABLE_MAPPING_TIMESTAMP_TZ} and the legacy paimon path.
     */
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    // ---- Flavor literals (the accepted paimon.catalog.type values) ----
    public static final String FILESYSTEM = "filesystem";
    public static final String HMS = "hms";
    public static final String REST = "rest";
    public static final String JDBC = "jdbc";

    /** Default catalog type when not specified. */
    private static final String DEFAULT_CATALOG_TYPE = FILESYSTEM;

    // Legacy PaimonExternalCatalog.checkProperties validated the table-handle cache knobs
    // (meta.cache.paimon.table.{enable,ttl-second,capacity}) via CacheSpec. FIX-4 restores ttl-second: it
    // now sizes the connector latest-snapshot cache (data) AND the generic schema cache (via
    // schemaCacheTtlSecondOverride). enable/capacity remain not-wired on the plugin path, so they are
    // still reported as ignored (R2) -- ttl-second is intentionally excluded from this set since it again
    // takes effect.
    private static final String DEAD_TABLE_CACHE_PREFIX = "meta.cache.paimon.table.";

    @ConnectorProperty(names = {PAIMON_CATALOG_TYPE}, required = false,
            description = "The metastore backend: filesystem, hms, rest or jdbc.")
    private String catalogType = DEFAULT_CATALOG_TYPE;

    @ConnectorProperty(names = {ENABLE_MAPPING_VARBINARY}, required = false,
            description = "Map paimon BINARY/VARBINARY to Doris VARBINARY instead of STRING.")
    private boolean enableMappingVarbinary;

    @ConnectorProperty(names = {ENABLE_MAPPING_TIMESTAMP_TZ}, required = false,
            description = "Map paimon TIMESTAMP_WITH_LOCAL_TIME_ZONE to Doris TIMESTAMPTZ.")
    private boolean enableMappingTimestampTz;

    private final Map<String, String> raw;
    private String flavor;

    private PaimonCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    /**
     * Binds and derives. Pure, idempotent, and free of I/O — it runs on every connector build.
     *
     * <p>Unknown keys are never rejected: the same map carries engine keys ({@code type},
     * {@code meta.cache.*}, ...) and storage keys ({@code s3.*}, ...), and ALTER CATALOG can only
     * overwrite a key, never remove one, so a rejected unknown key could not be repaired.
     */
    public static PaimonCatalogProperties of(Map<String, String> properties) {
        PaimonCatalogProperties p = new PaimonCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        p.flavor = p.catalogType.toLowerCase(Locale.ROOT);
        return p;
    }

    /**
     * The checks that only ever ran against a CREATE/ALTER CATALOG statement, kept out of
     * {@link #of(Map)} so they cannot break the rebuild of a catalog that predates them.
     *
     * <p>Order is load-bearing and matches the provider method this replaces: an invalid meta-cache
     * value is rejected outright, while a valid-but-unwired enable/capacity is still reported as
     * ignored; then the table options; then the backend's own rules.
     *
     * @return this, so the provider can call it in one expression
     */
    public PaimonCatalogProperties checkCreateTimeOnlyRules() {
        checkMetaCacheProperties(raw);
        warnIgnoredDeadTableCacheKeys(raw);
        // #65955: an unknown or unparseable paimon.table-option.* must fail the CREATE/ALTER CATALOG.
        // Upstream got this from AbstractPaimonProperties.initNormalizeAndCheckProps(), which the SPI
        // path no longer runs; this is that path's fail-fast hook.
        PaimonTableOptions.extract(raw);
        // Selects the backend by paimon.catalog.type and enforces its fail-fast rules (warehouse, uri,
        // HMS kerberos forbidIf/requireIf, JDBC driver_class-when-driver_url, REST dlf-token AK/SK).
        // Dispatched on the RAW map rather than the derived flavor: the filesystem provider claims an
        // ABSENT catalog-type token, and resolving the default here first would change which backend an
        // odd value lands on. Storage plays no part in validation, so an empty storage map is passed.
        // The bound instance is deliberately not kept -- it was built without storage and must not be
        // mistaken for the one the HMS catalog build needs.
        MetaStoreProviders.bind(raw, Collections.emptyMap()).validate();
        return this;
    }

    /**
     * Byte-for-byte parity with the (deleted) legacy {@code PaimonExternalCatalog.checkProperties}:
     * {@code table.enable} must be boolean, {@code table.ttl-second} must be a long &ge; -1,
     * {@code table.capacity} must be a long &ge; 0. Absent keys are skipped.
     */
    private static void checkMetaCacheProperties(Map<String, String> properties) {
        CacheSpec.checkBooleanProperty(properties.get(PaimonConnector.TABLE_CACHE_ENABLE),
                PaimonConnector.TABLE_CACHE_ENABLE);
        CacheSpec.checkLongProperty(properties.get(PaimonConnector.TABLE_CACHE_TTL_SECOND),
                -1L, PaimonConnector.TABLE_CACHE_TTL_SECOND);
        CacheSpec.checkLongProperty(properties.get(PaimonConnector.TABLE_CACHE_CAPACITY),
                0L, PaimonConnector.TABLE_CACHE_CAPACITY);
        CacheSpec.checkDataSizeProperty(properties.get(PaimonConnector.PARTITION_VIEW_CACHE_MAX_WEIGHT),
                PaimonConnector.PARTITION_VIEW_CACHE_MAX_WEIGHT);
    }

    // R2: warn (do not reject, do not strip) when a CREATE/ALTER CATALOG carries the now-dead paimon
    // table-cache knobs, so the operator learns their cache tuning no longer takes effect on the plugin
    // path. Deliberately NOT in of(): that runs on every lazy rebuild, which would turn a one-off notice
    // into a recurring log line.
    private static void warnIgnoredDeadTableCacheKeys(Map<String, String> properties) {
        List<String> dead = properties.keySet().stream()
                .filter(k -> k.startsWith(DEAD_TABLE_CACHE_PREFIX))
                // ttl-second is restored (FIX-4): it sizes the snapshot cache + schema cache TTL, so it is NOT dead.
                .filter(k -> !k.equals(PaimonConnector.TABLE_CACHE_TTL_SECOND))
                .sorted()
                .collect(Collectors.toList());
        if (!dead.isEmpty()) {
            LOG.warn("Paimon catalog cache property/properties {} no longer take effect on the plugin path "
                    + "(the table metadata cache configuration is obsolete) and are ignored.", dead);
        }
    }

    /** The metastore backend, lower-cased; {@code filesystem} when the catalog does not name one. */
    public String getFlavor() {
        return flavor;
    }

    public boolean isEnableMappingVarbinary() {
        return enableMappingVarbinary;
    }

    public boolean isEnableMappingTimestampTz() {
        return enableMappingTimestampTz;
    }

    /**
     * The raw catalog properties, unmodifiable. Kept because several consumers forward whole key
     * NAMESPACES the connector does not model — {@code paimon.*} into the catalog options,
     * {@code fs./dfs./hadoop.} into the Hadoop config, {@code jdbc.*} and {@code paimon.jni.*} into the
     * BE-bound scan options. Those are copy-all passthroughs, not properties this class owns.
     */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
