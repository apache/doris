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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything a user writes in {@code CREATE CATALOG} for an hms catalog, bound and validated in one
 * step. Deployment-level settings — one per FE rather than one per catalog — are the other half and
 * live in {@link HmsConf}.
 *
 * <p>{@link #of(Map)} binds, derives and validates together: if an instance exists, its properties are
 * usable. It does no I/O and is idempotent, because it runs at CREATE, at ALTER validation (on the
 * merged candidate) and on every connector rebuild — including on an FE replaying the edit log.
 *
 * <p><b>Unknown keys are accepted, always.</b> The same map carries engine keys ({@code type},
 * {@code meta.cache.*}, ...), storage keys ({@code s3.*}, {@code dfs.*}, ...) and the raw
 * {@code hive.*}/{@code hadoop.*} passthrough the metastore client and the sibling connectors read;
 * and {@code ALTER CATALOG} merges properties — it can overwrite a key but never remove one, so a key
 * refused here would leave a catalog no statement could repair. Bad <i>values</i> are refused;
 * unrecognized <i>names</i> are not.
 *
 * <p><b>What is deliberately NOT here.</b> The metastore connection type and the four Kerberos keys
 * ({@code hive.metastore.authentication.type}, the two principals, the keytab) are owned by
 * {@code HmsClientConfig} and by the shared {@code AbstractHmsMetaStoreProperties} holder, which
 * {@code HiveConnector.buildPluginAuthenticator} binds through {@code MetaStoreProviders}. Copying
 * them here would give those keys two sources of truth. CREATE TABLE / CREATE DATABASE statement
 * properties, the session variable names and the remote table parameters are not catalog properties at
 * all and live next to the single class that reads each of them.
 */
public final class HiveCatalogProperties {

    /** The Hive Metastore thrift URI. */
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    /**
     * The short form of {@link #HIVE_METASTORE_URIS}, accepted since the legacy HMS catalog. The
     * canonical spelling wins when a catalog carries both.
     */
    public static final String URI = "uri";

    /**
     * Selects the metastore implementation. Read by {@code HmsClientConfig} (which owns the key and
     * its accepted values); named here only so {@link #of} can reject a removed value by its message.
     */
    public static final String METASTORE_TYPE = HmsClientConfig.METASTORE_TYPE_KEY;

    /** Size of the metastore client pool. */
    public static final String HMS_CLIENT_POOL_SIZE = "hive.metastore.client.pool.size";

    /** Whether this catalog polls HMS notification events for incremental metadata refresh. */
    public static final String ENABLE_HMS_EVENTS_INCREMENTAL_SYNC = "hive.enable_hms_events_incremental_sync";

    /** Max notification events fetched per RPC when incremental event sync is enabled. */
    public static final String HMS_EVENTS_BATCH_SIZE_PER_RPC = "hive.hms_events_batch_size_per_rpc";

    /**
     * When {@code false}, a partition whose storage location does not exist fails the query loud
     * ({@code "Partition location does not exist"}); the default {@code true} tolerates it by skipping
     * the partition with a warning. Mirrors legacy {@code HiveExternalMetaCache} semantics.
     */
    public static final String IGNORE_ABSENT_PARTITIONS = "hive.ignore_absent_partitions";

    /**
     * Whether partition directories are listed recursively (descend into sub-directories). Default
     * {@code true} — legacy {@code HiveExternalMetaCache.getFileCache} defaulted the same. When
     * {@code false}, a table whose data lives in sub-directories silently loses those rows.
     */
    public static final String RECURSIVE_DIRECTORIES = "hive.recursive_directories";

    /** Root directory for the staging files an INSERT writes before they are committed. */
    public static final String STAGING_DIR = "hive.staging_dir";

    // Catalog-level type-mapping toggles (dot form), matching CatalogProperty and the iceberg/paimon
    // connectors. ExternalCatalog forwards these two keys to the connector; the earlier underscore
    // spellings were never populated, so the toggles silently no-op'd (hive BINARY always STRING,
    // timestamp never TIMESTAMPTZ).
    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    private static final int DEFAULT_HMS_CLIENT_POOL_SIZE = 8;

    /** Matches the engine's legacy {@code hms_events_batch_size_per_rpc} default. */
    private static final int DEFAULT_HMS_EVENTS_BATCH_SIZE = 500;

    private static final String DEFAULT_STAGING_DIR = "/tmp/.doris_staging";

    @ConnectorProperty(names = {HIVE_METASTORE_URIS, URI},
            description = "Hive Metastore thrift URI; 'uri' is accepted as the short form")
    private String metastoreUri;

    @ConnectorProperty(names = {HMS_CLIENT_POOL_SIZE}, required = false,
            description = "size of the metastore client pool")
    private int hmsClientPoolSize = DEFAULT_HMS_CLIENT_POOL_SIZE;

    @ConnectorProperty(names = {ENABLE_HMS_EVENTS_INCREMENTAL_SYNC}, required = false,
            description = "poll HMS notification events for incremental metadata refresh")
    private boolean enableHmsEventsIncrementalSync;

    @ConnectorProperty(names = {HMS_EVENTS_BATCH_SIZE_PER_RPC}, required = false,
            description = "max notification events fetched per RPC")
    private int hmsEventsBatchSizePerRpc = DEFAULT_HMS_EVENTS_BATCH_SIZE;

    @ConnectorProperty(names = {IGNORE_ABSENT_PARTITIONS}, required = false,
            description = "skip a partition whose location does not exist instead of failing the query")
    private boolean ignoreAbsentPartitions = true;

    @ConnectorProperty(names = {RECURSIVE_DIRECTORIES}, required = false,
            description = "descend into sub-directories when listing a partition's files")
    private boolean recursiveDirectories = true;

    @ConnectorProperty(names = {STAGING_DIR}, required = false,
            description = "root directory for the staging files an INSERT writes")
    private String stagingDir = DEFAULT_STAGING_DIR;

    @ConnectorProperty(names = {ENABLE_MAPPING_VARBINARY}, required = false,
            description = "map hive BINARY to VARBINARY instead of STRING")
    private boolean enableMappingVarbinary;

    @ConnectorProperty(names = {ENABLE_MAPPING_TIMESTAMP_TZ}, required = false,
            description = "map hive timestamp to TIMESTAMPTZ")
    private boolean enableMappingTimestampTz;

    private final Map<String, String> raw;
    private Map<String, String> hmsClientProperties;

    private HiveCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    public static HiveCatalogProperties of(Map<String, String> properties) {
        // Before anything else: a catalog naming a removed metastore type must be told so by name. This
        // has to precede the required-URI rule because a glue/dlf catalog carries no
        // hive.metastore.uris, and the URI message would shadow the one that explains what happened.
        String removedType = HmsClientConfig.removedMetastoreTypeError(properties);
        if (removedType != null) {
            throw new IllegalArgumentException(removedType);
        }

        HiveCatalogProperties p = new HiveCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        new ParamRules()
                .require(p.metastoreUri, "HMS URI ('" + HIVE_METASTORE_URIS + "') is required")
                .validate();
        p.hmsClientProperties = withCanonicalMetastoreUri(p.raw, p.metastoreUri);
        return p;
    }

    /**
     * The rules that hold for a {@code CREATE}/{@code ALTER CATALOG} statement but are not invariants of
     * a working catalog. A stored catalog that breaks one of them runs today — these checks were added
     * after the hms cutover and have only ever guarded the interactive doors — so refusing it in
     * {@link #of} (which runs on every rebuild) would take it away from its owner with no statement able
     * to repair it.
     *
     * @return this, so the provider's door reads as one statement
     */
    public HiveCatalogProperties checkCreateTimeOnlyRules() {
        // Restores the legacy HMSExternalCatalog.checkProperties fail-fast for the two meta-cache TTL
        // knobs: after the hms cutover an "hms" catalog is created via the SPI provider (not
        // HMSExternalCatalog), so the old per-property validation no longer ran and an invalid ttl (e.g.
        // -2) was silently accepted. Legacy semantics: the value, when present, must be a long >= 0
        // (CACHE_TTL_DISABLE_CACHE); < 0 is rejected. checkLongProperty emits the identical
        // "The parameter ... is wrong, value is ..." message.
        CacheSpec.checkLongProperty(raw.get("file.meta.cache.ttl-second"), 0L, "file.meta.cache.ttl-second");
        CacheSpec.checkLongProperty(raw.get("partition.cache.ttl-second"), 0L, "partition.cache.ttl-second");
        return this;
    }

    /**
     * The properties handed to the HMS client, which are the raw ones with the metastore URI restated
     * under its canonical key.
     *
     * <p>This is not cosmetic. {@code HmsConfHelper} copies the map verbatim into a {@code HiveConf},
     * and {@code HiveConf} knows only {@code hive.metastore.uris}: a catalog written with the
     * {@code uri} short form used to pass the presence check in {@code HiveConnector} and then leave
     * {@code METASTOREURIS} unset, so the client connected nowhere the user had configured.
     */
    public Map<String, String> getHmsClientProperties() {
        return hmsClientProperties;
    }

    private static Map<String, String> withCanonicalMetastoreUri(Map<String, String> raw, String uri) {
        if (uri.equals(raw.get(HIVE_METASTORE_URIS))) {
            return raw;
        }
        Map<String, String> canonical = new LinkedHashMap<>(raw);
        canonical.put(HIVE_METASTORE_URIS, uri);
        return Collections.unmodifiableMap(canonical);
    }

    public String getMetastoreUri() {
        return metastoreUri;
    }

    /**
     * A malformed value fails when the holder is built rather than falling back to the default, per the
     * migration rule for a typed holder. This also removes the asymmetry the hudi connector's own
     * migration left behind, where the same key was strict for hudi tables and lenient for hive tables
     * on one catalog.
     */
    public int getHmsClientPoolSize() {
        return hmsClientPoolSize;
    }

    public boolean isHmsEventsIncrementalSyncEnabled() {
        return enableHmsEventsIncrementalSync;
    }

    public int getHmsEventsBatchSizePerRpc() {
        return hmsEventsBatchSizePerRpc;
    }

    public boolean isIgnoreAbsentPartitions() {
        return ignoreAbsentPartitions;
    }

    public boolean isRecursiveDirectories() {
        return recursiveDirectories;
    }

    public String getStagingDir() {
        return stagingDir;
    }

    public boolean isEnableMappingVarbinary() {
        return enableMappingVarbinary;
    }

    public boolean isEnableMappingTimestampTz() {
        return enableMappingTimestampTz;
    }

    /** The catalog property map verbatim. Callers that reach the HMS client want {@link #getHmsClientProperties()}. */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
