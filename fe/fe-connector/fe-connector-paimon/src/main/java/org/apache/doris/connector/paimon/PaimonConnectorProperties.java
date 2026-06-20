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

/**
 * Property key constants for Paimon connector configuration.
 *
 * <p>Pure static-constant holder (no logic), mirroring the role of
 * {@code MCConnectorProperties}. Where a Doris-facing property accepts multiple
 * aliases (matching the legacy fe-core {@code @ConnectorProperty(names = {...})}
 * declarations), the aliases are exposed as a {@code String[]} in alias-priority
 * order so {@link PaimonCatalogFactory} can resolve them with
 * {@code firstNonBlank}.
 */
public final class PaimonConnectorProperties {

    /** Paimon catalog backend type: filesystem, hms, dlf, rest, jdbc. */
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";

    /** Warehouse location for the Paimon catalog. */
    public static final String WAREHOUSE = "warehouse";

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

    /** Default catalog type when not specified. */
    public static final String DEFAULT_CATALOG_TYPE = "filesystem";

    // ---- Flavor literals (the accepted paimon.catalog.type values) ----
    public static final String FILESYSTEM = "filesystem";
    public static final String HMS = "hms";
    public static final String REST = "rest";
    public static final String JDBC = "jdbc";
    public static final String DLF = "dlf";

    // ---- HMS flavor keys ----
    /** Hive metastore uri; primary key + the {@code "uri"} alias (legacy HMSBaseProperties). */
    public static final String[] HMS_URI = {"hive.metastore.uris", "uri"};
    public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS = "client-pool-cache.eviction-interval-ms";
    /** Default client-pool-cache eviction interval (ms) = 5 minutes (legacy default). */
    public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT = "300000";
    public static final String LOCATION_IN_PROPERTIES = "location-in-properties";
    public static final String LOCATION_IN_PROPERTIES_DEFAULT = "false";

    // ---- REST flavor keys ----
    public static final String[] REST_URI = {"paimon.rest.uri", "uri"};
    // REST_TOKEN_PROVIDER / REST_DLF_ACCESS_KEY_ID / REST_DLF_ACCESS_KEY_SECRET removed (P2-T03): the
    // REST dlf-token requireIf is now owned by RestMetaStoreProperties (the @ConnectorProperty aliases
    // live in fe-connector-metastore-spi); the connector no longer hand-checks them.

    // ---- JDBC flavor keys ----
    public static final String[] JDBC_URI = {"uri", "paimon.jdbc.uri"};
    public static final String[] JDBC_USER = {"paimon.jdbc.user", "jdbc.user"};
    public static final String[] JDBC_PASSWORD = {"paimon.jdbc.password", "jdbc.password"};
    public static final String[] JDBC_DRIVER_URL = {"paimon.jdbc.driver_url", "jdbc.driver_url"};
    public static final String[] JDBC_DRIVER_CLASS = {"paimon.jdbc.driver_class", "jdbc.driver_class"};

    // DLF flavor keys removed (P2-T03): the dlf.catalog.* assembly + endpoint-from-region derivation +
    // validation moved to DlfMetaStoreProperties in fe-connector-metastore-spi (its @ConnectorProperty
    // aliases are the single source of truth); the connector keeps only appendDlfOptions' literal Options.

    private PaimonConnectorProperties() {
    }
}
