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

    /** Whether to map Paimon BINARY/VARBINARY to Doris VARBINARY instead of STRING. */
    public static final String ENABLE_MAPPING_BINARY_AS_VARBINARY = "enable_mapping_binary_as_varbinary";

    /** Whether to map Paimon TIMESTAMP_WITH_LOCAL_TIME_ZONE to TIMESTAMPTZ. */
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable_mapping_timestamp_tz";

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
    public static final String REST_TOKEN_PROVIDER = "paimon.rest.token.provider";
    public static final String REST_DLF_ACCESS_KEY_ID = "paimon.rest.dlf.access-key-id";
    public static final String REST_DLF_ACCESS_KEY_SECRET = "paimon.rest.dlf.access-key-secret";

    // ---- JDBC flavor keys ----
    public static final String[] JDBC_URI = {"uri", "paimon.jdbc.uri"};
    public static final String[] JDBC_USER = {"paimon.jdbc.user", "jdbc.user"};
    public static final String[] JDBC_PASSWORD = {"paimon.jdbc.password", "jdbc.password"};
    public static final String[] JDBC_DRIVER_URL = {"paimon.jdbc.driver_url", "jdbc.driver_url"};
    public static final String[] JDBC_DRIVER_CLASS = {"paimon.jdbc.driver_class", "jdbc.driver_class"};

    // ---- DLF flavor keys (legacy AliyunDLFBaseProperties) ----
    public static final String[] DLF_ACCESS_KEY = {"dlf.access_key", "dlf.catalog.accessKeyId"};
    public static final String[] DLF_SECRET_KEY = {"dlf.secret_key", "dlf.catalog.accessKeySecret"};
    public static final String[] DLF_SESSION_TOKEN = {"dlf.session_token", "dlf.catalog.sessionToken"};
    public static final String DLF_REGION = "dlf.region";
    public static final String[] DLF_ENDPOINT = {"dlf.endpoint", "dlf.catalog.endpoint"};
    public static final String[] DLF_UID = {"dlf.catalog.uid", "dlf.uid"};
    public static final String[] DLF_CATALOG_ID = {"dlf.catalog.id", "dlf.catalog_id"};
    public static final String[] DLF_ACCESS_PUBLIC = {"dlf.access.public", "dlf.catalog.accessPublic"};
    public static final String DLF_ACCESS_PUBLIC_DEFAULT = "false";
    public static final String[] DLF_PROXY_MODE = {"dlf.catalog.proxyMode", "dlf.proxy.mode"};
    public static final String DLF_PROXY_MODE_DEFAULT = "DLF_ONLY";

    private PaimonConnectorProperties() {
    }
}
