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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Property constants for Hive connector configuration.
 * Mirrors keys from fe-core's HMS property classes without taking
 * a compile-time dependency on fe-core.
 */
public final class HiveConnectorProperties {

    private HiveConnectorProperties() {
    }

    // -- HMS connection --
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";

    // -- HMS client pool --
    public static final String HMS_CLIENT_POOL_SIZE = "hive.metastore.client.pool.size";
    public static final int DEFAULT_HMS_CLIENT_POOL_SIZE = 8;

    // -- authentication --
    public static final String AUTH_TYPE = "hive.metastore.authentication.type";
    public static final String SERVICE_PRINCIPAL = "hive.metastore.service.principal";
    public static final String CLIENT_PRINCIPAL = "hive.metastore.client.principal";
    public static final String CLIENT_KEYTAB = "hive.metastore.client.keytab";

    // -- table format detection --
    public static final String TABLE_TYPE_PARAM = "table_type";
    public static final String SPARK_TABLE_PROVIDER = "spark.sql.sources.provider";
    public static final String FLINK_CONNECTOR = "connector";

    // -- type mapping options --
    // Catalog-level property keys (dot form), matching CatalogProperty and the iceberg/paimon connectors.
    // ExternalCatalog forwards these two keys to the connector; the earlier underscore spellings were never
    // populated, so the toggles silently no-op'd (hive BINARY always STRING, timestamp never TIMESTAMPTZ).
    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    // -- CREATE TABLE / DATABASE property keys (legacy HiveMetadataOps) --
    public static final String CREATE_FILE_FORMAT = "file_format";
    public static final String CREATE_LOCATION = "location";
    public static final String CREATE_OWNER = "owner";
    public static final String CREATE_COMMENT = "comment";
    public static final String CREATE_TRANSACTIONAL = "transactional";
    /**
     * Property keys that legacy {@code HiveMetadataOps} stamps into the metastore table parameters under a
     * {@code doris.} prefix (so they round-trip). Mirrors legacy {@code HiveMetadataOps.DORIS_HIVE_KEYS}.
     */
    public static final Set<String> DORIS_HIVE_KEYS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(CREATE_FILE_FORMAT, CREATE_LOCATION)));
    public static final String DORIS_PROP_PREFIX = "doris.";

    // -- environment keys threaded from fe-core DefaultConnectorContext (must stay byte-identical there) --
    public static final String ENV_HIVE_DEFAULT_FILE_FORMAT = "hive_default_file_format";
    public static final String ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE = "enable_create_hive_bucket_table";
    public static final String ENV_DORIS_VERSION = "doris_version";
    /** Fallback default file format, matching legacy {@code Config.hive_default_file_format} default. */
    public static final String DEFAULT_FILE_FORMAT = "orc";

    // -- session variable read for a text table's compression default (legacy hive_text_compression) --
    public static final String SESSION_HIVE_TEXT_COMPRESSION = "hive_text_compression";
    public static final String TEXT_COMPRESSION_UNCOMPRESSED = "uncompressed";
    public static final String TEXT_COMPRESSION_PLAIN = "plain";

    // Session variable gating the OpenX-JSON "read the whole JSON row into one CSV column" mode (legacy
    // SessionVariable.read_hive_json_in_one_column). Byte-identical to the fe-core session-var name; it is
    // surfaced through ConnectorSession.getSessionProperties() (VariableMgr dumps all visible vars).
    public static final String SESSION_READ_HIVE_JSON_IN_ONE_COLUMN = "read_hive_json_in_one_column";

    /**
     * Bucket algorithm string produced by {@code CreateTableInfoToConnectorRequestConverter} for a
     * random (non-hash) distribution. Hive external tables only support hash bucketing.
     */
    public static final String BUCKET_ALGO_RANDOM = "doris_random";

    // ===== Metastore incremental event sync (per-catalog opt-in) =====

    /** Whether this catalog polls HMS notification events for incremental metadata refresh. */
    public static final String ENABLE_HMS_EVENTS_INCREMENTAL_SYNC =
            "hive.enable_hms_events_incremental_sync";

    /** Max notification events fetched per RPC when incremental event sync is enabled. */
    public static final String HMS_EVENTS_BATCH_SIZE_PER_RPC = "hive.hms_events_batch_size_per_rpc";

    /** Default batch size, matching the engine's legacy {@code hms_events_batch_size_per_rpc} default. */
    public static final int DEFAULT_HMS_EVENTS_BATCH_SIZE = 500;

    /**
     * When {@code false}, a partition whose storage location does not exist fails the query loud
     * ({@code "Partition location does not exist"}); the default {@code true} tolerates it by skipping
     * the partition with a warning. Mirrors legacy {@code HiveExternalMetaCache} semantics.
     */
    public static final String IGNORE_ABSENT_PARTITIONS = "hive.ignore_absent_partitions";

    /**
     * Parse an integer property with a default value.
     */
    public static int getInt(Map<String, String> props, String key, int defaultVal) {
        String value = props.get(key);
        if (value == null || value.isEmpty()) {
            return defaultVal;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    /**
     * Parse a boolean property with a default value.
     */
    public static boolean getBoolean(Map<String, String> props, String key, boolean defaultVal) {
        String value = props.get(key);
        if (value == null || value.isEmpty()) {
            return defaultVal;
        }
        return Boolean.parseBoolean(value.trim());
    }
}
