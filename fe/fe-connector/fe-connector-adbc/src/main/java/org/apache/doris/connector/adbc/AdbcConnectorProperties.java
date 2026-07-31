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

package org.apache.doris.connector.adbc;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Property constants for ADBC catalogs, and the environment keys the connector reads.
 *
 * <p>fe-core parses no connector properties, so everything a user writes in {@code CREATE CATALOG} is
 * interpreted here. FE-global settings that are not catalog properties arrive through
 * {@code ConnectorContext#getEnvironment()}; the {@link #ENV_DRIVERS_DIR} / {@link #ENV_DRIVER_SECURE_PATH}
 * keys must stay byte-identical to the writes in fe-core's {@code DefaultConnectorContext}.
 */
public final class AdbcConnectorProperties {

    private AdbcConnectorProperties() {
    }

    // -- driver --

    /**
     * The ADBC driver shared library. Named after the JDBC catalog's property for continuity, but it
     * accepts local references only: a bare file name (resolved under {@link #ENV_DRIVERS_DIR}), a
     * {@code file://} URL, or an absolute path. Remote schemes are rejected -- see
     * {@link AdbcDriverPathResolver} for why downloading it per node cannot be made safe.
     */
    public static final String DRIVER_URL = "driver_url";
    public static final String DRIVER_CHECKSUM = "driver_checksum";
    /** Optional; empty lets the driver manager infer the entry point symbol. */
    public static final String DRIVER_ENTRYPOINT = "driver_entrypoint";

    // -- connection --

    /**
     * The ADBC connection URI. It must pin the remote catalog (e.g. {@code postgresql://host:5432/mydb}),
     * because Doris flattens ADBC's three-level catalog/db_schema/table namespace onto its own two-level
     * database/table one.
     */
    public static final String URI = "uri";
    public static final String USER = "user";
    public static final String PASSWORD = "password";

    // -- SQL generation --

    /**
     * The SQL dialect to generate pushed-down queries in, by {@link AdbcDialect#name()}. Optional: when it
     * is absent the connector asks the driver for its vendor and falls back to ANSI, which is what an
     * unrecognized source gets. Set it when the source's vendor string is unhelpful or when its SQL differs
     * from what the vendor name implies.
     */
    public static final String SQL_DIALECT = "sql_dialect";

    /**
     * Prefix for options passed straight through to the driver. The prefix is PART OF THE OPTION NAME and
     * is NOT stripped: ADBC's own option names already start with {@code adbc.} (e.g.
     * {@code adbc.snowflake.sql.db}), so a user writes {@code "adbc.adbc.snowflake.sql.db"}. BE applies the
     * same rule to its own parameter map; the two must not diverge.
     */
    public static final String DRIVER_OPTION_PREFIX = "adbc.";

    // -- environment (not catalog properties) --

    public static final String ENV_DRIVERS_DIR = "adbc_drivers_dir";
    public static final String ENV_DRIVER_SECURE_PATH = "adbc_driver_secure_path";

    /**
     * Returns the {@code adbc.}-prefixed entries with their names kept intact, in iteration order.
     */
    public static Map<String, String> driverOptions(Map<String, String> properties) {
        Map<String, String> options = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(DRIVER_OPTION_PREFIX)) {
                options.put(entry.getKey(), entry.getValue());
            }
        }
        return options;
    }

    /** Returns a required property, or throws naming the property that is missing. */
    public static String require(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Required property '" + key + "' is missing for an adbc catalog");
        }
        return value.trim();
    }
}
