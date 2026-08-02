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
 * Property constants for ADBC catalogs, and the keys of this plugin's own settings file.
 *
 * <p>fe-core parses no connector properties, so everything a user writes in {@code CREATE CATALOG} is
 * interpreted here. Settings that are one-per-FE rather than one-per-catalog live in the plugin's
 * {@code adbc.conf} instead ({@link #CONF_DRIVERS_DIR} / {@link #CONF_DRIVER_SECURE_PATH}), read with
 * {@code ConnectorConf.get}. They have no fe.conf half: this connector is new, so there is no
 * deployment that configured them before {@code adbc.conf} existed, and adding a {@code @ConfField}
 * would tie a key name of this plugin into fe-core for nothing.
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

    /**
     * Optional MD5 of the driver library, verified at {@code CREATE CATALOG} against the file this FE
     * resolves. It guards the mistake this catalog type invites -- a hand-placed file that is the wrong
     * build, or a stale copy on one node -- which otherwise loads fine and surfaces much later as a query
     * failure that says nothing about a file. <b>It only sees the FE's copy</b>; see
     * {@link AdbcDriverPathResolver#checkChecksum}.
     */
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

    // -- partitioned read --

    /**
     * How a scan may be split into the driver's own partitions and spread over several backends: one of
     * {@link PartitionedReadMode}, spelled as its lowercase name.
     *
     * <p>Three states rather than a switch, because "use partitions when you can" and "use partitions or
     * fail" are different requirements and encoding them as two booleans admits a combination that
     * contradicts itself.
     */
    public static final String PARTITIONED_READ = "partitioned_read";

    /** What a catalog asks of partitioned execution. */
    public enum PartitionedReadMode {
        /**
         * Split the scan when the driver can, read it as one statement when it cannot. The default,
         * because parallelism is the point of reading through ADBC rather than one connection, and a
         * driver without partitions must still be usable.
         */
        AUTO,
        /**
         * Never ask for partitions. Asking is not free: on a Flight SQL source the call that returns them
         * <b>is</b> the query's execution, so planning gains a remote round trip and the source starts
         * working before Doris has committed to running the plan. This is the way back to the
         * single-statement path for a source that pays badly for that, or whose partitions Doris then
         * fails to read.
         */
        DISABLED,
        /**
         * Split the scan, or fail the query saying why.
         *
         * <p><b>For anything that must not silently lose its parallelism.</b> A test is the clearest
         * case: under {@link #AUTO} a driver that stops partitioning turns the test green while quietly
         * exercising the fallback instead of the path under test -- the failure looks exactly like a pass.
         * A deployment sized for N backends has the same problem in slower motion, which is why this is a
         * supported mode and not a test-only flag.
         */
        REQUIRED
    }

    /**
     * The most partitions one scan may plan. A guard rail against a pathological source, not a tuning knob:
     * each partition costs a scan range carrying an opaque descriptor of a few hundred bytes, so a million
     * of them would exhaust FE. Exceeding it fails the query rather than falling back to a single range,
     * because by then the source has already executed the query and a fallback would make it execute a
     * second time while the first result set sits unread.
     */
    public static final String MAX_PARTITIONS = "max_partitions";

    private static final int DEFAULT_MAX_PARTITIONS = 1024;

    /**
     * Prefix for options passed straight through to the driver. The prefix is PART OF THE OPTION NAME and
     * is NOT stripped: ADBC's own option names already start with {@code adbc.} (e.g.
     * {@code adbc.snowflake.sql.db}), so a user writes {@code "adbc.adbc.snowflake.sql.db"}. BE applies the
     * same rule to its own parameter map; the two must not diverge.
     */
    public static final String DRIVER_OPTION_PREFIX = "adbc.";

    // -- adbc.conf keys (not catalog properties) --

    /**
     * Directory a bare {@code driver_url} file name resolves under. Defaults to
     * {@code <DORIS_HOME>/plugins/adbc_drivers}, which is where build.sh creates the directory.
     */
    public static final String CONF_DRIVERS_DIR = "drivers_dir";

    /**
     * Semicolon-separated directories a driver may be loaded from; {@code *} allows any. Defaults to
     * {@link #DEFAULT_DRIVER_SECURE_PATH}.
     */
    public static final String CONF_DRIVER_SECURE_PATH = "driver_secure_path";

    /** The subdirectory of DORIS_HOME {@link #CONF_DRIVERS_DIR} defaults to. */
    public static final String DEFAULT_DRIVERS_SUBDIR = "/plugins/adbc_drivers";

    /** Allow any directory, matching the jdbc catalog's default. */
    public static final String DEFAULT_DRIVER_SECURE_PATH = "*";

    /** Engine-wide rather than this connector's setting, so it keeps coming from the environment. */
    public static final String ENV_DORIS_HOME = "doris_home";

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

    /**
     * Reads {@link #PARTITIONED_READ}, defaulting to {@link PartitionedReadMode#AUTO}.
     *
     * <p>An unrecognized value fails rather than falling back to the default: a typo that silently meant
     * AUTO would show up only as lost parallelism, or -- worse, for a catalog that asked for REQUIRED --
     * as the silent downgrade that mode exists to forbid.
     */
    public static PartitionedReadMode partitionedReadMode(Map<String, String> properties) {
        String value = properties.get(PARTITIONED_READ);
        if (value == null || value.trim().isEmpty()) {
            return PartitionedReadMode.AUTO;
        }
        String normalized = value.trim();
        for (PartitionedReadMode mode : PartitionedReadMode.values()) {
            if (mode.name().equalsIgnoreCase(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Property '" + PARTITIONED_READ + "' must be one of 'auto',"
                + " 'disabled' or 'required', but is '" + value + "'");
    }

    /** Reads {@link #MAX_PARTITIONS}, defaulting to {@value #DEFAULT_MAX_PARTITIONS}. */
    public static int maxPartitions(Map<String, String> properties) {
        String value = properties.get(MAX_PARTITIONS);
        if (value == null || value.trim().isEmpty()) {
            return DEFAULT_MAX_PARTITIONS;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Property '" + MAX_PARTITIONS
                    + "' must be a positive integer, but is '" + value + "'", e);
        }
        if (parsed < 1) {
            throw new IllegalArgumentException("Property '" + MAX_PARTITIONS
                    + "' must be at least 1, but is '" + value + "'");
        }
        return parsed;
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
