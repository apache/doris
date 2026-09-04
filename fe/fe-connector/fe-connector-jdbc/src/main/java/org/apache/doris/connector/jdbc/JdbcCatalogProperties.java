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

package org.apache.doris.connector.jdbc;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Everything a user writes in {@code CREATE CATALOG} for a JDBC catalog, bound and checked.
 *
 * <p>fe-core parses no connector properties, so this class is where they are interpreted. Settings
 * that are one-per-FE rather than one-per-catalog are not here -- they live in the plugin's
 * {@code jdbc.conf} and are read through {@link JdbcConf}.
 *
 * <p><b>{@link #of} is deliberately thin, and {@link #checkCreateTimeOnlyRules()} is not.</b> This
 * connector has always validated a great deal more at {@code CREATE}/{@code ALTER} than at run time:
 * the pool sizes had bounds no reader ever enforced, the boolean properties had to be spelled
 * {@code true}/{@code false} where the reader would have taken anything, and the database-list
 * consistency rule was checked once and never again. Those are rules about a statement, not
 * invariants of a working catalog -- a stored catalog that breaks them runs today. of() runs on every
 * rebuild, including on an FE replaying the edit log, so moving them into it would take such catalogs
 * away from their owners with no statement able to repair them. They stay in the method the
 * interactive doors call.
 *
 * <p><b>Unknown keys are accepted, always.</b> The same map carries engine keys ({@code type},
 * {@code meta.cache.*}, ...) and storage keys, and {@code ALTER CATALOG} merges properties: it can
 * overwrite a key but never remove one, so a key refused here would leave a catalog that no statement
 * could repair. Bad <i>values</i> are refused; unrecognized <i>names</i> are not.
 */
public final class JdbcCatalogProperties {

    /**
     * The prefix a user may put on any key, from when these were written as {@code jdbc.jdbc_url}. It
     * is stripped from every key alike, so it is not an alias of any one property.
     */
    public static final String JDBC_PROPERTIES_PREFIX = "jdbc.";

    // -- connection --

    public static final String JDBC_URL = "jdbc_url";
    public static final String USER = "user";
    public static final String PASSWORD = "password";

    // -- driver --

    public static final String DRIVER_CLASS = "driver_class";

    /** The driver jar: a bare file name (resolved under {@link JdbcConf#CONF_DRIVERS_DIR}) or a URL. */
    public static final String DRIVER_URL = "driver_url";

    /** MD5 of the driver jar. Computed and stored at CREATE when the user names none. */
    public static final String DRIVER_CHECKSUM = "checksum";

    // -- connection pool --

    public static final String CONNECTION_POOL_MIN_SIZE = "connection_pool_min_size";
    public static final String CONNECTION_POOL_MAX_SIZE = "connection_pool_max_size";
    public static final String CONNECTION_POOL_MAX_WAIT_TIME = "connection_pool_max_wait_time";
    public static final String CONNECTION_POOL_MAX_LIFE_TIME = "connection_pool_max_life_time";
    public static final String CONNECTION_POOL_KEEP_ALIVE = "connection_pool_keep_alive";

    // -- metadata filtering --

    public static final String ONLY_SPECIFIED_DATABASE = "only_specified_database";
    public static final String INCLUDE_DATABASE_LIST = "include_database_list";
    public static final String EXCLUDE_DATABASE_LIST = "exclude_database_list";

    // -- type mapping options --

    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    // -- identifier mapping --

    public static final String LOWER_CASE_META_NAMES = "lower_case_meta_names";
    public static final String META_NAMES_MAPPING = "meta_names_mapping";

    /**
     * Rejected as a catalog property: it was replaced by {@link #LOWER_CASE_META_NAMES}. The same name
     * is also a <i>session</i> variable that {@code JdbcConnectorMetadata} reads, which is a different
     * thing that happens to be spelled the same.
     */
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";

    /**
     * Owned by the engine, which parses the rules; this connector reads the same key to decide what it
     * may push down. The literal is copied rather than shared because the engine's classes are not on
     * this plugin's compile path.
     */
    public static final String FUNCTION_RULES = "function_rules";

    /**
     * Owned by the engine, which decides whether to run a connectivity test at CREATE. Validated here
     * (it is this connector's answer that a bad value is refused) but never read here, so it gets no
     * field -- mirroring a key's real owner rather than copying it.
     */
    public static final String TEST_CONNECTION = "test_connection";

    private static final int MAX_POOL_WAIT_TIME = 30000;
    private static final int MIN_POOL_LIFE_TIME = 150000;

    @ConnectorProperty(names = {JDBC_URL}, description = "the JDBC connection URL")
    private String jdbcUrl;

    @ConnectorProperty(names = {USER}, required = false, description = "remote user name")
    private String user = "";

    @ConnectorProperty(names = {PASSWORD}, required = false, sensitive = true,
            description = "remote password")
    private String password = "";

    @ConnectorProperty(names = {DRIVER_CLASS}, required = false,
            description = "the JDBC driver class name")
    private String driverClass = "";

    @ConnectorProperty(names = {DRIVER_URL}, required = false,
            description = "the driver jar: a bare file name under drivers_dir, or a URL")
    private String driverUrl = "";

    @ConnectorProperty(names = {DRIVER_CHECKSUM}, required = false,
            description = "MD5 of the driver jar")
    private String driverChecksum = "";

    @ConnectorProperty(names = {CONNECTION_POOL_MIN_SIZE}, required = false,
            description = "connections kept open when idle")
    private int connectionPoolMinSize = 1;

    @ConnectorProperty(names = {CONNECTION_POOL_MAX_SIZE}, required = false,
            description = "the most connections one FE may open to the source")
    private int connectionPoolMaxSize = 30;

    @ConnectorProperty(names = {CONNECTION_POOL_MAX_WAIT_TIME}, required = false,
            description = "milliseconds a query waits for a free connection")
    private int connectionPoolMaxWaitTime = 5000;

    @ConnectorProperty(names = {CONNECTION_POOL_MAX_LIFE_TIME}, required = false,
            description = "milliseconds before a pooled connection is retired")
    private int connectionPoolMaxLifeTime = 1800000;

    @ConnectorProperty(names = {CONNECTION_POOL_KEEP_ALIVE}, required = false,
            description = "keep idle connections alive rather than letting them time out")
    private boolean connectionPoolKeepAlive;

    @ConnectorProperty(names = {ONLY_SPECIFIED_DATABASE}, required = false,
            description = "expose only the database named in the URL")
    private boolean onlySpecifiedDatabase;

    @ConnectorProperty(names = {INCLUDE_DATABASE_LIST}, required = false,
            description = "comma-separated databases to expose")
    private String includeDatabaseList = "";

    @ConnectorProperty(names = {EXCLUDE_DATABASE_LIST}, required = false,
            description = "comma-separated databases to hide")
    private String excludeDatabaseList = "";

    @ConnectorProperty(names = {ENABLE_MAPPING_VARBINARY}, required = false,
            description = "map the source's binary types to VARBINARY")
    private boolean enableMappingVarbinary;

    @ConnectorProperty(names = {ENABLE_MAPPING_TIMESTAMP_TZ}, required = false,
            description = "map the source's timestamp-with-time-zone types")
    private boolean enableMappingTimestampTz;

    @ConnectorProperty(names = {FUNCTION_RULES}, required = false,
            description = "engine-owned: which functions may be pushed down")
    private String functionRules = "";

    @ConnectorProperty(names = {LOWER_CASE_META_NAMES}, required = false,
            description = "lower-case the source's database, table and column names")
    private boolean lowerCaseMetaNames;

    @ConnectorProperty(names = {META_NAMES_MAPPING}, required = false,
            description = "explicit name mapping, as JSON")
    private String metaNamesMapping = "";

    private final Map<String, String> raw;

    private JdbcCatalogProperties(Map<String, String> stripped) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(stripped));
    }

    /** For the validation doors, which care that a URL is present rather than what it normalizes to. */
    public static JdbcCatalogProperties of(Map<String, String> properties) {
        return of(properties, UnaryOperator.identity());
    }

    /**
     * Binds and validates, with {@code urlNormalizer} applied to {@link #JDBC_URL}.
     *
     * <p>The normalization is a parameter rather than something this class does, because it depends on
     * both halves of the configuration at once: the URL is a per-catalog property, but whether a SQL
     * Server URL gets {@code encrypt=false} appended is a per-FE setting
     * ({@link JdbcConf#forceSqlServerEncryptFalse}). A holder of per-catalog properties has no business
     * reading deployment config, and a class of deployment config has no business rewriting a catalog's
     * URL, so the connector -- which has both -- supplies the function.
     */
    public static JdbcCatalogProperties of(Map<String, String> properties,
            UnaryOperator<String> urlNormalizer) {
        JdbcCatalogProperties p = new JdbcCatalogProperties(stripJdbcPrefix(properties));
        ConnectorPropertiesUtils.bindConnectorProperties(p, p.raw);
        new ParamRules()
                .require(p.jdbcUrl, "Required property '" + JDBC_URL + "' is missing")
                .validate();
        p.jdbcUrl = urlNormalizer.apply(p.jdbcUrl);
        return p;
    }

    /**
     * The rules that apply to a statement a user is writing now, but not to a catalog already stored.
     *
     * <p>Every one of them has only ever run at {@code CREATE}/{@code ALTER}: no reader enforces the
     * pool bounds, no reader minds a boolean spelled {@code yes}, and the database-list rule is checked
     * nowhere else. Running them from {@link #of} instead would make a stored catalog that breaks one
     * of them unbuildable on the next FE restart, when it works today. See the class javadoc.
     */
    public JdbcCatalogProperties checkCreateTimeOnlyRules() {
        new ParamRules()
                .require(driverUrl, "Required property '" + DRIVER_URL + "' is missing")
                .require(driverClass, "Required property '" + DRIVER_CLASS + "' is missing")
                .validate();

        if (raw.containsKey(LOWER_CASE_TABLE_NAMES)) {
            throw new IllegalArgumentException(
                    "Jdbc catalog property lower_case_table_names is not supported,"
                            + " please use lower_case_meta_names instead");
        }

        // Spelled-out booleans: the binder's parseBoolean would read anything else as false, which is
        // how a typo becomes a silently disabled option.
        checkBoolean(ONLY_SPECIFIED_DATABASE);
        checkBoolean(LOWER_CASE_META_NAMES);
        checkBoolean(CONNECTION_POOL_KEEP_ALIVE);
        checkBoolean(TEST_CONNECTION);

        if (!onlySpecifiedDatabase
                && (!includeDatabaseList.isEmpty() || !excludeDatabaseList.isEmpty())) {
            throw new IllegalArgumentException(
                    "include_database_list and exclude_database_list "
                            + "cannot be set when only_specified_database is false");
        }

        new ParamRules()
                // ParamRules.check throws when the condition holds, so each states the FAILING case.
                .check(() -> connectionPoolMinSize < 0,
                        "connection_pool_min_size must be greater than or equal to 0")
                .check(() -> connectionPoolMaxSize < 1,
                        "connection_pool_max_size must be greater than or equal to 1")
                .check(() -> connectionPoolMaxSize < connectionPoolMinSize,
                        "connection_pool_max_size must be greater than or equal to connection_pool_min_size")
                .check(() -> connectionPoolMaxWaitTime < 0,
                        "connection_pool_max_wait_time must be greater than or equal to 0")
                .check(() -> connectionPoolMaxWaitTime > MAX_POOL_WAIT_TIME,
                        "connection_pool_max_wait_time must be less than or equal to " + MAX_POOL_WAIT_TIME)
                .check(() -> connectionPoolMaxLifeTime < MIN_POOL_LIFE_TIME,
                        "connection_pool_max_life_time must be greater than or equal to " + MIN_POOL_LIFE_TIME)
                .validate();

        if (!metaNamesMapping.isEmpty()) {
            try {
                // Both states of the session's lower_case_table_names, since it may enable it at runtime
                // and a mapping collision can appear only after lower-casing.
                new JdbcIdentifierMapper(false, lowerCaseMetaNames, metaNamesMapping);
                new JdbcIdentifierMapper(true, lowerCaseMetaNames, metaNamesMapping);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        return this;
    }

    private void checkBoolean(String key) {
        String value = raw.get(key);
        if (value != null && !value.isEmpty()
                && !value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException(key + " must be true or false");
        }
    }

    /**
     * Returns the properties with {@link #JDBC_PROPERTIES_PREFIX} removed from every key that carries
     * it, leaving an unprefixed key already present untouched -- the short spelling wins, as it did in
     * both places that used to resolve these keys.
     */
    private static Map<String, String> stripJdbcPrefix(Map<String, String> properties) {
        Map<String, String> stripped = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!entry.getKey().startsWith(JDBC_PROPERTIES_PREFIX)) {
                stripped.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(JDBC_PROPERTIES_PREFIX)) {
                String shortKey = key.substring(JDBC_PROPERTIES_PREFIX.length());
                if (!stripped.containsKey(shortKey)) {
                    stripped.put(shortKey, entry.getValue());
                } else {
                    // Keep the prefixed spelling too: it was left in the map before this class, and an
                    // unrecognized key must survive so ALTER CATALOG can still address it.
                    stripped.put(key, entry.getValue());
                }
            }
        }
        return stripped;
    }

    /** The URL, normalized when this instance was built by the connector. */
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getDriverUrl() {
        return driverUrl;
    }

    public String getDriverChecksum() {
        return driverChecksum;
    }

    public int getConnectionPoolMinSize() {
        return connectionPoolMinSize;
    }

    public int getConnectionPoolMaxSize() {
        return connectionPoolMaxSize;
    }

    public int getConnectionPoolMaxWaitTime() {
        return connectionPoolMaxWaitTime;
    }

    public int getConnectionPoolMaxLifeTime() {
        return connectionPoolMaxLifeTime;
    }

    public boolean isConnectionPoolKeepAlive() {
        return connectionPoolKeepAlive;
    }

    public boolean isOnlySpecifiedDatabase() {
        return onlySpecifiedDatabase;
    }

    public String getIncludeDatabaseList() {
        return includeDatabaseList;
    }

    public String getExcludeDatabaseList() {
        return excludeDatabaseList;
    }

    public boolean isEnableMappingVarbinary() {
        return enableMappingVarbinary;
    }

    public boolean isEnableMappingTimestampTz() {
        return enableMappingTimestampTz;
    }

    public String getFunctionRules() {
        return functionRules;
    }

    public boolean isLowerCaseMetaNames() {
        return lowerCaseMetaNames;
    }

    public String getMetaNamesMapping() {
        return metaNamesMapping;
    }

    /**
     * The catalog's properties with the {@code jdbc.} prefix resolved, unmodifiable.
     *
     * <p><b>It contains the password.</b> Pass it only to code that keeps it internal.
     */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
