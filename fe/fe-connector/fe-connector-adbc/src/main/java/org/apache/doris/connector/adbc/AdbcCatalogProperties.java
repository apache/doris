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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything a user writes in {@code CREATE CATALOG} for an ADBC catalog, bound and checked.
 *
 * <p>fe-core parses no connector properties, so this class is where they are interpreted. Settings that
 * are one-per-FE rather than one-per-catalog are not here -- they live in the plugin's {@code adbc.conf}
 * and are read through {@link AdbcConf}.
 *
 * <p>{@link #of(Map)} binds, derives and validates in one step, so an instance that exists has valid
 * properties and every reader downstream can use a getter instead of re-parsing the map. It reaches
 * neither the filesystem nor the source, and touches no {@code org.apache.arrow.adbc.*} type: it runs at
 * {@code CREATE CATALOG}, again on the merged candidate when {@code ALTER CATALOG} validates, and once
 * more every time the connector is rebuilt -- including on an FE replaying the edit log, where I/O would
 * mean a missing file could stop FE from starting. Checks that do need the filesystem or the source stay
 * in {@code AdbcConnector#preCreateValidation}.
 *
 * <p><b>Unknown keys are accepted, always.</b> The same map carries engine keys ({@code type},
 * {@code meta.cache.*}, ...) and storage keys ({@code s3.*}, ...), and {@code ALTER CATALOG} merges
 * properties: it can overwrite a key but never remove one, so a key refused here would leave a catalog
 * that no statement could repair. Bad <i>values</i> are refused; unrecognized <i>names</i> are not.
 */
public final class AdbcCatalogProperties {

    // -- driver --

    /**
     * The ADBC driver shared library. Named after the JDBC catalog's property for continuity, but it
     * accepts local references only: a bare file name (resolved under {@link AdbcConf#CONF_DRIVERS_DIR}), a
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

    /**
     * The most partitions one scan may plan. A guard rail against a pathological source, not a tuning knob:
     * each partition costs a scan range carrying an opaque descriptor of a few hundred bytes, so a million
     * of them would exhaust FE. Exceeding it fails the query rather than falling back to a single range,
     * because by then the source has already executed the query and a fallback would make it execute a
     * second time while the first result set sits unread.
     */
    public static final String MAX_PARTITIONS = "max_partitions";

    /**
     * Prefix for options passed straight through to the driver. The prefix is PART OF THE OPTION NAME and
     * is NOT stripped: ADBC's own option names already start with {@code adbc.} (e.g.
     * {@code adbc.snowflake.sql.db}), so a user writes {@code "adbc.adbc.snowflake.sql.db"}. BE applies the
     * same rule to its own parameter map; the two must not diverge.
     */
    public static final String DRIVER_OPTION_PREFIX = "adbc.";

    private static final int DEFAULT_MAX_PARTITIONS = 1024;

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

    @ConnectorProperty(names = {DRIVER_URL}, description = "the ADBC driver shared library; "
            + "bare name (under drivers_dir), file:// URL or absolute path")
    private String driverUrl;

    @ConnectorProperty(names = {DRIVER_CHECKSUM}, required = false,
            description = "optional MD5 of the driver library, checked at CREATE CATALOG")
    private String driverChecksum = "";

    @ConnectorProperty(names = {DRIVER_ENTRYPOINT}, required = false,
            description = "optional; empty lets the driver manager infer the entry point symbol")
    private String driverEntrypoint = "";

    @ConnectorProperty(names = {URI},
            description = "the ADBC connection URI; must pin the remote catalog")
    private String uri;

    @ConnectorProperty(names = {USER}, required = false, description = "remote user name")
    private String user = "";

    @ConnectorProperty(names = {PASSWORD}, required = false, sensitive = true,
            description = "remote password")
    private String password = "";

    @ConnectorProperty(names = {SQL_DIALECT}, required = false,
            description = "SQL dialect for pushed-down queries, by AdbcDialect name")
    private String sqlDialect = "";

    @ConnectorProperty(names = {PARTITIONED_READ}, required = false,
            description = "auto | disabled | required")
    private String partitionedRead = "";

    @ConnectorProperty(names = {MAX_PARTITIONS}, required = false,
            description = "guard rail: the most partitions one scan may plan")
    private int maxPartitions = DEFAULT_MAX_PARTITIONS;

    private final Map<String, String> raw;
    private PartitionedReadMode partitionedReadMode;
    private Map<String, String> driverOptions;

    private AdbcCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    public static AdbcCatalogProperties of(Map<String, String> properties) {
        AdbcCatalogProperties p = new AdbcCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        p.partitionedReadMode = parsePartitionedReadMode(p.partitionedRead);
        p.driverOptions = deriveDriverOptions(p.raw);
        new ParamRules()
                .require(p.driverUrl,
                        "Required property '" + DRIVER_URL + "' is missing for an adbc catalog")
                .require(p.uri,
                        "Required property '" + URI + "' is missing for an adbc catalog")
                // ParamRules.check throws when the condition holds, so this states the FAILING case.
                .check(() -> p.maxPartitions < 1, "Property '" + MAX_PARTITIONS
                        + "' must be at least 1, but is '" + p.maxPartitions + "'")
                .validate();
        checkMetaCacheProperties(properties);
        return p;
    }

    /**
     * The cache knobs, which {@code CacheSpec} otherwise reads leniently -- an unparseable ttl or capacity
     * falls back to the default instead of failing. That is the wrong shape for a value an operator typed:
     * the catalog would come up caching for a duration nobody chose, and nothing would say so. Bounds match
     * the other connectors': ttl {@code >= -1} (-1 is "never expire"), capacity {@code >= 0} (0 disables).
     *
     * <p>The keys themselves stay owned by {@code CacheSpec} rather than being mirrored as fields here --
     * copying a framework's key names is how the validator and the reader drift apart -- but validating
     * them belongs in this one call, so that "the properties are valid" covers every key this connector is
     * answerable for.
     */
    private static void checkMetaCacheProperties(Map<String, String> properties) {
        CacheSpec.PropertySpec spec = AdbcMetadataCache.propertySpec();
        CacheSpec.checkBooleanProperty(properties.get(spec.getEnableKey()), spec.getEnableKey());
        CacheSpec.checkLongProperty(properties.get(spec.getTtlKey()), -1L, spec.getTtlKey());
        CacheSpec.checkLongProperty(properties.get(spec.getCapacityKey()), 0L, spec.getCapacityKey());
    }

    /**
     * Reads {@link #PARTITIONED_READ}, defaulting to {@link PartitionedReadMode#AUTO}.
     *
     * <p>An unrecognized value fails rather than falling back to the default: a typo that silently meant
     * AUTO would show up only as lost parallelism, or -- worse, for a catalog that asked for REQUIRED --
     * as the silent downgrade that mode exists to forbid.
     */
    private static PartitionedReadMode parsePartitionedReadMode(String value) {
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

    /**
     * Returns the {@code adbc.}-prefixed entries with their names kept intact, in iteration order.
     */
    private static Map<String, String> deriveDriverOptions(Map<String, String> raw) {
        Map<String, String> options = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            if (entry.getKey().startsWith(DRIVER_OPTION_PREFIX)) {
                options.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(options);
    }

    public String getDriverUrl() {
        return driverUrl;
    }

    public String getDriverChecksum() {
        return driverChecksum;
    }

    public String getDriverEntrypoint() {
        return driverEntrypoint;
    }

    public String getUri() {
        return uri;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getSqlDialect() {
        return sqlDialect;
    }

    public PartitionedReadMode getPartitionedReadMode() {
        return partitionedReadMode;
    }

    public int getMaxPartitions() {
        return maxPartitions;
    }

    public Map<String, String> getDriverOptions() {
        return driverOptions;
    }

    /**
     * The catalog's properties as written, unmodifiable.
     *
     * <p><b>It contains the password.</b> Pass it only to code that keeps it internal -- never into
     * anything that can reach user-visible output, such as a {@code ConnectorTableSchema}'s table
     * properties.
     */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
