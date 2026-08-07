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

package org.apache.doris.connector.fluss;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Everything a user writes in {@code CREATE CATALOG} for a fluss catalog, bound, derived and checked.
 *
 * <p>{@link #of(Map)} binds, derives and validates in one step, so an instance that exists has valid
 * properties and every reader downstream takes a getter instead of re-parsing the map. It reaches neither
 * the filesystem nor the fluss cluster: it runs at {@code CREATE CATALOG}, again on the merged candidate
 * when {@code ALTER CATALOG} validates, and once more every time the connector is rebuilt — including on
 * an FE replaying the edit log, where I/O would mean an unreachable cluster could stop FE from starting.
 * Reachability is {@code testConnection}'s job.
 *
 * <p>Every rule here already ran on the rebuild path before this class existed ({@code FlussConnector}'s
 * constructor validated too), so none of them can newly break a catalog that was created successfully —
 * which is why this connector needs no separate create-time-only rule set.
 *
 * <p><b>Unknown keys are accepted, always.</b> The same map carries engine keys ({@code type},
 * {@code meta.cache.*}, ...) and storage keys ({@code s3.*}, ...), and {@code ALTER CATALOG} merges
 * properties: it can overwrite a key but never remove one, so a key refused here would leave a catalog
 * that no statement could repair. Bad <i>values</i> are refused; unrecognized <i>names</i> are not.
 *
 * <p><b>Naming rule.</b> Everything a user may write is prefixed {@code fluss.}, and every such key except
 * the Doris-only ones ({@link #UNION_READ_MODE}, {@link #UNION_READ_MAX_TAIL_ROWS}) is handed to the fluss
 * client verbatim with that prefix stripped — see {@link #getFlussClientConfig()}. Nothing here enumerates
 * the fluss client's own option set: fluss owns those names and adds to them between releases, and a
 * Doris-side allowlist would silently reject options that fluss understands perfectly well.
 *
 * <p>No lake/paimon connection property belongs here. The tiering service writes the lake catalog
 * configuration into each datalake-enabled table's own properties ({@code table.datalake.paimon.*}), so
 * union read reads it from the table, never from the catalog.
 */
public final class FlussCatalogProperties {

    /** Prefix every fluss catalog property carries; also what is stripped on the way to the client. */
    public static final String PROPERTY_PREFIX = "fluss.";

    /** Required. Comma-separated {@code host:port} list used to bootstrap the fluss cluster. */
    public static final String BOOTSTRAP_SERVERS = "fluss.bootstrap.servers";

    /**
     * Optional. How a scan of a datalake-enabled table combines the paimon lake with the fluss log.
     *
     * <p>Doris-only: it selects a planning strategy and is never passed to the fluss client. The three
     * values exist because "did this query actually read the lake?" is otherwise unobservable —
     * {@code auto} silently falls back to a fluss-only read when the lake has no readable snapshot yet,
     * which makes a union-read regression test pass for the wrong reason. {@code required} turns that
     * fallback into an error and {@code disabled} forces the fluss-only path, so a test can pin both sides
     * of the comparison.
     */
    public static final String UNION_READ_MODE = "fluss.union_read.mode";

    /**
     * Optional. How many log-tail rows one bucket of a primary-key table may contribute to a union read.
     *
     * <p>Doris-only. Reading a primary-key table's lake together with its log tail holds that tail in BE
     * memory — once as the set of keys that suppress lake rows, once as the rows the tail itself
     * contributes — so an unbounded tail is an unbounded allocation inside a process that serves every
     * other query too. Fluss's own engines have no such limit (Flink's tiering reader accumulates the tail
     * on the heap and lets the task manager die and restart if it does not fit), which is exactly the
     * difference: BE has no restart to fall back on. A tail is by design the data written within one
     * {@code table.datalake.freshness} period, so the default is far above any healthy table; hitting it
     * means tiering has stalled, and the error says so.
     */
    public static final String UNION_READ_MAX_TAIL_ROWS = "fluss.union_read.max_tail_rows";

    /** Default {@link #UNION_READ_MAX_TAIL_ROWS}: two million rows per bucket. */
    public static final long DEFAULT_MAX_TAIL_ROWS = 2_000_000L;

    /**
     * Optional. Whether a fluss BINARY/BYTES column reads as Doris VARBINARY instead of STRING.
     *
     * <p>Unprefixed on purpose: this is the engine-wide catalog property
     * ({@code CatalogProperty.ENABLE_MAPPING_VARBINARY}) that the hive, paimon and iceberg catalogs
     * already answer to, and a user should not have to learn a fluss-specific spelling for it. Being
     * unprefixed also keeps it out of {@link #getFlussClientConfig()} for free.
     */
    public static final String ENABLE_MAPPING_VARBINARY = "enable.mapping.varbinary";

    /**
     * Optional. Whether a fluss TIMESTAMP_LTZ column reads as Doris TIMESTAMPTZ instead of DATETIMEV2.
     * Engine-wide catalog property, same reasoning as {@link #ENABLE_MAPPING_VARBINARY}.
     */
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz";

    /** Value set of {@link #UNION_READ_MODE}. */
    public enum UnionReadMode {
        /** Union read when the lake has a readable snapshot, fluss-only when it does not. */
        AUTO,
        /** Union read, or fail: a datalake table with no readable lake snapshot is an error. */
        REQUIRED,
        /** Never union read: scan fluss only, even for a datalake table with a lake snapshot. */
        DISABLED;

        /** The lower-case spelling users write, and what {@code appendExplainInfo} prints. */
        public String propertyValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    @ConnectorProperty(names = {BOOTSTRAP_SERVERS},
            description = "comma-separated host:port list used to bootstrap the fluss cluster")
    private String bootstrapServers = "";

    @ConnectorProperty(names = {UNION_READ_MODE}, required = false,
            description = "how a datalake table's scan combines the lake with the log: "
                    + "auto | required | disabled")
    private String unionRead = "";

    @ConnectorProperty(names = {UNION_READ_MAX_TAIL_ROWS}, required = false,
            description = "guard rail: log-tail rows one bucket may contribute to a union read")
    private long maxTailRows = DEFAULT_MAX_TAIL_ROWS;

    @ConnectorProperty(names = {ENABLE_MAPPING_VARBINARY}, required = false,
            description = "map fluss BINARY/BYTES to Doris VARBINARY instead of STRING")
    private boolean enableMappingVarbinary;

    @ConnectorProperty(names = {ENABLE_MAPPING_TIMESTAMP_TZ}, required = false,
            description = "map fluss TIMESTAMP_LTZ to Doris TIMESTAMPTZ instead of DATETIMEV2")
    private boolean enableMappingTimestampTz;

    private UnionReadMode unionReadMode;
    private FlussTypeMapping.Options typeMappingOptions;
    private Map<String, String> flussClientConfig;

    private FlussCatalogProperties() {
    }

    /**
     * Binds, derives and validates. Pure, idempotent and free of I/O — it runs on every connector build.
     *
     * <p>A blank value counts as absent, framework-wide: the binder skips it, so the field keeps its
     * default. That is what makes {@code fluss.bootstrap.servers = "   "} report the same "is missing" as
     * omitting the key, and it is also why a blank tail ceiling reads as the default rather than as an
     * error.
     */
    public static FlussCatalogProperties of(Map<String, String> properties) {
        FlussCatalogProperties p = new FlussCatalogProperties();
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        new ParamRules()
                .require(p.bootstrapServers,
                        "Required property '" + BOOTSTRAP_SERVERS + "' is missing")
                // ParamRules.check throws when the condition holds, so this states the FAILING case.
                // Zero and negative are rejected rather than read as "no limit": a user who wants no
                // ceiling has no way to say so, which is the point.
                .check(() -> p.maxTailRows <= 0, "Invalid value '" + p.maxTailRows + "' for property '"
                        + UNION_READ_MAX_TAIL_ROWS + "'; expected a positive number of rows")
                .validate();
        checkBootstrapServerList(p.bootstrapServers);
        // Derived after the checks, so a catalog that fails validation fails on the property it got
        // wrong rather than on a value derived from it.
        p.unionReadMode = parseUnionReadMode(p.unionRead);
        p.typeMappingOptions =
                new FlussTypeMapping.Options(p.enableMappingVarbinary, p.enableMappingTimestampTz);
        p.flussClientConfig = deriveFlussClientConfig(properties, p.bootstrapServers);
        return p;
    }

    /**
     * Reads {@link #UNION_READ_MODE}, defaulting to {@link UnionReadMode#AUTO}.
     *
     * <p>An unrecognized value fails rather than falling back to the default: the difference between
     * {@code auto} and {@code required} is only ever visible as "the query returned fewer rows than it
     * should", so a typo that silently meant {@code auto} would defeat the mode it was trying to ask for.
     */
    private static UnionReadMode parseUnionReadMode(String value) {
        if (value.isEmpty()) {
            return UnionReadMode.AUTO;
        }
        for (UnionReadMode mode : UnionReadMode.values()) {
            if (mode.name().equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Invalid value '" + value + "' for property '"
                + UNION_READ_MODE + "'; expected one of auto, required, disabled");
    }

    /**
     * The fluss client configuration this catalog describes: every {@code fluss.}-prefixed property that is
     * not Doris-only, with the prefix stripped.
     *
     * <p>The bound bootstrap servers are written LAST, so the trimmed value the rest of the connector
     * validates and reports is also the one the client is configured with. Stripping the prefix off the raw
     * entry yields the same key, and letting the raw entry win would hand the client an untrimmed string
     * that no other reader here ever sees.
     *
     * <p>Sorted so that a configuration is printable and comparable in a test without the map iteration
     * order leaking in.
     */
    private static Map<String, String> deriveFlussClientConfig(Map<String, String> properties,
            String boundBootstrapServers) {
        Map<String, String> config = new TreeMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(PROPERTY_PREFIX) || isDorisOnly(key)) {
                continue;
            }
            config.put(key.substring(PROPERTY_PREFIX.length()), entry.getValue());
        }
        if (!boundBootstrapServers.isEmpty()) {
            config.put(BOOTSTRAP_SERVERS.substring(PROPERTY_PREFIX.length()), boundBootstrapServers);
        }
        return Collections.unmodifiableMap(config);
    }

    /** Whether {@code key} configures Doris's own behaviour and must not reach the fluss client. */
    private static boolean isDorisOnly(String key) {
        return UNION_READ_MODE.equals(key) || UNION_READ_MAX_TAIL_ROWS.equals(key);
    }

    private static void checkBootstrapServerList(String value) {
        if (value.isEmpty()) {
            // Already reported by the require() rule above; nothing to shape-check.
            return;
        }
        for (String entry : value.split(",", -1)) {
            String server = entry.trim();
            // lastIndexOf, not indexOf: a bracketed IPv6 literal ("[::1]:9123") contains colons of its own.
            int colon = server.lastIndexOf(':');
            if (colon <= 0 || colon == server.length() - 1) {
                throw new IllegalArgumentException("Invalid value '" + value + "' for property '"
                        + BOOTSTRAP_SERVERS + "'; expected a comma-separated host:port list");
            }
            int port;
            try {
                port = Integer.parseInt(server.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port in '" + server + "' for property '"
                        + BOOTSTRAP_SERVERS + "'; expected a number between 1 and 65535");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Invalid port in '" + server + "' for property '"
                        + BOOTSTRAP_SERVERS + "'; expected a number between 1 and 65535");
            }
        }
    }

    /** The declared bootstrap servers, trimmed. Never blank on an instance that exists. */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public UnionReadMode getUnionReadMode() {
        return unionReadMode;
    }

    /** The per-bucket log-tail ceiling for a union read. Always positive. */
    public long getMaxTailRows() {
        return maxTailRows;
    }

    /** The type-mapping switches this catalog declares; both default to off. */
    public FlussTypeMapping.Options getTypeMappingOptions() {
        return typeMappingOptions;
    }

    /**
     * The fluss client options, unmodifiable and prefix-stripped.
     *
     * <p><b>It can carry credentials</b> — a catalog that authenticates writes them as
     * {@code fluss.client.security.*}. Pass it to the fluss client and to the BE-side scanner that opens
     * the same connection, never to anything that can reach user-visible output.
     */
    public Map<String, String> getFlussClientConfig() {
        return flussClientConfig;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
