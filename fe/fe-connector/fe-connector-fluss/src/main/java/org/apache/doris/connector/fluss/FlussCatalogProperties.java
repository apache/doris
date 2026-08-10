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
 * <p>The lake connection is the one exception, and {@link #LAKE_OPTION_PREFIX} is the reason it has to be:
 * the fluss cluster's own lake configuration arrives inside each datalake table's properties, but it
 * arrives incomplete — anything that looks like a credential is stripped from it before it leaves the
 * cluster (see {@link PaimonSiblingProperties}). So a catalog needs a place to state the lake settings the
 * cluster cannot hand over, and these keys are it.
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
     * Optional. How many log-tail rows one union-read SCAN may hold across every bucket it touches.
     *
     * <p>Doris-only, and the scan-wide companion of {@link #UNION_READ_MAX_TAIL_ROWS}: that one bounds any
     * single bucket, this one bounds their sum. The two are not the same guard rail. BE caches the keys of
     * each tail it reads for the whole life of the scan, because the lake splits of one bucket arrive
     * spread among the splits of every other bucket and each of them needs its own tail to suppress with —
     * so a table with many partitions and many buckets retains {@code partitions * buckets} tails at once
     * while every individual one stays far below its own ceiling.
     *
     * <p>Checked while planning, from the offsets fluss already reported, so an over-large read is answered
     * by a plan that does not need the memory rather than by an allocation failure part-way through it.
     */
    public static final String UNION_READ_MAX_TOTAL_TAIL_ROWS =
            "fluss.union_read.max_total_tail_rows";

    /**
     * Default {@link #UNION_READ_MAX_TOTAL_TAIL_ROWS}: ten times {@link #DEFAULT_MAX_TAIL_ROWS}. A scan may
     * therefore hold ten maximal tails, or any number of ordinary ones — a healthy tail is the data written
     * within one {@code table.datalake.freshness} period, orders of magnitude below its own ceiling.
     */
    public static final long DEFAULT_MAX_TOTAL_TAIL_ROWS = 10 * DEFAULT_MAX_TAIL_ROWS;

    /**
     * Optional, repeated. Prefix of a lake connection setting this catalog states for itself, overriding
     * whatever its fluss cluster reports for the same setting. The tail is spelled the way paimon spells
     * it, which is also how it is spelled in the fluss cluster's own {@code datalake.paimon.*} block, so an
     * operator copies the tail across rather than translating it:
     *
     * <pre>
     *   fluss server.yaml     datalake.paimon.warehouse = s3://bucket/lake
     *   Doris catalog         "fluss.lake.paimon.warehouse" = "s3://bucket/lake"
     * </pre>
     *
     * <p>Per key, not wholesale: a catalog that states only the credentials keeps the warehouse, the
     * metastore and every other setting the cluster reports. That is what makes the common case — the
     * cluster's configuration is right except for what it is not allowed to send — a two-line catalog.
     *
     * <p>Doris-only in the sense that matters here: these keys configure the LAKE, not the fluss client, so
     * {@link #getFlussClientConfig()} must not see them.
     */
    public static final String LAKE_OPTION_PREFIX = "fluss.lake.paimon.";

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

    @ConnectorProperty(names = {UNION_READ_MAX_TOTAL_TAIL_ROWS}, required = false,
            description = "guard rail: log-tail rows one union-read scan may hold across all buckets")
    private long maxTotalTailRows = DEFAULT_MAX_TOTAL_TAIL_ROWS;

    @ConnectorProperty(names = {ENABLE_MAPPING_VARBINARY}, required = false,
            description = "map fluss BINARY/BYTES to Doris VARBINARY instead of STRING")
    private boolean enableMappingVarbinary;

    @ConnectorProperty(names = {ENABLE_MAPPING_TIMESTAMP_TZ}, required = false,
            description = "map fluss TIMESTAMP_LTZ to Doris TIMESTAMPTZ instead of DATETIMEV2")
    private boolean enableMappingTimestampTz;

    private UnionReadMode unionReadMode;
    private FlussTypeMapping.Options typeMappingOptions;
    private Map<String, String> flussClientConfig;
    private Map<String, String> lakeOverrides;

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
                .check(() -> p.maxTotalTailRows <= 0,
                        "Invalid value '" + p.maxTotalTailRows + "' for property '"
                                + UNION_READ_MAX_TOTAL_TAIL_ROWS
                                + "'; expected a positive number of rows")
                // A scan-wide ceiling below the per-bucket one cannot describe a scan that passes the
                // per-bucket check, so the pair would be unsatisfiable in a way neither value shows on
                // its own. Equal is legitimate: it says "one maximal tail, and no more".
                .check(() -> p.maxTotalTailRows < p.maxTailRows,
                        "Property '" + UNION_READ_MAX_TOTAL_TAIL_ROWS + "' (" + p.maxTotalTailRows
                                + ") is below '" + UNION_READ_MAX_TAIL_ROWS + "' (" + p.maxTailRows
                                + "), so no union read could satisfy both")
                .validate();
        checkBootstrapServerList(p.bootstrapServers);
        // Derived after the checks, so a catalog that fails validation fails on the property it got
        // wrong rather than on a value derived from it.
        p.unionReadMode = parseUnionReadMode(p.unionRead, "property '" + UNION_READ_MODE + "'");
        p.typeMappingOptions =
                new FlussTypeMapping.Options(p.enableMappingVarbinary, p.enableMappingTimestampTz);
        p.flussClientConfig = deriveFlussClientConfig(properties, p.bootstrapServers);
        p.lakeOverrides = extractLakeOverrides(properties);
        return p;
    }

    /**
     * The lake settings a catalog states for itself: every {@link #LAKE_OPTION_PREFIX} key with the prefix
     * stripped, so the result is already spelled the way the lake configuration is.
     *
     * <p>Static, and taking the raw map, because the storage half of these settings is read a second time
     * from a place that has only the raw map to work with — {@code Connector.deriveStorageProperties}, which
     * the engine calls with the catalog's current persisted properties. Both readers must agree on which
     * keys are lake overrides, and the only way to guarantee that is for there to be one implementation.
     *
     * <p>A blank value counts as no override, the same rule the binder applies to every bound property.
     * Here it is also the only way back: {@code ALTER CATALOG} can overwrite a key but never remove one, so
     * without it an override written once could never be handed back to the fluss cluster.
     *
     * <p>Sorted, so a configuration renders the same way every time it is compared or printed.
     */
    static Map<String, String> extractLakeOverrides(Map<String, String> properties) {
        Map<String, String> overrides = new TreeMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(LAKE_OPTION_PREFIX) && !entry.getValue().trim().isEmpty()) {
                overrides.put(entry.getKey().substring(LAKE_OPTION_PREFIX.length()), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(overrides);
    }

    /**
     * Reads a union-read mode, defaulting to {@link UnionReadMode#AUTO} when nothing was set.
     *
     * <p>An unrecognized value fails rather than falling back to the default: the difference between
     * {@code auto} and {@code required} is only ever visible as "the query returned fewer rows than it
     * should", so a typo that silently meant {@code auto} would defeat the mode it was trying to ask for.
     *
     * <p>Shared with the session variable that overrides {@link #UNION_READ_MODE} for one statement, so
     * that the two spell the same value set and refuse a typo the same way. Only the source differs, and
     * {@code setting} is how the message names it. Blank counts as unset on both sides: the property gets
     * that from the binder, which skips a blank value framework-wide, and the session variable gets it
     * here, its default being the empty string that means "follow the catalog".
     */
    static UnionReadMode parseUnionReadMode(String value, String setting) {
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return UnionReadMode.AUTO;
        }
        for (UnionReadMode mode : UnionReadMode.values()) {
            if (mode.name().equalsIgnoreCase(trimmed)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Invalid value '" + value + "' for " + setting
                + "; expected one of auto, required, disabled");
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

    /**
     * Whether {@code key} configures Doris's own behaviour and must not reach the fluss client.
     *
     * <p>The lake overrides are matched by PREFIX, not by name: their tails are paimon's option names, an
     * open set nothing here enumerates. Matching them by name would be impossible, and leaving them out
     * would hand the fluss client a pile of {@code lake.paimon.*} options it has never heard of.
     */
    private static boolean isDorisOnly(String key) {
        return UNION_READ_MODE.equals(key) || UNION_READ_MAX_TAIL_ROWS.equals(key)
                || UNION_READ_MAX_TOTAL_TAIL_ROWS.equals(key)
                || key.startsWith(LAKE_OPTION_PREFIX);
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

    /** The scan-wide log-tail ceiling for a union read. Always positive, never below {@link
     * #getMaxTailRows()}. */
    public long getMaxTotalTailRows() {
        return maxTotalTailRows;
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

    /**
     * The lake settings this catalog states for itself, unmodifiable and prefix-stripped. Empty when it
     * states none, which is the case for a catalog whose fluss cluster reports a complete configuration.
     *
     * <p><b>It can carry credentials</b> — that is the reason it exists ({@link #LAKE_OPTION_PREFIX}), and
     * unlike the bound properties it is NOT covered by {@link #toString()}'s masking, which only knows about
     * declared fields. Pass it to the lake sibling and to the storage layer, never to anything a user can
     * read.
     */
    public Map<String, String> getLakeOverrides() {
        return lakeOverrides;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
