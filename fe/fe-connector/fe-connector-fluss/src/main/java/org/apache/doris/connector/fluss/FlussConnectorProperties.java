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

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * The catalog properties a fluss catalog accepts, and their translation into a fluss client
 * configuration.
 *
 * <p><b>Naming rule.</b> Everything a user may write is prefixed {@code fluss.}, and every such key
 * except the Doris-only ones listed in {@link #isDorisOnly(String)} is handed to the fluss client
 * verbatim with that prefix stripped. So {@code fluss.bootstrap.servers} becomes fluss's
 * {@code bootstrap.servers} and {@code fluss.client.security.protocol} becomes
 * {@code client.security.protocol}. Nothing here enumerates the fluss client's own option set: fluss
 * owns those names and adds to them between releases, and a Doris-side allowlist would silently
 * reject options that fluss understands perfectly well.
 *
 * <p>No lake/paimon connection property belongs here. The tiering service writes the lake catalog
 * configuration into each datalake-enabled table's own properties ({@code table.datalake.paimon.*}),
 * so union read reads it from the table, never from the catalog.
 */
public final class FlussConnectorProperties {

    /** Prefix every fluss catalog property carries; also what is stripped on the way to the client. */
    public static final String PROPERTY_PREFIX = "fluss.";

    /** Required. Comma-separated {@code host:port} list used to bootstrap the fluss cluster. */
    public static final String BOOTSTRAP_SERVERS = "fluss.bootstrap.servers";

    /**
     * Optional. How a scan of a datalake-enabled table combines the paimon lake with the fluss log.
     *
     * <p>Doris-only: it selects a planning strategy and is never passed to the fluss client. The
     * three values exist because "did this query actually read the lake?" is otherwise unobservable —
     * {@code auto} silently falls back to a fluss-only read when the lake has no readable snapshot
     * yet, which makes a union-read regression test pass for the wrong reason. {@code required} turns
     * that fallback into an error and {@code disabled} forces the fluss-only path, so a test can pin
     * both sides of the comparison.
     */
    public static final String UNION_READ_MODE = "fluss.union_read.mode";

    /**
     * Optional. Whether a fluss BINARY/BYTES column reads as Doris VARBINARY instead of STRING.
     *
     * <p>Unprefixed on purpose: this is the engine-wide catalog property
     * ({@code CatalogProperty.ENABLE_MAPPING_VARBINARY}) that the hive, paimon and iceberg catalogs
     * already answer to, and a user should not have to learn a fluss-specific spelling for it. Being
     * unprefixed also keeps it out of {@link #toFlussClientConfig} for free.
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

        static UnionReadMode parse(String raw) {
            for (UnionReadMode mode : values()) {
                if (mode.name().equalsIgnoreCase(raw.trim())) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Invalid value '" + raw + "' for property '"
                    + UNION_READ_MODE + "'; expected one of auto, required, disabled");
        }

        /** The lower-case spelling users write, and what {@code appendExplainInfo} prints. */
        public String propertyValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private FlussConnectorProperties() {
    }

    /**
     * Fails on a catalog that cannot work, at {@code CREATE CATALOG} time rather than at first query.
     * Only checks what is decidable without touching the cluster; reachability is
     * {@code testConnection}'s job.
     */
    public static void validate(Map<String, String> properties) {
        validateBootstrapServers(bootstrapServers(properties));
        unionReadMode(properties);
    }

    /** The declared bootstrap servers, or the empty string when the property is absent. */
    public static String bootstrapServers(Map<String, String> properties) {
        String value = properties.get(BOOTSTRAP_SERVERS);
        return value == null ? "" : value.trim();
    }

    /** The declared union-read mode, {@link UnionReadMode#AUTO} when the property is absent. */
    public static UnionReadMode unionReadMode(Map<String, String> properties) {
        String value = properties.get(UNION_READ_MODE);
        return value == null ? UnionReadMode.AUTO : UnionReadMode.parse(value);
    }

    /** The type-mapping switches this catalog declares; both default to off. */
    public static FlussTypeMapping.Options typeMappingOptions(Map<String, String> properties) {
        return new FlussTypeMapping.Options(
                booleanValue(properties, ENABLE_MAPPING_VARBINARY),
                booleanValue(properties, ENABLE_MAPPING_TIMESTAMP_TZ));
    }

    private static boolean booleanValue(Map<String, String> properties, String key) {
        return Boolean.parseBoolean(properties.getOrDefault(key, "false"));
    }

    /**
     * The fluss client configuration this catalog describes: every {@code fluss.}-prefixed property
     * that is not Doris-only, with the prefix stripped.
     *
     * <p>Returned sorted so that a configuration is printable and comparable in a test without the map
     * iteration order leaking in.
     */
    public static Map<String, String> toFlussClientConfig(Map<String, String> properties) {
        Map<String, String> config = new TreeMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(PROPERTY_PREFIX) || isDorisOnly(key)) {
                continue;
            }
            config.put(key.substring(PROPERTY_PREFIX.length()), entry.getValue());
        }
        return config;
    }

    /** Whether {@code key} configures Doris's own behaviour and must not reach the fluss client. */
    private static boolean isDorisOnly(String key) {
        return UNION_READ_MODE.equals(key);
    }

    private static void validateBootstrapServers(String value) {
        if (value.isEmpty()) {
            throw new IllegalArgumentException(
                    "Required property '" + BOOTSTRAP_SERVERS + "' is missing");
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
}
