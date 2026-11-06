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

package org.apache.doris.connector.spi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * The security-sensitive property surface a connector exposes to the engine's CREATE / ALTER CATALOG
 * security gates and to the masking of catalog properties. It lets both stay connector-agnostic: the engine
 * applies its own policies — SSRF endpoint checking, the jdbc driver-url operator gate, and hiding secret
 * values — to the property keys declared here, instead of branching on the catalog type. A connector that
 * names no such property returns {@link #EMPTY}.
 *
 * <p>The keys are the connector's wire-stable, user-facing property names (documented aliases included).
 * They are declared here, on the provider, rather than read by the engine, because the engine must not
 * know any connector's property names — the same reason the connector owns {@code validateProperties}.
 */
public final class ConnectorSecuritySurface {

    /** For connectors that name no security-sensitive property. */
    public static final ConnectorSecuritySurface EMPTY = new ConnectorSecuritySurface(
            Collections.emptyList(), Collections.emptyList(), null, Collections.emptyList(),
            Collections.emptyList());

    private final List<String> ssrfEndpointKeys;
    private final List<String> jdbcDriverUrlKeys;
    private final String jdbcDriverUrlFlavorKey;
    private final List<String> sensitiveKeys;
    private final List<SensitiveKeyPrefix> sensitiveKeyPrefixes;

    private ConnectorSecuritySurface(List<String> ssrfEndpointKeys, List<String> jdbcDriverUrlKeys,
            String jdbcDriverUrlFlavorKey, List<String> sensitiveKeys,
            List<SensitiveKeyPrefix> sensitiveKeyPrefixes) {
        this.ssrfEndpointKeys = ssrfEndpointKeys;
        this.jdbcDriverUrlKeys = jdbcDriverUrlKeys;
        this.jdbcDriverUrlFlavorKey = jdbcDriverUrlFlavorKey;
        this.sensitiveKeys = sensitiveKeys;
        this.sensitiveKeyPrefixes = sensitiveKeyPrefixes;
    }

    /**
     * Declares the user-supplied property keys whose value names a {@code host:port} (optionally
     * scheme-prefixed, comma-separated for HA) the FE connects to, so the engine can reject an intranet
     * target at CREATE / ALTER CATALOG.
     *
     * <p>Declare every key the connector's client may read that destination from, including a spelling
     * the client prefers over the documented one: an undeclared key is never probed. The engine probes the
     * values as written and, while an SSRF checker is configured, refuses one that carries a {@code ${...}}
     * placeholder.
     */
    public static ConnectorSecuritySurface ofSsrfEndpoints(String... keys) {
        return new ConnectorSecuritySurface(Arrays.asList(keys), Collections.emptyList(), null,
                Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Declares the jdbc-flavored {@code driver_url} property keys whose value the connector hands to a
     * class loader, so the engine can apply its driver-jar operator gate on both CREATE and ALTER.
     *
     * @param flavorKey the property whose candidate value must be {@code "jdbc"} for the keys to be live,
     *                  or {@code null} when they are always live
     * @param keys      the driver_url property names (documented aliases included)
     */
    public static ConnectorSecuritySurface ofJdbcDriverUrls(String flavorKey, String... keys) {
        return new ConnectorSecuritySurface(Collections.emptyList(), Arrays.asList(keys), flavorKey,
                Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Declares the property keys whose value is a secret, for a connector that names no other
     * security-sensitive property. See {@link #withSensitiveKeys(Collection)} for what the engine does with
     * them.
     */
    public static ConnectorSecuritySurface ofSensitiveKeys(String... keys) {
        return EMPTY.withSensitiveKeys(keys);
    }

    /**
     * Declares a namespace of secret keys, for a connector that names no other security-sensitive property.
     * See {@link #withSensitiveKeyPrefix} for the keys it covers.
     */
    public static ConnectorSecuritySurface ofSensitiveKeyPrefix(String prefix, String... markers) {
        return EMPTY.withSensitiveKeyPrefix(prefix, markers);
    }

    /** Same as {@link #withSensitiveKeys(Collection)}. */
    public ConnectorSecuritySurface withSensitiveKeys(String... keys) {
        return withSensitiveKeys(Arrays.asList(keys));
    }

    /**
     * Returns a copy of this surface that also declares {@code keys} as secrets: passwords, tokens, keys.
     * The engine masks their values where it shows catalog properties, such as SHOW CREATE CATALOG, and in
     * the text of the CREATE / ALTER CATALOG statements it keeps in the audit log, matching the key
     * case-insensitively. The keys are registered with the provider at FE startup, so they cover catalogs
     * created before an upgrade too, but not a catalog whose connector plugin is not loaded. Storage
     * credentials the engine already masks for every catalog ({@code s3.secret_key} and the like) need not
     * be declared.
     *
     * <p>A connector that marks its secret properties {@code sensitive} passes their names here
     * ({@code ConnectorPropertiesUtils.getSensitiveKeys}), so one mark covers its own logs and the engine's
     * output alike.
     */
    public ConnectorSecuritySurface withSensitiveKeys(Collection<String> keys) {
        List<String> merged = new ArrayList<>(sensitiveKeys);
        merged.addAll(keys);
        return new ConnectorSecuritySurface(ssrfEndpointKeys, jdbcDriverUrlKeys, jdbcDriverUrlFlavorKey,
                Collections.unmodifiableList(merged), sensitiveKeyPrefixes);
    }

    /**
     * Returns a copy of this surface that also declares as secrets the keys that start with {@code prefix}
     * and whose rest, after the prefix, contains one of {@code markers} (words such as {@code "password"}
     * or {@code "token"}); with no marker, every key under the prefix is a secret. The prefix and the
     * markers are matched case-insensitively, and the engine masks a matching key wherever it masks one
     * declared with {@link #withSensitiveKeys(Collection)}.
     *
     * <p>This is for a namespace the connector passes through to a client or library without knowing its
     * option names, such as Fluss's lake options or Trino's connector properties: a credential option
     * there can arrive with a new client release, so it cannot be declared by name.
     */
    public ConnectorSecuritySurface withSensitiveKeyPrefix(String prefix, String... markers) {
        List<SensitiveKeyPrefix> merged = new ArrayList<>(sensitiveKeyPrefixes);
        merged.add(new SensitiveKeyPrefix(prefix,
                Collections.unmodifiableList(new ArrayList<>(Arrays.asList(markers)))));
        return new ConnectorSecuritySurface(ssrfEndpointKeys, jdbcDriverUrlKeys, jdbcDriverUrlFlavorKey,
                sensitiveKeys, Collections.unmodifiableList(merged));
    }

    /** The endpoint property keys to check against the SSRF policy; empty when none. */
    public List<String> ssrfEndpointKeys() {
        return ssrfEndpointKeys;
    }

    /** The jdbc driver_url property keys to check against the operator gate; empty when none. */
    public List<String> jdbcDriverUrlKeys() {
        return jdbcDriverUrlKeys;
    }

    /**
     * The flavor key gating {@link #jdbcDriverUrlKeys()}: when non-null, those keys are live only if this
     * property's value is {@code "jdbc"}; {@code null} means always live.
     */
    public String jdbcDriverUrlFlavorKey() {
        return jdbcDriverUrlFlavorKey;
    }

    /** The property keys whose values the engine masks when it shows catalog properties; empty when none. */
    public List<String> sensitiveKeys() {
        return sensitiveKeys;
    }

    /** The namespaces of secret keys declared with {@link #withSensitiveKeyPrefix}; empty when none. */
    public List<SensitiveKeyPrefix> sensitiveKeyPrefixes() {
        return sensitiveKeyPrefixes;
    }

    /** A namespace of secret keys; see {@link #withSensitiveKeyPrefix} for the keys it covers. */
    public static final class SensitiveKeyPrefix {
        private final String prefix;
        private final List<String> markers;
        private final String lowerCasePrefix;
        private final List<String> lowerCaseMarkers;

        private SensitiveKeyPrefix(String prefix, List<String> markers) {
            this.prefix = prefix;
            this.markers = markers;
            this.lowerCasePrefix = prefix.toLowerCase(Locale.ROOT);
            List<String> lowerCase = new ArrayList<>();
            for (String marker : markers) {
                lowerCase.add(marker.toLowerCase(Locale.ROOT));
            }
            this.lowerCaseMarkers = lowerCase;
        }

        /** The namespace, such as {@code "trino."}. */
        public String prefix() {
            return prefix;
        }

        /** The words one of which the rest of a key must contain; empty when every key under it is a secret. */
        public List<String> markers() {
            return markers;
        }

        /** Whether {@code key} is one of the secrets this namespace covers. */
        public boolean covers(String key) {
            String lowerCaseKey = key.toLowerCase(Locale.ROOT);
            if (!lowerCaseKey.startsWith(lowerCasePrefix)) {
                return false;
            }
            if (lowerCaseMarkers.isEmpty()) {
                return true;
            }
            String rest = lowerCaseKey.substring(lowerCasePrefix.length());
            for (String marker : lowerCaseMarkers) {
                if (rest.contains(marker)) {
                    return true;
                }
            }
            return false;
        }
    }
}
