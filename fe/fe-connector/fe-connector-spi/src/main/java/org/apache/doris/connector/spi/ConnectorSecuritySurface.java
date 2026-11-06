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
import java.util.Collections;
import java.util.List;

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
            Collections.emptyList(), Collections.emptyList(), null, Collections.emptyList());

    private final List<String> ssrfEndpointKeys;
    private final List<String> jdbcDriverUrlKeys;
    private final String jdbcDriverUrlFlavorKey;
    private final List<String> sensitiveKeys;

    private ConnectorSecuritySurface(List<String> ssrfEndpointKeys, List<String> jdbcDriverUrlKeys,
            String jdbcDriverUrlFlavorKey, List<String> sensitiveKeys) {
        this.ssrfEndpointKeys = ssrfEndpointKeys;
        this.jdbcDriverUrlKeys = jdbcDriverUrlKeys;
        this.jdbcDriverUrlFlavorKey = jdbcDriverUrlFlavorKey;
        this.sensitiveKeys = sensitiveKeys;
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
                Collections.emptyList());
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
                Collections.emptyList());
    }

    /**
     * Declares the property keys whose value is a secret, for a connector that names no other
     * security-sensitive property. See {@link #withSensitiveKeys} for what the engine does with them.
     */
    public static ConnectorSecuritySurface ofSensitiveKeys(String... keys) {
        return EMPTY.withSensitiveKeys(keys);
    }

    /**
     * Returns a copy of this surface that also declares {@code keys} as secrets: passwords, tokens, keys.
     * The engine masks their values where it shows catalog properties, such as SHOW CREATE CATALOG,
     * matching the key case-insensitively. The keys are registered with the provider at FE startup, so
     * they cover catalogs created before an upgrade too, but not a catalog whose connector plugin is not
     * loaded. Storage credentials the engine already masks for every catalog ({@code s3.secret_key} and
     * the like) need not be declared.
     */
    public ConnectorSecuritySurface withSensitiveKeys(String... keys) {
        List<String> merged = new ArrayList<>(sensitiveKeys);
        merged.addAll(Arrays.asList(keys));
        return new ConnectorSecuritySurface(ssrfEndpointKeys, jdbcDriverUrlKeys, jdbcDriverUrlFlavorKey,
                Collections.unmodifiableList(merged));
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
}
