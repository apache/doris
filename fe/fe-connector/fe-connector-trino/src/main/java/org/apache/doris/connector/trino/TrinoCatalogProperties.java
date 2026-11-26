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

package org.apache.doris.connector.trino;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything a user writes in {@code CREATE CATALOG} for a trino-connector catalog, bound and
 * validated in one step. Deployment-level settings — one per FE rather than one per catalog — are the
 * other half and live in {@link TrinoConf}.
 *
 * <p>{@link #of(Map)} binds, derives and validates together: if an instance exists, its properties are
 * usable. It does no I/O and is idempotent, because it runs at CREATE, at ALTER validation (on the
 * merged candidate) and on every connector rebuild — including on an FE replaying the edit log. The
 * heavyweight part (loading Trino plugins, resolving the connector factory) stays behind
 * {@code TrinoDorisConnector}'s lazy init, which {@code preCreateValidation} pulls forward to CREATE.
 *
 * <p><b>Unknown keys are accepted, always.</b> The same map carries engine keys ({@code type},
 * {@code meta.cache.*}, ...) and storage keys, and {@code ALTER CATALOG} merges properties — it can
 * overwrite a key but never remove one, so a key refused here would leave a catalog no statement could
 * repair. Bad <i>values</i> are refused; unrecognized <i>names</i> are not.
 */
public final class TrinoCatalogProperties {

    private static final Logger LOG = LogManager.getLogger(TrinoCatalogProperties.class);

    /**
     * The prefix on every property destined for the Trino connector itself. Unlike the adbc
     * connector's driver options, the prefix is <b>stripped</b>: what the Trino connector — and the
     * BE-side scanner — see is the bare Trino property name.
     */
    public static final String TRINO_PROPERTIES_PREFIX = "trino.";

    /** The Trino connector to bridge to, e.g. {@code postgresql}. */
    public static final String CONNECTOR_NAME = TRINO_PROPERTIES_PREFIX + "connector.name";

    /**
     * Per-catalog override of the deployment-level plugin directory ({@link TrinoConf#CONF_PLUGIN_DIR}),
     * which it wins over. The combination is resolved by {@code TrinoBootstrap.resolvePluginDir}: an
     * A-class key overriding a B-class setting belongs to neither class alone.
     */
    public static final String PLUGIN_DIR = TRINO_PROPERTIES_PREFIX + "plugin.dir";

    /** The bare key {@link #CONNECTOR_NAME} carries once the prefix is stripped. */
    private static final String STRIPPED_CONNECTOR_NAME =
            CONNECTOR_NAME.substring(TRINO_PROPERTIES_PREFIX.length());

    @ConnectorProperty(names = {CONNECTOR_NAME},
            description = "the Trino connector to bridge to, e.g. postgresql")
    private String connectorName;

    @ConnectorProperty(names = {PLUGIN_DIR}, required = false,
            description = "per-catalog override of the deployment-level Trino plugin directory")
    private String pluginDirOverride = "";

    private final Map<String, String> raw;
    private Map<String, String> trinoProperties;

    private TrinoCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    public static TrinoCatalogProperties of(Map<String, String> properties) {
        TrinoCatalogProperties p = new TrinoCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        new ParamRules()
                .require(p.connectorName, "Required property '" + CONNECTOR_NAME + "' is missing")
                .validate();
        p.connectorName = correctDeprecatedSpelling(p.connectorName);
        p.trinoProperties = deriveTrinoProperties(p.raw, p.connectorName);
        return p;
    }

    /**
     * Trino renamed its dashed connector names to underscores; its {@code ConnectorName} constructor
     * now rejects anything outside {@code [a-z][a-z0-9_]*} outright, so the old spellings have to be
     * translated rather than passed through.
     */
    private static String correctDeprecatedSpelling(String name) {
        if (name.indexOf('-') < 0) {
            return name;
        }
        String corrected = name.replace('-', '_');
        LOG.warn("Using deprecated connector name '{}', corrected to '{}'", name, corrected);
        return corrected;
    }

    /**
     * The {@code trino.*} properties with the prefix stripped — what both the Trino connector and the
     * BE-side scanner consume. {@code connector.name} is kept in the map (BE reads it back out of this
     * very payload; {@code TrinoBootstrap} removes it before handing the rest to Trino) and carries the
     * <b>corrected</b> spelling: it used to carry the raw one, so a catalog written with a dashed name
     * worked for metadata on FE and failed every scan on BE, where the same name goes straight into
     * Trino's {@code ConnectorName}.
     */
    private static Map<String, String> deriveTrinoProperties(Map<String, String> raw, String connectorName) {
        Map<String, String> stripped = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            if (entry.getKey().startsWith(TRINO_PROPERTIES_PREFIX)) {
                stripped.put(entry.getKey().substring(TRINO_PROPERTIES_PREFIX.length()), entry.getValue());
            }
        }
        stripped.put(STRIPPED_CONNECTOR_NAME, connectorName);
        return Collections.unmodifiableMap(stripped);
    }

    /** The Trino connector name, with any deprecated dashed spelling already corrected. */
    public String getConnectorName() {
        return connectorName;
    }

    /** The per-catalog plugin directory, or {@code ""} when this catalog names none. */
    public String getPluginDirOverride() {
        return pluginDirOverride;
    }

    public Map<String, String> getTrinoProperties() {
        return trinoProperties;
    }

    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
