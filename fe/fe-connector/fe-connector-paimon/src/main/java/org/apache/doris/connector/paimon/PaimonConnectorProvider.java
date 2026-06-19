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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SPI entry point for the Paimon connector.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * Returns type {@code "paimon"} matching the CatalogFactory dispatch key.
 */
public class PaimonConnectorProvider implements ConnectorProvider {

    private static final Logger LOG = LogManager.getLogger(PaimonConnectorProvider.class);

    // Legacy PaimonExternalCatalog.checkProperties validated these table-handle cache knobs
    // (meta.cache.paimon.table.{enable,ttl-second,capacity}) via CacheSpec. On the plugin path they are dead:
    // a cut-over paimon table reports meta-cache engine "default" (not "paimon"), so it never reaches
    // PaimonExternalMetaCache, which these keys size. Re-imposing CacheSpec validation would only reject malformed
    // values for a knob that no longer does anything; instead warn the operator the keys are ignored (R2).
    private static final String DEAD_TABLE_CACHE_PREFIX = "meta.cache.paimon.table.";

    @Override
    public String getType() {
        return "paimon";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new PaimonConnector(properties, context);
    }

    /**
     * Validates catalog properties at CREATE CATALOG time via the shared metastore parsers (P2-T03):
     * {@link MetaStoreProviders#bind} selects the backend by {@code paimon.catalog.type} and the bound
     * {@code MetaStoreProperties.validate()} enforces the per-flavor fail-fast rules (warehouse, uri,
     * HMS kerberos forbidIf/requireIf, DLF AK/SK + endpoint-or-region + OSS storage, JDBC
     * driver_class-when-driver_url, REST dlf-token AK/SK). These restore the true-legacy
     * {@code HMSBaseProperties}/{@code AliyunDLFBaseProperties}/{@code ParamRules} rules. Storage is not
     * needed for validation, so an empty storage map is passed; an unknown {@code paimon.catalog.type}
     * makes {@code bind} throw (no provider supports it). Throws {@link IllegalArgumentException}, which
     * the caller ({@code PluginDrivenExternalCatalog.checkProperties}) wraps into a DdlException.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        warnIgnoredDeadTableCacheKeys(properties);
        MetaStoreProviders.bind(properties, Collections.emptyMap()).validate();
    }

    // R2: warn (do not reject, do not strip) when a CREATE/ALTER CATALOG carries the now-dead paimon
    // table-cache knobs, so the operator learns their cache tuning no longer takes effect on the plugin path.
    private static void warnIgnoredDeadTableCacheKeys(Map<String, String> properties) {
        List<String> dead = properties.keySet().stream()
                .filter(k -> k.startsWith(DEAD_TABLE_CACHE_PREFIX))
                .sorted()
                .collect(Collectors.toList());
        if (!dead.isEmpty()) {
            LOG.warn("Paimon catalog cache property/properties {} no longer take effect on the plugin path "
                    + "(the table metadata cache configuration is obsolete) and are ignored.", dead);
        }
    }
}
