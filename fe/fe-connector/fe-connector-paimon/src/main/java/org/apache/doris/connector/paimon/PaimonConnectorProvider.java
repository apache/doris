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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.JdbcDriverUrlSecurity;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SPI entry point for the Paimon connector.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * Returns type {@code "paimon"} matching the CatalogFactory dispatch key.
 */
public class PaimonConnectorProvider implements ConnectorProvider {

    private static final Logger LOG = LogManager.getLogger(PaimonConnectorProvider.class);

    // Legacy PaimonExternalCatalog.checkProperties validated the table-handle cache knobs
    // (meta.cache.paimon.table.{enable,ttl-second,capacity}) via CacheSpec. FIX-4 restores ttl-second: it now
    // sizes the connector latest-snapshot cache (data) AND the generic schema cache (via
    // schemaCacheTtlSecondOverride). enable/capacity remain not-wired on the plugin path, so they are still
    // reported as ignored (R2) — ttl-second is intentionally excluded from this set since it again takes effect.
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
     * {@code CREATE TABLE ... ENGINE=paimon} keeps working; omitting the clause is equivalent. The engine
     * keyword is legacy syntax the connector owns, not the catalog type and not the displayed engine name.
     */
    @Override
    public Set<String> acceptedCreateTableEngineNames() {
        return Collections.singleton("paimon");
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
     *
     * <p>The meta-cache knobs are validated first (restoring the legacy
     * {@code PaimonExternalCatalog.checkProperties} fail-fast dropped at the SPI cutover), so a bad
     * {@code meta.cache.paimon.table.*} value is rejected at CREATE/ALTER. This runs before the
     * dead-knob warning: an invalid value is rejected outright, while a valid-but-unwired enable/capacity
     * is still reported as ignored.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        checkMetaCacheProperties(properties);
        warnIgnoredDeadTableCacheKeys(properties);
        // The mandatory, non-configurable driver_url rule shared with the jdbc and iceberg-jdbc catalogs
        // (all three reach the same URLClassLoader + Class.forName sink from a catalog property). This
        // hook is what the engine runs on CREATE *and* ALTER CATALOG, so it is the one that keeps a
        // traversal / non-bare-name driver jar out of PaimonConnector's class loader;
        // preCreateValidation covers CREATE only. Never runs on replay.
        driverUrlsToValidate(properties).forEach(JdbcDriverUrlSecurity::check);
        // #65955: an unknown or unparseable paimon.table-option.* must fail the CREATE/ALTER CATALOG.
        // Upstream got this from AbstractPaimonProperties.initNormalizeAndCheckProps(), which the SPI
        // path no longer runs; validateProperties is this path's fail-fast hook.
        PaimonTableOptions.extract(properties);
        MetaStoreProviders.bind(properties, Collections.emptyMap()).validate();
    }

    @Override
    public void validatePropertiesForUpdate(
            Map<String, String> currentProperties, Map<String, String> updatedProperties) {
        PaimonReaderOptions.validateCatalogProperties(updatedProperties);
        Map<String, String> candidate = currentProperties == null
                ? new HashMap<>() : new HashMap<>(currentProperties);
        candidate.putAll(updatedProperties);

        // Old images could contain arbitrary paimon.table-option.* values. Validate the complete
        // catalog candidate, but retain only safe legacy reader values unless this ALTER touched them.
        candidate.keySet().removeIf(PaimonTableOptions::isTableOptionProperty);
        PaimonReaderOptions.compatibleCatalogOptions(currentProperties == null
                        ? Collections.emptyMap() : currentProperties)
                .forEach((key, value) -> candidate.put(PaimonReaderOptions.TABLE_OPTION_PREFIX + key, value));
        updatedProperties.forEach((key, value) -> {
            if (PaimonTableOptions.isTableOptionProperty(key)) {
                candidate.put(key, value);
            }
        });
        validateProperties(candidate);
    }

    /**
     * Only the jdbc flavor loads a driver jar; on every other flavor {@code jdbc.driver_url} is dead config
     * that never reaches a class loader, so declaring it would turn a previously-accepted catalog into a
     * CREATE/ALTER failure for no security gain. See {@link ConnectorProvider#driverUrlsToValidate}.
     */
    @Override
    public List<String> driverUrlsToValidate(Map<String, String> properties) {
        if (!PaimonConnectorProperties.JDBC.equals(PaimonCatalogFactory.resolveFlavor(properties))) {
            return Collections.emptyList();
        }
        String driverUrl = PaimonCatalogFactory.firstNonBlank(
                properties, PaimonConnectorProperties.JDBC_DRIVER_URL);
        return StringUtils.isBlank(driverUrl)
                ? Collections.emptyList() : Collections.singletonList(driverUrl);
    }

    /**
     * Byte-for-byte parity with the (deleted) legacy {@code PaimonExternalCatalog.checkProperties}:
     * {@code table.enable} must be boolean, {@code table.ttl-second} must be a long &ge; -1, {@code
     * table.capacity} must be a long &ge; 0. Absent keys are skipped.
     */
    private static void checkMetaCacheProperties(Map<String, String> properties) {
        CacheSpec.checkBooleanProperty(properties.get(PaimonConnector.TABLE_CACHE_ENABLE),
                PaimonConnector.TABLE_CACHE_ENABLE);
        CacheSpec.checkLongProperty(properties.get(PaimonConnector.TABLE_CACHE_TTL_SECOND),
                -1L, PaimonConnector.TABLE_CACHE_TTL_SECOND);
        CacheSpec.checkLongProperty(properties.get(PaimonConnector.TABLE_CACHE_CAPACITY),
                0L, PaimonConnector.TABLE_CACHE_CAPACITY);
    }

    // R2: warn (do not reject, do not strip) when a CREATE/ALTER CATALOG carries the now-dead paimon
    // table-cache knobs, so the operator learns their cache tuning no longer takes effect on the plugin path.
    private static void warnIgnoredDeadTableCacheKeys(Map<String, String> properties) {
        List<String> dead = properties.keySet().stream()
                .filter(k -> k.startsWith(DEAD_TABLE_CACHE_PREFIX))
                // ttl-second is restored (FIX-4): it sizes the snapshot cache + schema cache TTL, so it is NOT dead.
                .filter(k -> !k.equals(PaimonConnector.TABLE_CACHE_TTL_SECOND))
                .sorted()
                .collect(Collectors.toList());
        if (!dead.isEmpty()) {
            LOG.warn("Paimon catalog cache property/properties {} no longer take effect on the plugin path "
                    + "(the table metadata cache configuration is obsolete) and are ignored.", dead);
        }
    }
}
