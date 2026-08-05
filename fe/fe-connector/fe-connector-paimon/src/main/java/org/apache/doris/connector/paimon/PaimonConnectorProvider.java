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

import org.apache.doris.connector.metastore.paimon.jdbc.PaimonJdbcMetaStoreProperties;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.JdbcDriverUrlSecurity;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SPI entry point for the Paimon connector.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * Returns type {@code "paimon"} matching the CatalogFactory dispatch key.
 */
public class PaimonConnectorProvider implements ConnectorProvider {

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

    @Override
    public String displayEngineName() {
        // System-table diagnostics use this connector-owned name; keep the canonical product spelling while
        // catalog routing and CREATE TABLE continue to accept the lowercase type through their own contracts.
        return "Paimon";
    }

    /**
     * Binds and validates through the typed holder. {@code of(...)} carries what the connector cannot
     * run without; {@code checkCreateTimeOnlyRules()} carries the rules that only ever applied to a
     * statement -- the meta-cache knobs, the dead-knob warning, the paimon table options, and the
     * backend's own fail-fast rules -- which is why none of them run on the connector build path.
     *
     * <p>ALTER validates the merged candidate separately to tolerate persisted unknown cache entries and
     * legacy reader options. Throws {@link IllegalArgumentException}, which the caller
     * ({@code PluginDrivenExternalCatalog.checkProperties}) wraps into a DdlException.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        PaimonCatalogProperties.of(properties).checkCreateTimeOnlyRules();
        // The mandatory, non-configurable driver_url rule shared with the jdbc and iceberg-jdbc catalogs
        // (all three reach the same URLClassLoader + Class.forName sink from a catalog property). The
        // engine runs this hook on CREATE; ALTER goes through the validatePropertiesForUpdate override
        // below, which does NOT funnel back here and therefore runs the same rule itself.
        // preCreateValidation covers CREATE only. Never runs on replay.
        driverUrlsToValidate(properties).forEach(JdbcDriverUrlSecurity::check);
    }

    /**
     * Only the jdbc flavor loads a driver jar; on every other flavor {@code jdbc.driver_url} /
     * {@code paimon.jdbc.driver_url} is dead config that never reaches a class loader, so declaring it
     * would turn a previously-accepted catalog into a CREATE/ALTER failure for no security gain.
     * Declaring it here is what lets the engine apply the operator's {@code jdbc_driver_secure_path} /
     * {@code jdbc_driver_url_white_list} policy on ALTER CATALOG, which never reaches
     * {@code PaimonConnector#preCreateValidation} (where CREATE applies it). See
     * {@link ConnectorProvider#driverUrlsToValidate}.
     */
    @Override
    public List<String> driverUrlsToValidate(Map<String, String> properties) {
        if (!PaimonCatalogProperties.JDBC.equals(PaimonCatalogProperties.of(properties).getFlavor())) {
            return Collections.emptyList();
        }
        String driverUrl = PaimonJdbcMetaStoreProperties.of(properties).getDriverUrl();
        return StringUtils.isBlank(driverUrl)
                ? Collections.emptyList() : Collections.singletonList(driverUrl);
    }

    @Override
    public void validatePropertiesForUpdate(
            Map<String, String> currentProperties, Map<String, String> updatedProperties) {
        PaimonReaderOptions.validateCatalogProperties(updatedProperties);
        Map<String, String> candidate = currentProperties == null
                ? new HashMap<>() : new HashMap<>(currentProperties);
        candidate.putAll(updatedProperties);
        // This override does NOT funnel back to validateProperties, so the mandatory driver_url rule
        // must run here as well or an ALTER would escape it (see ConnectorProvider#driverUrlsToValidate).
        // Checked on the stored shape, before the table-option munging below (which never touches
        // driver_url keys).
        driverUrlsToValidate(candidate).forEach(JdbcDriverUrlSecurity::check);

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
        PaimonCatalogProperties.of(candidate).checkCreateTimeOnlyRules(updatedProperties);
    }

}
