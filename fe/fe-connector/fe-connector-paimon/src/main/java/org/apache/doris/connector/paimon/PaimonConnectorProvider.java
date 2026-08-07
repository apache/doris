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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Collections;
import java.util.HashMap;
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

    /**
     * Binds and validates through the typed holder. {@code of(...)} carries what the connector cannot
     * run without; {@code checkCreateTimeOnlyRules()} carries the rules that only ever applied to a
     * statement -- the meta-cache knobs, the dead-knob warning, the paimon table options, and the
     * backend's own fail-fast rules -- which is why none of them run on the connector build path.
     *
     * <p>This also serves ALTER through the SPI default {@code validatePropertiesForUpdate}, which
     * merges and calls back here. Throws {@link IllegalArgumentException}, which the caller
     * ({@code PluginDrivenExternalCatalog.checkProperties}) wraps into a DdlException.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        PaimonCatalogProperties.of(properties).checkCreateTimeOnlyRules();
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

}
