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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * SPI entry point for the Hive (HMS) connector plugin.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * The type is {@code "hms"} to match the existing catalog type in CatalogFactory.</p>
 */
public class HiveConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "hms";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new HiveConnector(properties, context);
    }

    /**
     * An HMS catalog has always created hive-engine tables, so {@code CREATE TABLE ... ENGINE=hive} keeps
     * working. The name deliberately differs from {@link #getType()} and from the {@code hms} a table
     * displays: the engine keyword and the catalog type are separate legacy vocabularies.
     */
    @Override
    public Set<String> acceptedCreateTableEngineNames() {
        return Collections.singleton("hive");
    }

    @Override
    public boolean providesEventSource() {
        // HiveConnector returns an HmsEventSource, and an HMS catalog must seed its event cursor even on an
        // FE that never queries it (see MetastoreEventSyncDriver).
        return true;
    }

    /**
     * Binds and validates through the typed holder; the ALTER door reaches this same method through the
     * SPI default {@code validatePropertiesForUpdate}, which validates the merged candidate.
     * {@code IllegalArgumentException} — which both halves throw — is required: it is the only type
     * {@code PluginDrivenExternalCatalog.checkProperties} unwraps, preserving the message verbatim.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        HiveCatalogProperties.of(properties).checkCreateTimeOnlyRules();
    }
}
