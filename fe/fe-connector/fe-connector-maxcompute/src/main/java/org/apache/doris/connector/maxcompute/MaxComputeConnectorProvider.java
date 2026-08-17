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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * SPI entry point for the MaxCompute (ODPS) connector plugin.
 */
public class MaxComputeConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "max_compute";
    }

    @Override
    public Connector create(Map<String, String> properties,
            ConnectorContext context) {
        return new MaxComputeDorisConnector(properties, context);
    }

    /**
     * {@code CREATE TABLE ... ENGINE=maxcompute} keeps working; omitting the clause is equivalent. The engine
     * keyword is legacy syntax the connector owns, not the catalog type and not the displayed engine name.
     */
    @Override
    public Set<String> acceptedCreateTableEngineNames() {
        return Collections.singleton("maxcompute");
    }

    /**
     * Spelled without the underscore that {@link #getType()} carries: the catalog type is the internal token a
     * user writes in {@code CREATE CATALOG}, whereas this is the product name shown in the {@code ENGINE}
     * column and after {@code ENGINE=}. It coincides with the accepted CREATE TABLE engine name above, but the
     * two are answered separately — nothing keeps them equal, and for other connectors they differ.
     */
    @Override
    public String displayEngineName() {
        return "maxcompute";
    }

    /**
     * Validates catalog properties at CREATE CATALOG time, mirroring the fail-fast checks of the legacy
     * {@code MaxComputeExternalCatalog.checkProperties}. All of it is {@link MCCatalogProperties}: building
     * one binds, derives and validates, so this door and the connector cannot disagree about what a valid
     * catalog is. Throws {@link IllegalArgumentException}, which the caller
     * ({@code PluginDrivenExternalCatalog.checkProperties}) wraps into a DdlException.
     *
     * <p>The trailing call adds the one rule that belongs to a statement rather than to a catalog: a new
     * catalog must spell {@code mc.endpoint}, while catalogs stored with a legacy spelling go on resolving.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        MCCatalogProperties.of(properties).checkCreateTimeOnlyRules();
    }
}
