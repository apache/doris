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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * SPI entry point for the Iceberg connector plugin.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * The type is {@code "iceberg"} to match the existing catalog type in CatalogFactory.
 * Internally dispatches to all Iceberg catalog backends (REST, HMS, Glue, DLF,
 * JDBC, Hadoop, S3Tables) via the Iceberg SDK's {@code CatalogUtil}.</p>
 */
public class IcebergConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "iceberg";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new IcebergConnector(properties, context);
    }

    /**
     * {@code CREATE TABLE ... ENGINE=iceberg} keeps working; omitting the clause is equivalent. The engine
     * keyword is legacy syntax the connector owns, not the catalog type and not the displayed engine name.
     */
    @Override
    public Set<String> acceptedCreateTableEngineNames() {
        return Collections.singleton("iceberg");
    }

    /**
     * Validates catalog properties at CREATE CATALOG time. Everything this used to spell out lives on
     * {@link IcebergCatalogProperties} now: the meta-cache knobs and the per-flavor backend rules alike are
     * CREATE-time-only, so they belong next to the binding rather than beside it, and keeping them out of
     * {@code of(Map)} is what lets a catalog created before a rule existed still come back after an FE
     * restart. Throws {@link IllegalArgumentException}, which {@code PluginDrivenExternalCatalog
     * .checkProperties} wraps into a DdlException.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        IcebergCatalogProperties.of(properties).checkCreateTimeOnlyRules();
    }
}
