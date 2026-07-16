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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;

/**
 * SPI entry point for the Hudi connector plugin.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 *
 * <p><b>The type {@code "hudi"} is a SIBLING-ONLY type string — NOT a user-facing catalog type.</b> There is no
 * {@code type=hudi} catalog and no {@code HudiExternalCatalog}: a hudi table is always parasitic on an HMS
 * catalog (legacy {@code HMSExternalTable} with {@code dlaType == HUDI}). After the HMS cutover this connector is
 * built only as an embedded <em>sibling</em> of the hive {@code hms} gateway, resolved through
 * {@code ConnectorContext.createSiblingConnector("hudi", ...)} — which bypasses
 * {@code CatalogFactory.SPI_READY_TYPES}. <b>NEVER add {@code "hudi"} to {@code SPI_READY_TYPES}</b> and never add
 * a {@code case "hudi"} to the catalog factory: doing so would build a standalone
 * {@code PluginDrivenExternalCatalog} around this connector with no fe-core catalog class backing it (the exact
 * model mismatch this type string otherwise invites).
 */
public class HudiConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        // Sibling-only lookup key for createSiblingConnector("hudi", ...); see the class javadoc.
        // NOT a user-facing catalog type; never add "hudi" to CatalogFactory.SPI_READY_TYPES.
        return "hudi";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new HudiConnector(properties, context);
    }
}
