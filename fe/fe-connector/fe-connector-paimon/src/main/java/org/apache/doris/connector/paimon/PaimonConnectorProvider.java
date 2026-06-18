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

import java.util.Collections;
import java.util.Map;

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
        MetaStoreProviders.bind(properties, Collections.emptyMap()).validate();
    }
}
