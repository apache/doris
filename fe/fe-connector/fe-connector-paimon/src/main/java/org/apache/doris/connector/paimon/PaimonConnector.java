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
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.io.IOException;
import java.util.Map;

/**
 * Paimon connector implementation managing the lifecycle of a
 * {@link org.apache.paimon.catalog.Catalog} instance.
 *
 * <p>The Paimon Catalog is lazily created on first metadata access.
 * It supports multiple catalog backends (filesystem, HMS, DLF, REST, JDBC)
 * determined by the {@code paimon.catalog.type} property.
 */
public class PaimonConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(PaimonConnector.class);

    private final Map<String, String> properties;
    private volatile Catalog catalog;

    public PaimonConnector(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new PaimonConnectorMetadata(ensureCatalog(), properties);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new PaimonScanPlanProvider(properties);
    }

    private Catalog ensureCatalog() {
        if (catalog == null) {
            synchronized (this) {
                if (catalog == null) {
                    catalog = createCatalog();
                }
            }
        }
        return catalog;
    }

    private Catalog createCatalog() {
        Options options = Options.fromMap(properties);
        CatalogContext context = CatalogContext.create(options);
        try {
            return CatalogFactory.createCatalog(context);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Paimon catalog: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        Catalog cat = catalog;
        if (cat != null) {
            try {
                cat.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Paimon catalog", e);
            }
        }
    }
}
