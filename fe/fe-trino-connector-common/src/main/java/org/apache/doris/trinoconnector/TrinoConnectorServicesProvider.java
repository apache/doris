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

package org.apache.doris.trinoconnector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogConnector;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorName;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.server.ForStartup;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.util.Executors;
import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

public class TrinoConnectorServicesProvider
        implements ConnectorServicesProvider {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorServicesProvider.class);
    private final CatalogFactory catalogFactory;
    private final List<CatalogProperties> catalogProperties;
    private final Executor executor;

    private final ConcurrentMap<String, CatalogConnector> catalogs = new ConcurrentHashMap<>();

    public TrinoConnectorServicesProvider(String catalogName, String connectorName, CatalogFactory catalogFactory,
            Map<String, String> properties, @ForStartup Executor executor) {
        this.catalogFactory = Objects.requireNonNull(catalogFactory, "catalogFactory is null");
        ImmutableList.Builder<CatalogProperties> catalogProperties = ImmutableList.builder();

        // Modified code
        Preconditions.checkState(connectorName != null, "TrinoConnector properties does not contain connector.name");
        if (connectorName.indexOf('-') >= 0) {
            String deprecatedConnectorName = connectorName;
            connectorName = connectorName.replace('-', '_');
            LOG.warn("You are using the deprecated connector name '{}'. The correct connector name is '{}'",
                    deprecatedConnectorName, connectorName);
        }
        catalogProperties.add(new CatalogProperties(
                CatalogHandle.createRootCatalogHandle(catalogName, new CatalogVersion("default")),
                new ConnectorName(connectorName),
                ImmutableMap.copyOf(properties)));

        this.catalogProperties = catalogProperties.build();
        this.executor = Objects.requireNonNull(executor, "executor is null");
    }

    @PreDestroy
    public void stop() {
        for (CatalogConnector connector : catalogs.values()) {
            connector.shutdown();
        }
        catalogs.clear();
    }

    @Override
    public void loadInitialCatalogs() {
        Executors.executeUntilFailure(
                executor,
                catalogProperties.stream()
                        .map(catalog -> (Callable<?>) () -> {
                            String catalogName = catalog.getCatalogHandle().getCatalogName();
                            LOG.info("-- Loading catalog {} --", catalogName);
                            CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                            catalogs.put(catalogName, newCatalog);
                            LOG.info("-- Added catalog {} using connector {} --",
                                    catalogName, catalog.getConnectorName());
                            return null;
                        })
                        .collect(ImmutableList.toImmutableList()));
    }

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs) {
        List<CatalogProperties> missingCatalogs = catalogs.stream()
                .filter(catalog -> !this.catalogs.containsKey(catalog.getCatalogHandle().getCatalogName()))
                .collect(ImmutableList.toImmutableList());

        if (!missingCatalogs.isEmpty()) {
            throw new TrinoException(StandardErrorCode.CATALOG_NOT_AVAILABLE, "Missing catalogs: " + missingCatalogs);
        }
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle) {
        CatalogConnector catalogConnector = catalogs.get(catalogHandle.getCatalogName());
        Preconditions.checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse) {
        // static catalogs do not need management
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector) {
        Objects.requireNonNull(connector, "connector is null");

        CatalogConnector catalog = catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE,
                new ConnectorName(GlobalSystemConnector.NAME), connector);
        if (catalogs.putIfAbsent(GlobalSystemConnector.NAME, catalog) != null) {
            throw new IllegalStateException("Global system catalog already registered");
        }
    }
}
