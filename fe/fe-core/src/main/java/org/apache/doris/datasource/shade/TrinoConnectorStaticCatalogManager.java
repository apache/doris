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
// copied from https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/connector/StaticCatalogManager.java

package org.apache.doris.datasource.shade;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.ImmutableList;
import static com.google.common.collect.ImmutableList.toImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.CatalogConnector;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorName;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.ForStartup;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.util.Executors.executeUntilFailure;
import jakarta.annotation.PreDestroy;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

public class TrinoConnectorStaticCatalogManager
        implements CatalogManager, ConnectorServicesProvider
{
    private static final Logger log = Logger.get(io.trino.connector.StaticCatalogManager.class);

    private enum State { CREATED, INITIALIZED, STOPPED }

    private final CatalogFactory catalogFactory;
    private final List<CatalogProperties> catalogProperties;
    private final Executor executor;

    private final ConcurrentMap<String, CatalogConnector> catalogs = new ConcurrentHashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(
            State.CREATED);

    public TrinoConnectorStaticCatalogManager(String catalogName, String connectorName, CatalogFactory catalogFactory,
            Map<String, String> properties, @ForStartup Executor executor)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        ImmutableList.Builder<CatalogProperties> catalogProperties = ImmutableList.builder();

        // Modified code
        checkState(connectorName != null, "TrinoConnector properties does not contain connector.name");
        if (connectorName.indexOf('-') >= 0) {
            String deprecatedConnectorName = connectorName;
            connectorName = connectorName.replace('-', '_');
            log.warn("You are using the deprecated connector name '%s'. The correct connector name is '%s'", deprecatedConnectorName, connectorName);
        }
        catalogProperties.add(new CatalogProperties(
                createRootCatalogHandle(catalogName, new CatalogVersion("default")),
                new ConnectorName(connectorName),
                ImmutableMap.copyOf(properties)));

        this.catalogProperties = catalogProperties.build();
        this.executor = requireNonNull(executor, "executor is null");
    }

    @PreDestroy
    public void stop()
    {
        if (state.getAndSet(
                State.STOPPED) == State.STOPPED) {
            return;
        }

        for (CatalogConnector connector : catalogs.values()) {
            connector.shutdown();
        }
        catalogs.clear();
    }

    @Override
    public void loadInitialCatalogs()
    {
        if (!state.compareAndSet(
                State.CREATED, State.INITIALIZED)) {
            return;
        }

        executeUntilFailure(
                executor,
                catalogProperties.stream()
                        .map(catalog -> (Callable<?>) () -> {
                            String catalogName = catalog.getCatalogHandle().getCatalogName();
                            log.info("-- Loading catalog %s --", catalogName);
                            CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                            catalogs.put(catalogName, newCatalog);
                            log.info("-- Added catalog %s using connector %s --", catalogName, catalog.getConnectorName());
                            return null;
                        })
                        .collect(toImmutableList()));
    }

    @Override
    public Set<String> getCatalogNames()
    {
        return ImmutableSet.copyOf(catalogs.keySet());
    }

    @Override
    public Optional<Catalog> getCatalog(String catalogName)
    {
        return Optional.ofNullable(catalogs.get(catalogName))
                .map(CatalogConnector::getCatalog);
    }

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs)
    {
        List<CatalogProperties> missingCatalogs = catalogs.stream()
                .filter(catalog -> !this.catalogs.containsKey(catalog.getCatalogHandle().getCatalogName()))
                .collect(toImmutableList());

        if (!missingCatalogs.isEmpty()) {
            throw new TrinoException(CATALOG_NOT_AVAILABLE, "Missing catalogs: " + missingCatalogs);
        }
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
    {
        // static catalogs do not need management
    }

    @Override
    public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
    {
        // static catalog manager does not propagate catalogs between machines
        return Optional.empty();
    }

    @Override
    public Set<CatalogHandle> getActiveCatalogs()
    {
        // static catalog manager does not differentiate between active and not. Nor does it need to prune
        return ImmutableSet.of();
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        CatalogConnector catalogConnector = catalogs.get(catalogHandle.getCatalogName());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector)
    {
        requireNonNull(connector, "connector is null");

        CatalogConnector catalog = catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME), connector);
        if (catalogs.putIfAbsent(GlobalSystemConnector.NAME, catalog) != null) {
            throw new IllegalStateException("Global system catalog already registered");
        }
    }

    @Override
    public void createCatalog(String catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
    {
        throw new TrinoException(NOT_SUPPORTED, "CREATE CATALOG is not supported by the static catalog store");
    }

    @Override
    public void dropCatalog(String catalogName, boolean exists)
    {
        throw new TrinoException(NOT_SUPPORTED, "DROP CATALOG is not supported by the static catalog store");
    }
}
