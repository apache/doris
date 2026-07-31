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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

/**
 * A factory to create catalog instance of log or covert catalog into log.
 */
public class CatalogFactory {
    private static final Logger LOG = LogManager.getLogger(CatalogFactory.class);

    // The catalog types the engine implements itself, i.e. the ones served by the switch in createCatalog below
    // rather than by a connector plugin. These names are RESERVED: ConnectorPluginManager refuses a provider
    // that claims one of them, so a plugin can never shadow an engine built-in and the order in which the two
    // are consulted cannot change behaviour.
    // Keep in sync with that switch. A name listed here with no case there would be reported as "no connector
    // plugin claimed", which CatalogFactoryPluginRoutingTest catches.
    // Package-visible so CatalogFactoryPluginRoutingTest can assert each name is really served by that switch.
    static final Set<String> BUILTIN_CATALOG_TYPES = ImmutableSet.of("lakesoul", "doris", "test");

    /** Returns true if the engine implements this catalog type itself, i.e. no connector plugin may claim it. */
    public static boolean isBuiltinCatalogType(String catalogType) {
        return catalogType != null && BUILTIN_CATALOG_TYPES.contains(catalogType.toLowerCase());
    }

    /**
     * create the catalog instance from catalog log.
     */
    public static CatalogIf createFromLog(CatalogLog log) throws DdlException {
        return createCatalog(log.getCatalogId(), log.getCatalogName(), log.getResource(),
                log.getComment(), log.getProps(), true);
    }

    /**
     * create the catalog instance from CreateCatalogCommand.
     */
    public static CatalogIf createFromCommand(long catalogId, CreateCatalogCommand cmd)
            throws DdlException {
        return createCatalog(catalogId, cmd.getCatalogName(), cmd.getResource(),
                cmd.getComment(), cmd.getProperties(), false);
    }

    private static CatalogIf createCatalog(long catalogId, String name, String resource, String comment,
            Map<String, String> props, boolean isReplay) throws DdlException {
        // get catalog type from resource or properties
        String catalogType;
        if (!Strings.isNullOrEmpty(resource)) {
            Resource catalogResource = Env.getCurrentEnv().getResourceMgr().getResource(resource);
            if (catalogResource == null) {
                // This is temp bug fix to continue replaying edit log even if resource doesn't exist.
                // In new version, create catalog with resource is not allowed by default.
                LOG.warn("Resource doesn't exist: {} when create catalog {}", resource, name);
                catalogType = "hms";
            } else {
                catalogType = catalogResource.getType().name().toLowerCase();
            }
        } else {
            String type = props.get(CatalogMgr.CATALOG_TYPE_PROP);
            if (Strings.isNullOrEmpty(type)) {
                throw new DdlException("Missing property 'type' in properties");
            }
            catalogType = type.toLowerCase();
        }

        // create catalog
        ExternalCatalog catalog = null;

        // Ensure the resolved catalog type is always persisted in props.
        // For resource-backed catalogs, the type is derived from the Resource object
        // and may not be present in the original props. Without this, GSON deserialization
        // after FE restart would lose the type and initLocalObjectsImpl() would fail.
        props.putIfAbsent(CatalogMgr.CATALOG_TYPE_PROP, catalogType);

        // Ask the connector plugins first. Any registered provider that claims this type and can stand on its
        // own as a catalog wins; the engine keeps no list of accepted types, so installing a plugin is all it
        // takes to make its type usable here. Returns null when nothing claims it — including for a
        // sibling-only connector, whose type must never become a catalog (see ConnectorProvider
        // .isStandaloneCatalogType).
        Connector spiConnector;
        try {
            spiConnector = ConnectorFactory.createStandaloneCatalogConnector(
                    catalogType, props, new DefaultConnectorContext(name, catalogId));
        } catch (RuntimeException | Error e) {
            if (!isReplay) {
                // Creating a catalog interactively must still fail loud: the user is waiting for the error.
                throw e;
            }
            // On replay we must not propagate. The edit-log replay fallback turns any exception from a
            // replayed operation into System.exit(-1), so a connector constructor that rejects a property an
            // older FE stored without validating — or a half-installed plugin throwing NoClassDefFoundError —
            // would keep the whole FE from starting instead of making one catalog unusable. Fall through to
            // the degraded registration below; the failure resurfaces at first access as a query error.
            // (The image path never had this problem: it builds the connector lazily.)
            LOG.warn("Connector for catalog type '{}' failed to build while replaying catalog '{}'. "
                    + "Registering it in degraded mode; accessing it will fail until the cause is fixed.",
                    catalogType, name, e);
            spiConnector = null;
        }
        if (spiConnector != null) {
            LOG.info("Created plugin-driven catalog '{}' via SPI connector for type '{}'",
                    name, catalogType);
            catalog = new PluginDrivenExternalCatalog(
                    catalogId, name, resource, props, comment, spiConnector);
        } else {
            // No plugin claimed it: try the catalog types the engine implements itself. Keep the set of names
            // in BUILTIN_CATALOG_TYPES above in sync with the cases here.
            switch (catalogType) {
                case "lakesoul":
                    throw new DdlException("Lakesoul catalog is no longer supported");
                case "doris":
                    catalog = new RemoteDorisExternalCatalog(
                            catalogId, name, resource, props, comment);
                    break;
                case "test":
                    if (!FeConstants.runningUnitTest) {
                        throw new DdlException("test catalog is only for FE unit test");
                    }
                    catalog = new TestExternalCatalog(
                            catalogId, name, resource, props, comment);
                    break;
                default:
                    // Neither a connector plugin nor the engine knows this type.
                    if (isReplay) {
                        // During replay we must not throw: the edit-log replay fallback turns an exception here
                        // into System.exit(-1), so a missing plugin would keep the whole FE from starting
                        // instead of just making one catalog unusable. Register a degraded catalog; it throws
                        // at first access with a clear message from initLocalObjectsImpl().
                        LOG.warn("No connector plugin claimed catalog type '{}'. Catalog '{}' will be "
                                + "registered in degraded mode until the plugin is available.",
                                catalogType, name);
                        catalog = new PluginDrivenExternalCatalog(
                                catalogId, name, resource, props, comment, null);
                    } else {
                        throw new DdlException("No connector plugin claimed catalog type '" + catalogType
                                + "'. Installed connector types: " + ConnectorFactory.getStandaloneCatalogTypes()
                                + ". Ensure the connector plugin is installed in the plugin directory "
                                + "configured by connector_plugin_root.");
                    }
                    break;
            }
        }

        // set some default properties if missing when creating catalog.
        // both replaying the creating logic will call this method.
        catalog.setDefaultPropsIfMissing(isReplay);

        if (!isReplay) {
            try {
                catalog.checkWhenCreating();
                // This will check if the customized access controller can be created successfully.
                // If failed, it will throw exception and the catalog will not be created.
                try {
                    catalog.initAccessController(true);
                } catch (Throwable e) {
                    LOG.warn("Failed to init access controller", e);
                    throw new DdlException("Failed to init access controller: " + e.getMessage());
                }
            } catch (DdlException e) {
                catalog.onClose();
                throw e;
            }
        }
        return catalog;
    }
}

