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
import org.apache.doris.connector.api.Connector;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalogFactory;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalCatalogFactory;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalogFactory;
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

    // Only these catalog types are routed through the SPI connector path.
    // Other types (hms, iceberg, paimon, trino-connector, hudi, max_compute) still use
    // their built-in ExternalCatalog implementations until their ConnectorProviders are fully ready.
    private static final Set<String> SPI_READY_TYPES = ImmutableSet.of("jdbc", "es");

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

        // Try SPI connector plugin path first, but only for whitelisted types.
        // Returns null if no ConnectorProvider matches the catalog type.
        Connector spiConnector = null;
        if (SPI_READY_TYPES.contains(catalogType)) {
            spiConnector = ConnectorFactory.createConnector(
                    catalogType, props, new DefaultConnectorContext(name, catalogId));
        }
        if (spiConnector != null) {
            LOG.info("Created plugin-driven catalog '{}' via SPI connector for type '{}'",
                    name, catalogType);
            catalog = new PluginDrivenExternalCatalog(
                    catalogId, name, resource, props, comment, spiConnector);
        } else if (SPI_READY_TYPES.contains(catalogType)) {
            // SPI-only type but no connector provider loaded.
            if (isReplay) {
                // During replay we must not throw — FE startup would be blocked.
                // Register a degraded catalog; it will throw at first access with a
                // clear error message from initLocalObjectsImpl().
                LOG.warn("No SPI connector plugin loaded for type '{}'. Catalog '{}' will be "
                        + "registered in degraded mode until the plugin is available.",
                        catalogType, name);
                catalog = new PluginDrivenExternalCatalog(
                        catalogId, name, resource, props, comment, null);
            } else {
                throw new DdlException("No connector plugin loaded for catalog type '"
                        + catalogType + "'. Ensure the connector plugin is installed in the "
                        + "plugin directory configured by connector_plugin_root.");
            }
        }

        // Fallback to built-in catalog types if no SPI connector matched.
        if (catalog == null) {
            switch (catalogType) {
                case "hms":
                    catalog = new HMSExternalCatalog(catalogId, name, resource, props, comment);
                    break;
                case "iceberg":
                    catalog = IcebergExternalCatalogFactory.createCatalog(
                            catalogId, name, resource, props, comment);
                    break;
                case "paimon":
                    catalog = PaimonExternalCatalogFactory.createCatalog(
                            catalogId, name, resource, props, comment);
                    break;
                case "trino-connector":
                    catalog = TrinoConnectorExternalCatalogFactory.createCatalog(
                            catalogId, name, resource, props, comment);
                    break;
                case "max_compute":
                    catalog = new MaxComputeExternalCatalog(
                            catalogId, name, resource, props, comment);
                    break;
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
                    throw new DdlException("Unknown catalog type: " + catalogType);
            }
        }

        // set some default properties if missing when creating catalog.
        // both replaying the creating logic will call this method.
        catalog.setDefaultPropsIfMissing(isReplay);

        if (!isReplay) {
            catalog.checkWhenCreating();
            // This will check if the customized access controller can be created successfully.
            // If failed, it will throw exception and the catalog will not be created.
            try {
                catalog.initAccessController(true);
            } catch (Throwable e) {
                LOG.warn("Failed to init access controller", e);
                throw new DdlException("Failed to init access controller: " + e.getMessage());
            }
        }
        return catalog;
    }
}


