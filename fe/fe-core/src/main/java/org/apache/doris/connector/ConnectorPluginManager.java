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

package org.apache.doris.connector;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadFailure;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages lifecycle of ConnectorProvider plugins.
 *
 * <p>Discovery order:
 * 1. ServiceLoader scan (classpath-based built-ins / test overrides)
 * 2. DirectoryPluginRuntimeManager scan (production plugin directories)
 *
 * <p>The first provider that returns {@code supports(catalogType, props) == true} is used.
 * Classpath providers have higher priority than directory-loaded providers.
 *
 * <p>Unlike {@link org.apache.doris.fs.FileSystemPluginManager}, this class returns
 * {@code null} from {@link #createConnector} when no provider matches, allowing
 * fe-core to gracefully fall back to the existing hardcoded CatalogFactory logic
 * during the migration period.
 */
public class ConnectorPluginManager {

    private static final Logger LOG = LogManager.getLogger(ConnectorPluginManager.class);

    /** The API version that this FE build supports. Increment on breaking SPI changes. */
    static final int CURRENT_API_VERSION = 1;

    // Connector SPI and filesystem SPI classes must be parent-first so that all
    // instances of shared interfaces/classes are loaded by a single ClassLoader.
    private static final List<String> CONNECTOR_PARENT_FIRST_PREFIXES =
            Arrays.asList("org.apache.doris.connector.", "org.apache.doris.filesystem.");

    private final List<ConnectorProvider> providers = new CopyOnWriteArrayList<>();
    private final DirectoryPluginRuntimeManager<ConnectorProvider> runtimeManager =
            new DirectoryPluginRuntimeManager<>();
    private final ClassLoadingPolicy classLoadingPolicy =
            new ClassLoadingPolicy(CONNECTOR_PARENT_FIRST_PREFIXES);

    /** Called at FE startup to load built-in providers from classpath. */
    public void loadBuiltins() {
        ServiceLoader.load(ConnectorProvider.class)
                .forEach(p -> {
                    providers.add(p);
                    LOG.info("Registered built-in connector provider: {}", p.getType());
                });
    }

    /**
     * Loads connector provider plugins from plugin root directories.
     * Failures are logged as warnings; partial success is allowed.
     *
     * @param pluginRoots directories to scan for connector plugin subdirectories
     */
    public void loadPlugins(List<Path> pluginRoots) {
        LoadReport<ConnectorProvider> report = runtimeManager.loadAll(
                pluginRoots,
                ConnectorPluginManager.class.getClassLoader(),
                ConnectorProvider.class,
                classLoadingPolicy);

        LOG.info("Connector plugin load summary: rootsScanned={}, dirsScanned={}, "
                        + "successCount={}, failureCount={}",
                report.getRootsScanned(), report.getDirsScanned(),
                report.getSuccesses().size(), report.getFailures().size());

        for (LoadFailure failure : report.getFailures()) {
            LOG.warn("Connector plugin load failure: dir={}, stage={}, message={}, cause={}",
                    failure.getPluginDir(), failure.getStage(), failure.getMessage(),
                    failure.getCause());
        }

        for (PluginHandle<ConnectorProvider> handle : report.getSuccesses()) {
            providers.add(handle.getFactory());
            LOG.info("Loaded connector plugin: name={}, pluginDir={}, jarCount={}",
                    handle.getPluginName(), handle.getPluginDir(),
                    handle.getResolvedJars().size());
        }
    }

    /**
     * Creates a Connector for the given catalog type by selecting the first supporting provider.
     *
     * <p>Returns {@code null} if no provider supports the given catalog type.
     * This allows fe-core to gracefully fall back to the existing hardcoded CatalogFactory
     * switch-case during the migration period.
     *
     * @param catalogType the catalog type (e.g. "hive", "iceberg", "es")
     * @param properties  catalog configuration properties
     * @param context     runtime context provided by fe-core
     * @return a ready-to-use Connector, or {@code null} if no provider matches
     */
    public Connector createConnector(
            String catalogType, Map<String, String> properties, ConnectorContext context) {
        for (ConnectorProvider provider : providers) {
            if (provider.supports(catalogType, properties)) {
                int providerVersion = provider.apiVersion();
                if (providerVersion != CURRENT_API_VERSION) {
                    LOG.warn("Skipping connector provider '{}': apiVersion={} (expected {})",
                            provider.getType(), providerVersion, CURRENT_API_VERSION);
                    continue;
                }
                LOG.info("Creating connector via provider '{}' for catalogType='{}'",
                        provider.getType(), catalogType);
                return provider.create(properties, context);
            }
        }
        LOG.debug("No ConnectorProvider supports catalogType='{}'. Registered: {}",
                catalogType, providerNames());
        return null;
    }

    /** Returns the type names of all registered providers. */
    public List<String> getRegisteredTypes() {
        List<String> types = new ArrayList<>();
        for (ConnectorProvider p : providers) {
            types.add(p.getType());
        }
        return types;
    }

    /**
     * Validates catalog properties using the matching provider.
     * Does nothing if no provider matches.
     *
     * @throws IllegalArgumentException if validation fails
     */
    public void validateProperties(String catalogType, Map<String, String> properties) {
        for (ConnectorProvider provider : providers) {
            if (provider.supports(catalogType, properties)) {
                if (provider.apiVersion() != CURRENT_API_VERSION) {
                    throw new IllegalArgumentException(
                            "Connector provider '" + provider.getType()
                                    + "' has incompatible API version " + provider.apiVersion()
                                    + " (expected " + CURRENT_API_VERSION + ")");
                }
                provider.validateProperties(properties);
                return;
            }
        }
    }

    /** Registers a provider at highest priority (index 0). For testing overrides. */
    public void registerProvider(ConnectorProvider provider) {
        providers.add(0, provider);
    }

    private List<String> providerNames() {
        List<String> names = new ArrayList<>();
        for (ConnectorProvider p : providers) {
            names.add(p.getType());
        }
        return names;
    }
}
