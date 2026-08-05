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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorConfFile;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.datasource.CatalogFactory;
import org.apache.doris.extension.loader.ApiVersionGate;
import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadFailure;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;
import org.apache.doris.extension.loader.PluginRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages lifecycle of ConnectorProvider plugins.
 *
 * <p>Discovery order:
 * 1. ServiceLoader scan (classpath-based built-ins / test overrides)
 * 2. DirectoryPluginRuntimeManager scan (production plugin directories)
 *
 * <p>The first provider that returns {@code supports(catalogType, props) == true} is used.
 * Classpath providers have higher priority than directory-loaded providers. A provider's
 * {@code getType()} must be unique and must not name a catalog type the engine implements itself; both are
 * checked when the provider is discovered (see {@link #registerDiscovered}).
 *
 * <p>Unlike {@link org.apache.doris.fs.FileSystemPluginManager}, this class returns
 * {@code null} from {@link #createConnector} when no provider matches, leaving it to the caller to decide
 * how to fail — {@code CatalogFactory} then tries the catalog types the engine implements itself, and
 * a gateway asking for a sibling fails with its own connector-specific message.
 */
public class ConnectorPluginManager {

    private static final Logger LOG = LogManager.getLogger(ConnectorPluginManager.class);

    /**
     * The connector plugin API contract this FE serves. Built from the version filtered into
     * fe-connector-spi at build time, and anchored on {@link ConnectorProvider} so that it is read from the
     * very artifact carrying the SPI. A missing or malformed resource is a build defect and fails class
     * initialization loudly rather than degrading into a check that admits everything.
     */
    private static final ApiVersionGate API_VERSION_GATE =
            ApiVersionGate.forFamily("connector", ConnectorProvider.class);

    // Connector SPI and filesystem SPI classes must be parent-first so that all
    // instances of shared interfaces/classes are loaded by a single ClassLoader.
    private static final List<String> CONNECTOR_PARENT_FIRST_PREFIXES =
            Arrays.asList("org.apache.doris.connector.", "org.apache.doris.filesystem.");

    /** Family label in the process-wide {@link PluginRegistry}. */
    private static final String PLUGIN_FAMILY = "CONNECTOR";

    /**
     * Engine names {@code CREATE TABLE ... ENGINE=<name>} resolves inside the engine itself, so no plugin may
     * claim one: {@code olap} is the internal catalog's, and the other three are retired table types that
     * still owe the user a specific "use X instead" message from {@code InternalCatalog}. Letting a plugin
     * shadow one of these would silently redirect a statement the engine answers for.
     */
    private static final Set<String> RESERVED_CREATE_TABLE_ENGINE_NAMES =
            new HashSet<>(Arrays.asList("olap", "mysql", "odbc", "broker"));

    private final List<ConnectorProvider> providers = new CopyOnWriteArrayList<>();
    /** Lower-cased type names already claimed by a discovered provider. Guards {@code getType()} uniqueness. */
    private final Set<String> claimedTypes = ConcurrentHashMap.newKeySet();
    /** Lower-cased create-table engine names already claimed. Same uniqueness rule as {@link #claimedTypes}. */
    private final Set<String> claimedEngineNames = ConcurrentHashMap.newKeySet();
    private final DirectoryPluginRuntimeManager<ConnectorProvider> runtimeManager =
            new DirectoryPluginRuntimeManager<>();
    private final ClassLoadingPolicy classLoadingPolicy =
            new ClassLoadingPolicy(CONNECTOR_PARENT_FIRST_PREFIXES);
    /**
     * Each directory-loaded provider's own {@code <name>.conf}, read once at load and served back through
     * {@link ConnectorContext#getConnectorConfig()}.
     *
     * <p>An {@link IdentityHashMap} because the key is the provider instance and lookup must not call
     * {@code equals}/{@code hashCode} on plugin code — the same rule
     * {@link DirectoryPluginRuntimeManager} follows by snapshotting {@code name()} once at load and never
     * re-entering the plugin on a query path. Written only during startup, read when a catalog is built,
     * so a synchronized wrapper is enough.
     */
    private final Map<ConnectorProvider, Map<String, String>> connectorConfigs =
            Collections.synchronizedMap(new IdentityHashMap<>());

    /** Called at FE startup to load built-in providers from classpath. */
    public void loadBuiltins() {
        loadBuiltins(ConnectorProvider.class.getClassLoader());
    }

    /** Classloader seam used to verify embedded/classpath providers with their own defining jars. */
    void loadBuiltins(ClassLoader classLoader) {
        Iterator<ServiceLoader.Provider<ConnectorProvider>> providers =
                ServiceLoader.load(ConnectorProvider.class, classLoader).stream().iterator();
        while (providers.hasNext()) {
            ServiceLoader.Provider<ConnectorProvider> descriptor = providers.next();
            Class<? extends ConnectorProvider> providerClass = descriptor.type();
            String rejection = API_VERSION_GATE.rejectionReasonForClass(providerClass);
            if (rejection != null) {
                // Gate the descriptor before get(): an incompatible provider constructor may have
                // side effects or link against an API that this FE must never execute.
                LOG.warn("Skip built-in connector provider {}: {}", providerClass.getName(), rejection);
                continue;
            }
            ConnectorProvider provider;
            try {
                provider = descriptor.get();
            } catch (ServiceConfigurationError | RuntimeException e) {
                LOG.warn("Skip built-in connector provider {}: construction failed",
                        providerClass.getName(), e);
                continue;
            }
            try {
                // Snapshot self-reported metadata before publishing the provider
                // so one throwing implementation is rejected cleanly instead of
                // aborting startup or being active without an inventory row.
                PluginRegistry.getInstance().registerBuiltin(PLUGIN_FAMILY, provider);
            } catch (RuntimeException e) {
                LOG.warn("Skip built-in connector provider {}: self-reported metadata failed",
                        providerClass.getName(), e);
                continue;
            }
            // Deliberately outside that catch: registerDiscovered's fail-loud
            // IllegalStateException reports a build error, not a "skip this one" condition.
            if (registerDiscovered(provider, true)) {
                LOG.info("Registered built-in connector provider: {}", provider.getType());
            }
        }
    }

    /**
     * Admits a discovered provider after checking its {@code getType()} contract: non-blank, not an engine
     * built-in catalog type name, and not already claimed (compared case-insensitively, because
     * {@link ConnectorProvider#supports} matches case-insensitively and catalog types are lower-cased before
     * routing). Refusing a reserved name here is what makes it impossible for a plugin to shadow a catalog type
     * the engine implements itself — routing order then cannot matter. The engine names it claims for
     * {@code CREATE TABLE ... ENGINE=} go through the same two checks.
     *
     * @param failFast {@code true} for the classpath batch: two providers claiming one type name there is a
     *                 build error and must fail loud. {@code false} for the plugin-directory batch: that is a
     *                 deployment accident, so the offender is skipped and logged, preserving
     *                 {@link #loadPlugins}'s partial-success contract (one bad plugin dir must not stop FE).
     * @return true if the provider was admitted
     */
    boolean registerDiscovered(ConnectorProvider provider, boolean failFast) {
        String type = provider.getType();
        Set<String> engineNames = provider.acceptedCreateTableEngineNames();
        String problem = typeNameProblem(type);
        if (problem == null) {
            problem = createTableEngineNameProblem(engineNames);
        }
        // Claim last, and only once nothing else can reject: a failed claim must not leave a name taken.
        if (problem == null && !claimedTypes.add(type.toLowerCase())) {
            problem = "type name '" + type + "' is already claimed by another registered connector provider";
        }
        if (problem != null) {
            String message = "Rejected connector provider " + provider.getClass().getName() + ": " + problem;
            if (failFast) {
                throw new IllegalStateException(message);
            }
            LOG.error("{}. The connector will not be available.", message);
            return false;
        }
        for (String engineName : engineNames) {
            claimedEngineNames.add(engineName.toLowerCase());
        }
        providers.add(provider);
        return true;
    }

    private static String typeNameProblem(String type) {
        if (type == null || type.trim().isEmpty()) {
            return "getType() returned a blank type name";
        }
        if (CatalogFactory.isBuiltinCatalogType(type)) {
            return "type name '" + type + "' is reserved for a catalog type the engine implements itself";
        }
        return null;
    }

    /**
     * Same uniqueness rule as the type name, applied to the engine names a provider claims for
     * {@code CREATE TABLE ... ENGINE=}. Two plugins answering to one engine name would make the statement
     * mean whichever registered first, so the second is refused at registration and routing order cannot
     * matter — mirroring how a duplicate catalog type is handled.
     */
    private String createTableEngineNameProblem(Set<String> engineNames) {
        for (String engineName : engineNames) {
            if (engineName == null || engineName.trim().isEmpty()) {
                return "acceptedCreateTableEngineNames() returned a blank engine name";
            }
            String lower = engineName.toLowerCase();
            if (RESERVED_CREATE_TABLE_ENGINE_NAMES.contains(lower)) {
                return "create-table engine name '" + engineName
                        + "' is reserved for an engine name the engine resolves itself";
            }
            if (claimedEngineNames.contains(lower)) {
                return "create-table engine name '" + engineName
                        + "' is already claimed by another registered connector provider";
            }
        }
        return null;
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
                classLoadingPolicy,
                API_VERSION_GATE);

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
            // Built-ins (and earlier-loaded plugins) must never be displaced by a
            // same-name directory jar.
            if (hasProviderNamed(handle.getPluginName())) {
                LOG.warn("Skip connector plugin '{}' from {}: name conflicts with an already "
                        + "registered provider", handle.getPluginName(), handle.getPluginDir());
                runtimeManager.discard(handle.getPluginName());
                continue;
            }
            // The inventory row is written only for a provider that was actually admitted, so
            // information_schema.extensions never lists a connector the routing table cannot reach.
            if (registerDiscovered(handle.getFactory(), false)) {
                PluginRegistry.getInstance().registerExternal(PLUGIN_FAMILY, handle);
                loadConnectorConfig(handle);
                LOG.info("Loaded connector plugin: name={}, pluginDir={}, jarCount={}",
                        handle.getPluginName(), handle.getPluginDir(),
                        handle.getResolvedJars().size());
            } else {
                // registerDiscovered already logged why it refused. Release the runtime here too, so
                // every "loaded from a directory but not admitted" exit discards the plugin's
                // classloader — the same pairing the name-conflict path above and both of
                // FileSystemPluginManager's reject paths follow.
                runtimeManager.discard(handle.getPluginName());
            }
        }
    }

    /**
     * Reads this plugin's own {@code <name>.conf}, if it ships one, so that
     * {@link ConnectorContext#getConnectorConfig()} can serve it later.
     *
     * <p>Nothing here can keep the plugin from being registered. A conf file that cannot be read is
     * reported and the connector proceeds without it: refusing the plugin would make the catalog type
     * vanish entirely, and the only thing a user would then see is {@code CREATE CATALOG} answering
     * "no provider supports type", which points nowhere near a bad file. Every setting reachable this way
     * either has a default or a fe.conf fallback, so proceeding is a real degradation path, not a guess.
     */
    private void loadConnectorConfig(PluginHandle<ConnectorProvider> handle) {
        Map<String, String> conf = Collections.emptyMap();
        try {
            conf = ConnectorConfFile.load(handle.getPluginDir(), handle.getPluginName());
        } catch (IOException e) {
            LOG.error("Failed to read connector plugin conf {} in {}; the connector starts without it "
                            + "and falls back to fe.conf", ConnectorConfFile.fileName(handle.getPluginName()),
                    handle.getPluginDir(), e);
        }
        connectorConfigs.put(handle.getFactory(), conf);
        if (!conf.isEmpty()) {
            // Key names only. A value here is administrator-supplied and may name a credential path or
            // an internal host; the point of the line is to let an operator confirm the file was found.
            LOG.info("Connector plugin conf loaded: name={}, file={}, keys={}", handle.getPluginName(),
                    ConnectorConfFile.fileName(handle.getPluginName()), conf.keySet());
        }
    }

    private boolean hasProviderNamed(String name) {
        for (ConnectorProvider p : providers) {
            if (name.equals(p.name())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a Connector for the given catalog type by selecting the first supporting provider, with no
     * regard for whether that connector may stand on its own as a catalog.
     *
     * <p><b>This is the sibling-lookup entry point</b> ({@code ConnectorContext.createSiblingConnector}): a
     * sibling-only connector — one whose table format is parasitic on another connector's metastore — is
     * reachable <em>only</em> here, so this method must never filter on
     * {@link ConnectorProvider#isStandaloneCatalogType()}. Use
     * {@link #createStandaloneCatalogConnector} to build a catalog.
     *
     * <p>Returns {@code null} if no provider supports the given catalog type; the caller decides how to fail.
     *
     * @param catalogType the catalog type (e.g. "hms", "iceberg", "es")
     * @param properties  catalog configuration properties
     * @param context     runtime context provided by fe-core
     * @return a ready-to-use Connector, or {@code null} if no provider matches
     */
    public Connector createConnector(
            String catalogType, Map<String, String> properties, ConnectorContext context) {
        return createConnector(catalogType, properties, context, false);
    }

    /**
     * Creates a Connector to back a standalone catalog, i.e. one named by the {@code type} property of a
     * {@code CREATE CATALOG}. Same selection as {@link #createConnector} except that a provider declaring
     * {@link ConnectorProvider#isStandaloneCatalogType()} {@code == false} is passed over, because building a
     * catalog around it would produce a catalog with no engine-side semantics behind it.
     *
     * @return a ready-to-use Connector, or {@code null} if no provider claims the type as a standalone catalog
     */
    public Connector createStandaloneCatalogConnector(
            String catalogType, Map<String, String> properties, ConnectorContext context) {
        return createConnector(catalogType, properties, context, true);
    }

    private Connector createConnector(String catalogType, Map<String, String> properties,
            ConnectorContext context, boolean standaloneOnly) {
        for (ConnectorProvider provider : providers) {
            if (provider.supports(catalogType, properties)) {
                if (standaloneOnly && !provider.isStandaloneCatalogType()) {
                    LOG.info("Provider '{}' claims catalogType='{}' but is not a standalone catalog type; "
                            + "it can only be built as an embedded sibling.", provider.getType(), catalogType);
                    continue;
                }
                LOG.info("Creating connector via provider '{}' for catalogType='{}'",
                        provider.getType(), catalogType);
                return provider.create(properties, withConnectorConfig(context, provider));
            }
        }
        LOG.debug("No ConnectorProvider supports catalogType='{}' (standaloneOnly={}). Registered: {}",
                catalogType, standaloneOnly, providerNames());
        return null;
    }

    /**
     * Attaches the chosen provider's plugin conf to the context handed to {@code create}.
     *
     * <p>It has to happen here rather than in the context itself: fe-core builds a
     * {@link ConnectorContext} for a catalog before it knows which plugin will claim the type, so the
     * conf can only be layered on once the provider is picked. Keeping it out of
     * {@code DefaultConnectorContext} is also what keeps the engine's context free of any connector's
     * key names.
     *
     * <p>A sibling connector ({@code ConnectorContext.createSiblingConnector}) comes back through this
     * same method, so it is handed <em>its own</em> plugin's conf rather than inheriting the gateway's.
     * That is the intent: the conf belongs to the plugin, not to the catalog.
     *
     * <p>An empty conf is not wrapped. The interface default already answers an empty map, and one fewer
     * decorator is one fewer place a future {@link ConnectorContext} method can be lost in.
     */
    private ConnectorContext withConnectorConfig(ConnectorContext base, ConnectorProvider provider) {
        Map<String, String> conf = connectorConfigs.get(provider);
        return conf == null || conf.isEmpty() ? base : new ConnectorConfigContext(base, conf);
    }

    /**
     * Finds the provider that would back a catalog of this type, without creating a connector. For engine
     * decisions that must be answered for a catalog that may not be initialized yet — asking the connector
     * would force-initialize it. Same selection as {@link #createConnector}: first provider that supports the
     * type.
     *
     * @return the matching provider, or empty if none matches
     */
    public Optional<ConnectorProvider> findProvider(String catalogType, Map<String, String> properties) {
        for (ConnectorProvider provider : providers) {
            if (provider.supports(catalogType, properties)) {
                return Optional.of(provider);
            }
        }
        return Optional.empty();
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
     * Returns the type names that can be written in {@code CREATE CATALOG}, sorted. Excludes sibling-only
     * connectors: naming one of those in a diagnostic would point the user at a type they cannot create.
     */
    public List<String> getStandaloneCatalogTypes() {
        List<String> types = new ArrayList<>();
        for (ConnectorProvider p : providers) {
            if (p.isStandaloneCatalogType()) {
                types.add(p.getType());
            }
        }
        Collections.sort(types);
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
                provider.validateProperties(properties);
                return;
            }
        }
    }

    /** Validates an ALTER candidate through the matching provider without mutating catalog state. */
    public void validatePropertiesForUpdate(String catalogType,
            Map<String, String> currentProperties, Map<String, String> updatedProperties) {
        for (ConnectorProvider provider : providers) {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            try {
                // Directory providers may resolve validation helpers through TCCL, so selection and
                // validation must run under the same plugin loader and never leak it to the FE caller.
                thread.setContextClassLoader(provider.getClass().getClassLoader());
                Map<String, String> matchProperties = currentProperties == null
                        ? Collections.emptyMap() : currentProperties;
                if (provider.supports(catalogType, matchProperties)) {
                    provider.validatePropertiesForUpdate(currentProperties, updatedProperties);
                    return;
                }
            } finally {
                thread.setContextClassLoader(previous);
            }
        }
    }

    /**
     * Registers a provider at highest priority (index 0). For testing overrides.
     *
     * <p>Deliberately bypasses {@link #registerDiscovered}'s uniqueness check: shadowing an already-registered
     * type is exactly what this method exists for (several tests stand in for a real plugin this way).
     */
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
