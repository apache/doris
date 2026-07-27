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

package org.apache.doris.fs;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.service.FrontendOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory for filesystem instances.
 *
 * <p>Call {@link #initPluginManager(FileSystemPluginManager)} at FE startup before any
 * {@code getFileSystem()} call. In production, providers are loaded from the plugin directory
 * configured via {@code filesystem_plugin_root}. In unit tests, providers are discovered from
 * the test classpath via ServiceLoader.
 */
public final class FileSystemFactory {

    private static final Logger LOG = LogManager.getLogger(FileSystemFactory.class);

    // Plugin manager singleton, set at FE startup
    private static volatile FileSystemPluginManager pluginManager;

    // Fallback provider cache for non-initialized environments (tests, migration phase)
    private static volatile List<FileSystemProvider> cachedProviders = null;

    private FileSystemFactory() {}

    // =========================================================
    // SPI API — returns spi.FileSystem
    // =========================================================

    /**
     * Sets the plugin manager singleton. Called once at FE startup before any
     * {@code getFileSystem()} invocation.
     */
    public static void initPluginManager(FileSystemPluginManager manager) {
        pluginManager = manager;
        // The storage facade keeps its own handle so datasource.storage never has to reach
        // back into this runtime layer (dependency direction stays fs -> datasource.storage).
        StorageAdapter.initPluginManager(manager);
    }

    /**
     * Returns the plugin manager singleton set at FE startup, or null when not initialized
     * (unit-test / migration path). Callers needing raw-props binding go through
     * {@link FileSystemPluginManager#bindPrimary}/{@code bindAll} on this instance.
     */
    public static FileSystemPluginManager getPluginManager() {
        return pluginManager;
    }

    /**
     * SPI entry point: selects a provider and creates the filesystem.
     *
     * <p>If {@link #initPluginManager} has been called (production path),
     * delegates to {@link FileSystemPluginManager#createFileSystem}.
     * Otherwise falls back to ServiceLoader discovery (unit-test / migration path).
     *
     * @param properties key-value storage configuration
     * @return initialized {@code org.apache.doris.filesystem.FileSystem}
     * @throws IOException if no provider matches or creation fails
     */
    public static org.apache.doris.filesystem.FileSystem getFileSystem(Map<String, String> properties)
            throws IOException {
        FileSystemPluginManager mgr = pluginManager;
        if (mgr != null) {
            return mgr.createFileSystem(properties);
        }
        // Fallback: ServiceLoader discovery (unit-test / migration path)
        List<FileSystemProvider> providers = getProviders();
        List<String> tried = new ArrayList<>();
        for (FileSystemProvider provider : providers) {
            if (provider.supports(properties)) {
                LOG.debug("FileSystemFactory: selected SPI provider '{}' for keys={}",
                        provider.name(), properties.keySet());
                return provider.create(properties);
            }
            tried.add(provider.name());
        }
        throw new IOException(String.format(
                "No FileSystemProvider found for properties %s. Tried: %s. "
                        + "Ensure the corresponding fe-filesystem-xxx jar is on the classpath.",
                properties.keySet(), tried));
    }

    /**
     * SPI entry point accepting an already-bound typed properties object (e.g. from
     * {@code StorageAdapter.getSpiProperties()}). Unlike {@link #getFileSystem(Map)}, no
     * provider re-selection happens: the provider that produced the binding (matched by
     * {@code providerName()}) creates the filesystem, so the routing decision made at bind
     * time is preserved even for property maps several providers would accept.
     */
    public static org.apache.doris.filesystem.FileSystem getFileSystem(
            FileSystemProperties spiProperties) throws IOException {
        String providerName = spiProperties.providerName();
        FileSystemPluginManager mgr = pluginManager;
        List<FileSystemProvider> providers = mgr != null ? mgr.getProviders() : getProviders();
        for (FileSystemProvider provider : providers) {
            if (provider.name().equalsIgnoreCase(providerName)) {
                try {
                    return provider.createUntyped(spiProperties);
                } catch (UnsupportedOperationException e) {
                    // Providers without typed creation (BROKER/LOCAL) keep the map entry point.
                    return provider.create(new HashMap<>(spiProperties.rawProperties()));
                }
            }
        }
        throw new IOException(String.format(
                "No FileSystemProvider named '%s' is loaded. "
                        + "Ensure the corresponding fe-filesystem-xxx jar is on the classpath.",
                providerName));
    }

    /**
     * SPI entry point accepting the {@link StorageAdapter} facade; delegates to
     * {@link #getFileSystem(FileSystemProperties)} so the
     * bind-time provider routing decision is preserved. No converter marker keys involved.
     */
    public static org.apache.doris.filesystem.FileSystem getFileSystem(StorageAdapter adapter)
            throws IOException {
        return getFileSystem(adapter.getSpiProperties());
    }

    /**
     * Returns all discovered SPI providers via ServiceLoader. Uses cached list after first load.
     * Used only in the fallback path when pluginManager is not initialized.
     * Package-private for testing.
     */
    static List<FileSystemProvider> getProviders() {
        if (cachedProviders == null) {
            synchronized (FileSystemFactory.class) {
                if (cachedProviders == null) {
                    List<FileSystemProvider> providers = new ArrayList<>();
                    ServiceLoader<FileSystemProvider> loader = ServiceLoader.load(
                            FileSystemProvider.class,
                            Thread.currentThread().getContextClassLoader());
                    loader.forEach(providers::add);
                    LOG.info("FileSystemFactory: loaded {} SPI provider(s): {}",
                            providers.size(),
                            providers.stream().map(FileSystemProvider::name)
                                    .collect(java.util.stream.Collectors.joining(", ")));
                    cachedProviders = providers;
                }
            }
        }
        return cachedProviders;
    }

    /**
     * Clears the SPI provider cache and plugin manager. For testing only.
     */
    static void clearProviderCache() {
        cachedProviders = null;
        pluginManager = null;
    }

    /**
     * Creates a {@link org.apache.doris.filesystem.FileSystem} for the given {@link BrokerDesc}.
     *
     * <p>For broker storage, resolves the live broker host:port via
     * {@code BrokerMgr} and delegates to {@link #getBrokerFileSystem}. For all other storage
     * types (HDFS, S3, etc.), delegates to {@link #getFileSystem(StorageAdapter)} so the
     * bind-time provider routing decision is preserved.
     *
     * <p>This is the preferred entry point for fe-core code that holds a {@code BrokerDesc}
     * and wants to perform filesystem operations (list, delete, read, write) via the unified
     * {@code FileSystem} SPI without caring about the underlying storage type.
     *
     * @param brokerDesc descriptor for the target storage
     * @return initialized {@link org.apache.doris.filesystem.FileSystem}; caller must close it
     * @throws UserException if the broker cannot be resolved or filesystem creation fails
     */
    public static org.apache.doris.filesystem.FileSystem getFileSystem(BrokerDesc brokerDesc)
            throws UserException {
        StorageAdapter adapter = brokerDesc.getStorageAdapter();
        if (adapter != null && adapter.getType() == StorageTypeId.BROKER) {
            try {
                String localIP = FrontendOptions.getLocalHostAddress();
                FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getBroker(adapter.getBrokerName(), localIP);
                String clientId = NetUtils.getHostPortInAccessibleFormat(localIP, Config.edit_log_port);
                return getBrokerFileSystem(broker.host, broker.port, clientId, adapter.getBrokerParams());
            } catch (AnalysisException | IOException e) {
                throw new UserException("Failed to create broker filesystem for '"
                        + brokerDesc.getName() + "': " + e.getMessage(), e);
            }
        }
        try {
            return getFileSystem(adapter);
        } catch (IOException e) {
            throw new UserException("Failed to create filesystem for '"
                    + brokerDesc.getName() + "': " + e.getMessage(), e);
        }
    }

    /**
     * Creates a broker-backed {@link org.apache.doris.filesystem.FileSystem} using a
     * pre-resolved broker endpoint.
     *
     * <p>The caller is responsible for resolving the broker name to a live host:port via
     * {@code BrokerMgr.getBroker()} before calling this method. This keeps {@code BrokerMgr}
     * coupling in fe-core only; the {@code fe-filesystem-broker} module has zero fe-core dependency.
     *
     * @param host        live broker host (already resolved from BrokerMgr)
     * @param port        live broker Thrift port
     * @param clientId    FE identifier sent to broker for logging (e.g. "host:editLogPort")
     * @param brokerParams broker-specific params (username, password, hadoop config, ...)
     * @return initialized {@code org.apache.doris.filesystem.FileSystem}
     * @throws IOException if the broker filesystem provider is not found or creation fails
     */
    public static org.apache.doris.filesystem.FileSystem getBrokerFileSystem(
            String host, int port, String clientId, Map<String, String> brokerParams) throws IOException {
        Map<String, String> props = new HashMap<>(brokerParams);
        props.put("_STORAGE_TYPE_", "BROKER");
        props.put("BROKER_HOST", host);
        props.put("BROKER_PORT", String.valueOf(port));
        props.put("BROKER_CLIENT_ID", clientId);
        return getFileSystem(props);
    }
}
