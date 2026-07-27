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
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
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
     * SPI entry point accepting legacy {@link StorageProperties}.
     * Converts via {@link StoragePropertiesConverter} then delegates to
     * {@link #getFileSystem(Map)}.
     */
    public static org.apache.doris.filesystem.FileSystem getFileSystem(StorageProperties storageProperties)
            throws IOException {
        return getFileSystem(StoragePropertiesConverter.toMap(storageProperties));
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
     * <p>For broker storage ({@link BrokerProperties}), resolves the live broker host:port via
     * {@code BrokerMgr} and delegates to {@link #getBrokerFileSystem}. For all other storage
     * types (HDFS, S3, etc.), converts via {@link StoragePropertiesConverter} and delegates to
     * {@link #getFileSystem(StorageProperties)}.
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
        StorageProperties sp = brokerDesc.getStorageProperties();
        if (sp instanceof BrokerProperties) {
            BrokerProperties bp = (BrokerProperties) sp;
            try {
                String localIP = FrontendOptions.getLocalHostAddress();
                FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getBroker(bp.getBrokerName(), localIP);
                String clientId = NetUtils.getHostPortInAccessibleFormat(localIP, Config.edit_log_port);
                return getBrokerFileSystem(broker.host, broker.port, clientId, bp.getBrokerParams());
            } catch (AnalysisException | IOException e) {
                throw new UserException("Failed to create broker filesystem for '"
                        + brokerDesc.getName() + "': " + e.getMessage(), e);
            }
        }
        try {
            return getFileSystem(sp);
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
