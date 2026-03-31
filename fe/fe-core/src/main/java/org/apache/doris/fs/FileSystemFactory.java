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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.fs.remote.RemoteFileSystem;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory for filesystem instances.
 *
 * <h2>Two APIs</h2>
 * <ul>
 *   <li><b>Legacy API</b> ({@code get(StorageProperties)} / {@code get(FileSystemType, Map)}) —
 *       delegates to {@link StorageTypeMapper}; returns {@link RemoteFileSystem}. All existing
 *       fe-core callers continue to use this path unchanged.</li>
 *   <li><b>SPI API</b> ({@code getFileSystem(Map)} / {@code getFileSystem(StorageProperties)}) —
 *       delegates to {@link FileSystemPluginManager}; returns
 *       {@code org.apache.doris.filesystem.spi.FileSystem}. New code should use this path.</li>
 * </ul>
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
    // Legacy API — backward compatible, returns RemoteFileSystem
    // =========================================================

    /**
     * Legacy entry point. Returns a {@link RemoteFileSystem} via {@link StorageTypeMapper}.
     *
     * @deprecated New code should use {@link #getFileSystem(Map)} instead.
     */
    @Deprecated
    public static RemoteFileSystem get(StorageProperties storageProperties) {
        return StorageTypeMapper.create(storageProperties);
    }

    /**
     * Legacy entry point by enum type. Returns a {@link RemoteFileSystem} via
     * {@link StorageTypeMapper}.
     *
     * @deprecated New code should use {@link #getFileSystem(Map)} instead.
     */
    @Deprecated
    public static RemoteFileSystem get(FileSystemType fileSystemType, Map<String, String> properties)
            throws UserException {
        List<StorageProperties> storagePropertiesList = StorageProperties.createAll(properties);
        for (StorageProperties storageProperties : storagePropertiesList) {
            if (storageProperties.getStorageName().equalsIgnoreCase(fileSystemType.name())) {
                return StorageTypeMapper.create(storageProperties);
            }
        }
        throw new RuntimeException("Unsupported file system type: " + fileSystemType);
    }

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
     * @return initialized {@code org.apache.doris.filesystem.spi.FileSystem}
     * @throws IOException if no provider matches or creation fails
     */
    public static org.apache.doris.filesystem.spi.FileSystem getFileSystem(Map<String, String> properties)
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
    public static org.apache.doris.filesystem.spi.FileSystem getFileSystem(StorageProperties storageProperties)
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
}
