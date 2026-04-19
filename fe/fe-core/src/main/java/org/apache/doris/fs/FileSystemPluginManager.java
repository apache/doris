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

import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadFailure;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages lifecycle of FileSystemProvider plugins.
 *
 * <p>Discovery order:
 * 1. ServiceLoader scan (classpath-based built-ins / test overrides)
 * 2. DirectoryPluginRuntimeManager scan (production plugin directories)
 *
 * <p>The first provider that returns {@code supports(props) == true} is used.
 * Classpath providers have higher priority than directory-loaded providers.
 */
public class FileSystemPluginManager {

    private static final Logger LOG = LogManager.getLogger(FileSystemPluginManager.class);

    // Hadoop and AWS SDK classes must be parent-first so that all instances of
    // shared interfaces/classes are loaded by a single ClassLoader.
    //
    // Without parent-first for org.apache.hadoop.*:
    //   The plugin's child-first loader loads its own copy of RpcEngine (an interface
    //   in hadoop-common), while ProtobufRpcEngine2 (which implements RpcEngine) is
    //   already loaded by the parent app ClassLoader. A cross-ClassLoader cast then
    //   fails with: "ProtobufRpcEngine2 cannot be cast to RpcEngine".
    //
    // Without parent-first for software.amazon.awssdk.*:
    //   The plugin's child-first loader loads its own awssdk:core copy, while the
    //   FE core ClassLoader's s3-transfer-manager SPI registers
    //   ApplyUserAgentInterceptor against the parent's ExecutionInterceptor.
    //   The resulting cross-ClassLoader isAssignableFrom check fails with:
    //   "ApplyUserAgentInterceptor does not implement ExecutionInterceptor API"
    private static final List<String> FS_PARENT_FIRST_PREFIXES =
            Arrays.asList("org.apache.doris.filesystem.", "software.amazon.awssdk.", "org.apache.hadoop.");

    private final List<FileSystemProvider> providers = new CopyOnWriteArrayList<>();
    private final DirectoryPluginRuntimeManager<FileSystemProvider> runtimeManager =
            new DirectoryPluginRuntimeManager<>();
    private final ClassLoadingPolicy classLoadingPolicy =
            new ClassLoadingPolicy(FS_PARENT_FIRST_PREFIXES);

    /** Called at FE startup to load built-in providers from classpath. */
    public void loadBuiltins() {
        ServiceLoader.load(FileSystemProvider.class)
                .forEach(p -> {
                    providers.add(p);
                    LOG.info("Registered built-in filesystem provider: {}", p.name());
                });
    }

    /**
     * Loads filesystem provider plugins from plugin root directories.
     * Failures are logged as warnings; partial success is allowed.
     *
     * @param pluginRoots directories to scan for filesystem plugin subdirectories
     */
    public void loadPlugins(List<Path> pluginRoots) {
        LoadReport<FileSystemProvider> report = runtimeManager.loadAll(
                pluginRoots,
                FileSystemPluginManager.class.getClassLoader(),
                FileSystemProvider.class,
                classLoadingPolicy);

        LOG.info("Filesystem plugin load summary: rootsScanned={}, dirsScanned={}, "
                        + "successCount={}, failureCount={}",
                report.getRootsScanned(), report.getDirsScanned(),
                report.getSuccesses().size(), report.getFailures().size());

        for (LoadFailure failure : report.getFailures()) {
            LOG.warn("Filesystem plugin load failure: dir={}, stage={}, message={}, cause={}",
                    failure.getPluginDir(), failure.getStage(), failure.getMessage(),
                    failure.getCause());
        }

        for (PluginHandle<FileSystemProvider> handle : report.getSuccesses()) {
            providers.add(handle.getFactory());
            LOG.info("Loaded filesystem plugin: name={}, pluginDir={}, jarCount={}",
                    handle.getPluginName(), handle.getPluginDir(),
                    handle.getResolvedJars().size());
        }
    }

    /**
     * Creates a FileSystem for the given properties by selecting the first supporting provider.
     *
     * @throws IOException if no provider supports the properties, or creation fails
     */
    public FileSystem createFileSystem(Map<String, String> properties) throws IOException {
        for (FileSystemProvider provider : providers) {
            if (provider.supports(properties)) {
                return provider.create(properties);
            }
        }
        throw new IOException("No FileSystemProvider supports the given properties: "
                + properties.get("_STORAGE_TYPE_") + ". Registered providers: " + providerNames());
    }

    /** Registers a provider at highest priority. For testing overrides. */
    public void registerProvider(FileSystemProvider provider) {
        providers.add(0, provider);
    }

    private String providerNames() {
        List<String> names = new ArrayList<>();
        for (FileSystemProvider p : providers) {
            names.add(p.name());
        }
        return names.toString();
    }
}
