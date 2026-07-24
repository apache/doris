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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.datasource.storage.StorageRegistry;
import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.DirectoryPluginRuntimeManager;
import org.apache.doris.extension.loader.LoadFailure;
import org.apache.doris.extension.loader.LoadReport;
import org.apache.doris.extension.loader.PluginHandle;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
    // Without parent-first for org.apache.doris.foundation.*:
    //   The SPI api signatures reference foundation types (HadoopStorageProperties
    //   .getExecutionAuthenticator() returns foundation ExecutionAuthenticator; providers throw
    //   foundation StoragePropertiesException). Plugin zips bundle their own fe-foundation jar,
    //   so a child-first load produces a second Class object for the same type and FE startup
    //   dies in itable initialization with:
    //   "LinkageError: loader constraint violation ... HadoopStorageProperties ...
    //    different Class objects for the type ExecutionAuthenticator used in the signature"
    //   (and cross-loader catch blocks for StoragePropertiesException would silently stop
    //   matching). Foundation is a shared contract layer by definition — always parent-first.
    private static final List<String> FS_PARENT_FIRST_PREFIXES =
            Arrays.asList("org.apache.doris.filesystem.", "org.apache.doris.foundation.",
                    "software.amazon.awssdk.", "org.apache.hadoop.");

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
                    DatasourcePrintableMap.registerSensitiveKeys(p.sensitivePropertyKeys());
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
            FileSystemProvider provider = handle.getFactory();
            providers.add(provider);
            DatasourcePrintableMap.registerSensitiveKeys(provider.sensitivePropertyKeys());
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
        DatasourcePrintableMap.registerSensitiveKeys(provider.sensitivePropertyKeys());
    }

    /** Returns an unmodifiable view of the loaded providers, in registration order. */
    public List<FileSystemProvider> getProviders() {
        return Collections.unmodifiableList(providers);
    }

    /**
     * Selection priority for raw-user-props binding = {@code StorageRegistry.Provider} declaration
     * order (single source of truth; mirrors the legacy StorageProperties.PROVIDERS registry
     * order, with JFS hoisted ahead of HDFS so a jfs:// uri beats HDFS's key-hint guess).
     */

    private static final String DEPRECATED_OSS_HDFS_SUPPORT = "oss.hdfs.enabled";

    /**
     * Binds the first matching provider for raw user properties, mirroring
     * {@code StorageProperties.createPrimary}: fixed priority, explicit {@code fs.<x>.support}
     * flags first-class, heuristic guesses globally disabled once any explicit flag is present,
     * and no default fallback — throws when nothing matches.
     */
    public FileSystemProperties bindPrimary(Map<String, String> properties) {
        boolean useGuess = !hasAnyExplicitFsSupport(properties);
        Map<String, String> probeView = withProbeContext(properties);
        for (StorageRegistry.Provider meta : StorageRegistry.Provider.values()) {
            FileSystemProvider provider = providerByName(meta.name());
            if (provider == null) {
                continue;
            }
            if (provider.supportsExplicit(properties) || (useGuess && provider.supportsGuess(probeView))) {
                return provider.bind(properties);
            }
        }
        // Providers not in the StorageRegistry.Provider table (out-of-tree plugins) are consulted
        // after every known provider, in registration order — adding a plugin needs no fe-core
        // priority-list change and can never preempt the legacy fourteen.
        for (FileSystemProvider provider : providers) {
            if (StorageRegistry.Provider.byName(provider.name()).isPresent()) {
                continue;
            }
            if (provider.supportsExplicit(properties) || (useGuess && provider.supportsGuess(probeView))) {
                return provider.bind(properties);
            }
        }
        throw new StoragePropertiesException(
                "No supported storage type found. Please check your configuration."
                        + " Loaded filesystem providers: " + providerNames());
    }

    /**
     * Binds every matching provider for raw user properties, mirroring
     * {@code StorageProperties.createAll}: multi-hit is allowed, OSS-HDFS and OSS stay mutually
     * exclusive, and when no explicit flag is present and no HDFS-family provider matched, a
     * default HDFS binding is inserted at index 0 (fe-core's default-HDFS fallback).
     */
    public List<FileSystemProperties> bindAll(Map<String, String> properties) {
        boolean useGuess = !hasAnyExplicitFsSupport(properties);
        Map<String, String> probeView = withProbeContext(properties);
        List<FileSystemProperties> result = new ArrayList<>();
        boolean ossHdfsMatched = false;
        boolean hdfsFamilyMatched = false;
        for (StorageRegistry.Provider meta : StorageRegistry.Provider.values()) {
            FileSystemProvider provider = providerByName(meta.name());
            if (provider == null) {
                continue;
            }
            if (meta == StorageRegistry.Provider.OSS && ossHdfsMatched) {
                continue;
            }
            // JFS and HDFS were ONE typed class in fe-core (jfs rode HdfsProperties), so a map
            // matching both (jfs fs.defaultFS also trips HDFS's key-hint guess) produced a
            // single instance there. The plugin split makes them two providers sharing
            // StorageTypeId.HDFS — bind at most one or the type-keyed catalog map collides
            // ("Duplicate storage type: HDFS", caught by external_table_p0 jfs catalog tests).
            if (meta == StorageRegistry.Provider.HDFS && hdfsFamilyMatched) {
                continue;
            }
            if (provider.supportsExplicit(properties) || (useGuess && provider.supportsGuess(probeView))) {
                result.add(provider.bind(properties));
                if (meta == StorageRegistry.Provider.OSS_HDFS) {
                    ossHdfsMatched = true;
                }
                // fe-core adds its default-HDFS fallback only when no HdfsProperties instance is
                // in the result; jfs rode HdfsProperties there, so JFS counts as HDFS here.
                // OSS_HDFS does NOT: fe-core's OSSHdfsProperties is not an HdfsProperties, and
                // createAll happily prepends the HDFS fallback next to it.
                if (meta == StorageRegistry.Provider.HDFS || meta == StorageRegistry.Provider.JFS) {
                    hdfsFamilyMatched = true;
                }
            }
        }
        // Unlisted (out-of-tree) providers append after the known set, registration order.
        for (FileSystemProvider provider : providers) {
            if (StorageRegistry.Provider.byName(provider.name()).isPresent()) {
                continue;
            }
            if (provider.supportsExplicit(properties) || (useGuess && provider.supportsGuess(probeView))) {
                result.add(provider.bind(properties));
            }
        }
        if (useGuess && !hdfsFamilyMatched) {
            FileSystemProvider hdfs = providerByName("HDFS");
            if (hdfs != null) {
                result.add(0, hdfs.bind(properties));
            }
        }
        return result;
    }

    /**
     * Probe-context key carrying {@code Config.azure_blob_host_suffixes} into provider guess
     * probes. Providers cannot see fe-core Config; routing runs here in fe-core, so the live
     * (admin-extensible) suffix list is injected into a COPY used only for
     * {@code supportsGuess} probes — the map handed to {@code bind()} stays untouched, so the
     * marker can never leak into bound properties, backend maps, or persisted state.
     */
    public static final String AZURE_HOST_SUFFIXES_PROBE_KEY = "_AZURE_HOST_SUFFIXES_";

    /** Builds the guess-probe view of the raw properties (see {@link #AZURE_HOST_SUFFIXES_PROBE_KEY}). */
    public static Map<String, String> withProbeContext(Map<String, String> properties) {
        String[] suffixes = Config.azure_blob_host_suffixes;
        if (suffixes == null || suffixes.length == 0) {
            return properties;
        }
        Map<String, String> probeView = new HashMap<>(properties);
        probeView.put(AZURE_HOST_SUFFIXES_PROBE_KEY, String.join(",", suffixes));
        return probeView;
    }

    private FileSystemProvider providerByName(String name) {
        for (FileSystemProvider p : providers) {
            if (name.equalsIgnoreCase(p.name())) {
                return p;
            }
        }
        return null;
    }

    /**
     * True when the user explicitly declared any filesystem via {@code fs.<x>.support=true} or the
     * legacy {@code oss.hdfs.enabled=true}. Counterpart of
     * {@code StorageProperties.hasAnyExplicitFsSupport}; checked structurally so this class never
     * needs the full dialect list.
     */
    private static boolean hasAnyExplicitFsSupport(Map<String, String> properties) {
        if ("true".equalsIgnoreCase(properties.getOrDefault(DEPRECATED_OSS_HDFS_SUPPORT, "false"))) {
            return true;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key != null && key.startsWith("fs.") && key.endsWith(".support")
                    && "true".equalsIgnoreCase(entry.getValue())) {
                return true;
            }
        }
        return false;
    }

    private String providerNames() {
        List<String> names = new ArrayList<>();
        for (FileSystemProvider p : providers) {
            names.add(p.name());
        }
        return names.toString();
    }
}
