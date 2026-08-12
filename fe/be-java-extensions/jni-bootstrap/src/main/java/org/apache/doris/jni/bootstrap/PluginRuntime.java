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

package org.apache.doris.jni.bootstrap;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.JniWriterFactory;
import org.apache.doris.jni.spi.SpiVersion;
import org.apache.doris.jni.spi.ThreadContextClassLoader;
import org.apache.doris.jni.spi.UdfExecutorFactory;
import org.apache.doris.jni.spi.utils.JniUtil;

import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loads plugins from a directory and hands BE the objects it asks for.
 *
 * <p>Loading is lazy and happens at most once per plugin: the first request for a plugin builds its
 * classloader, runs {@link ServiceLoader} and indexes the factories, and every later request reads
 * the cached {@link PluginHandle} - including a cached failure. Nothing is eagerly resolved beyond
 * the plugin object and its factories, so a class missing from a plugin's jars is reported when
 * something touches it, with the name of the class, instead of taking the process down at startup.
 *
 * <p>All plugin code runs with the plugin's classloader installed as the thread context
 * classloader. BE's threads have no meaningful one, and ServiceLoader and most plugin libraries
 * consult it.
 */
final class PluginRuntime {

    private static final Logger LOG = Logger.getLogger(PluginRuntime.class.getName());

    private final Path pluginDir;
    private final ClassLoader spiClassLoader;
    private final ConcurrentHashMap<String, PluginHandle> plugins = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> loadLocks = new ConcurrentHashMap<>();

    PluginRuntime(Path pluginDir, ClassLoader spiClassLoader) {
        this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir");
        this.spiClassLoader = Objects.requireNonNull(spiClassLoader, "spiClassLoader");
    }

    // ------------------------------------------------------------------
    // What BE calls.
    // ------------------------------------------------------------------

    /**
     * Creates a scanner or a writer. Factory names are unique within a plugin, so one entry point
     * serves both and BE needs a single method id.
     */
    Object createInstance(String pluginName, String factoryName, int batchSize, Map<String, String> params) {
        PluginHandle handle = usable(pluginName);
        JniScannerFactory scannerFactory = handle.scannerFactories().get(factoryName);
        JniWriterFactory writerFactory = handle.writerFactories().get(factoryName);
        if (scannerFactory == null && writerFactory == null) {
            throw new IllegalArgumentException(unknownFactoryMessage(handle, factoryName));
        }
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(handle.classLoader())) {
            Object instance = scannerFactory != null
                    ? scannerFactory.create(batchSize, params)
                    : writerFactory.create(batchSize, params);
            if (instance == null) {
                throw new IllegalStateException("Java plugin '" + pluginName + "' factory '" + factoryName
                        + "' returned null");
            }
            return instance;
        }
    }

    /**
     * Creates a UDF executor. The parameters stay a byte array all the way into the plugin: thrift
     * is not on the SPI boundary, so only the plugin can decode them.
     */
    Object createUdfExecutor(String pluginName, String factoryName, byte[] thriftParams) throws Exception {
        PluginHandle handle = usable(pluginName);
        UdfExecutorFactory factory = handle.udfExecutorFactories().get(factoryName);
        if (factory == null) {
            throw new IllegalArgumentException(unknownFactoryMessage(handle, factoryName));
        }
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(handle.classLoader())) {
            Object executor = factory.create(thriftParams);
            if (executor == null) {
                throw new IllegalStateException("Java plugin '" + pluginName + "' UDF factory '" + factoryName
                        + "' returned null");
            }
            return executor;
        }
    }

    /**
     * Forwards DROP FUNCTION to whichever plugin caches compiled user functions. Broadcast rather
     * than addressed: BE drops a function without knowing which plugin executed it, and a plugin
     * that never cached the signature does nothing.
     */
    void cleanUdfCache(String functionSignature) {
        for (PluginHandle handle : plugins.values()) {
            if (handle.state() != PluginHandle.State.READY) {
                continue;
            }
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(handle.classLoader())) {
                for (UdfExecutorFactory factory : handle.udfExecutorFactories().values()) {
                    factory.invalidate(functionSignature);
                }
            } catch (Throwable t) {
                // One plugin failing to drop its cache must not stop the others, and DROP FUNCTION
                // has already been committed on the FE side by now.
                LOG.log(Level.WARNING, "Java plugin '" + handle.name() + "' failed to invalidate '"
                        + functionSignature + "': " + JniUtil.throwableToString(t));
            }
        }
    }

    /** Everything the registry knows, for BE to expose. Only reports plugins already touched. */
    String statusJson() {
        StringBuilder json = new StringBuilder(256).append("{\"pluginDir\":");
        appendJsonString(json, pluginDir.toString());
        json.append(",\"apiVersion\":");
        appendJsonString(json, SpiVersion.version());
        json.append(",\"plugins\":[");
        boolean first = true;
        for (PluginHandle handle : new TreeMap<>(plugins).values()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append("{\"name\":");
            appendJsonString(json, handle.name());
            json.append(",\"state\":\"").append(handle.state()).append('"');
            if (handle.declaredApiVersion() != null) {
                json.append(",\"declaredApiVersion\":");
                appendJsonString(json, handle.declaredApiVersion());
            }
            if (handle.failure() != null) {
                json.append(",\"error\":");
                appendJsonString(json, handle.failure());
            }
            appendJsonNames(json, "scanners", handle.scannerFactories().keySet());
            appendJsonNames(json, "writers", handle.writerFactories().keySet());
            appendJsonNames(json, "udfExecutors", handle.udfExecutorFactories().keySet());
            json.append('}');
        }
        return json.append("]}").toString();
    }

    /**
     * Loads every deployed plugin, so that a broken deployment is reported at startup rather than
     * by the first user query. Never throws: each plugin's failure is already contained in its
     * handle.
     */
    void warmup() {
        for (String name : deployedPluginNames()) {
            plugin(name);
        }
    }

    // ------------------------------------------------------------------
    // Loading.
    // ------------------------------------------------------------------

    PluginHandle plugin(String name) {
        PluginHandle cached = plugins.get(name);
        if (cached != null) {
            return cached;
        }
        // Per-plugin lock: loading takes seconds, and one slow plugin must not delay the first
        // query against an unrelated one. Loading outside computeIfAbsent on purpose - it does I/O
        // and classloading, which must not run while a ConcurrentHashMap bin lock is held.
        synchronized (loadLocks.computeIfAbsent(name, key -> new Object())) {
            PluginHandle handle = plugins.get(name);
            if (handle == null) {
                handle = load(name);
                plugins.put(name, handle);
            }
            return handle;
        }
    }

    private PluginHandle usable(String name) {
        PluginHandle handle = plugin(name);
        switch (handle.state()) {
            case READY:
                return handle;
            case NOT_DEPLOYED:
                throw new IllegalStateException("Java plugin '" + name + "' is not deployed: no directory "
                        + pluginDir.resolve(name) + ". Install it, or build BE without excluding it.");
            case FAILED:
            default:
                throw new IllegalStateException("Java plugin '" + name + "' failed to load: " + handle.failure());
        }
    }

    private PluginHandle load(String name) {
        Path dir = pluginDir.resolve(name);
        if (!Files.isDirectory(dir)) {
            LOG.fine(() -> "Java plugin '" + name + "' is not deployed at " + dir);
            return PluginHandle.notDeployed(name);
        }
        try {
            return loadDeployed(name, dir);
        } catch (Throwable t) {
            // Deliberately Throwable: a plugin jar built against another JDK or missing a
            // transitive dependency surfaces as a LinkageError, and containing that is the whole
            // point of loading plugins one at a time.
            String failure = JniUtil.throwableToString(t);
            LOG.log(Level.WARNING, "Java plugin '" + name + "' failed to load from " + dir + ": "
                    + JniUtil.throwableToStackTrace(t));
            return PluginHandle.failed(name, failure);
        }
    }

    private PluginHandle loadDeployed(String name, Path dir) throws IOException {
        List<URL> jars = jarsIn(dir);
        if (jars.isEmpty()) {
            throw new IllegalStateException("no jars in " + dir);
        }

        DorisPluginClassLoader classLoader = new DorisPluginClassLoader(name, jars, spiClassLoader);
        if (classLoader.packagesSpiClasses()) {
            throw new IllegalStateException("the plugin packages the SPI itself. jni-spi must be declared"
                    + " with <scope>provided</scope>; a bundled copy produces classes BE cannot exchange"
                    + " with, and every hand-off across the boundary would fail as a ClassCastException"
                    + " between two identically named types");
        }

        DorisPlugin plugin;
        List<JniScannerFactory> scannerFactories;
        List<JniWriterFactory> writerFactories;
        List<UdfExecutorFactory> udfExecutorFactories;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            plugin = soleProvider(name, classLoader);
            scannerFactories = copyOf(plugin.getScannerFactories());
            writerFactories = copyOf(plugin.getWriterFactories());
            udfExecutorFactories = copyOf(plugin.getUdfExecutorFactories());
        }

        String declaredApiVersion = PluginApiVersions.declaredBy(plugin.getClass());
        checkApiVersion(declaredApiVersion);

        Map<String, JniScannerFactory> scanners = new LinkedHashMap<>();
        Map<String, JniWriterFactory> writers = new LinkedHashMap<>();
        Map<String, UdfExecutorFactory> udfExecutors = new LinkedHashMap<>();
        for (JniScannerFactory factory : scannerFactories) {
            index(scanners, factory.getName(), factory, "scanner", writers.keySet(), udfExecutors.keySet());
        }
        for (JniWriterFactory factory : writerFactories) {
            index(writers, factory.getName(), factory, "writer", scanners.keySet(), udfExecutors.keySet());
        }
        for (UdfExecutorFactory factory : udfExecutorFactories) {
            index(udfExecutors, factory.getName(), factory, "UDF executor", scanners.keySet(), writers.keySet());
        }

        LOG.info("Java plugin '" + name + "' loaded from " + dir + " (api " + declaredApiVersion
                + ", scanners=" + scanners.keySet() + ", writers=" + writers.keySet()
                + ", udfExecutors=" + udfExecutors.keySet() + ")");
        return PluginHandle.ready(name, declaredApiVersion, classLoader, scanners, writers, udfExecutors);
    }

    private DorisPlugin soleProvider(String name, DorisPluginClassLoader classLoader) {
        Iterator<DorisPlugin> providers = ServiceLoader.load(DorisPlugin.class, classLoader).iterator();
        if (!providers.hasNext()) {
            throw new IllegalStateException("no META-INF/services/" + DorisPlugin.class.getName()
                    + " entry in any of its jars, so nothing declares what this plugin provides");
        }
        DorisPlugin plugin = providers.next();
        if (providers.hasNext()) {
            throw new IllegalStateException("more than one " + DorisPlugin.class.getSimpleName()
                    + " provider (" + plugin.getClass().getName() + ", " + providers.next().getClass().getName()
                    + ", ...). A plugin directory addresses exactly one plugin; split them into separate"
                    + " directories under " + pluginDir);
        }
        // The directory name is the plugin's identity, so nothing here has to agree with it.
        LOG.fine(() -> "Java plugin '" + name + "' provided by " + plugin.getClass().getName());
        return plugin;
    }

    private void checkApiVersion(String declaredApiVersion) {
        if (declaredApiVersion == null) {
            throw new IllegalStateException("its jar declares no " + SpiVersion.MANIFEST_ATTRIBUTE
                    + " manifest attribute, so there is no way to tell which BE it was built for."
                    + " Rebuild it with this Doris version's build");
        }
        int declaredMajor = SpiVersion.majorOf(declaredApiVersion);
        if (declaredMajor != SpiVersion.major()) {
            throw new IllegalStateException("it was built against plugin API " + declaredApiVersion
                    + " but this BE serves " + SpiVersion.version()
                    + ". Majors must match; deploy the plugin directory that ships with this BE");
        }
    }

    private static <T> void index(Map<String, T> target, String factoryName, T factory, String kind,
            Iterable<String> otherKindA, Iterable<String> otherKindB) {
        if (factoryName == null || factoryName.trim().isEmpty()) {
            throw new IllegalStateException(kind + " factory " + factory.getClass().getName()
                    + " has a blank name; BE addresses factories by name");
        }
        // One namespace across all three kinds: BE sends a plugin name and a factory name, and a
        // key that could mean two things would resolve by whichever kind is looked up first.
        if (target.containsKey(factoryName) || contains(otherKindA, factoryName)
                || contains(otherKindB, factoryName)) {
            throw new IllegalStateException("two factories are both named '" + factoryName
                    + "'; names must be unique within a plugin");
        }
        target.put(factoryName, factory);
    }

    private static boolean contains(Iterable<String> names, String name) {
        for (String candidate : names) {
            if (candidate.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static <T> List<T> copyOf(Iterable<T> values) {
        List<T> copy = new ArrayList<>();
        if (values != null) {
            for (T value : values) {
                if (value != null) {
                    copy.add(value);
                }
            }
        }
        return copy;
    }

    private List<String> deployedPluginNames() {
        List<String> names = new ArrayList<>();
        if (!Files.isDirectory(pluginDir)) {
            LOG.info("No Java plugin directory at " + pluginDir + "; BE runs without Java plugins");
            return names;
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(pluginDir)) {
            for (Path entry : entries) {
                if (Files.isDirectory(entry)) {
                    names.add(entry.getFileName().toString());
                }
            }
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Cannot list Java plugin directory " + pluginDir + ": " + e);
        }
        names.sort(String::compareTo);
        return names;
    }

    private static List<URL> jarsIn(Path dir) throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir, "*.jar")) {
            for (Path entry : entries) {
                files.add(entry);
            }
        }
        // Deterministic order so that a duplicate class resolves the same way on every node.
        files.sort(Path::compareTo);
        List<URL> urls = new ArrayList<>(files.size());
        for (Path file : files) {
            urls.add(file.toUri().toURL());
        }
        return urls;
    }

    private static String unknownFactoryMessage(PluginHandle handle, String factoryName) {
        return "Java plugin '" + handle.name() + "' has no factory named '" + factoryName
                + "'. It provides scanners " + handle.scannerFactories().keySet()
                + ", writers " + handle.writerFactories().keySet()
                + ", UDF executors " + handle.udfExecutorFactories().keySet();
    }

    private static void appendJsonNames(StringBuilder json, String field, Iterable<String> names) {
        json.append(",\"").append(field).append("\":[");
        boolean first = true;
        for (String name : names) {
            if (!first) {
                json.append(',');
            }
            first = false;
            appendJsonString(json, name);
        }
        json.append(']');
    }

    private static void appendJsonString(StringBuilder json, String value) {
        json.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    json.append("\\\"");
                    break;
                case '\\':
                    json.append("\\\\");
                    break;
                case '\n':
                    json.append("\\n");
                    break;
                case '\r':
                    json.append("\\r");
                    break;
                case '\t':
                    json.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        json.append(String.format("\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
            }
        }
        json.append('"');
    }
}
