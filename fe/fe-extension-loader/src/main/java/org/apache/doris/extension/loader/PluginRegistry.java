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

package org.apache.doris.extension.loader;

import org.apache.doris.extension.spi.PluginFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.JarFile;

/**
 * Process-wide registry of loaded plugins across all plugin families
 * (filesystem, connector, authentication, lineage, ...).
 *
 * <p>This is the single fact source behind {@code information_schema.plugins}.
 * It only stores load-time snapshots (plain strings); listing the registry
 * never executes plugin code.
 *
 * <p>Rules:
 * <ul>
 *   <li>Primary key is {@code (type, name)}. The first registration wins;
 *       later registrations with the same key are rejected, never silently
 *       overridden. Family managers register built-ins before external plugins,
 *       so a directory jar can never displace a built-in row.</li>
 *   <li>Rows only exist for successfully loaded plugins. Load failures are
 *       reported via logs by the loading side and never enter the registry.</li>
 * </ul>
 */
public final class PluginRegistry {

    /** Where a plugin was loaded from. */
    public enum PluginSource {
        /** Bundled on the FE classpath, discovered via ServiceLoader. */
        BUILTIN,
        /** Loaded from a plugin directory jar. */
        EXTERNAL
    }

    /** Immutable load-time snapshot of one plugin. */
    public static final class PluginRecord {
        private final String type;
        private final String name;
        private final String version;
        private final String description;
        private final PluginSource source;
        private final Instant loadTime;

        public PluginRecord(String type, String name, String version, String description,
                PluginSource source, Instant loadTime) {
            this.type = Objects.requireNonNull(type, "plugin record requires a non-null family type");
            this.name = Objects.requireNonNull(name, "plugin record requires a non-null plugin name");
            this.version = version;
            this.description = description == null ? "" : description;
            this.source = Objects.requireNonNull(source, "plugin record requires a non-null source");
            this.loadTime = Objects.requireNonNull(loadTime, "plugin record requires a non-null load time");
        }

        public String getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        /** Plugin release version from jar MANIFEST Implementation-Version, may be null. */
        public String getVersion() {
            return version;
        }

        public String getDescription() {
            return description;
        }

        public PluginSource getSource() {
            return source;
        }

        public Instant getLoadTime() {
            return loadTime;
        }
    }

    private static final Logger LOG = LogManager.getLogger(PluginRegistry.class);

    private static final PluginRegistry INSTANCE = new PluginRegistry();

    private final ConcurrentMap<String, PluginRecord> recordsByKey = new ConcurrentHashMap<>();

    private PluginRegistry() {
    }

    public static PluginRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Registers a load-time snapshot for one plugin.
     *
     * <p>Rejects (returns false) when the name is invalid or the
     * {@code (type, name)} key is already taken. Rejection never throws so a
     * bad self-reported name cannot break FE startup paths that register
     * built-ins; callers decide how to react.
     *
     * @param type plugin family label, e.g. "FILESYSTEM", "AUTHENTICATION"
     * @param name plugin identity within the family
     * @param version plugin release version, may be null when unknown
     * @param description one-line description, may be null
     * @param source BUILTIN or EXTERNAL
     * @return true if the record was inserted, false if rejected
     */
    public boolean register(String type, String name, String version, String description, PluginSource source) {
        if (type == null || type.trim().isEmpty()) {
            LOG.warn("Reject plugin registration with null or blank family type: name={}", name);
            return false;
        }
        String validationError = PluginNames.validate(name);
        if (validationError != null) {
            LOG.warn("Reject plugin registration with invalid name: type={}, name={}, reason={}",
                    type, name, validationError);
            return false;
        }
        PluginRecord record = new PluginRecord(type, name.trim(), version, description, source, Instant.now());
        PluginRecord existing = recordsByKey.putIfAbsent(key(record.getType(), record.getName()), record);
        if (existing != null) {
            // The same plugin may be legitimately loaded by more than one manager
            // instance within a family (e.g. authentication); keep the first row.
            LOG.warn("Skip duplicated plugin registration: type={}, name={}, existingSource={}, newSource={}",
                    record.getType(), record.getName(), existing.getSource(), source);
            return false;
        }
        LOG.info("Registered plugin: type={}, name={}, version={}, source={}",
                record.getType(), record.getName(), version, source);
        return true;
    }

    /**
     * Registers a classpath built-in factory. Snapshots {@code name()} and
     * {@code description()} now; the version comes from the Implementation-Version
     * of the jar that bundles the factory class, when available.
     */
    public boolean registerBuiltin(String type, PluginFactory factory) {
        return register(type, factory.name(), implementationVersionOf(factory.getClass()),
                factory.description(), PluginSource.BUILTIN);
    }

    /** Registers a directory-loaded plugin from its load-time handle snapshot. */
    public boolean registerExternal(String type, PluginHandle<?> handle) {
        return register(type, handle.getPluginName(), handle.getVersion(),
                handle.getDescription(), PluginSource.EXTERNAL);
    }

    /** Returns a snapshot of all registered plugins sorted by (type, name). */
    public List<PluginRecord> list() {
        List<PluginRecord> records = new ArrayList<>(recordsByKey.values());
        records.sort(Comparator.comparing(PluginRecord::getType).thenComparing(PluginRecord::getName));
        return records;
    }

    /** Removes all records. Only for tests. */
    public void clearForTest() {
        recordsByKey.clear();
    }

    /**
     * Best-effort release version of a classpath (built-in) plugin, from the
     * Implementation-Version of the jar that defined the given class.
     *
     * <p>Reads the defining jar's manifest directly instead of
     * {@code Package.getImplementationVersion()}: Package metadata is fixed by
     * whichever jar defines the package first on a classloader, so with two
     * same-package factories from different jars the later plugin would report
     * the earlier jar's version. Falls back to Package metadata only when the
     * defining jar cannot be determined (e.g. classes directory in tests).
     *
     * @return the version, or null when unavailable
     */
    public static String implementationVersionOf(Class<?> clazz) {
        Path definingJar = ManifestVersions.jarOf(clazz);
        if (definingJar != null) {
            try (JarFile jarFile = new JarFile(definingJar.toFile())) {
                return ManifestVersions.fromManifest(jarFile, ManifestVersions.packagePathOf(clazz));
            } catch (IOException e) {
                return null;
            }
        }
        Package pkg = clazz.getPackage();
        return pkg == null ? null : pkg.getImplementationVersion();
    }

    private static String key(String type, String name) {
        return type + " " + name;
    }
}
