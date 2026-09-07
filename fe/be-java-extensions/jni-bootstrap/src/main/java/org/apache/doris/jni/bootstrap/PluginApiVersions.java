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
import org.apache.doris.jni.spi.SpiVersion;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.List;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Reads the plugin API version a plugin was built against, from the manifest of the jar that
 * defines its plugin class.
 *
 * <p>It has to come from the jar, not from the plugin object. A method on the SPI - even one the
 * plugin overrides - is compiled against BE's copy of the SPI and executes inside BE's own class,
 * so it reports what BE believes and can never disagree. The manifest attribute is written when the
 * plugin is built and travels with the artifact, which is the only thing that makes the comparison
 * meaningful. The FE plugin loader reached the same conclusion independently; see
 * {@code org.apache.doris.extension.loader.ApiVersionGate}.
 *
 * <p>The jar is located through the class's code source rather than {@code Package}: package
 * metadata is fixed per package name and classloader by whichever jar defines it first, so two
 * plugins sharing a package prefix would report each other's version.
 */
final class PluginApiVersions {

    private static final String SERVICE_ENTRY = "META-INF/services/" + DorisPlugin.class.getName();

    private PluginApiVersions() {
    }

    /**
     * What the jars of a plugin directory declare, read before any of their classes is loaded.
     * A plain class rather than a record: this module compiles at source level 11, so that a
     * plugin author on an older JDK can still build against the loader.
     */
    static final class Declared {
        /**
         * Whether any jar declares the {@link DorisPlugin} service at all. When false there is
         * nothing to gate, and the caller lets the far better "nothing declares what this plugin
         * provides" message win.
         */
        private final boolean providerJarFound;
        /** The declaring jar's manifest attribute, or null when it carries none. */
        private final String apiVersion;

        private Declared(boolean providerJarFound, String apiVersion) {
            this.providerJarFound = providerJarFound;
            this.apiVersion = apiVersion;
        }

        boolean providerJarFound() {
            return providerJarFound;
        }

        String apiVersion() {
            return apiVersion;
        }
    }

    /**
     * The declared version of the jar that declares the {@link DorisPlugin} service, read straight
     * out of its manifest.
     *
     * <p>This exists so the version gate can run <em>before</em> the plugin is instantiated. Going
     * through the loaded plugin class instead means ServiceLoader has already constructed it and
     * its factory getters have already returned SPI types - which is exactly what a mismatched SPI
     * major breaks, as a LinkageError with none of the wording the gate produces. Reading a zip
     * entry costs nothing and loads no class.
     *
     * <p>When several jars declare the service the first in the directory's fixed order wins here;
     * that case is rejected properly a moment later, by the sole-provider check.
     */
    static Declared declaredByProviderJar(List<URL> jars) throws IOException {
        for (URL jar : jars) {
            Path path = pathOf(jar);
            if (path == null) {
                continue;
            }
            try (JarFile jarFile = new JarFile(path.toFile())) {
                if (jarFile.getEntry(SERVICE_ENTRY) == null) {
                    continue;
                }
                Manifest manifest = jarFile.getManifest();
                return new Declared(true, manifest == null
                        ? null : manifest.getMainAttributes().getValue(SpiVersion.MANIFEST_ATTRIBUTE));
            }
        }
        return new Declared(false, null);
    }

    private static Path pathOf(URL jar) {
        try {
            return Paths.get(jar.toURI());
        } catch (URISyntaxException | RuntimeException e) {
            return null;
        }
    }

    /**
     * @return the declared version, or null when the class did not come from a jar file or the jar
     *         carries no such attribute - both of which the caller must treat as "unknown", never
     *         as "compatible"
     */
    static String declaredBy(Class<?> pluginClass) throws IOException {
        Path jar = jarOf(pluginClass);
        if (jar == null) {
            return null;
        }
        try (JarFile jarFile = new JarFile(jar.toFile())) {
            Manifest manifest = jarFile.getManifest();
            if (manifest == null) {
                return null;
            }
            // Main attributes only: the version describes the artifact, and <manifestEntries>
            // writes there.
            return manifest.getMainAttributes().getValue(SpiVersion.MANIFEST_ATTRIBUTE);
        }
    }

    private static Path jarOf(Class<?> clazz) {
        try {
            CodeSource codeSource = clazz.getProtectionDomain().getCodeSource();
            if (codeSource == null || codeSource.getLocation() == null) {
                return null;
            }
            Path path = Paths.get(codeSource.getLocation().toURI());
            return Files.isRegularFile(path) ? path : null;
        } catch (URISyntaxException | RuntimeException e) {
            return null;
        }
    }
}
