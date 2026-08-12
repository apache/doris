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

import org.apache.doris.jni.spi.SpiVersion;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
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

    private PluginApiVersions() {
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
