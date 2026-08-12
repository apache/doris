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

package org.apache.doris.utframe;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Writes a plugin jar the way a release really lays one out, for tests that need the FE to load a plugin
 * the way it loads a third-party one instead of finding it on the test class path.
 *
 * <p>The difference matters more than it looks. A class on the test class path is loaded by the test's own
 * classloader from a directory of {@code .class} files, so it has no jar and therefore no MANIFEST: the
 * plugin API version it declares cannot be read, and every type it exchanges with the engine is already the
 * engine's own. A plugin loaded from here has neither of those properties, which is the whole of what the
 * loader has to get right.
 *
 * <p>Layout written, one plugin per subdirectory of the root:
 *
 * <pre>
 * &lt;root&gt;/&lt;pluginDirName&gt;/&lt;pluginDirName&gt;.jar
 *      META-INF/MANIFEST.MF                  # the declared plugin API version
 *      META-INF/services/&lt;spi interface&gt;     # naming the factory class
 *      &lt;factory class&gt;.class
 *      &lt;each of alsoBundled&gt;.class
 * </pre>
 */
public final class PluginJarWriter {

    private PluginJarWriter() {
    }

    /**
     * Writes one plugin into {@code root} and returns {@code root}, which is what a plugin directory
     * configuration names.
     *
     * @param pluginDirName the subdirectory, and the jar's base name. Deliberately not required to equal the
     *         plugin's name: the loader takes the name from the factory, and a test passing a different one
     *         here is what keeps that true.
     * @param manifestAttribute the family's plugin API version attribute, e.g.
     *         {@code Doris-Authorization-Plugin-Api-Version}
     * @param declaredApiVersion the version to declare, or null to omit the attribute entirely - which is
     *         what a plugin written with no awareness of the contract looks like
     * @param alsoBundled further classes whose bytes go into the jar: the rest of the plugin, or a copy of
     *         something the FE also has, to test which copy wins
     */
    public static Path writePluginRoot(Path root, String pluginDirName, Class<?> factoryClass,
            Class<?> spiInterface, String manifestAttribute, String declaredApiVersion,
            List<Class<?>> alsoBundled) throws IOException {
        Path jarPath = root.resolve(pluginDirName).resolve(pluginDirName + ".jar");
        Files.createDirectories(jarPath.getParent());

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (declaredApiVersion != null) {
            manifest.getMainAttributes().putValue(manifestAttribute, declaredApiVersion);
        }
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
            writeClassEntry(jar, factoryClass);
            for (Class<?> bundled : alsoBundled) {
                writeClassEntry(jar, bundled);
            }
            jar.putNextEntry(new JarEntry("META-INF/services/" + spiInterface.getName()));
            jar.write((factoryClass.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
        return root;
    }

    private static void writeClassEntry(JarOutputStream jar, Class<?> clazz) throws IOException {
        String classEntry = clazz.getName().replace('.', '/') + ".class";
        jar.putNextEntry(new JarEntry(classEntry));
        try (InputStream classBytes = clazz.getClassLoader().getResourceAsStream(classEntry)) {
            if (classBytes == null) {
                // Silently writing an empty entry would produce a jar whose plugin fails to load for a
                // reason that has nothing to do with what the test is checking.
                throw new IllegalStateException("class bytes not found for " + clazz.getName()
                        + "; a nested or synthetic class has to be bundled by name too");
            }
            byte[] buffer = new byte[8192];
            int read;
            while ((read = classBytes.read(buffer)) != -1) {
                jar.write(buffer, 0, read);
            }
        }
        jar.closeEntry();
    }
}
