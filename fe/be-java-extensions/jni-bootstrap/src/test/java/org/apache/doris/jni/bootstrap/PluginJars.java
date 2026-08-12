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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Builds real plugin directories for the tests: a jar with a manifest, a service descriptor and
 * class bytes copied out of the test classpath.
 *
 * <p>Assembling actual jars rather than pointing the loader at {@code target/test-classes} is what
 * makes the isolation tests real. The classes end up reachable only through the plugin's own URLs,
 * so the loader has to produce its own copy, and the manifest carries the API version the same way
 * a built plugin does.
 */
final class PluginJars {

    private PluginJars() {
    }

    /** A plugin directory that loads: one jar, correct manifest, one service provider. */
    static Path deploy(Path pluginsDir, String pluginName, Class<?> pluginClass, Class<?>... alsoInclude)
            throws IOException {
        return deploy(pluginsDir, pluginName, pluginClass.getName(), SpiVersion.version(),
                classes(pluginClass, alsoInclude));
    }

    /** Full control, for the malformed cases. */
    static Path deploy(Path pluginsDir, String pluginName, String serviceImplName, String apiVersion,
            Class<?>[] classes) throws IOException {
        Path dir = Files.createDirectories(pluginsDir.resolve(pluginName));
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (apiVersion != null) {
            manifest.getMainAttributes().putValue(SpiVersion.MANIFEST_ATTRIBUTE, apiVersion);
        }
        try (JarOutputStream jar = new JarOutputStream(
                Files.newOutputStream(dir.resolve(pluginName + ".jar")), manifest)) {
            if (serviceImplName != null) {
                write(jar, "META-INF/services/" + DorisPlugin.class.getName(),
                        (serviceImplName + "\n").getBytes(StandardCharsets.UTF_8));
            }
            Set<String> written = new HashSet<>();
            for (Class<?> clazz : classes) {
                String resource = clazz.getName().replace('.', '/') + ".class";
                // Callers name the plugin class and then list everything it needs, so the plugin
                // class itself usually appears twice; a duplicate jar entry is a hard error.
                if (written.add(resource)) {
                    write(jar, resource, bytesOf(resource));
                }
            }
        }
        return dir;
    }

    /** Adds a file to an existing plugin directory, for cases that need a second, broken jar. */
    static void addRawJar(Path pluginDir, String fileName, byte[] content) throws IOException {
        Files.write(pluginDir.resolve(fileName), content);
    }

    /** Packs BE's own SPI classes into the plugin, the mistake a provided-scope dependency avoids. */
    static void addSpiClass(Path pluginDir, String jarName) throws IOException {
        String resource = DorisPlugin.class.getName().replace('.', '/') + ".class";
        try (JarOutputStream jar = new JarOutputStream(
                Files.newOutputStream(pluginDir.resolve(jarName)), new Manifest())) {
            write(jar, resource, bytesOf(resource));
        }
    }

    private static Class<?>[] classes(Class<?> first, Class<?>[] rest) {
        Class<?>[] all = new Class<?>[rest.length + 1];
        all[0] = first;
        System.arraycopy(rest, 0, all, 1, rest.length);
        return all;
    }

    private static void write(JarOutputStream jar, String name, byte[] content) throws IOException {
        jar.putNextEntry(new JarEntry(name));
        jar.write(content);
        jar.closeEntry();
    }

    private static byte[] bytesOf(String resource) throws IOException {
        try (InputStream in = PluginJars.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("test fixture " + resource + " is not on the test classpath");
            }
            java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
            copy(in, buffer);
            return buffer.toByteArray();
        }
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] chunk = new byte[8192];
        int read;
        while ((read = in.read(chunk)) >= 0) {
            out.write(chunk, 0, read);
        }
    }
}
