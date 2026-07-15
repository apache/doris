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

import org.apache.doris.extension.loader.testplugins.BadNameTestPluginFactory;
import org.apache.doris.extension.loader.testplugins.MetadataTestPluginFactory;
import org.apache.doris.extension.spi.PluginFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Verifies load-time metadata snapshotting: MANIFEST Implementation-Version,
 * description snapshot, and self-reported name validation.
 */
class DirectoryPluginRuntimeManagerMetadataTest {

    @TempDir
    Path tempDir;

    @Test
    void testManifestVersionAndDescriptionSnapshot() throws IOException {
        Path root = tempDir.resolve("root");
        createPluginJar(root.resolve("metadata-test").resolve("metadata-test.jar"),
                MetadataTestPluginFactory.class, "3.1.4");

        DirectoryPluginRuntimeManager<PluginFactory> manager = new DirectoryPluginRuntimeManager<>();
        LoadReport<PluginFactory> report = manager.loadAll(
                Collections.singletonList(root),
                Thread.currentThread().getContextClassLoader(),
                PluginFactory.class,
                null);

        Assertions.assertEquals(1, report.getSuccesses().size(), () -> failures(report));
        PluginHandle<PluginFactory> handle = report.getSuccesses().get(0);
        Assertions.assertEquals("metadata-test", handle.getPluginName());
        Assertions.assertEquals("Metadata snapshot test plugin", handle.getDescription());
        Assertions.assertEquals("3.1.4", handle.getImplementationVersion());
    }

    @Test
    void testMissingManifestVersionDegradesToNull() throws IOException {
        Path root = tempDir.resolve("root-no-version");
        createPluginJar(root.resolve("metadata-test").resolve("metadata-test.jar"),
                MetadataTestPluginFactory.class, null);

        DirectoryPluginRuntimeManager<PluginFactory> manager = new DirectoryPluginRuntimeManager<>();
        LoadReport<PluginFactory> report = manager.loadAll(
                Collections.singletonList(root),
                Thread.currentThread().getContextClassLoader(),
                PluginFactory.class,
                null);

        Assertions.assertEquals(1, report.getSuccesses().size(), () -> failures(report));
        Assertions.assertNull(report.getSuccesses().get(0).getImplementationVersion());
    }

    @Test
    void testInvalidSelfReportedNameIsLoadFailure() throws IOException {
        Path root = tempDir.resolve("root-bad-name");
        createPluginJar(root.resolve("bad-name").resolve("bad-name.jar"),
                BadNameTestPluginFactory.class, "1.0");

        DirectoryPluginRuntimeManager<PluginFactory> manager = new DirectoryPluginRuntimeManager<>();
        LoadReport<PluginFactory> report = manager.loadAll(
                Collections.singletonList(root),
                Thread.currentThread().getContextClassLoader(),
                PluginFactory.class,
                null);

        Assertions.assertTrue(report.getSuccesses().isEmpty());
        Assertions.assertEquals(1, report.getFailures().size());
        Assertions.assertTrue(report.getFailures().get(0).getMessage().contains("Invalid plugin name"),
                () -> report.getFailures().get(0).getMessage());
    }

    /**
     * Builds a plugin jar containing the factory's class bytes, a ServiceLoader
     * registration, and (optionally) a MANIFEST Implementation-Version.
     */
    private static void createPluginJar(Path jarPath, Class<? extends PluginFactory> factoryClass,
            String implementationVersion) throws IOException {
        Files.createDirectories(jarPath.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (implementationVersion != null) {
            manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, implementationVersion);
        }
        String classEntry = factoryClass.getName().replace('.', '/') + ".class";
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
            jar.putNextEntry(new JarEntry(classEntry));
            try (InputStream classBytes = factoryClass.getClassLoader().getResourceAsStream(classEntry)) {
                Assertions.assertNotNull(classBytes, "class bytes not found: " + classEntry);
                byte[] buffer = new byte[8192];
                int read;
                while ((read = classBytes.read(buffer)) != -1) {
                    jar.write(buffer, 0, read);
                }
            }
            jar.closeEntry();
            jar.putNextEntry(new JarEntry("META-INF/services/" + PluginFactory.class.getName()));
            jar.write((factoryClass.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
    }

    private static String failures(LoadReport<PluginFactory> report) {
        StringBuilder sb = new StringBuilder("load failures:");
        List<LoadFailure> failures = report.getFailures();
        for (LoadFailure failure : failures) {
            sb.append(" [").append(failure.getStage()).append("] ").append(failure.getMessage());
        }
        return sb.toString();
    }
}
