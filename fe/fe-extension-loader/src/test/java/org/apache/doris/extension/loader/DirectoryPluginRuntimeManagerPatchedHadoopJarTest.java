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

import org.apache.doris.extension.loader.testplugins.MetadataTestPluginFactory;
import org.apache.doris.extension.spi.PluginFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * hadoop-deps ships the DORIS-PATCH {@code org.apache.hadoop.fs.FileSystem} (credential-aware
 * {@code doris.fs.cache.key}). Connector plugins bundle their own hadoop-common and are loaded
 * child-first for {@code org.apache.hadoop.*}, so the patched class only wins inside a plugin
 * classloader when its jar precedes hadoop-common on that classloader's URL list — neither
 * directory position nor {@code collectJars()}'s lexicographic order delivers that
 * ({@code "hadoop-common-x.y.z.jar"} sorts BEFORE {@code "hadoop-deps-*.jar"}).
 *
 * <p>The assertions probe a resource rather than a class: {@code URLClassLoader} resolves classes
 * and resources through the same {@code URLClassPath}, in URL order, so a resource that resolves
 * out of hadoop-deps proves a same-named class would too — without hand-assembling two colliding
 * copies of {@code org.apache.hadoop.fs.FileSystem}.
 */
class DirectoryPluginRuntimeManagerPatchedHadoopJarTest {

    private static final ApiVersionGate GATE = DirectoryPluginRuntimeManagerMetadataTest.TEST_GATE;

    /** Under org/apache/hadoop/ so the FE test classpath (the parent loader) cannot supply it. */
    private static final String MARKER = "org/apache/hadoop/fs/doris-patch-marker.txt";

    @TempDir
    Path tempDir;

    @Test
    void testPatchedJarInLibWinsOverBundledHadoopCommon() throws IOException {
        Path pluginDir = tempDir.resolve("lib-layout").resolve("metadata-test");
        // The connector plugin zips put EVERY jar under lib/, the factory jar included.
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                pluginDir.resolve("lib").resolve("metadata-test.jar"),
                MetadataTestPluginFactory.class, "3.1.4");
        createMarkerJar(pluginDir.resolve("lib").resolve("hadoop-common-3.4.2.jar"), "vanilla");
        createMarkerJar(pluginDir.resolve("lib").resolve("hadoop-deps-1.2-SNAPSHOT.jar"), "patched");

        PluginHandle<PluginFactory> handle = loadSingle(pluginDir.getParent());
        Assertions.assertEquals("patched", readMarker(handle.getClassLoader()));
    }

    @Test
    void testPatchedJarAtPluginRootDoesNotStarveServiceDiscovery() throws IOException {
        // Discovery narrows to root-level jars whenever any exist. A hadoop-deps.jar dropped at the
        // root registers no service, so without excluding it from that set the plugin would fail to
        // load ("no factory found") for a layout that is otherwise perfectly valid.
        Path pluginDir = tempDir.resolve("root-layout").resolve("metadata-test");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                pluginDir.resolve("lib").resolve("metadata-test.jar"),
                MetadataTestPluginFactory.class, "3.1.4");
        createMarkerJar(pluginDir.resolve("lib").resolve("hadoop-common-3.4.2.jar"), "vanilla");
        createMarkerJar(pluginDir.resolve("hadoop-deps-1.2-SNAPSHOT.jar"), "patched");

        PluginHandle<PluginFactory> handle = loadSingle(pluginDir.getParent());
        Assertions.assertEquals("patched", readMarker(handle.getClassLoader()));
    }

    @Test
    void testPluginWithoutPatchedJarIsUnaffected() throws IOException {
        Path pluginDir = tempDir.resolve("no-patch").resolve("metadata-test");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                pluginDir.resolve("lib").resolve("metadata-test.jar"),
                MetadataTestPluginFactory.class, "3.1.4");
        createMarkerJar(pluginDir.resolve("lib").resolve("hadoop-common-3.4.2.jar"), "vanilla");

        PluginHandle<PluginFactory> handle = loadSingle(pluginDir.getParent());
        Assertions.assertEquals("vanilla", readMarker(handle.getClassLoader()));
    }

    private PluginHandle<PluginFactory> loadSingle(Path root) {
        DirectoryPluginRuntimeManager<PluginFactory> manager = new DirectoryPluginRuntimeManager<>();
        LoadReport<PluginFactory> report = manager.loadAll(Collections.singletonList(root),
                Thread.currentThread().getContextClassLoader(), PluginFactory.class, null, GATE);
        Assertions.assertEquals(1, report.getSuccesses().size(),
                () -> "expected one loaded plugin, failures: " + report.getFailures());
        return report.getSuccesses().get(0);
    }

    private static String readMarker(ClassLoader loader) throws IOException {
        URL url = loader.getResource(MARKER);
        Assertions.assertNotNull(url, "marker resource not visible to the plugin classloader");
        try (InputStream in = url.openStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void createMarkerJar(Path jarPath, String marker) throws IOException {
        Files.createDirectories(jarPath.getParent());
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath))) {
            jar.putNextEntry(new JarEntry(MARKER));
            jar.write(marker.getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
    }
}
