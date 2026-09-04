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

package org.apache.doris.connector;

import org.apache.doris.extension.loader.ClassLoadingPolicy;
import org.apache.doris.extension.loader.PluginLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * A connector plugin that bundles vanilla hadoop-common and ships no {@code hadoop-deps} must still
 * run on the Doris-patched {@code org.apache.hadoop.fs.FileSystem} — the one whose {@code Cache.Key}
 * carries {@code doris.fs.cache.key.<scheme>}. That is the case of a third-party or previous-release
 * plugin, which cannot be asked to bundle anything, and which FE can no longer compensate for after
 * the blanket {@code fs.<scheme>.impl.disable.cache=true} defaults were removed.
 *
 * <p>{@link ConnectorPluginManager#CONNECTOR_PARENT_FIRST_PREFIXES} is the whole mechanism: it
 * resolves {@code org.apache.hadoop.} through the FE kernel. These tests build the exact layout —
 * a plugin classpath carrying its own vanilla {@code FileSystem.class} and nothing else — and pin
 * both halves of the prefix contract: the parent wins where it has the class, and the plugin's own
 * copy is still used where it does not.
 */
public class ConnectorPluginHadoopPatchTest {

    private static final String FILESYSTEM_CLASS = "org.apache.hadoop.fs.FileSystem";
    private static final String FILESYSTEM_ENTRY = "org/apache/hadoop/fs/FileSystem.class";

    @TempDir
    Path tempDir;

    @Test
    public void testConnectorPluginResolvesThePatchedFileSystemFromTheKernel() throws Exception {
        ClassLoader pluginLoader = pluginLoaderFor(ConnectorPluginManager.CONNECTOR_PARENT_FIRST_PREFIXES);

        // Same Class object as the kernel's, so the plugin shares the credential-aware cache instead
        // of opening its own vanilla one.
        Assertions.assertSame(org.apache.hadoop.fs.FileSystem.class, pluginLoader.loadClass(FILESYSTEM_CLASS));
        Assertions.assertDoesNotThrow(() -> Class.forName("org.apache.hadoop.fs.FileSystem$Cache$Key",
                false, pluginLoader).getDeclaredField("dorisCacheKey"));
    }

    @Test
    public void testWithoutTheHadoopPrefixThePluginGetsItsOwnUnpatchedCopy() throws Exception {
        // Non-vacuity check for the test above, and a statement of what the prefix buys: with the
        // pre-existing connector prefixes alone, the bundled vanilla class wins child-first.
        ClassLoader pluginLoader = pluginLoaderFor(
                Arrays.asList("org.apache.doris.connector.", "org.apache.doris.filesystem."));

        Class<?> pluginCopy = pluginLoader.loadClass(FILESYSTEM_CLASS);
        Assertions.assertNotSame(org.apache.hadoop.fs.FileSystem.class, pluginCopy);
        Assertions.assertSame(pluginLoader, pluginCopy.getClassLoader());
    }

    @Test
    public void testParentFirstFallsBackToThePluginWhenTheKernelHasNothingToOffer() throws Exception {
        // The prefix is a delegation ORDER, not an exclusive claim. It is what keeps the entry from
        // stranding a plugin whose hadoop the kernel does not carry (hbase under hudi, the
        // huaweicloud fs.obs.* classes under paimon), and what lets it degrade on its own once the
        // FE kernel stops shipping hadoop classes at all.
        //
        // Path rather than FileSystem: with an empty parent the loader has to DEFINE the class, and
        // that resolves its supertypes. Path's are all java.*, FileSystem's are more hadoop.
        Path jar = jarWith("org/apache/hadoop/fs/Path.class",
                classBytes("org/apache/hadoop/fs/Path.class", false), "hadoop-fs-path.jar");
        ClassLoader emptyKernel = new URLClassLoader(new URL[0], null);
        ClassLoader pluginLoader = loaderOver(jar, ConnectorPluginManager.CONNECTOR_PARENT_FIRST_PREFIXES,
                emptyKernel);

        Class<?> loaded = pluginLoader.loadClass("org.apache.hadoop.fs.Path");
        Assertions.assertSame(pluginLoader, loaded.getClassLoader());
    }

    private ClassLoader pluginLoaderFor(List<String> parentFirstPrefixes) throws IOException {
        return loaderOver(vanillaFileSystemJar(), parentFirstPrefixes, getClass().getClassLoader());
    }

    private ClassLoader loaderOver(Path jar, List<String> parentFirstPrefixes, ClassLoader parent)
            throws IOException {
        URL[] urls = {jar.toUri().toURL()};
        return new PluginLoader(new ClassLoadingPolicy(parentFirstPrefixes).toParentFirstPackages())
                .createClassLoader(urls, parent);
    }

    /**
     * A one-entry jar holding the vanilla {@code FileSystem.class}, i.e. what a plugin that bundles
     * hadoop-common and no hadoop-deps puts on its own classpath.
     */
    private Path vanillaFileSystemJar() throws IOException {
        return jarWith(FILESYSTEM_ENTRY, classBytes(FILESYSTEM_ENTRY, true), "hadoop-common-vanilla.jar");
    }

    private Path jarWith(String entry, byte[] bytes, String jarName) throws IOException {
        Path jarPath = tempDir.resolve(jarName);
        if (Files.exists(jarPath)) {
            return jarPath;
        }
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath))) {
            jar.putNextEntry(new JarEntry(entry));
            jar.write(bytes);
            jar.closeEntry();
        }
        return jarPath;
    }

    /**
     * Reads a class file off this test's own classpath rather than hand-assembling one. With {@code
     * skipPatched}, the hadoop-deps copy is passed over — {@code getResources} returns every copy on
     * the classpath, and hadoop-deps is the one that precedes hadoop-common.
     */
    private byte[] classBytes(String entry, boolean skipPatched) throws IOException {
        Enumeration<URL> copies = getClass().getClassLoader().getResources(entry);
        while (copies.hasMoreElements()) {
            URL url = copies.nextElement();
            if (skipPatched && url.toExternalForm().contains("hadoop-deps")) {
                continue;
            }
            try (InputStream in = url.openStream()) {
                return in.readAllBytes();
            }
        }
        throw new IllegalStateException("no" + (skipPatched ? " unpatched " : " ") + entry
                + " on the test classpath; this test needs hadoop-common alongside hadoop-deps");
    }
}
