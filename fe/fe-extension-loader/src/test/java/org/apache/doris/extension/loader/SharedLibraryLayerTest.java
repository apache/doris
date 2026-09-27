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

import org.apache.doris.extension.loader.testplugins.SharedLibraryProbe;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * The shared library layer's contract: absence is a no-op, an unreadable root is not, one layer per
 * physical root, and jar order is the layout convention (bundle root before {@code lib/}, bundles by
 * name).
 *
 * <p>Each test gets its own {@code @TempDir}, which keeps them independent of the process-wide
 * memoization the layer deliberately does.
 */
class SharedLibraryLayerTest {

    private static final String PROBE = SharedLibraryProbe.class.getName();

    @TempDir
    Path tempDir;

    @Test
    void testAbsentRootYieldsParentUnchanged() {
        ClassLoader parent = getClass().getClassLoader();
        Assertions.assertSame(parent, SharedLibraryLayer.resolve(tempDir.resolve("not-installed"), parent));
    }

    @Test
    void testNullRootYieldsParentUnchanged() {
        ClassLoader parent = getClass().getClassLoader();
        Assertions.assertSame(parent, SharedLibraryLayer.resolve(null, parent));
    }

    @Test
    void testRootWithoutJarsYieldsParentUnchanged() throws IOException {
        Path root = tempDir.resolve("shared");
        Files.createDirectories(root.resolve("hadoop").resolve("lib"));
        Files.createFile(root.resolve("hadoop").resolve("README.txt"));

        ClassLoader parent = getClass().getClassLoader();
        Assertions.assertSame(parent, SharedLibraryLayer.resolve(root, parent),
                "a bundle directory carrying no jar must not change the loader graph");
    }

    @Test
    void testSameRootYieldsTheSameLayer() throws IOException {
        Path root = tempDir.resolve("shared");
        createProbeJar(root.resolve("hadoop").resolve("hadoop-deps.jar"));

        ClassLoader parent = getClass().getClassLoader();
        ClassLoader first = SharedLibraryLayer.resolve(root, parent);
        ClassLoader second = SharedLibraryLayer.resolve(root, parent);

        Assertions.assertNotSame(parent, first, "a root holding a jar must produce a layer");
        Assertions.assertSame(first, second,
                "two families resolving the same root must share one layer, or they get two copies "
                        + "of every class in it");
        // The same root spelled differently is still the same root.
        Assertions.assertSame(first, SharedLibraryLayer.resolve(root.resolve("hadoop").resolve(".."), parent));
    }

    @Test
    void testSymlinkedRootYieldsTheSameLayer() throws IOException {
        Path root = tempDir.resolve("shared");
        createProbeJar(root.resolve("hadoop").resolve("hadoop-deps.jar"));
        Path alias = tempDir.resolve("current-shared");
        try {
            Files.createSymbolicLink(alias, root);
        } catch (IOException | UnsupportedOperationException e) {
            Assumptions.abort("this filesystem does not support symbolic links: " + e);
            return;
        }

        ClassLoader parent = getClass().getClassLoader();

        // A release directory reached through a "current" symlink and through its real path is one
        // set of jars. Two layers over it would be two copies of every shared class, and a shared
        // library is shared precisely because a second copy of it changes behavior.
        Assertions.assertSame(SharedLibraryLayer.resolve(root, parent),
                SharedLibraryLayer.resolve(alias, parent),
                "the memoization key is the physical root, not the spelling the caller used");
    }

    @Test
    void testUnreadableRootIsRaisedRatherThanTakenForAbsent() throws IOException {
        Path loop = tempDir.resolve("shared");
        try {
            Files.createSymbolicLink(loop, loop);
        } catch (IOException | UnsupportedOperationException e) {
            Assumptions.abort("this filesystem does not support symbolic links: " + e);
            return;
        }

        ClassLoader parent = getClass().getClassLoader();

        // A root that is there but cannot be read is a deployment mistake, and answering it with
        // "nothing installed" turns it into a NoClassDefFoundError much later and somewhere else.
        Assertions.assertThrows(UncheckedIOException.class,
                () -> SharedLibraryLayer.resolve(loop, parent));
        // Nothing was memoized, so repairing the root does not need a restart to take effect.
        Assertions.assertThrows(UncheckedIOException.class,
                () -> SharedLibraryLayer.resolve(loop, parent));
    }

    @Test
    void testLayerLoadsItsOwnCopyChildFirst() throws Exception {
        Path root = tempDir.resolve("shared");
        Path jar = root.resolve("hadoop").resolve("hadoop-deps.jar");
        createProbeJar(jar);

        ClassLoader layer = SharedLibraryLayer.resolve(root, getClass().getClassLoader());
        Class<?> loaded = layer.loadClass(PROBE);

        Assertions.assertNotSame(SharedLibraryProbe.class, loaded,
                "the layer is child-first, so it must define its own copy rather than delegate");
        Assertions.assertEquals(jar.toRealPath(), codeSourceOf(loaded));
    }

    @Test
    void testBundleRootJarPrecedesLibJar() throws Exception {
        Path root = tempDir.resolve("shared");
        Path rootJar = root.resolve("hadoop").resolve("hadoop-deps.jar");
        Path libJar = root.resolve("hadoop").resolve("lib").resolve("hadoop-common.jar");
        createProbeJar(rootJar);
        createProbeJar(libJar);

        ClassLoader layer = SharedLibraryLayer.resolve(root, getClass().getClassLoader());

        Assertions.assertEquals(rootJar.toRealPath(), codeSourceOf(layer.loadClass(PROBE)),
                "the bundle root is how a bundle ships a patched class that also exists under lib/");
    }

    @Test
    void testBundlesResolveInNameOrder() throws Exception {
        Path root = tempDir.resolve("shared");
        Path alpha = root.resolve("alpha").resolve("alpha.jar");
        Path beta = root.resolve("beta").resolve("beta.jar");
        // Created out of order, so a passing assertion cannot be filesystem listing order.
        createProbeJar(beta);
        createProbeJar(alpha);

        ClassLoader layer = SharedLibraryLayer.resolve(root, getClass().getClassLoader());

        Assertions.assertEquals(alpha.toRealPath(), codeSourceOf(layer.loadClass(PROBE)));
    }

    /**
     * The jar a class was actually defined from. Physical path on both sides of the comparison: the
     * layer builds its URLs from the resolved root, and a temporary directory is itself reached
     * through a symlink on macOS.
     */
    private static Path codeSourceOf(Class<?> clazz) throws URISyntaxException, IOException {
        return Paths.get(clazz.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
    }

    /** A bundle jar holding one copy of the probe class, so several jars can compete for it. */
    private static void createProbeJar(Path jarPath) throws IOException {
        Files.createDirectories(jarPath.getParent());
        String classEntry = PROBE.replace('.', '/') + ".class";
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(jarPath))) {
            jar.putNextEntry(new JarEntry(classEntry));
            try (InputStream classBytes = SharedLibraryProbe.class.getClassLoader()
                    .getResourceAsStream(classEntry)) {
                Assertions.assertNotNull(classBytes, "class bytes not found: " + classEntry);
                byte[] buffer = new byte[8192];
                int read;
                while ((read = classBytes.read(buffer)) != -1) {
                    jar.write(buffer, 0, read);
                }
            }
            jar.closeEntry();
        }
    }
}
