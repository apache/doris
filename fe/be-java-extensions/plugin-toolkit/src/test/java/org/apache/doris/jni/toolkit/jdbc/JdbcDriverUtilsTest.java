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

package org.apache.doris.jni.toolkit.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

class JdbcDriverUtilsTest {

    /**
     * One classloader per driver jar, not per query. A fresh one per scan coexists with the driver
     * classes a pooled connection is still holding, and the two are unrelated types with the same
     * names - which surfaces far from here, as a cast failure inside a driver.
     */
    @Test
    void theSameDriverJarAlwaysGetsTheSameClassLoader(@TempDir Path dir) throws IOException {
        String jar = driverJar(dir, "one.jar");
        ClassLoader parent = getClass().getClassLoader();

        ClassLoader first = JdbcDriverUtils.driverClassLoader(jar, parent);
        ClassLoader second = JdbcDriverUtils.driverClassLoader(jar, parent);

        Assertions.assertSame(first, second);
    }

    /**
     * ...but the same jar under two plugins is two driver worlds. Sharing one classloader across
     * them would hand plugin B classes whose supertypes came from plugin A.
     */
    @Test
    void sameJarUnderADifferentParentIsADifferentClassLoader(@TempDir Path dir) throws IOException {
        String jar = driverJar(dir, "one.jar");

        ClassLoader underA = JdbcDriverUtils.driverClassLoader(jar, new URLClassLoader(new URL[0], null));
        ClassLoader underB = JdbcDriverUtils.driverClassLoader(jar, new URLClassLoader(new URL[0], null));

        Assertions.assertNotSame(underA, underB);
    }

    /** The driver resolves from its jar, and still sees the plugin that will call it. */
    @Test
    void theDriverIsLoadableAndTheParentIsVisible(@TempDir Path dir) throws Exception {
        String jar = driverJar(dir, "driver.jar");
        ClassLoader parent = getClass().getClassLoader();

        ClassLoader loader = JdbcDriverUtils.driverClassLoader(jar, parent);

        Assertions.assertSame(parent, loader.getParent());
        Assertions.assertNotNull(loader.getResource("com/example/Driver.class"),
                "the driver jar must be on this classloader's own search path");
    }

    /**
     * Verification belongs to creating the classloader, so it costs one check per jar rather than
     * one per query - the check downloads the jar to hash it.
     */
    @Test
    void theJarIsVerifiedOnceNotOnEveryRequest(@TempDir Path dir) throws IOException {
        String jar = driverJar(dir, "verified.jar");
        ClassLoader parent = getClass().getClassLoader();
        AtomicInteger checks = new AtomicInteger();

        JdbcDriverUtils.driverClassLoader(jar, parent, url -> checks.incrementAndGet());
        JdbcDriverUtils.driverClassLoader(jar, parent, url -> checks.incrementAndGet());
        JdbcDriverUtils.driverClassLoader(jar, parent);

        Assertions.assertEquals(1, checks.get());
    }

    /**
     * A rejected jar must leave nothing behind. Caching the failed attempt would make the next
     * request succeed against a classloader that was never allowed to exist.
     */
    @Test
    void rejectedJarIsNotCached(@TempDir Path dir) throws IOException {
        String jar = driverJar(dir, "rejected.jar");
        ClassLoader parent = getClass().getClassLoader();
        AtomicInteger checks = new AtomicInteger();
        JdbcDriverUtils.DriverJarVerifier rejecting = url -> {
            checks.incrementAndGet();
            throw new IllegalStateException("Checksum mismatch for JDBC driver.");
        };

        Assertions.assertThrows(IllegalStateException.class,
                () -> JdbcDriverUtils.driverClassLoader(jar, parent, rejecting));
        Assertions.assertThrows(IllegalStateException.class,
                () -> JdbcDriverUtils.driverClassLoader(jar, parent, rejecting));

        Assertions.assertEquals(2, checks.get(), "the second attempt must re-verify, not reuse a"
                + " classloader that was never created");
    }

    /**
     * Invalidation is how a user recovers from deploying a wrong driver jar: without it the first
     * failure would stand until BE restarts, and fixing the catalog would appear to do nothing.
     */
    @Test
    void invalidatingLetsTheNextRequestRebuildAndReverify(@TempDir Path dir) throws IOException {
        String jar = driverJar(dir, "replaced.jar");
        ClassLoader parent = getClass().getClassLoader();
        AtomicInteger checks = new AtomicInteger();

        ClassLoader before = JdbcDriverUtils.driverClassLoader(jar, parent, url -> checks.incrementAndGet());
        JdbcDriverUtils.invalidate(jar, parent);
        ClassLoader after = JdbcDriverUtils.driverClassLoader(jar, parent, url -> checks.incrementAndGet());

        Assertions.assertNotSame(before, after);
        Assertions.assertEquals(2, checks.get());
    }

    @Test
    void malformedUrlNamesTheOffendingValue() {
        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcDriverUtils.driverClassLoader("not a url", getClass().getClassLoader()));

        Assertions.assertTrue(failure.getMessage().contains("not a url"), failure.getMessage());
    }

    /** A jar with one class in it, enough to be a stand-in for a real driver. */
    private static String driverJar(Path dir, String name) throws IOException {
        Path jar = dir.resolve(name);
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            out.putNextEntry(new JarEntry("com/example/Driver.class"));
            out.write("not real bytecode".getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return jar.toUri().toURL().toString();
    }
}
