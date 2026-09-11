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

package org.apache.doris.common.util;

import org.apache.doris.authorization.AccessContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * The classloader the deprecated {@code AccessControllerFactory} channel installs plugins through.
 *
 * <p>A source still implementing that interface is the one this release asks to recompile against
 * {@code org.apache.doris.authorization}, so it is also the one likeliest to ship those classes inside its
 * own jar. Loaded child-first they become a second set of types and the engine refuses every answer carrying
 * them, naming the same class twice - which is fail-closed but unactionable. Both directions are pinned
 * here, because a list that takes everything parent-first would break the channel in the other direction.
 */
class ChildFirstClassLoaderTest {

    @TempDir
    Path tempDir;

    /** Bytes no {@code defineClass} accepts, so a child-first load of the entry is an error and not a miss. */
    private static final byte[] NOT_A_CLASS_FILE = "definitely not a class file".getBytes(StandardCharsets.UTF_8);

    @Test
    void authorizationVocabularyComesFromTheFeEvenWhenThePluginShipsItsOwn() throws Exception {
        URL pluginJar = jarWithClassEntries(AccessContext.class.getName());

        try (ChildFirstClassLoader plugin = new ChildFirstClassLoader(
                new URL[] {pluginJar}, getClass().getClassLoader())) {
            Assertions.assertSame(AccessContext.class, plugin.loadClass(AccessContext.class.getName()),
                    "a plugin bundling the authorization api now defines its own copy of it, so every answer"
                            + " it hands back is refused as a type the engine does not recognise");
        }
    }

    @Test
    void everythingElseStillComesFromThePluginFirst() throws Exception {
        String published = "org.apache.doris.catalog.authorizer.probe.ProbeAccessControllerFactory";
        URL pluginJar = jarWithClassEntries(published);

        try (ChildFirstClassLoader plugin = new ChildFirstClassLoader(
                new URL[] {pluginJar}, getClass().getClassLoader())) {
            // Reaching defineClass at all is the assertion: the bytes are refused, which only a child-first
            // load of the plugin's own entry can produce. A parent-first one would miss and end in
            // ClassNotFoundException instead.
            Assertions.assertThrows(ClassFormatError.class, () -> plugin.loadClass(published),
                    "the classes a plugin publishes are no longer loaded from the plugin");
        }
    }

    private URL jarWithClassEntries(String... classNames) throws IOException {
        Path jar = tempDir.resolve("probe-plugin.jar");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            for (String className : classNames) {
                out.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
                out.write(NOT_A_CLASS_FILE);
                out.closeEntry();
            }
        }
        return jar.toUri().toURL();
    }
}
