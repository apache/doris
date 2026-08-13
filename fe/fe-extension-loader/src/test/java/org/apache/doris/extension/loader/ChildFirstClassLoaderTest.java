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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

class ChildFirstClassLoaderTest {

    @TempDir
    Path tempDir;

    /**
     * A plugin's own configuration file lives in the FE's conf directory, not in the plugin, and the plugin
     * has to be able to read it.
     *
     * <p>{@code $DORIS_HOME/conf} is on the FE's class path, which is the parent of every plugin classloader,
     * and a library inside a plugin looks its configuration up through the classloader that loaded it - its
     * plugin's. The Ranger sources are the live example: {@code RangerConfiguration.getFileLocation} calls
     * {@code RangerConfiguration.class.getClassLoader().getResource("ranger-<service>-security.xml")}, and
     * since those classes moved into a plugin that call reaches this classloader. Finding nothing is not an
     * error there - Ranger carries on with an empty configuration, which authorizes against no policies at
     * all - so this has to be pinned rather than left to be noticed.
     *
     * <p>It works because {@link ChildFirstClassLoader} overrides only the plural {@code getResources}; the
     * singular one keeps the JDK's parent-first search. Overriding that one too, for symmetry with the class
     * loading policy, is the change this test exists to fail.
     */
    @Test
    void configurationFileOnlyTheFeHasIsVisibleToAPlugin() throws IOException {
        Path feConf = Files.createDirectory(tempDir.resolve("conf"));
        Files.write(feConf.resolve("probe-plugin-security.xml"),
                "<configuration/>".getBytes(StandardCharsets.UTF_8));
        Path pluginJars = Files.createDirectory(tempDir.resolve("plugin"));

        try (URLClassLoader fe = new URLClassLoader(new URL[] {feConf.toUri().toURL()}, null);
                ChildFirstClassLoader plugin = new ChildFirstClassLoader(
                        new URL[] {pluginJars.toUri().toURL()}, fe, Collections.emptyList())) {
            Assertions.assertNotNull(plugin.getResource("probe-plugin-security.xml"),
                    "a plugin can no longer read a configuration file out of the FE's conf directory");
            Assertions.assertTrue(plugin.getResources("probe-plugin-security.xml").hasMoreElements(),
                    "the child-first plural lookup lost its fall-back to the parent");
        }
    }
}
