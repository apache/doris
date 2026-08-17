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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The defaults this class falls back to when {@code JvmLauncher::_build_options()} did not set the
 * properties are second, independent copies of paths BE also holds in its own config. Nothing makes
 * the compiler check that the two agree, and a disagreement does not fail a build - it makes every
 * plugin report "is not deployed" at query time, or a hadoop configuration file go unread. These
 * tests are what holds them together.
 */
public class PluginRegistryDefaultDirTest {

    private static final String PLUGIN_DIR_PROPERTY = "doris.jni.plugin.dir";
    private static final String HADOOP_CONF_DIR_PROPERTY = "doris.jni.hadoop.conf.dir";

    private final Map<String, String> saved = new LinkedHashMap<>();

    @BeforeEach
    public void clearProperties() {
        for (String property : new String[] {PLUGIN_DIR_PROPERTY, HADOOP_CONF_DIR_PROPERTY}) {
            saved.put(property, System.getProperty(property));
            System.clearProperty(property);
        }
    }

    @AfterEach
    public void restoreProperties() {
        saved.forEach((property, value) -> {
            if (value == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, value);
            }
        });
    }

    /**
     * lib/ is the engine tree a package upgrade replaces wholesale. A plugin deployed there would
     * not survive one, which is why the family root sits under plugins/ instead. An operator who
     * sets nothing gets exactly this path, so it is the one that has to be right.
     *
     * <p>Asserted as a suffix rather than as "does not contain /lib/": the latter also depends on
     * where the code happens to be checked out.
     */
    @Test
    public void defaultPluginDirLivesUnderPlugins() {
        Path dir = PluginRegistry.pluginDir();
        Assertions.assertTrue(dir.endsWith(Paths.get("plugins", "jni")), dir.toString());
    }

    /** Keep in sync with the BE config jni_plugin_hadoop_conf_dir. */
    @Test
    public void defaultHadoopConfDirLivesUnderPlugins() {
        Path dir = PluginRegistry.hadoopConfDir();
        Assertions.assertTrue(dir.endsWith(Paths.get("plugins", "hadoop_conf")), dir.toString());
    }

    /** An explicitly configured directory wins; the default is only a fallback. */
    @Test
    public void configuredPropertyWinsOverTheDefault() {
        System.setProperty(PLUGIN_DIR_PROPERTY, "/somewhere/else");
        System.setProperty(HADOOP_CONF_DIR_PROPERTY, "/somewhere/hadoop");

        Assertions.assertEquals(Paths.get("/somewhere/else"), PluginRegistry.pluginDir());
        Assertions.assertEquals(Paths.get("/somewhere/hadoop"), PluginRegistry.hadoopConfDir());
    }
}
