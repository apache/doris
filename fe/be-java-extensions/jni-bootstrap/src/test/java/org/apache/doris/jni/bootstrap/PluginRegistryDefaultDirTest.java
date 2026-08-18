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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        // Null environment, so this asserts the default and not whatever HADOOP_CONF_DIR happens
        // to be on the machine running it.
        Path dir = PluginRegistry.hadoopConfDir(null);
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

    /**
     * HADOOP_CONF_DIR is how a deployment names the cluster's real hadoop configuration, and
     * start_be.sh puts it on the SYSTEM class path - which a plugin classloader cannot reach. So
     * an upgrade to plugin isolation would otherwise take that configuration away from every Java
     * scanner while the native reader kept it, and an HDFS HA nameservice would stop resolving.
     */
    @Test
    public void hadoopConfDirEnvIsUsedWhenThePluginConfDirIsAbsent(@TempDir Path existing) {
        Assertions.assertEquals(existing, PluginRegistry.hadoopConfDir(existing.toString()),
                "an existing HADOOP_CONF_DIR should answer when plugins/hadoop_conf is not there");
    }

    /**
     * ...but only as a fallback. A configured property, and an existing plugin conf directory, are
     * both deliberate statements about where the files are; reading somewhere else instead would
     * be worse than reading nothing.
     */
    @Test
    public void hadoopConfDirEnvNeverOverridesAConfiguredDirectory(@TempDir Path existing) {
        System.setProperty(HADOOP_CONF_DIR_PROPERTY, "/somewhere/hadoop");
        Assertions.assertEquals(Paths.get("/somewhere/hadoop"),
                PluginRegistry.hadoopConfDir(existing.toString()));
    }

    /** A HADOOP_CONF_DIR pointing at nothing is not an answer either. */
    @Test
    public void missingHadoopConfDirEnvLeavesTheDefaultInPlace() {
        Path dir = PluginRegistry.hadoopConfDir("/definitely/not/a/directory");
        Assertions.assertTrue(dir.endsWith(Paths.get("plugins", "hadoop_conf")), dir.toString());
    }

    /**
     * The shape a real BE is in, and the reason the two cases below exist at all: BE does not leave
     * this property unset. {@code JvmLauncher::_build_options()} pushes it on every startup, from a
     * config whose default is {@code <DORIS_HOME>/plugins/hadoop_conf} - so a fallback that asks
     * "was a property set" never runs, and the cluster whose HDFS HA nameservice lives in
     * $HADOOP_CONF_DIR loses it on upgrade with a green test suite watching.
     */
    @Test
    public void thePropertyCarryingTheDefaultDoesNotSuppressTheEnv(@TempDir Path existing) {
        System.setProperty(HADOOP_CONF_DIR_PROPERTY,
                Paths.get("/opt/doris/be", "plugins", "hadoop_conf").toString());

        Assertions.assertEquals(existing, PluginRegistry.hadoopConfDir(existing.toString()),
                "BE always sets this property, so carrying the default value must count as "
                        + "'not configured'");
    }

    /**
     * The other half of the same problem: build.sh creates plugins/hadoop_conf in every output
     * tree, so the directory EXISTS on a deployment nobody has dropped a file into. An empty drop
     * point answers no question, and must not stand in the way of the environment.
     */
    @Test
    public void anEmptyPluginConfDirDoesNotSuppressTheEnv(@TempDir Path emptyDefault,
            @TempDir Path existing) {
        System.setProperty(HADOOP_CONF_DIR_PROPERTY, emptyDefault.resolve("plugins")
                .resolve("hadoop_conf").toString());
        Assertions.assertTrue(emptyDefault.resolve("plugins").resolve("hadoop_conf").toFile()
                .mkdirs());

        Assertions.assertEquals(existing, PluginRegistry.hadoopConfDir(existing.toString()));
    }

    /** ...and once an operator drops a file in, that directory is the answer again. */
    @Test
    public void populatedPluginConfDirWinsOverTheEnv(@TempDir Path populatedDefault,
            @TempDir Path existing) throws IOException {
        Path conf = populatedDefault.resolve("plugins").resolve("hadoop_conf");
        Files.createDirectories(conf);
        Files.write(conf.resolve("core-site.xml"), "<configuration/>".getBytes(StandardCharsets.UTF_8));
        System.setProperty(HADOOP_CONF_DIR_PROPERTY, conf.toString());

        Assertions.assertEquals(conf, PluginRegistry.hadoopConfDir(existing.toString()));
    }

    /**
     * The other end of "keep in sync with the BE config": the two constants above are pinned by the
     * cases at the top of this file, but pinning a Java string to itself proves nothing about the
     * C++ side it is a copy of. This reads be/src/common/config.cpp and compares.
     *
     * <p>Skipped rather than failed when the file cannot be found, so that running these tests out
     * of an unpacked jar or a partial checkout stays possible; inside the repo it always runs.
     */
    @Test
    public void theBeConfigDefaultsAreTheSamePathsAsTheJavaDefaults() throws IOException {
        Path config = repoFile("be/src/common/config.cpp");
        Assumptions.assumeTrue(config != null, "be/src/common/config.cpp not found from "
                + Paths.get("").toAbsolutePath());
        String text = new String(Files.readAllBytes(config), StandardCharsets.UTF_8);

        Assertions.assertEquals("${DORIS_HOME}/plugins/jni", definedString(text, "jni_plugin_dir"),
                "jni_plugin_dir and PluginRegistry.DEFAULT_PLUGIN_SUBDIR name different directories;"
                        + " a BE started without the property would find no plugin at all");
        Assertions.assertEquals("${DORIS_HOME}/plugins/hadoop_conf",
                definedString(text, "jni_plugin_hadoop_conf_dir"),
                "jni_plugin_hadoop_conf_dir and PluginRegistry.DEFAULT_HADOOP_CONF_SUBDIR name "
                        + "different directories; hadoop configuration would go unread");
    }

    private static String definedString(String text, String name) {
        Matcher matcher = Pattern.compile(
                "DEFINE_String\\(\\s*" + Pattern.quote(name) + "\\s*,\\s*\"([^\"]*)\"").matcher(text);
        Assertions.assertTrue(matcher.find(), "no DEFINE_String(" + name + ", ...) in config.cpp");
        return matcher.group(1);
    }

    /** Walks up from the working directory, which surefire sets to the module root. */
    private static Path repoFile(String relative) {
        for (Path dir = Paths.get("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            Path candidate = dir.resolve(relative);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        return null;
    }
}
