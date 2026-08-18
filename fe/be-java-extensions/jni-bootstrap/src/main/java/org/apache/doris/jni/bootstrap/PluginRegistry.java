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

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * The single Java entry point BE calls into.
 *
 * <p>Everything here is static and speaks only primitives, String, Map and byte[], because each
 * method is reached through a JNI method id resolved on this class. Keeping the surface this narrow
 * is what let the old arrangement - BE looking up a concrete class by name and guessing at its
 * constructor - be replaced by something the compiler checks.
 *
 * <p>Failures come back as exceptions, which BE renders through
 * {@code JniUtil.throwableToString}. Two of them are worth recognising in the field:
 * "is not deployed" means the plugin directory is absent, and "failed to load" means it is present
 * but unusable, with the cause appended.
 */
public final class PluginRegistry {

    /** Set by {@code JvmLauncher::_build_options()} from the {@code jni_plugin_dir} BE config. */
    private static final String PLUGIN_DIR_PROPERTY = "doris.jni.plugin.dir";

    /**
     * Set the same way, from the {@code jni_plugin_hadoop_conf_dir} BE config. The hadoop
     * configuration files every plugin can read; see the DorisPluginClassLoader constructor for
     * why a plugin needs a directory of its own for them.
     */
    private static final String HADOOP_CONF_DIR_PROPERTY = "doris.jni.hadoop.conf.dir";

    // Keep in sync with the BE configs jni_plugin_dir and jni_plugin_hadoop_conf_dir. These are the
    // values a BE started without the properties above gets, and PluginRegistryDefaultDirTest is
    // what keeps them from drifting.
    private static final String DEFAULT_PLUGIN_SUBDIR = "plugins/jni";
    private static final String DEFAULT_HADOOP_CONF_SUBDIR = "plugins/hadoop_conf";

    private static volatile PluginRuntime runtime;

    private PluginRegistry() {
    }

    /**
     * Creates a scanner or a writer.
     *
     * @param plugin      plugin directory name, for example {@code paimon}
     * @param factoryName factory name within that plugin
     * @param batchSize   rows per batch
     * @param params      scan or write parameters; see the SPI's PROTOCOL.md for the reserved keys
     */
    public static Object createInstance(String plugin, String factoryName, int batchSize,
            Map<String, String> params) {
        return runtime().createInstance(plugin, factoryName, batchSize, params);
    }

    /** Creates a UDF, UDAF or UDTF executor; the kind is the factory name. */
    public static Object createUdfExecutor(String plugin, String factoryName, byte[] thriftParams)
            throws Exception {
        return runtime().createUdfExecutor(plugin, factoryName, thriftParams);
    }

    /**
     * Called on DROP FUNCTION so plugins can release what they compiled for that function.
     *
     * @param functionId        FE's id for the dropped function; the identity plugins cache by
     * @param functionSignature its rendered signature, for logs and for the id-less case
     */
    public static void cleanUdfCache(long functionId, String functionSignature) {
        runtime().cleanUdfCache(functionId, functionSignature);
    }

    /** State of every plugin loaded so far, as JSON, for BE to surface. */
    public static String pluginStatusJson() {
        return runtime().statusJson();
    }

    /**
     * Loads every deployed plugin now instead of on first use.
     *
     * <p>BE calls this after the JVM comes up when warmup is enabled, so that a plugin broken by a
     * bad deployment is in the log before any user query hits it. It never throws: each plugin's
     * failure stays attached to that plugin.
     */
    public static void warmup() {
        runtime().warmup();
    }

    private static PluginRuntime runtime() {
        PluginRuntime current = runtime;
        if (current != null) {
            return current;
        }
        synchronized (PluginRegistry.class) {
            if (runtime == null) {
                JniLogging.configure();
                runtime = new PluginRuntime(pluginDir(), PluginRegistry.class.getClassLoader(),
                        hadoopConfDir());
            }
            return runtime;
        }
    }

    // Package-private rather than private so PluginRegistryDefaultDirTest can pin the default.
    static Path pluginDir() {
        return directory(PLUGIN_DIR_PROPERTY, DEFAULT_PLUGIN_SUBDIR);
    }

    /**
     * Where plugins read hadoop configuration files from.
     *
     * <p>Falls back to {@code HADOOP_CONF_DIR} when the plugin conf directory holds nothing. That
     * environment variable is how a deployment points at the cluster's real {@code /etc/hadoop/conf}
     * - it is what resolves an HDFS HA nameservice - and {@code bin/start_be.sh} puts it on the
     * <em>system</em> class path, which a plugin classloader cannot reach. Without this fallback an
     * upgrade turns every Java scanner on such a cluster into "unknown host: <nameservice>" while
     * the native reader keeps working, and nothing in the migration notes says to copy the files.
     *
     * <p>Deliberately a fallback and not a merge: two directories both answering
     * {@code core-site.xml} would make which one wins depend on classloader order, and
     * {@code Configuration.getResource} takes only the first. An operator who populates
     * {@code plugins/hadoop_conf} means it to be the answer - which is why the test is what that
     * directory HOLDS and not whether it exists: build.sh creates it empty in every output tree.
     */
    static Path hadoopConfDir() {
        return hadoopConfDir(System.getenv("HADOOP_CONF_DIR"));
    }

    /** The environment is a parameter so the fallback is testable; nothing else may pass it. */
    static Path hadoopConfDir(String hadoopConfDirEnv) {
        Path configured = directory(HADOOP_CONF_DIR_PROPERTY, DEFAULT_HADOOP_CONF_SUBDIR);
        // Only when nothing was configured and the default holds nothing. A directory an operator
        // named explicitly is the answer whether or not it exists yet - silently reading somewhere
        // else would be worse than reading nothing.
        //
        // Both halves of that test are narrower than they look, and both had to be: on a real BE
        // this fallback was unreachable. JvmLauncher::_build_options() pushes
        // -Ddoris.jni.hadoop.conf.dir unconditionally, from a config whose default is
        // <DORIS_HOME>/plugins/hadoop_conf, so "was a property set" is always true - hence
        // namedExplicitly(). And build.sh CREATES that directory in every output tree, so
        // "does it exist" is always true too - hence holdsFiles(). Getting either wrong turns an
        // upgrade on a cluster that resolves its HDFS HA nameservice through $HADOOP_CONF_DIR into
        // "unknown host: <nameservice>" on every Java scanner, while the native reader keeps
        // working.
        if (namedExplicitly() || holdsFiles(configured)) {
            return configured;
        }
        if (hadoopConfDirEnv != null && !hadoopConfDirEnv.trim().isEmpty()) {
            Path fallback = Paths.get(hadoopConfDirEnv.trim());
            if (Files.isDirectory(fallback)) {
                return fallback;
            }
        }
        // Returned rather than null so the caller logs the directory an operator was meant to fill.
        return configured;
    }

    /**
     * Whether the hadoop conf directory was named by somebody rather than defaulted into. The
     * property carrying the BE config's own default is indistinguishable from an operator setting
     * that same path by hand, and treating both as "defaulted" is the safe way round: the two name
     * the same directory, so the only thing at stake is whether an empty one suppresses the
     * environment fallback.
     */
    private static boolean namedExplicitly() {
        String property = System.getProperty(HADOOP_CONF_DIR_PROPERTY);
        return property != null && !property.trim().isEmpty()
                && !Paths.get(property.trim()).endsWith(DEFAULT_HADOOP_CONF_SUBDIR);
    }

    /** Whether the directory holds anything at all; an empty drop point is not an answer. */
    private static boolean holdsFiles(Path dir) {
        if (!Files.isDirectory(dir)) {
            return false;
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
            return entries.iterator().hasNext();
        } catch (IOException | RuntimeException e) {
            // Unreadable is not empty. An operator who filled this directory and got the
            // permissions wrong must not silently be served the environment's one instead.
            return true;
        }
    }

    private static Path directory(String property, String defaultSubdir) {
        String configured = System.getProperty(property);
        if (configured != null && !configured.trim().isEmpty()) {
            return Paths.get(configured.trim());
        }
        String dorisHome = System.getenv("DORIS_HOME");
        if (dorisHome != null && !dorisHome.trim().isEmpty()) {
            return Paths.get(dorisHome.trim(), defaultSubdir);
        }
        return Paths.get(new File("").getAbsolutePath(), defaultSubdir);
    }
}
