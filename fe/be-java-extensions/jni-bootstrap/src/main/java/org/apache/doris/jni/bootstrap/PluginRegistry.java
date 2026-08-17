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

    /** Called on DROP FUNCTION so plugins can release what they compiled for that signature. */
    public static void cleanUdfCache(String functionSignature) {
        runtime().cleanUdfCache(functionSignature);
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

    static Path hadoopConfDir() {
        return directory(HADOOP_CONF_DIR_PROPERTY, DEFAULT_HADOOP_CONF_SUBDIR);
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
