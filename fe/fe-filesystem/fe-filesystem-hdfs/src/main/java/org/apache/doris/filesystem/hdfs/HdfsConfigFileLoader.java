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

package org.apache.doris.filesystem.hdfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads Hadoop XML configuration files (e.g. {@code hdfs-site.xml} / {@code core-site.xml}) referenced by the
 * {@code hadoop.config.resources} property into a key-value map. This mirrors the legacy fe-core
 * {@code CatalogConfigFileUtils.loadConfigurationFromHadoopConfDir} but lives in fe-filesystem-hdfs so the
 * module stays a leaf that does not depend on fe-core / fe-common.
 *
 * <p>The base directory under which the named resource files are resolved is computed by
 * {@link #resolveHadoopConfigDir()}: the operator-configured {@code Config.hadoop_config_dir}, bridged in via
 * the {@link #CONFIG_DIR_PROPERTY} system property (a plugin leaf cannot import fe-core {@code Config}), with a
 * {@code $DORIS_HOME/plugins/hadoop_conf/} fallback that matches {@code Config.hadoop_config_dir}'s own default.
 * hadoop-client is already on this module's classpath, so the Configuration parsing needs no extra dependency.
 */
public final class HdfsConfigFileLoader {

    /**
     * System property the engine sets to {@code Config.hadoop_config_dir} so this plugin leaf resolves
     * {@code hadoop.config.resources} files under the operator-configured directory. fe-core
     * {@code FileSystemFactory.bindAllStorageProperties} sets it before binding; keep the key in sync there.
     */
    public static final String CONFIG_DIR_PROPERTY = "doris.hadoop.config.dir";

    /**
     * Optional explicit override (used by tests). When blank, the directory is resolved from
     * {@link #CONFIG_DIR_PROPERTY}, falling back to {@code $DORIS_HOME/plugins/hadoop_conf/}.
     */
    public static volatile String hadoopConfigDirOverride = null;

    private HdfsConfigFileLoader() {
    }

    static String resolveHadoopConfigDir() {
        if (StringUtils.isNotBlank(hadoopConfigDirOverride)) {
            return hadoopConfigDirOverride;
        }
        String fromEngine = System.getProperty(CONFIG_DIR_PROPERTY);
        if (StringUtils.isNotBlank(fromEngine)) {
            return fromEngine;
        }
        String home = System.getenv("DORIS_HOME");
        if (StringUtils.isBlank(home)) {
            home = System.getProperty("doris.home", "");
        }
        return home + "/plugins/hadoop_conf/";
    }

    /**
     * Loads the comma-separated config files (each resolved under {@link #hadoopConfigDir}) and returns all
     * resolved Hadoop configuration entries as a mutable map. Returns an empty map when {@code resourcesPath}
     * is blank. The underlying {@link Configuration} is created with Hadoop's defaults loaded, matching the
     * legacy behavior (the BE receives the full resolved set).
     *
     * @param resourcesPath comma-separated list of config file names; may be blank
     * @return a mutable map of the loaded Hadoop configuration entries (never null)
     * @throws IllegalArgumentException if a referenced file is missing
     */
    public static Map<String, String> loadConfigMap(String resourcesPath) {
        Map<String, String> confMap = new HashMap<>();
        if (StringUtils.isBlank(resourcesPath)) {
            return confMap;
        }
        Configuration conf = new Configuration();
        String baseDir = resolveHadoopConfigDir();
        for (String resource : resourcesPath.split(",")) {
            String resourcePath = baseDir + resource.trim();
            File file = new File(resourcePath);
            if (file.exists() && file.isFile()) {
                conf.addResource(new Path(file.toURI()));
            } else {
                throw new IllegalArgumentException("Config resource file does not exist: " + resourcePath);
            }
        }
        for (Map.Entry<String, String> entry : conf) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        return confMap;
    }
}
