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

package org.apache.doris.property;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * Loads Hadoop XML configuration files (e.g. {@code hdfs-site.xml} / {@code core-site.xml}) referenced by
 * {@code hadoop.config.resources} into a Hadoop {@link Configuration}. This mirrors the legacy fe-core
 * {@code CatalogConfigFileUtils.loadConfigurationFromHadoopConfDir} but lives in fe-property so the module does
 * not depend on fe-core/fe-common.
 *
 * <p>The base directory under which the named resource files are resolved is {@link #hadoopConfigDir}. It defaults
 * to {@code $DORIS_HOME/plugins/hadoop_conf/} (matching legacy {@code Config.hadoop_config_dir}); the engine may
 * override it at startup. hadoop-common is a {@code provided} dependency of fe-property — present at compile time
 * and supplied at runtime by every consumer (fe-core, SPI connector plugins) — and is never packaged in this jar.
 */
public final class PropertyConfigLoader {

    /**
     * Base directory under which {@code hadoop.config.resources} file names are resolved. Defaults to
     * {@code $DORIS_HOME/plugins/hadoop_conf/}; the engine may overwrite it at startup to match its own
     * {@code Config.hadoop_config_dir}.
     */
    public static volatile String hadoopConfigDir = defaultHadoopConfigDir();

    private PropertyConfigLoader() {
    }

    private static String defaultHadoopConfigDir() {
        String home = System.getenv("DORIS_HOME");
        if (StringUtils.isBlank(home)) {
            home = System.getProperty("doris.home", "");
        }
        return home + "/plugins/hadoop_conf/";
    }

    /**
     * Loads a Hadoop {@link Configuration} from the comma-separated list of file names, each resolved under
     * {@link #hadoopConfigDir}.
     *
     * @param resourcesPath comma-separated list of config file names
     * @return a Hadoop Configuration with the named files added as resources
     * @throws IllegalArgumentException if the input is blank or a referenced file is missing
     */
    public static Configuration loadConfigurationFromHadoopConfDir(String resourcesPath) {
        if (StringUtils.isBlank(resourcesPath)) {
            throw new IllegalArgumentException("Config resource path is empty");
        }
        Configuration conf = new Configuration();
        for (String resource : resourcesPath.split(",")) {
            String resourcePath = hadoopConfigDir + resource.trim();
            File file = new File(resourcePath);
            if (file.exists() && file.isFile()) {
                conf.addResource(new Path(file.toURI()));
            } else {
                throw new IllegalArgumentException("Config resource file does not exist: " + resourcePath);
            }
        }
        return conf;
    }
}
