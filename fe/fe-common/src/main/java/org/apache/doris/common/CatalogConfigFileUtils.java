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

package org.apache.doris.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.File;
import java.util.function.BiConsumer;

public class CatalogConfigFileUtils {

    /**
     * Loads configuration files from the specified directory into a Hadoop Configuration or HiveConf object.
     *
     * @param resourcesPath The comma-separated list of configuration resource files to load.
     *                      This must not be null or empty.
     * @param configDir The base directory where the configuration files are located.
     * @param addResourceMethod A method reference to add the resource to the configuration.
     * @param <T> The type of configuration object (either Hadoop Configuration or HiveConf).
     * @return The populated configuration object.
     * @throws IllegalArgumentException If the provided resourcesPath is blank, or if any of the specified
     *                                  configuration files do not exist or are not regular files.
     */
    private static <T> T loadConfigFromDir(String resourcesPath, String configDir,
                                           BiConsumer<T, Path> addResourceMethod) {
        // Check if the provided resourcesPath is blank and throw an exception if so.
        if (StringUtils.isBlank(resourcesPath)) {
            throw new IllegalArgumentException("Config resource path is empty");
        }

        // Create a new configuration object.
        T conf = (T) (configDir.equals(Config.hadoop_config_dir) ? new Configuration(false) : new HiveConf());

        // Iterate over the comma-separated list of resource files.
        for (String resource : resourcesPath.split(",")) {
            // Construct the full path to the resource file.
            String resourcePath = configDir + resource.trim();
            File file = new File(resourcePath);

            // Check if the file exists and is a regular file; if not, throw an exception.
            if (file.exists() && file.isFile()) {
                // Add the resource file to the configuration object.
                addResourceMethod.accept(conf, new Path(file.toURI()));
            } else {
                // Throw an exception if the file does not exist or is not a regular file.
                throw new IllegalArgumentException("Config resource file does not exist: " + resourcePath);
            }
        }
        return conf;
    }

    /**
     * Loads the Hadoop configuration files from the specified directory.
     * @param resourcesPath The comma-separated list of Hadoop configuration resource files to load.
     * @return The Hadoop `Configuration` object with the loaded configuration files.
     * @throws IllegalArgumentException If the provided `resourcesPath` is blank, or if any of the specified
     *                                  configuration files do not exist or are not regular files.
     */
    public static Configuration loadConfigurationFromHadoopConfDir(String resourcesPath) {
        return loadConfigFromDir(resourcesPath, Config.hadoop_config_dir, Configuration::addResource);
    }

    /**
     * Loads the Hive configuration files from the specified directory.
     * @param resourcesPath The comma-separated list of Hive configuration resource files to load.
     * @return The HiveConf object with the loaded configuration files.
     * @throws IllegalArgumentException If the provided `resourcesPath` is blank, or if any of the specified
     *                                  configuration files do not exist or are not regular files.
     */
    public static HiveConf loadHiveConfFromHiveConfDir(String resourcesPath) {
        return loadConfigFromDir(resourcesPath, Config.hadoop_config_dir, HiveConf::addResource);
    }
}
