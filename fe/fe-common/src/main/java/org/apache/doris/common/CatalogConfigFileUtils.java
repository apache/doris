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
import java.util.function.Supplier;

public class CatalogConfigFileUtils {

    /**
     * Generic method to load configuration files (e.g., Hadoop or Hive) from a directory.
     *
     * @param resourcesPath     Comma-separated list of resource file names to be loaded.
     * @param configDir         Directory prefix where the configuration files reside.
     * @param configSupplier    Supplier that creates a new configuration object
     *                          (e.g., new Configuration or new HiveConf).
     * @param addResourceMethod Method to add a resource file to the configuration object.
     * @param <T>               Type of the configuration (e.g., Configuration or HiveConf).
     * @return A configuration object loaded with the given resource files.
     * @throws IllegalArgumentException if the resourcesPath is empty or if any file does not exist.
     */
    private static <T> T loadConfigFromDir(String resourcesPath, String configDir,
                                           Supplier<T> configSupplier,
                                           BiConsumer<T, Path> addResourceMethod) {
        // Check if the provided resourcesPath is blank and throw an exception if so.
        if (StringUtils.isBlank(resourcesPath)) {
            throw new IllegalArgumentException("Config resource path is empty");
        }

        // Create a new configuration object.
        T conf = configSupplier.get();

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
     * Loads a Hadoop Configuration object from a list of files under the specified config directory.
     *
     * @param resourcesPath Comma-separated list of file names to be loaded.
     * @return A Hadoop Configuration object.
     * @throws IllegalArgumentException if the input is invalid or files are missing.
     */
    public static Configuration loadConfigurationFromHadoopConfDir(String resourcesPath) {
        return loadConfigFromDir(
                resourcesPath,
                Config.hadoop_config_dir,
                Configuration::new,
                Configuration::addResource
        );
    }

    /**
     * Loads a HiveConf object from a list of files under the specified config directory.
     *
     * @param resourcesPath Comma-separated list of file names to be loaded.
     * @return A HiveConf object.
     * @throws IllegalArgumentException if the input is invalid or files are missing.
     */
    public static HiveConf loadHiveConfFromHiveConfDir(String resourcesPath) {
        return loadConfigFromDir(
                resourcesPath,
                Config.hadoop_config_dir,
                HiveConf::new,
                HiveConf::addResource
        );
    }
}
