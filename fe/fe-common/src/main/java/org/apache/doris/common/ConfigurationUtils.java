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

import java.io.File;

public class ConfigurationUtils {

    /**
     * Loads the Hadoop configuration files from the specified directory.
     * <p>
     * This method reads a comma-separated list of resource files from the given
     * `resourcesPath`, constructs their absolute paths based on the `Config.external_catalog_config_dir`,
     * and then loads these files into a Hadoop `Configuration` object.
     *
     * @param resourcesPath The comma-separated list of Hadoop configuration resource files to load.
     *                      This must not be null or empty.
     * @return The Hadoop `Configuration` object with the loaded configuration files.
     * @throws IllegalArgumentException If the provided `resourcesPath` is blank, or if any of the specified
     *                                  configuration files do not exist or are not regular files.
     */
    public static Configuration loadConfigurationFromHadoopConfDir(String resourcesPath) {
        // Check if the provided resourcesPath is blank and throw an exception if so.
        if (StringUtils.isBlank(resourcesPath)) {
            throw new IllegalArgumentException("Hadoop config resource path is empty");
        }

        // Create a new Hadoop Configuration object without loading default resources.
        Configuration conf = new Configuration(false);

        // Iterate over the comma-separated list of resource files.
        for (String resource : resourcesPath.split(",")) {
            // Construct the full path to the resource file.
            String resourcePath = Config.hadoop_config_dir + File.separator + resource.trim();
            File file = new File(resourcePath);

            // Check if the file exists and is a regular file; if not, throw an exception.
            if (file.exists() && file.isFile()) {
                // Add the resource file to the Hadoop Configuration object.
                conf.addResource(new Path(file.toURI()));
            } else {
                // Throw an exception if the file does not exist or is not a regular file.
                throw new IllegalArgumentException("Hadoop config resource file does not exist: " + resourcePath);
            }
        }
        // Return the populated Hadoop Configuration object.
        return conf;
    }
}