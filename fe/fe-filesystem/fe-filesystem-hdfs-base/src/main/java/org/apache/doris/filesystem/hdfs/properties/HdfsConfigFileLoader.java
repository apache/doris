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

package org.apache.doris.filesystem.hdfs.properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads Hadoop xml config files (hdfs-site.xml / core-site.xml ...) into a key-value map.
 *
 * <p>Faithful port of the Hadoop branch of fe-common's {@code CatalogConfigFileUtils},
 * minus the HiveConf path and the direct dependency on fe-core's {@code Config}. The config
 * directory prefix is supplied by the caller (sourced from the {@code _HADOOP_CONFIG_DIR_}
 * property that fe-core injects) instead of being read from {@code Config.hadoop_config_dir}.
 */
public final class HdfsConfigFileLoader {

    private HdfsConfigFileLoader() {
    }

    public static Map<String, String> load(String resourcesPath, String configDir) {
        Map<String, String> result = new HashMap<>();
        if (StringUtils.isBlank(resourcesPath)) {
            return result;
        }
        String dir = configDir == null ? "" : configDir;
        Configuration conf = new Configuration();
        for (String resource : resourcesPath.split(",")) {
            String path = dir + resource.trim();
            File file = new File(path);
            if (file.exists() && file.isFile()) {
                conf.addResource(new Path(file.toURI()));
            } else {
                throw new IllegalArgumentException("Config resource file does not exist: " + path);
            }
        }
        for (Map.Entry<String, String> entry : conf) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
