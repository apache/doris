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

package org.apache.doris.plugin;

import org.apache.doris.common.Config;
import org.apache.doris.common.EnvUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesUtils {

    public static final Logger LOG = LogManager.getLogger(PropertiesUtils.class);

    public static Map<String, String> loadAccessControllerPropertiesOrNull() throws IOException {
        String configFilePath = EnvUtils.getDorisHome() + Config.authorization_config_file_path;
        if (new File(configFilePath).exists()) {
            Properties properties = new Properties();
            properties.load(Files.newInputStream(Paths.get(configFilePath)));
            return propertiesToMap(properties);
        }
        return null;
    }

    public static Properties loadAuthenticationConfigFile() throws Exception {
        String configFilePath = EnvUtils.getDorisHome() + Config.authentication_config_file_path;
        if (new File(configFilePath).exists()) {
            LOG.info("Loading authenticate configuration file: {}", configFilePath);
            Properties properties = new Properties();
            properties.load(Files.newInputStream(Paths.get(configFilePath)));
            return properties;
        }
        return new Properties();
    }

    public static Map<String, String> propertiesToMap(Properties properties) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            map.put(key, value);
        }
        return map;
    }
}
