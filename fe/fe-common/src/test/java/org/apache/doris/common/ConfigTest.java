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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class ConfigTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config config = new Config();
        // create an empty config file to initialize Config
        Path tempFile = Files.createTempFile("fe_ut_", ".conf");
        tempFile.toFile().deleteOnExit();
        config.init(tempFile.toAbsolutePath().toString());
    }

    // A sensitive config (fe_meta_auth_token) must never be dumped in plaintext by any config
    // API: both Config.dump() and ConfigBase.getConfigInfo() return the mask instead of the value.
    @Test
    public void testSensitiveConfigIsMaskedWhenSet() {
        String old = Config.fe_meta_auth_token;
        try {
            Config.fe_meta_auth_token = "super-secret-token";

            Map<String, String> dumped = ConfigBase.dump();
            Assert.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, dumped.get("fe_meta_auth_token"));

            String value = configInfoValue("fe_meta_auth_token");
            Assert.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, value);
        } finally {
            Config.fe_meta_auth_token = old;
        }
    }

    // The legacy cluster secret auth_token is also marked sensitive, so it is masked by every
    // config dump API too (it leaks through /rest/v1/config/fe otherwise).
    @Test
    public void testAuthTokenIsMaskedWhenSet() {
        String old = Config.auth_token;
        try {
            Config.auth_token = "super-secret-auth-token";

            Assert.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, ConfigBase.dump().get("auth_token"));
            Assert.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, configInfoValue("auth_token"));
        } finally {
            Config.auth_token = old;
        }
    }

    // An empty sensitive config is left as-is (no secret to hide), so "unset" stays visible.
    @Test
    public void testEmptySensitiveConfigIsNotMasked() {
        String old = Config.fe_meta_auth_token;
        try {
            Config.fe_meta_auth_token = "";

            Assert.assertEquals("", ConfigBase.dump().get("fe_meta_auth_token"));
            Assert.assertEquals("", configInfoValue("fe_meta_auth_token"));
        } finally {
            Config.fe_meta_auth_token = old;
        }
    }

    private static String configInfoValue(String key) {
        for (List<String> row : ConfigBase.getConfigInfo(null)) {
            if (row.get(0).equals(key)) {
                return row.get(1);
            }
        }
        throw new IllegalStateException("config not found: " + key);
    }

    @Test
    public void testSetEmptyArray() throws ConfigException {
        ConfigBase.setMutableConfig("s3_load_endpoint_white_list", "a,b,c");
        ConfigBase.setMutableConfig("s3_load_endpoint_white_list", "");
        Assert.assertEquals("array length should be 0", 0, Config.s3_load_endpoint_white_list.length);
    }

    // File-path and jdbc-driver security configs must only be settable in fe.conf (ops), never at runtime
    // via ADMIN SET FRONTEND CONFIG. setMutableConfig is exactly that runtime entrypoint, so it must reject them.
    @Test
    public void testSecurityPathConfigsAreNotRuntimeMutable() {
        String[] opsOnlyConfigs = {
                "jdbc_driver_url_white_list",
                "jdbc_drivers_dir",
                "jdbc_driver_secure_path",
                "tmp_dir",
                "plugin_dir",
        };
        for (String key : opsOnlyConfigs) {
            ConfigException e = Assert.assertThrows(key + " should not be runtime-mutable",
                    ConfigException.class, () -> ConfigBase.setMutableConfig(key, "x"));
            Assert.assertTrue(e.getMessage().contains("is not mutable"));
        }
    }
}
