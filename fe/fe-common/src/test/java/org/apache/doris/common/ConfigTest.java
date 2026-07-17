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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class ConfigTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config config = new Config();
        // create an empty config file to initialize Config
        Path tempFile = Files.createTempFile("fe_ut_", ".conf");
        tempFile.toFile().deleteOnExit();
        config.init(tempFile.toAbsolutePath().toString());
    }

    @Test
    public void testSetEmptyArray() throws ConfigException {
        ConfigBase.setMutableConfig("s3_load_endpoint_white_list", "a,b,c");
        ConfigBase.setMutableConfig("s3_load_endpoint_white_list", "");
        Assert.assertEquals("array length should be 0", 0, Config.s3_load_endpoint_white_list.length);
    }

    @Test
    public void testInvalidS3ClientHttpScheme() throws Exception {
        String originalScheme = Config.s3_client_http_scheme;
        Path tempFile = Files.createTempFile("fe_invalid_s3_scheme_", ".conf");
        try {
            Files.write(tempFile, "s3_client_http_scheme = ftp".getBytes(StandardCharsets.UTF_8));
            Config config = new Config();
            try {
                config.init(tempFile.toAbsolutePath().toString());
                Assert.fail("Expected invalid s3_client_http_scheme to fail FE configuration initialization");
            } catch (IllegalArgumentException e) {
                Assert.assertEquals("Invalid s3_client_http_scheme: ftp, only http and https are supported",
                        e.getMessage());
            }
        } finally {
            Config.s3_client_http_scheme = originalScheme;
            Files.deleteIfExists(tempFile);
        }
    }
}
