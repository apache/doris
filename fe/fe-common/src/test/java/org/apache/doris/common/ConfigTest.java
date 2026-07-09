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
    public void testRejectDeprecatedInvertedIndexV1WithWhitespace() throws Exception {
        String originFormat = Config.inverted_index_storage_format;
        try {
            ConfigBase.setMutableConfig("inverted_index_storage_format", "V2");
            ConfigException dynamicException = Assert.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("inverted_index_storage_format", " V1 "));
            Assert.assertTrue(dynamicException.getMessage().contains("Inverted index V1 is deprecated"));
            Assert.assertEquals("V2", Config.inverted_index_storage_format);

            Config.inverted_index_storage_format = "V2";
            ConfigException startupException = Assert.assertThrows(ConfigException.class,
                    () -> new ConfigBase.RejectStartupInvertedIndexV1Handler().handle(
                            Config.class.getField("inverted_index_storage_format"), " V1 "));
            Assert.assertTrue(startupException.getMessage().contains("inverted_index_storage_format=V1"));
            Assert.assertEquals("V2", Config.inverted_index_storage_format);
        } finally {
            Config.inverted_index_storage_format = originFormat;
        }
    }
}
