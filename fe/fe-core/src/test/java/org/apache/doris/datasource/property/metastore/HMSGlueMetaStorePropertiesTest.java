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

package org.apache.doris.datasource.property.metastore;

import com.google.common.collect.ImmutableBiMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HMSGlueMetaStorePropertiesTest {
    private HiveGlueMetaStoreProperties properties;

    @BeforeEach
    public void setUp() {
        Map<String, String> config = ImmutableBiMap.of(
                "aws.glue.endpoint", "https://glue.us-west-2.amazonaws.com",
                "aws.region", "us-west-2",
                "aws.glue.session-token", "dummy-session-token",
                "aws.glue.access-key", "dummy-access-key",
                "aws.glue.secret-key", "dummy-secret-key",
                "aws.glue.max-error-retries", "10",
                "aws.glue.max-connections", "20",
                "aws.glue.connection-timeout", "60000",
                "aws.glue.socket-timeout", "45000",
                "aws.glue.catalog.separator", "::"
        );
        properties = new HiveGlueMetaStoreProperties(config);
    }

    @Test
    public void testInitNormalizeAndCheckPropsSetsHiveConfCorrectly() {
        properties.initNormalizeAndCheckProps();
        HiveConf hiveConf = properties.getHiveConf();
        Assertions.assertEquals("https://glue.us-west-2.amazonaws.com", hiveConf.get("aws.glue.endpoint"));
        Assertions.assertEquals("us-west-2", hiveConf.get("aws.region"));
        Assertions.assertEquals("dummy-session-token", hiveConf.get("aws.glue.session-token"));
        Assertions.assertEquals("dummy-access-key", hiveConf.get("aws.glue.access-key"));
        Assertions.assertEquals("dummy-secret-key", hiveConf.get("aws.glue.secret-key"));
        Assertions.assertEquals("10", hiveConf.get("aws.glue.max-error-retries"));
        Assertions.assertEquals("20", hiveConf.get("aws.glue.max-connections"));
        Assertions.assertEquals("60000", hiveConf.get("aws.glue.connection-timeout"));
        Assertions.assertEquals("45000", hiveConf.get("aws.glue.socket-timeout"));
        Assertions.assertEquals("::", hiveConf.get("aws.glue.catalog.separator"));
        Assertions.assertEquals("glue", hiveConf.get("hive.metastore.type"));
    }

    @Test
    public void testConstructorSetsTypeCorrectly() {
        Assertions.assertEquals(AbstractHiveProperties.Type.GLUE, properties.getType());
    }

    @Test
    public void testMissingRequiredPropertyThrows() {
        Map<String, String> incompleteConfig = new HashMap<>(properties.getOrigProps());
        incompleteConfig.remove("aws.glue.secret-key");
        HiveGlueMetaStoreProperties props = new HiveGlueMetaStoreProperties(incompleteConfig);
        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, props::initNormalizeAndCheckProps);
        Assertions.assertTrue(exception.getMessage().contains("glue.access_key and glue.secret_key must be set together"));
    }

    @Test
    public void testInvalidNumberPropertyThrows() {
        Map<String, String> invalidConfig = new HashMap<>(properties.getOrigProps());
        invalidConfig.put("aws.glue.max-error-retries", "notANumber");
        HiveGlueMetaStoreProperties props = new HiveGlueMetaStoreProperties(invalidConfig);
        Exception exception = Assertions.assertThrows(RuntimeException.class, props::initNormalizeAndCheckProps);
        Assertions.assertTrue(exception.getMessage().contains("Failed to set property"));
    }

    @Test
    public void testDefaultValuesAreAppliedWhenMissing() {
        Map<String, String> partialConfig = new HashMap<>(properties.getOrigProps());
        partialConfig.remove("aws.glue.max-error-retries");
        HiveGlueMetaStoreProperties props = new HiveGlueMetaStoreProperties(partialConfig);
        props.initNormalizeAndCheckProps();
        HiveConf hiveConf = props.getHiveConf();
        Assertions.assertEquals(String.valueOf(HiveGlueMetaStoreProperties.DEFAULT_MAX_RETRY), hiveConf.get("aws.glue.max-error-retries"));
    }

    @Test
    public void testUnsupportedPropertyIsIgnored() {
        Map<String, String> configWithExtra = new HashMap<>(properties.getOrigProps());
        configWithExtra.put("some.unsupported.property", "value");
        HiveGlueMetaStoreProperties props = new HiveGlueMetaStoreProperties(configWithExtra);
        props.initNormalizeAndCheckProps();
        HiveConf hiveConf = props.getHiveConf();
        Assertions.assertNull(hiveConf.get("some.unsupported.property"));
    }
}
