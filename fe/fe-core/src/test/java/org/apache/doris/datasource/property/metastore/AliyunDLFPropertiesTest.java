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

import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.metastore.MetastoreProperties.Type;

import com.google.common.collect.Maps;
import org.apache.paimon.options.Options;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.util.Map;

public class AliyunDLFPropertiesTest {

    @Before
    public void setUp() {
        Config.hadoop_config_dir = getClass().getClassLoader().getResource("property").getPath();
    }

    @Test
    public void testBasicProperties() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);

        Assert.assertEquals("test_access_key", options.get("dlf.catalog.accessKeyId"));
        Assert.assertEquals("test_secret_key", options.get("dlf.catalog.accessKeySecret"));
        Assert.assertEquals("cn-hangzhou", options.get("dlf.catalog.region"));
        Assert.assertEquals("test_uid", options.get("dlf.catalog.uid"));
        Assert.assertEquals("false", options.get("dlf.catalog.accessPublic"));
        Assert.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", options.get("dlf.catalog.endpoint"));
        Assert.assertEquals("DLF_ONLY", options.get("dlf.catalog.proxyMode"));
        Assert.assertEquals("false", options.get("dlf.catalog.createDefaultDBIfNotExist"));
    }

    @Test
    public void testCustomEndpoint() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.endpoint", "custom.endpoint.com");
        props.put("dlf.uid", "test_uid");
        props.put("dlf.access.public", "true");

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);

        Assert.assertEquals("custom.endpoint.com", options.get("dlf.catalog.endpoint"));
    }

    @Test
    public void testPublicAccess() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");
        props.put("dlf.access.public", "true");

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);

        Assert.assertEquals("dlf.cn-hangzhou.aliyuncs.com", options.get("dlf.catalog.endpoint"));
    }

    @Test
    public void testOtherDlfProperties() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");
        props.put("dlf.custom.property", "custom_value");

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);

        Assert.assertEquals("custom_value", options.get("dlf.custom.property"));
    }

    @Test
    public void testResourceConfigPropName() {
        Map<String, String> props = Maps.newHashMap();
        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Assert.assertEquals("dlf.resource_config", dlfProperties.getResourceConfigPropName());
    }

    @Test
    public void testEmptyProperties() {
        Map<String, String> props = Maps.newHashMap();
        try {
            MetastoreProperties.create(Type.DLF, props);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Any of these required properties should be mentioned in the error message
            Assert.assertTrue("Error message should mention the missing property",
                    e.getMessage().contains("is required"));
        }
    }

    @Test
    public void testMissingRequiredProperties() {
        Map<String, String> props = Maps.newHashMap();
        try {
            MetastoreProperties.create(Type.DLF, props);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Any of these required properties should be mentioned in the error message
            Assert.assertTrue("Error message should mention the missing property",
                    e.getMessage().contains("dlf.access_key") || 
                    e.getMessage().contains("dlf.catalog.accessKeyId"));
        }
    }

    @Test
    public void testMissingAccessKey() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");
        
        try {
            MetastoreProperties.create(Type.DLF, props);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Error message should mention access_key",
                    e.getMessage().contains("dlf.access_key") || 
                    e.getMessage().contains("dlf.catalog.accessKeyId"));
            Assert.assertTrue("Error message should mention it's required",
                    e.getMessage().contains("is required"));
        }
    }

    @Test
    public void testEmptyRequiredProperty() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "");  // Empty access key
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");
        
        try {
            MetastoreProperties.create(Type.DLF, props);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Error message should mention access_key",
                    e.getMessage().contains("dlf.access_key") || 
                    e.getMessage().contains("dlf.catalog.accessKeyId"));
            Assert.assertTrue("Error message should mention it's required",
                    e.getMessage().contains("is required"));
        }
    }

    @Test
    public void testMissingSecretKey() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");
        
        try {
            MetastoreProperties.create(Type.DLF, props);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Error message should mention secret_key",
                    e.getMessage().contains("dlf.secret_key") || 
                    e.getMessage().contains("dlf.catalog.accessKeySecret"));
            Assert.assertTrue("Error message should mention it's required",
                    e.getMessage().contains("is required"));
        }
    }

    @Test
    public void testMissingRegion() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.uid", "test_uid");
        
        try {
            MetastoreProperties.create(Type.DLF, props);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Error message should mention region",
                    e.getMessage().contains("dlf.region"));
            Assert.assertTrue("Error message should mention it's required",
                    e.getMessage().contains("is required"));
        }
    }

    @Test
    public void testMissingUid() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        
        try {
            MetastoreProperties.create(Type.DLF, props);
            Assert.fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Error message should mention uid",
                    e.getMessage().contains("dlf.uid") || 
                    e.getMessage().contains("dlf.catalog.uid"));
            Assert.assertTrue("Error message should mention it's required",
                    e.getMessage().contains("is required"));
        }
    }

    @Test
    public void testLoadConfigFromFile() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.resource_config", "dlf.xml");
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        // Verify that the properties are loaded correctly
        // Note: This assumes the config file exists and contains certain properties
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);
        Assert.assertEquals("test_access_key", options.get("dlf.catalog.accessKeyId"));
        // read from dlf.xml
        Assert.assertEquals("123456789", options.get("dlf.catalog.uid"));
        Assert.assertEquals("test.endpoint", options.get("dlf.catalog.endpoint"));
        // override by direct properties
        Assert.assertEquals("test_access_key", options.get("dlf.catalog.accessKeyId"));
    }

    @Test
    public void testPropertyOverride() {
        // Test that properties from original props override those from config file
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.resource_config", "test-dlf-config.xml");
        props.put("dlf.access_key", "override_access_key");  // This should override any value from config file
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);
        Assert.assertEquals("override_access_key", options.get("dlf.catalog.accessKeyId"));
    }

    @Test
    public void testHash() {
        // 给定的字符串
        String key = "export_p0_test_label";

        // 计算 hash 值
        int hash = spread(key.hashCode());

        // 打印结果
        System.out.println("Key: " + key);
        System.out.println("Key.hashCode(): " + key.hashCode());
        System.out.println("Spread Hash: " + hash);
        System.out.println("Hash: " + ((1024 - 1) & hash));
    }

    private static int spread(int h) {
        return (h ^ (h >>> 16)) & 0x7fffffff; // 保证结果为非负数
    }

    @Test
    public void testAlternativePropertyNames() {
        Map<String, String> props = Maps.newHashMap();
        // Test alternative property name "dlf.catalog.accessKeyId" instead of "dlf.access_key"
        props.put("dlf.catalog.accessKeyId", "test_access_key");
        props.put("dlf.catalog.accessKeySecret", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);
        Assert.assertEquals("test_access_key", options.get("dlf.catalog.accessKeyId"));
    }

    @Test
    public void testNonRequiredProperties() {
        Map<String, String> props = Maps.newHashMap();
        // Set all required properties
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");
        // Don't set non-required property dlf.endpoint

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);
        // Should still work and use default endpoint
        Assert.assertEquals("dlf-vpc.cn-hangzhou.aliyuncs.com", options.get("dlf.catalog.endpoint"));
    }

    @Test
    public void testEmptyResourceConfig() {
        Map<String, String> props = Maps.newHashMap();
        props.put("dlf.access_key", "test_access_key");
        props.put("dlf.secret_key", "test_secret_key");
        props.put("dlf.region", "cn-hangzhou");
        props.put("dlf.uid", "test_uid");
        props.put("dlf.resource_config", "");  // Empty resource config

        AliyunDLFProperties dlfProperties = (AliyunDLFProperties) MetastoreProperties.create(Type.DLF, props);
        Options options = new Options();
        dlfProperties.toPaimonOptions(options);
        // Should still work with direct properties
        Assert.assertEquals("test_access_key", options.get("dlf.catalog.accessKeyId"));
    }
} 
