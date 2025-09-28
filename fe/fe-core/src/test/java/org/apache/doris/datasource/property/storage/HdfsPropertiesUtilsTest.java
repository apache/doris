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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HdfsPropertiesUtilsTest {

    private static final Set<String> supportSchema = ImmutableSet.of("hdfs", "viewfs");

    @Test
    public void testCheckLoadPropsAndReturnUri_success() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "hdfs://localhost:9000/data/file.txt");

        String result = HdfsPropertiesUtils.validateAndGetUri(props, null, null, supportSchema);
        Assertions.assertEquals("hdfs://localhost:9000/data/file.txt", result);
    }

    @Test
    public void testCheckLoadPropsAndReturnUri_emptyProps() {
        Map<String, String> props = new HashMap<>();

        Exception exception = Assertions.assertThrows(UserException.class, () -> {
            HdfsPropertiesUtils.validateAndGetUri(props, null, null, supportSchema);
        });
        Assertions.assertEquals("errCode = 2, detailMessage = props is empty", exception.getMessage());
    }

    @Test
    public void testCheckLoadPropsAndReturnUri_missingUriKey() {
        Map<String, String> props = new HashMap<>();
        props.put("path", "xxx");

        Exception exception = Assertions.assertThrows(StoragePropertiesException.class, () -> {
            HdfsPropertiesUtils.validateAndGetUri(props, "", "", supportSchema);
        });
        Assertions.assertEquals("props must contain uri", exception.getMessage());
    }

    @Test
    public void testConvertUrlToFilePath_valid() throws Exception {
        String uri = "viewfs://cluster/user/test";
        String result = HdfsPropertiesUtils.convertUrlToFilePath(uri, "", supportSchema);
        Assertions.assertEquals("viewfs://cluster/user/test", result);
    }

    @Test
    public void testConvertUrlToFilePath_invalidSchema() {
        String uri = "s3://bucket/file.txt";

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            HdfsPropertiesUtils.convertUrlToFilePath(uri, "", supportSchema);
        });
        Assertions.assertTrue(exception.getMessage().contains("Unsupported schema: s3"));
    }

    @Test
    public void testConvertUrlToFilePath_blankUri() {
        String uri = "   ";

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            HdfsPropertiesUtils.convertUrlToFilePath(uri, "", supportSchema);
        });
        Assertions.assertTrue(exception.getMessage().contains("Property 'uri' is required"));
    }

    @Test
    public void testConstructDefaultFsFromUri_valid() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "hdfs://localhost:8020/data");

        String result = HdfsPropertiesUtils.extractDefaultFsFromUri(props, supportSchema);
        Assertions.assertEquals("hdfs://localhost:8020", result);
    }

    @Test
    public void testConstructDefaultFsFromUri_viewfs() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "viewfs://cluster/path");

        String result = HdfsPropertiesUtils.extractDefaultFsFromUri(props, supportSchema);
        Assertions.assertEquals("viewfs://cluster", result);
    }

    @Test
    public void testConstructDefaultFsFromUri_invalidSchema() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "obs://bucket/test");
        Assertions.assertNull(HdfsPropertiesUtils.extractDefaultFsFromUri(props, supportSchema));
    }

    @Test
    public void testConstructDefaultFsFromUri_emptyProps() {
        Map<String, String> props = new HashMap<>();
        String result = HdfsPropertiesUtils.extractDefaultFsFromUri(props, supportSchema);
        Assertions.assertNull(result);
    }

    @Test
    public void testConstructDefaultFsFromUri_missingUri() {
        Map<String, String> props = new HashMap<>();
        props.put("x", "y");

        String result = HdfsPropertiesUtils.extractDefaultFsFromUri(props, supportSchema);
        Assertions.assertNull(result);
    }

    @Test
    public void testConstructDefaultFsFromUri_blankUri() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "  ");

        String result = HdfsPropertiesUtils.extractDefaultFsFromUri(props, supportSchema);
        Assertions.assertNull(result);
    }

    @Test
    public void testConvertUrlToFilePath_uppercaseSchema() throws Exception {
        String uri = "HDFS://localhost:9000/test";
        String result = HdfsPropertiesUtils.convertUrlToFilePath(uri, "", supportSchema);
        Assertions.assertEquals("HDFS://localhost:9000/test", result);
    }

    @Test
    public void testValidHaConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("dfs.nameservices", "ns1");
        config.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        config.put("dfs.namenode.rpc-address.ns1.nn1", "127.0.0.1:8020");
        config.put("dfs.namenode.rpc-address.ns1.nn2", "127.0.0.2:8020");
        config.put("dfs.client.failover.proxy.provider.ns1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        // Valid HA configuration should pass without exception
        Assertions.assertDoesNotThrow(() -> HdfsPropertiesUtils.checkHaConfig(config));
    }

    @Test
    public void testNoNameservices() {
        Map<String, String> config = new HashMap<>();
        // No dfs.nameservices configured → not HA mode, should not throw
        Assertions.assertDoesNotThrow(() -> HdfsPropertiesUtils.checkHaConfig(config));
    }

    @Test
    public void testMissingHaNamenodes() {
        Map<String, String> config = new HashMap<>();
        config.put("dfs.nameservices", "ns1");
        // dfs.ha.namenodes.ns1 missing
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HdfsPropertiesUtils.checkHaConfig(config));
        Assertions.assertTrue(ex.getMessage().contains("dfs.ha.namenodes.ns1"));
    }

    @Test
    public void testNotEnoughNamenodes() {
        Map<String, String> config = new HashMap<>();
        config.put("dfs.nameservices", "ns1");
        config.put("dfs.ha.namenodes.ns1", "nn1"); // Only 1 namenode
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HdfsPropertiesUtils.checkHaConfig(config));
        Assertions.assertTrue(ex.getMessage().contains("HA requires at least 2 namenodes"));
    }

    @Test
    public void testMissingRpcAddress() {
        Map<String, String> config = new HashMap<>();
        config.put("dfs.nameservices", "ns1");
        config.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        config.put("dfs.namenode.rpc-address.ns1.nn1", "127.0.0.1:8020");
        // nn2 rpc-address missing
        config.put("dfs.client.failover.proxy.provider.ns1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HdfsPropertiesUtils.checkHaConfig(config));
        Assertions.assertTrue(ex.getMessage().contains("dfs.namenode.rpc-address.ns1.nn2"));
    }

    @Test
    public void testMissingFailoverProvider() {
        Map<String, String> config = new HashMap<>();
        config.put("dfs.nameservices", "ns1");
        config.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        config.put("dfs.namenode.rpc-address.ns1.nn1", "127.0.0.1:8020");
        config.put("dfs.namenode.rpc-address.ns1.nn2", "127.0.0.2:8020");
        // failover provider missing

        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HdfsPropertiesUtils.checkHaConfig(config));
        Assertions.assertTrue(ex.getMessage().contains("dfs.client.failover.proxy.provider.ns1"));
    }
}
