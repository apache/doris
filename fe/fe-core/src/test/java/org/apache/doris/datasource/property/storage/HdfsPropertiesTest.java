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

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsPropertiesTest {

    @Test
    public void testBasicHdfsCreate() throws UserException {
        // Test 1: Check default authentication type (should be "simple")
        Map<String, String> simpleHdfsProperties = new HashMap<>();
        simpleHdfsProperties.put("uri", "hdfs://test/1.orc");
        Assertions.assertEquals(HdfsProperties.class,  StorageProperties.createPrimary(simpleHdfsProperties).getClass());
        Map<String, String> origProps = createBaseHdfsProperties();
        List<StorageProperties> storageProperties = StorageProperties.createAll(origProps);
        HdfsProperties hdfsProperties = (HdfsProperties) storageProperties.get(0);
        Configuration conf = hdfsProperties.getHadoopStorageConfig();
        Assertions.assertEquals("simple", conf.get("hadoop.security.authentication"));

        // Test 2: Kerberos without necessary configurations (should throw exception)
        origProps.put("hdfs.authentication.type", "kerberos");
        assertKerberosConfigException(origProps, "HDFS authentication type is kerberos, but principal or keytab is not set");

        // Test 3: Kerberos with missing principal (should throw exception)
        origProps.put("hdfs.authentication.kerberos.principal", "hadoop");
        assertKerberosConfigException(origProps, "HDFS authentication type is kerberos, but principal or keytab is not set");

        // Test 4: Kerberos with complete config (should succeed)
        origProps.put("hdfs.authentication.kerberos.keytab", "keytab");
        HdfsProperties properties = (HdfsProperties) StorageProperties.createAll(origProps)
                .get(0);  // No exception expected
        Configuration configuration = properties.getHadoopStorageConfig();
        Assertions.assertEquals("kerberos", configuration.get("hdfs.security.authentication"));
        Assertions.assertEquals("hadoop", configuration.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("keytab", configuration.get("hadoop.kerberos.keytab"));
    }

    @Test
    public void testBasicHdfsPropertiesCreateByConfigFile() throws UserException {
        // Test 1: Check loading of config resources
        Map<String, String> origProps = createBaseHdfsProperties();
        URL hiveFileUrl = HdfsPropertiesTest.class.getClassLoader().getResource("plugins");
        Config.hadoop_config_dir = hiveFileUrl.getPath().toString() + "/hadoop_conf/";
        origProps.put("hadoop.config.resources", "hadoop/core-site.xml,hadoop/hdfs-site.xml");

        // Test 2: Missing config resources (should throw exception)
        assertConfigResourceException(origProps, "Config resource file does not exist");

        // Test 3: Valid config resources (should succeed)
        origProps.put("hadoop.config.resources", "hadoop1/core-site.xml,hadoop1/hdfs-site.xml");
        origProps.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        origProps.put("dfs.namenode.rpc-address.ns1.nn1", "localhost:9000");
        origProps.put("dfs.namenode.rpc-address.ns1.nn2", "localhost:9001");
        origProps.put("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        List<StorageProperties> storageProperties = StorageProperties.createAll(origProps);
        HdfsProperties hdfsProperties = (HdfsProperties) storageProperties.get(0);
        Configuration conf = hdfsProperties.getHadoopStorageConfig();
        Assertions.assertEquals("hdfs://localhost:9000", conf.get("fs.defaultFS"));
        Assertions.assertEquals("ns1", conf.get("dfs.nameservices"));

        // Test 4: Kerberos without necessary configurations (should throw exception)
        origProps.put("hdfs.authentication.type", "kerberos");
        assertKerberosConfigException(origProps, "HDFS authentication type is kerberos, but principal or keytab is not set");

        // Test 5: Kerberos with missing principal (should throw exception)
        origProps.put("hdfs.authentication.kerberos.principal", "hadoop");
        assertKerberosConfigException(origProps, "HDFS authentication type is kerberos, but principal or keytab is not set");

        // Test 6: Kerberos with complete config (should succeed)
        origProps.put("hdfs.authentication.kerberos.keytab", "keytab");
        hdfsProperties = (HdfsProperties) StorageProperties.createAll(origProps).get(0);  // No exception expected
        Configuration configuration = hdfsProperties.getHadoopStorageConfig();
        Assertions.assertEquals("kerberos", configuration.get("hdfs.security.authentication"));
        Assertions.assertEquals("hadoop", configuration.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("keytab", configuration.get("hadoop.kerberos.keytab"));
        Assertions.assertEquals("hdfs://localhost:9000", configuration.get("fs.defaultFS"));

    }

    @Test
    public void testNonParamsException() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        Assertions.assertThrowsExactly(StoragePropertiesException.class, () -> StorageProperties.createPrimary(origProps));
        origProps.put("nonhdfs", "hdfs://localhost:9000");
        Assertions.assertThrowsExactly(StoragePropertiesException.class, () -> {
            StorageProperties.createPrimary(origProps);
        });
        origProps.put(StorageProperties.FS_HDFS_SUPPORT, "true");
        HdfsProperties hdfsProperties = (HdfsProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("HDFS", hdfsProperties.getStorageName());
        Assertions.assertNotEquals(null, hdfsProperties.getHadoopStorageConfig());
        Assertions.assertNotEquals(null, hdfsProperties.getBackendConfigProperties());
        Map<String, String> resourceNullVal = new HashMap<>();
        resourceNullVal.put(StorageProperties.FS_HDFS_SUPPORT, "true");
        StorageProperties.createPrimary(resourceNullVal).getBackendConfigProperties();
    }

    @Test
    public void testBasicCreateByOldProperties() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("hdfs.authentication.type", "simple");
        origProps.put("fs.defaultFS", "hdfs://localhost:9000");
        StorageProperties properties = StorageProperties.createAll(origProps).get(0);
        Assertions.assertEquals(properties.getClass(), HdfsProperties.class);
        origProps.put("dfs.nameservices", "ns1");
        origProps.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        origProps.put("dfs.namenode.rpc-address.ns1.nn1", "localhost:9000");
        origProps.put("dfs.namenode.rpc-address.ns1.nn2", "localhost:9001");
        origProps.put("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        origProps.put("hadoop.async.threads.max", "10");
        properties = StorageProperties.createAll(origProps).get(0);
        Assertions.assertEquals(properties.getClass(), HdfsProperties.class);
        Map<String, String> beProperties = properties.getBackendConfigProperties();
        Assertions.assertEquals("hdfs://localhost:9000", beProperties.get("fs.defaultFS"));
        Assertions.assertEquals("ns1", beProperties.get("dfs.nameservices"));
        Assertions.assertEquals("nn1,nn2", beProperties.get("dfs.ha.namenodes.ns1"));
        Assertions.assertEquals("10", beProperties.get("hadoop.async.threads.max"));


    }

    // Helper methods to reduce code duplication
    private Map<String, String> createBaseHdfsProperties() {
        Map<String, String> origProps = Maps.newHashMap();
        origProps.put(StorageProperties.FS_HDFS_SUPPORT, "true");
        return origProps;
    }

    private void assertKerberosConfigException(Map<String, String> origProps, String expectedMessage) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), expectedMessage);
    }

    private void assertConfigResourceException(Map<String, String> origProps, String expectedMessage) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), expectedMessage);
    }

    @Test
    public void checkUriParamsTests() throws UserException {
        Map<String, String> origProps = createBaseHdfsProperties();
        origProps.put("fs.defaultFS", "hdfs://localhost:9000");
        origProps.put("uri", "s3://region/bucket/");
        HdfsProperties hdfsProperties = (HdfsProperties) StorageProperties.createAll(origProps).get(0);
        HdfsProperties finalHdfsProperties = hdfsProperties;
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            finalHdfsProperties.validateAndGetUri(origProps);
        });

        origProps.put("uri", "hdfs://localhost:9000/test");
        origProps.put("hadoop.username", "test");
        hdfsProperties = (HdfsProperties) StorageProperties.createAll(origProps).get(0);
        Assertions.assertEquals("test", hdfsProperties.getBackendConfigProperties().get("hadoop.username"));
        Assertions.assertEquals("hdfs://localhost:9000/test", hdfsProperties.validateAndGetUri(origProps));
        HdfsProperties finalHdfsProperties1 = hdfsProperties;
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            finalHdfsProperties1.validateAndNormalizeUri("");
        });
        Assertions.assertEquals("hdfs://localhost:9000/test", hdfsProperties.validateAndNormalizeUri("hdfs://localhost:9000/test"));

    }
}
