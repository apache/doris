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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class HDFSPropertiesTest {


    @Test
    public void testBasicHdfsCreate() {
        // Test 1: Check default authentication type (should be "simple")
        Map<String, String> origProps = createBaseHdfsProperties();
        List<StorageProperties> storageProperties = StorageProperties.create(origProps);
        HDFSProperties hdfsProperties = (HDFSProperties) storageProperties.get(0);
        Configuration conf = new Configuration();
        hdfsProperties.toHadoopConfiguration(conf);
        Assertions.assertEquals("simple", conf.get("hadoop.security.authentication"));

        // Test 2: Kerberos without necessary configurations (should throw exception)
        origProps.put("hdfs.authentication.type", "kerberos");
        assertKerberosConfigException(origProps, "HDFS authentication type is kerberos, but principal or keytab is not set");

        // Test 3: Kerberos with missing principal (should throw exception)
        origProps.put("hdfs.authentication.kerberos.principal", "hadoop");
        assertKerberosConfigException(origProps, "HDFS authentication type is kerberos, but principal or keytab is not set");

        // Test 4: Kerberos with complete config (should succeed)
        origProps.put("hdfs.authentication.kerberos.keytab", "keytab");
        HDFSProperties properties = (HDFSProperties) StorageProperties.create(origProps).get(0);  // No exception expected
        Configuration configuration = new Configuration(false);
        properties.toHadoopConfiguration(configuration);
        Assertions.assertEquals("kerberos", configuration.get("hdfs.authentication.type"));
        Assertions.assertEquals("hadoop", configuration.get("hdfs.authentication.kerberos.principal"));
        Assertions.assertEquals("keytab", configuration.get("hdfs.authentication.kerberos.keytab"));
    }

    @Test
    public void testBasicHdfsPropertiesCreateByConfigFile() {
        // Test 1: Check loading of config resources
        Map<String, String> origProps = createBaseHdfsProperties();
        URL hiveFileUrl = HDFSPropertiesTest.class.getClassLoader().getResource("plugins");
        Config.hadoop_config_dir = hiveFileUrl.getPath().toString() + "/hadoop_conf";
        origProps.put("hadoop.config.resources", "hadoop/core-site.xml,hadoop/hdfs-site.xml");

        // Test 2: Missing config resources (should throw exception)
        assertConfigResourceException(origProps, "Config resource file does not exist");

        // Test 3: Valid config resources (should succeed)
        origProps.put("hadoop.config.resources", "hadoop1/core-site.xml,hadoop1/hdfs-site.xml");
        List<StorageProperties> storageProperties = StorageProperties.create(origProps);
        HDFSProperties hdfsProperties = (HDFSProperties) storageProperties.get(0);
        Configuration conf = new Configuration();
        hdfsProperties.toHadoopConfiguration(conf);
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
        hdfsProperties = (HDFSProperties) StorageProperties.create(origProps).get(0);  // No exception expected
        Configuration configuration = new Configuration(false);
        hdfsProperties.toHadoopConfiguration(configuration);
        Assertions.assertEquals("kerberos", configuration.get("hdfs.authentication.type"));
        Assertions.assertEquals("hadoop", configuration.get("hdfs.authentication.kerberos.principal"));
        Assertions.assertEquals("keytab", configuration.get("hdfs.authentication.kerberos.keytab"));
        Assertions.assertEquals("hdfs://localhost:9000", configuration.get("fs.defaultFS"));

    }

    // Helper methods to reduce code duplication
    private Map<String, String> createBaseHdfsProperties() {
        Map<String, String> origProps = Maps.newHashMap();
        origProps.put(StorageProperties.FS_HDFS_SUPPORT, "true");
        return origProps;
    }

    private void assertKerberosConfigException(Map<String, String> origProps, String expectedMessage) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.create(origProps), expectedMessage);
    }

    private void assertConfigResourceException(Map<String, String> origProps, String expectedMessage) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.create(origProps), expectedMessage);
    }
}
