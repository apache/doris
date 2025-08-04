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
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class HMSPropertiesTest {

    @Test
    public void testHiveConfDirNotExist() {
        Map<String, String> params = new HashMap<>();
        params.put("hive.conf.resources", "/opt/hive-site.xml");
        params.put("hive.metastore.type", "hms");
        Map<String, String> finalParams = params;
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(finalParams));
    }

    @Test
    public void testHiveConfDirExist() throws UserException {
        URL hiveFileUrl = HMSPropertiesTest.class.getClassLoader().getResource("plugins");
        Config.hadoop_config_dir = hiveFileUrl.getPath().toString();
        Map<String, String> params = new HashMap<>();
        params.put("hive.conf.resources", "/hive-conf/hive1/hive-site.xml");
        params.put("type", "hms");
        HMSProperties hmsProperties;
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(params));
        params.put("hive.metastore.uris", "thrift://default:9083");
        hmsProperties = (HMSProperties) MetastoreProperties.create(params);
        HiveConf hiveConf = hmsProperties.getHiveConf();
        Assertions.assertNotNull(hiveConf);
        Assertions.assertEquals("/user/hive/default", hiveConf.get("hive.metastore.warehouse.dir"));
    }

    @Test
    public void testBasicParamsTest() throws UserException {
        Map<String, String> notValidParams = new HashMap<>();
        notValidParams.put("hive.metastore.type", "hms");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(notValidParams));
        // Step 1: Set up initial parameters for HMSProperties
        Map<String, String> params = createBaseParams();

        // Step 2: Test HMSProperties to PaimonOptions and Conf conversion
        HMSProperties hmsProperties = getHMSProperties(params);
        Assertions.assertNotNull(hmsProperties);
        Assertions.assertEquals("thrift://127.0.0.1:9083", hmsProperties.getHiveConf().get("hive.metastore.uris"));
        Assertions.assertEquals(HadoopExecutionAuthenticator.class, hmsProperties.getExecutionAuthenticator().getClass());
        params.put("hadoop.security.authentication", "simple");
        hmsProperties = getHMSProperties(params);
        Assertions.assertNotNull(hmsProperties);
        Assertions.assertEquals(HadoopExecutionAuthenticator.class, hmsProperties.getExecutionAuthenticator().getClass());
        params.put("hive.metastore.authentication.type", "kerberos");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(params),
                "Hive metastore authentication type is kerberos, but service principal, client principal or client keytab is not set.");
        params.put("hive.metastore.client.principal", "hive/127.0.0.1@EXAMPLE.COM");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(params),
                "Hive metastore authentication type is kerberos, but client keytab is not set.");
        params.put("hive.metastore.client.keytab", "/path/to/keytab");
        hmsProperties = getHMSProperties(params);
        Assertions.assertNotNull(hmsProperties);
        Assertions.assertEquals(HadoopExecutionAuthenticator.class, hmsProperties.getExecutionAuthenticator().getClass());
        params.put("hive.metastore.authentication.type", "simple");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(params),
                "Hive metastore authentication type is simple, but service principal, client principal or client keytab is set.");

    }

    private Map<String, String> createBaseParams() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "hms");
        params.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        params.put("hive.metastore.authentication.type", "simple");
        return params;
    }

    private HMSProperties getHMSProperties(Map<String, String> params) throws UserException {
        return (HMSProperties) MetastoreProperties.create(params);
    }

    @Test
    public void testRefreshParams() throws UserException {
        Map<String, String> params = createBaseParams();
        HMSProperties hmsProperties = getHMSProperties(params);
        Assertions.assertFalse(hmsProperties.isHmsEventsIncrementalSyncEnabled());
        params.put("hive.enable_hms_events_incremental_sync", "true");
        hmsProperties = getHMSProperties(params);
        Assertions.assertTrue(hmsProperties.isHmsEventsIncrementalSyncEnabled());
        params.put("hive.enable_hms_events_incremental_sync", "xxxx");
        hmsProperties = getHMSProperties(params);
        Assertions.assertFalse(hmsProperties.isHmsEventsIncrementalSyncEnabled());
        params.put("hive.hms_events_batch_size_per_rpc", "false");
        Assertions.assertThrows(IllegalArgumentException.class, () -> getHMSProperties(params));
        params.put("hive.hms_events_batch_size_per_rpc", "123");
        hmsProperties = getHMSProperties(params);
        Assertions.assertEquals(123, hmsProperties.getHmsEventsBatchSizePerRpc());
    }

    @Test
    public void testHmsKerberosParams() throws UserException {
        Map<String, String> params = createBaseParams();
        params.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        params.put("hive.metastore.sasl.enabled", "true");
        params.put("hive.metastore.authentication.type", "kerberos");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(params));
        //params.put("hive.metastore.client.principal", "hive/127.0.0.1@EXAMPLE.COM");
        params.put("hive.metastore.client.keytab", "/path/to/keytab");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(params),
                "Hive metastore authentication type is kerberos, but service principal, client principal or client keytab is not set.");
        params.put("hive.metastore.client.principal", "hive/127.0.0.1@EXAMPLE.COM");
        HMSProperties hmsProperties = getHMSProperties(params);
        Assertions.assertNotNull(hmsProperties);
    }
}
