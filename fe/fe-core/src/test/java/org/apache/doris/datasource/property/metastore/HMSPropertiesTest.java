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

import org.apache.paimon.options.Options;
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
        params.put("metastore.type", "hms");
        Map<String, String> finalParams = params;
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(finalParams));
    }

    @Test
    public void testHiveConfDirExist() {
        URL hiveFileUrl = HMSPropertiesTest.class.getClassLoader().getResource("plugins");
        Config.hadoop_config_dir = hiveFileUrl.getPath().toString();
        Map<String, String> params = new HashMap<>();
        params.put("hive.conf.resources", "hive-conf/hive1/hive-site.xml");
        params.put("metastore.type", "hms");
        HMSProperties hmsProperties;
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(params));
        params.put("hive.metastore.uris", "thrift://default:9083");
        hmsProperties = (HMSProperties) MetastoreProperties.create(params);
        Map<String, String> hiveConf = hmsProperties.loadConfigFromFile("hive.conf.resources");
        Assertions.assertNotNull(hiveConf);
        Assertions.assertEquals("/user/hive/default", hiveConf.get("hive.metastore.warehouse.dir"));
    }

    @Test
    public void testBasicParamsTest() {
        // Step 1: Set up initial parameters for HMSProperties
        Map<String, String> params = createBaseParams();

        // Step 2: Test HMSProperties to PaimonOptions and Conf conversion
        HMSProperties hmsProperties = getHMSProperties(params);
        testHmsToPaimonOptions(hmsProperties);

        // Step 3: Test HMSProperties to Iceberg Hive Catalog properties conversion
        testHmsToIcebergHiveCatalog(hmsProperties);

        // Step 4: Test invalid scenario when both SASL and kerberos are enabled
        params.put("hive.metastore.sasl.enabled", "true");
        params.put("hive.metastore.authentication.type", "kerberos");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetastoreProperties.create(params));
    }

    private Map<String, String> createBaseParams() {
        Map<String, String> params = new HashMap<>();
        params.put("metastore.type", "hms");
        params.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        params.put("hive.metastore.authentication.type", "simple");
        return params;
    }

    private HMSProperties getHMSProperties(Map<String, String> params) {
        return (HMSProperties) MetastoreProperties.create(params);
    }

    private void testHmsToPaimonOptions(HMSProperties hmsProperties) {
        Options paimonOptions = new Options();
        hmsProperties.toPaimonOptionsAndConf(paimonOptions);
        Assertions.assertEquals("thrift://127.0.0.1:9083", paimonOptions.get("uri"));
    }

    private void testHmsToIcebergHiveCatalog(HMSProperties hmsProperties) {
        Map<String, String> icebergMSParams = new HashMap<>();
        hmsProperties.toIcebergHiveCatalogProperties(icebergMSParams);
        Assertions.assertEquals("thrift://127.0.0.1:9083", icebergMSParams.get("uri"));
    }
}
