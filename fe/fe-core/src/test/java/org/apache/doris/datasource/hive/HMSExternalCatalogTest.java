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

package org.apache.doris.datasource.hive;

import org.apache.doris.common.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class HMSExternalCatalogTest {
    private Map<String, String> properties;
    private String confDir;

    @Before
    public void setUp() {
        properties = new HashMap<>();

        // Get the path to test configuration files
        URL url = getClass().getClassLoader().getResource("data/hadoop-conf");
        confDir = url.getPath();

        // Add configuration directory property
        properties.put("hadoop.conf.dir", confDir);

        // Add HDFS configuration that will override values in hdfs-site.xml
        properties.put("dfs.nameservices", "testcluster");  // override "defaultcluster"
        properties.put("dfs.ha.namenodes.testcluster", "nn1,nn2");  // override "default1,default2"
        properties.put("dfs.namenode.rpc-address.testcluster.nn1", "node1:8020");  // override "default1:8020"
        properties.put("dfs.namenode.rpc-address.testcluster.nn2", "node2:8020");  // new property
        properties.put("dfs.client.failover.proxy.provider.testcluster",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        // Add Hive configuration that will override values in hive-site.xml
        properties.put("hive.metastore.uris", "thrift://localhost:9083");  // override "thrift://default:9083"
        properties.put("hive.metastore.warehouse.dir", "/user/hive/warehouse");  // override "/user/hive/default"
    }

    @Test
    public void testLoadHdfsConfiguration() {
        // First load configuration without properties to verify default values
        HdfsConfiguration defaultConf = new HdfsConfiguration();
        Assert.assertEquals("defaultcluster", defaultConf.get("dfs.nameservices"));
        Assert.assertEquals("default1,default2", defaultConf.get("dfs.ha.namenodes.testcluster"));
        Assert.assertEquals("default1:8020", defaultConf.get("dfs.namenode.rpc-address.testcluster.nn1"));

        // Then load configuration with properties to verify overridden values
        Configuration hdfsConf = HMSExternalCatalog.loadHdfsConfiguration(properties);
        Assert.assertEquals("testcluster", hdfsConf.get("dfs.test-from-file"));
        Assert.assertEquals("testcluster", hdfsConf.get("dfs.nameservices"));
        Assert.assertEquals("nn1,nn2", hdfsConf.get("dfs.ha.namenodes.testcluster"));
        Assert.assertEquals("node1:8020", hdfsConf.get("dfs.namenode.rpc-address.testcluster.nn1"));
        Assert.assertEquals("node2:8020", hdfsConf.get("dfs.namenode.rpc-address.testcluster.nn2"));
        Assert.assertEquals(
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                hdfsConf.get("dfs.client.failover.proxy.provider.testcluster"));
    }

    @Test
    public void testLoadHiveConf() {
        // First load configuration without properties to verify default values
        HiveConf defaultConf = new HiveConf();
        Assert.assertEquals("thrift://default:9083", defaultConf.get("hive.metastore.uris"));
        Assert.assertEquals("/user/hive/default", defaultConf.get("hive.metastore.warehouse.dir"));

        // Then load configuration with properties to verify overridden values
        HiveConf hiveConf = HMSExternalCatalog.loadHiveConf(properties);
        Assert.assertEquals("thrift://localhost:9083", hiveConf.get("hive.metastore.uris"));
        Assert.assertEquals("/user/hive/warehouse", hiveConf.get("hive.metastore.warehouse.dir"));
        Assert.assertEquals(String.valueOf(Config.hive_metastore_client_timeout_second),
                hiveConf.get(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.name()));
    }
}
