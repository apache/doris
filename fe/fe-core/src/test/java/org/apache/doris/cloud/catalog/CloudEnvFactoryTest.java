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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CloudEnvFactoryTest {

    @Test
    public void testCreate() throws Exception {
        Config.cloud_unique_id = "test_cloud";
        EnvFactory envFactory = EnvFactory.getInstance();
        Assert.assertTrue(envFactory instanceof CloudEnvFactory);
        Assert.assertTrue(Env.getCurrentEnv() instanceof CloudEnv);
        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(envFactory.createEnv(false) instanceof CloudEnv);
        Assert.assertTrue(envFactory.createInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(envFactory.createPartition() instanceof CloudPartition);
        Assert.assertTrue(envFactory.createTablet() instanceof CloudTablet);
        Assert.assertTrue(envFactory.createReplica() instanceof CloudReplica);

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "100");
        PropertyAnalyzer.getInstance().rewriteOlapProperties(
                "catalog_not_exist", "db_not_exist", properties);
        Assert.assertEquals("1", properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
    }

}
