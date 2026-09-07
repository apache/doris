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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.qe.CloudCoordinator;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

public class CloudEnvFactoryTest {

    @Test
    public void testCreate() throws Exception {
        Config.cloud_unique_id = "test_cloud";
        EnvFactory envFactory = EnvFactory.getInstance();
        Assertions.assertTrue(envFactory instanceof CloudEnvFactory);
        Assertions.assertTrue(Env.getCurrentEnv() instanceof CloudEnv);
        Assertions.assertTrue(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assertions.assertTrue(envFactory.createEnv(false) instanceof CloudEnv);
        Assertions.assertTrue(envFactory.createInternalCatalog() instanceof CloudInternalCatalog);
        Assertions.assertTrue(envFactory.createPartition() instanceof CloudPartition);
        Assertions.assertTrue(envFactory.createTablet() instanceof CloudTablet);
        Assertions.assertTrue(envFactory.createReplica() instanceof CloudReplica);

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "100");
        PropertyAnalyzer.getInstance().rewriteOlapProperties(
                "catalog_not_exist", "db_not_exist", properties);
        Assertions.assertEquals("1", properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
    }

    @Test
    public void testLegacyLoadCoordinatorSetsFunctionVersionOptions() {
        boolean runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        try {
            ConnectContext context = new ConnectContext();
            context.getSessionVariable().setEnableNereidsDistributePlanner(false);
            context.setThreadLocalInfo();
            Coordinator coordinator = new CloudEnvFactory().createCoordinator(
                    1L, new TUniqueId(1L, 1L), new DescriptorTable(),
                    Collections.emptyList(), Collections.emptyList(), "UTC", false, false);

            Assertions.assertTrue(coordinator instanceof CloudCoordinator);
            Assertions.assertTrue(coordinator.getQueryOptions().isSetNewVersionUnixTimestamp());
            Assertions.assertTrue(coordinator.getQueryOptions().isNewVersionUnixTimestamp());
            Assertions.assertTrue(coordinator.getQueryOptions().isSetNewVersionPercentile());
            Assertions.assertTrue(coordinator.getQueryOptions().isNewVersionPercentile());
            Assertions.assertTrue(coordinator.getQueryOptions().isSetNewVersionBitmapOpCount());
            Assertions.assertTrue(coordinator.getQueryOptions().isNewVersionBitmapOpCount());
        } finally {
            ConnectContext.remove();
            FeConstants.runningUnitTest = runningUnitTest;
        }
    }

}
