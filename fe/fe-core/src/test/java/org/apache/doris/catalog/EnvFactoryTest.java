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

package org.apache.doris.catalog;

import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudEnvFactory;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EnvFactoryTest {

    @Test
    public void testCreate() throws Exception {
        Config.cloud_unique_id = "";
        EnvFactory envFactory = EnvFactory.getInstance();
        Assertions.assertTrue(envFactory instanceof EnvFactory);
        Assertions.assertFalse(envFactory instanceof CloudEnvFactory);
        Assertions.assertTrue(Env.getCurrentEnv() instanceof Env);
        Assertions.assertFalse(Env.getCurrentEnv() instanceof CloudEnv);
        Assertions.assertTrue(Env.getCurrentInternalCatalog() instanceof InternalCatalog);
        Assertions.assertFalse(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assertions.assertTrue(envFactory.createEnv(false) instanceof Env);
        Assertions.assertFalse(envFactory.createEnv(false) instanceof CloudEnv);
        Assertions.assertTrue(envFactory.createInternalCatalog() instanceof InternalCatalog);
        Assertions.assertFalse(envFactory.createInternalCatalog() instanceof CloudInternalCatalog);
        Assertions.assertTrue(envFactory.createPartition() instanceof Partition);
        Assertions.assertFalse(envFactory.createPartition() instanceof CloudPartition);
        Assertions.assertTrue(envFactory.createTablet() instanceof Tablet);
        Assertions.assertFalse(envFactory.createTablet() instanceof CloudTablet);
        Assertions.assertTrue(envFactory.createReplica() instanceof Replica);
        Assertions.assertFalse(envFactory.createReplica() instanceof CloudReplica);
    }

}
