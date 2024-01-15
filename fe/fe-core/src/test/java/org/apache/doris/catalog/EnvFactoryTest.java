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
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;

import org.junit.Assert;
import org.junit.Test;

public class EnvFactoryTest {

    @Test
    public void testCreate() throws Exception {
        Config.cloud_unique_id = "";
        Assert.assertTrue(Env.getCurrentEnv() instanceof Env);
        Assert.assertFalse(Env.getCurrentEnv() instanceof CloudEnv);
        Assert.assertTrue(Env.getCurrentInternalCatalog() instanceof InternalCatalog);
        Assert.assertFalse(Env.getCurrentInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(EnvFactory.createEnv(false) instanceof Env);
        Assert.assertFalse(EnvFactory.createEnv(false) instanceof CloudEnv);
        Assert.assertTrue(EnvFactory.createInternalCatalog() instanceof InternalCatalog);
        Assert.assertFalse(EnvFactory.createInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(EnvFactory.createPartition() instanceof Partition);
        Assert.assertFalse(EnvFactory.createPartition() instanceof CloudPartition);
        Assert.assertTrue(EnvFactory.createReplica() instanceof Replica);
        Assert.assertFalse(EnvFactory.createReplica() instanceof CloudReplica);

        Config.cloud_unique_id = "test_cloud";
        Assert.assertTrue(EnvFactory.createEnv(false) instanceof CloudEnv);
        Assert.assertTrue(EnvFactory.createInternalCatalog() instanceof CloudInternalCatalog);
        Assert.assertTrue(EnvFactory.createPartition() instanceof CloudPartition);
        Assert.assertTrue(EnvFactory.createReplica() instanceof CloudReplica);
    }

}

