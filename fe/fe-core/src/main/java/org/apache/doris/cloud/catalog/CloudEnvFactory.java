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

import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.common.util.CloudPropertyAnalyzer;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.cloud.transaction.CloudGlobalTransactionMgr;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import java.lang.reflect.Type;
import java.util.Map;

public class CloudEnvFactory extends EnvFactory {

    public CloudEnvFactory() {
    }

    @Override
    public Env createEnv(boolean isCheckpointCatalog) {
        return new CloudEnv(isCheckpointCatalog);
    }

    @Override
    public InternalCatalog createInternalCatalog() {
        return new CloudInternalCatalog();
    }

    @Override
    public SystemInfoService createSystemInfoService() {
        return new CloudSystemInfoService();
    }

    @Override
    public Type getPartitionClass() {
        return CloudPartition.class;
    }

    @Override
    public Partition createPartition() {
        return new CloudPartition();
    }

    @Override
    public Type getTabletClass() {
        return CloudTablet.class;
    }

    @Override
    public Tablet createTablet() {
        return new CloudTablet();
    }

    @Override
    public Tablet createTablet(long tabletId) {
        return new CloudTablet(tabletId);
    }

    @Override
    public Replica createReplica() {
        return new CloudReplica();
    }

    @Override
    public ReplicaAllocation createDefReplicaAllocation() {
        return new ReplicaAllocation((short) 1);
    }

    @Override
    public PropertyAnalyzer createPropertyAnalyzer() {
        return new CloudPropertyAnalyzer();
    }

    @Override
    public DynamicPartitionProperty createDynamicPartitionProperty(Map<String, String> properties) {
        return new CloudDynamicPartitionProperty(properties);
    }

    @Override
    public GlobalTransactionMgrIface createGlobalTransactionMgr(Env env) {
        return new CloudGlobalTransactionMgr();
    }
}
