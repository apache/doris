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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.CloudTabletStatMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.common.util.CloudPropertyAnalyzer;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.load.CleanCopyJobScheduler;
import org.apache.doris.cloud.load.CloudBrokerLoadJob;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.cloud.load.CloudRoutineLoadManager;
import org.apache.doris.cloud.planner.CloudGroupCommitPlanner;
import org.apache.doris.cloud.qe.CloudCoordinator;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.cloud.transaction.CloudGlobalTransactionMgr;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.loadv2.LoadJobScheduler;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import org.apache.thrift.TException;

import java.lang.reflect.Type;
import java.util.List;
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
    public Replica createReplica(Replica.ReplicaContext context) {
        return new CloudReplica(context);
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

    @Override
    public BrokerLoadJob createBrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt,
            UserIdentity userInfo) throws MetaNotFoundException {
        return new CloudBrokerLoadJob(dbId, label, brokerDesc, originStmt, userInfo);
    }

    @Override
    public BrokerLoadJob createBrokerLoadJob() {
        return new CloudBrokerLoadJob();
    }

    @Override
    public Coordinator createCoordinator(ConnectContext context, Analyzer analyzer, Planner planner,
                                         StatsErrorEstimator statsErrorEstimator) {
        return new CloudCoordinator(context, analyzer, planner, statsErrorEstimator);
    }

    @Override
    public Coordinator createCoordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                         List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                         String timezone, boolean loadZeroTolerance, boolean enableProfile) {
        return new CloudCoordinator(jobId, queryId, descTable, fragments, scanNodes, timezone, loadZeroTolerance,
                                enableProfile);
    }

    @Override
    public GroupCommitPlanner createGroupCommitPlanner(Database db, OlapTable table, List<String> targetColumnNames,
            TUniqueId queryId, String groupCommit) throws UserException, TException {
        return  new CloudGroupCommitPlanner(db, table, targetColumnNames, queryId, groupCommit);
    }

    @Override
    public RoutineLoadManager createRoutineLoadManager() {
        return new CloudRoutineLoadManager();
    }

    public LoadManager createLoadManager(LoadJobScheduler loadJobScheduler,
                                        CleanCopyJobScheduler cleanCopyJobScheduler) {
        return new CloudLoadManager(loadJobScheduler, cleanCopyJobScheduler);
    }

    public MasterDaemon createTabletStatMgr() {
        return new CloudTabletStatMgr();
    }
}
