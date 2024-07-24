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

package org.apache.doris.load.sync.canal;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TExecPlanFragmentParamsList;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class CanalSyncDataTest {
    private static final Logger LOG = LogManager.getLogger(CanalSyncDataTest.class);

    private String binlogFile = "mysql-bin.000001";
    private long offset = 0;
    private long nextId = 1000L;
    private int batchSize = 8192;
    private long channelId = 100001L;

    ReentrantLock getLock;

    CanalConnector connector;

    @Mocked
    CanalSyncJob syncJob;
    @Mocked
    Database database;
    @Mocked
    OlapTable table;
    @Mocked
    Env env;
    @Mocked
    Backend backend;
    @Mocked
    StreamLoadTask streamLoadTask;
    @Mocked
    StreamLoadPlanner streamLoadPlanner;
    @Mocked
    SystemInfoService systemInfoService;

    InternalService.PExecPlanFragmentResult beginOkResult = InternalService.PExecPlanFragmentResult.newBuilder()
            .setStatus(Types.PStatus.newBuilder().setStatusCode(0).build()).build(); // begin txn OK

    InternalService.PExecPlanFragmentResult beginFailResult = InternalService.PExecPlanFragmentResult.newBuilder()
            .setStatus(Types.PStatus.newBuilder().setStatusCode(1).build()).build(); // begin txn CANCELLED

    InternalService.PCommitResult commitOkResult = InternalService.PCommitResult.newBuilder()
            .setStatus(Types.PStatus.newBuilder().setStatusCode(0).build()).build(); // commit txn OK

    InternalService.PCommitResult commitFailResult = InternalService.PCommitResult.newBuilder()
            .setStatus(Types.PStatus.newBuilder().setStatusCode(1).build()).build(); // commit txn CANCELLED

    InternalService.PRollbackResult abortOKResult = InternalService.PRollbackResult.newBuilder()
            .setStatus(Types.PStatus.newBuilder().setStatusCode(0).build()).build(); // abort txn OK

    InternalService.PSendDataResult sendDataOKResult = InternalService.PSendDataResult.newBuilder()
            .setStatus(Types.PStatus.newBuilder().setStatusCode(0).build()).build(); // send data OK

    @Before
    public void setUp() throws Exception {

        List<Long> backendIds = Lists.newArrayList(104L);
        Map<Long, Backend> map = Maps.newHashMap();
        map.put(104L, backend);
        ImmutableMap<Long, Backend> backendMap = ImmutableMap.copyOf(map);
        TPipelineInstanceParams localParams = new TPipelineInstanceParams().setFragmentInstanceId(new TUniqueId())
                .setPerNodeScanRanges(Maps.newHashMap());
        TPipelineFragmentParams execPlanFragmentParams = new TPipelineFragmentParams()
                .setLocalParams(Lists.newArrayList());
        execPlanFragmentParams.getLocalParams().add(localParams);

        new Expectations() {
            {
                env.getNextId();
                minTimes = 0;
                result = 101L;

                syncJob.getId();
                minTimes = 0;
                result = 100L;

                database.getId();
                minTimes = 0;
                result = 102L;

                table.getId();
                minTimes = 0;
                result = 103L;

                table.getName();
                minTimes = 0;
                result = "testTbl";

                streamLoadPlanner.plan((TUniqueId) any);
                minTimes = 0;
                result = execPlanFragmentParams;

                systemInfoService.selectBackendIdsForReplicaCreation((ReplicaAllocation) any,
                        Maps.newHashMap(), (TStorageMedium) any, false, true);
                minTimes = 0;
                result = backendIds;

                systemInfoService.getAllBackendsByAllCluster();
                minTimes = 0;
                result = backendMap;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("127.0.0.1", 11111), "test", "user", "passwd");

        new MockUp<SimpleCanalConnector>() {
            @Mock
            void connect() throws CanalClientException {
            }

            @Mock
            void disconnect() throws CanalClientException {
            }

            @Mock
            Message getWithoutAck(int var1, Long var2, TimeUnit var3) throws CanalClientException {
                offset += batchSize * 1; // Simply set one entry as one byte
                return CanalTestUtil.fetchMessage(
                        ++nextId, false, batchSize, binlogFile, offset, "mysql_db", "mysql_tbl");
            }

            @Mock
            void rollback() throws CanalClientException {
            }

            @Mock
            void ack(long var1) throws CanalClientException {
            }

            @Mock
            void subscribe(String var1) throws CanalClientException {
            }
        };

        getLock = new ReentrantLock();
    }

    @Test
    public void testBeginTxnFail(@Mocked GlobalTransactionMgr transactionMgr) throws Exception {

        new Expectations() {
            {
                transactionMgr.beginTransaction(anyLong, (List<Long>) any, anyString,
                        (TransactionState.TxnCoordinator) any, (TransactionState.LoadJobSourceType) any, anyLong);
                minTimes = 0;
                result = new AnalysisException("test exception");

                Env.getCurrentGlobalTransactionMgr();
                minTimes = 0;
                result = transactionMgr;
            }
        };

        CanalSyncDataConsumer consumer = new CanalSyncDataConsumer(
                syncJob, connector, getLock, false);
        CanalSyncDataReceiver receiver = new CanalSyncDataReceiver(
                syncJob, connector, "test", "mysql_db.mysql_tbl", consumer, 8192, getLock);
        CanalSyncChannel channel = new CanalSyncChannel(
                channelId, syncJob, database, table, Lists.newArrayList("a", "b"), "mysql_db", "mysql_tbl");

        Map<Long, CanalSyncChannel> idToChannels = Maps.newHashMap();
        idToChannels.put(channel.getId(), channel);
        consumer.setChannels(idToChannels);

        consumer.start();
        receiver.start();

        try {
            Thread.sleep(3000L);
        } finally {
            receiver.stop();
            consumer.stop();
        }

        Assert.assertEquals("position:N/A", consumer.getPositionInfo());
        LOG.info(consumer.getPositionInfo());
    }

    @Test
    public void testNormal(@Mocked GlobalTransactionMgr transactionMgr,
                           @Mocked BackendServiceProxy backendServiceProxy,
                           @Mocked Future<InternalService.PExecPlanFragmentResult> execFuture,
                           @Mocked Future<InternalService.PCommitResult> commitFuture,
                           @Mocked Future<InternalService.PSendDataResult> sendDataFuture) throws Exception {

        new Expectations() {
            {
                transactionMgr.beginTransaction(anyLong, (List<Long>) any, anyString,
                        (TransactionState.TxnCoordinator) any, (TransactionState.LoadJobSourceType) any, anyLong);
                minTimes = 0;
                result = 105L;

                backendServiceProxy.execPlanFragmentsAsync((TNetworkAddress) any, (TExecPlanFragmentParamsList) any,
                        anyBoolean);
                minTimes = 0;
                result = execFuture;

                backendServiceProxy.commit((TNetworkAddress) any, (Types.PUniqueId) any, (Types.PUniqueId) any);
                minTimes = 0;
                result = commitFuture;

                backendServiceProxy.sendData((TNetworkAddress) any, (Types.PUniqueId) any, (Types.PUniqueId) any,
                        (List<InternalService.PDataRow>) any);
                minTimes = 0;
                result = sendDataFuture;

                execFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = beginOkResult;

                commitFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = commitOkResult;

                sendDataFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = sendDataOKResult;

                Env.getCurrentGlobalTransactionMgr();
                minTimes = 0;
                result = transactionMgr;

                BackendServiceProxy.getInstance();
                minTimes = 0;
                result = backendServiceProxy;
            }
        };

        CanalSyncDataConsumer consumer = new CanalSyncDataConsumer(
                syncJob, connector, getLock, false);
        CanalSyncDataReceiver receiver = new CanalSyncDataReceiver(
                syncJob, connector, "test", "mysql_db.mysql_tbl", consumer, 8192, getLock);
        CanalSyncChannel channel = new CanalSyncChannel(
                channelId, syncJob, database, table, Lists.newArrayList("a", "b"), "mysql_db", "mysql_tbl");

        Map<Long, CanalSyncChannel> idToChannels = Maps.newHashMap();
        idToChannels.put(channel.getId(), channel);
        consumer.setChannels(idToChannels);

        consumer.start();
        receiver.start();

        try {
            Thread.sleep(Config.sync_commit_interval_second * 1000);
        } finally {
            receiver.stop();
            consumer.stop();
        }

        LOG.info(consumer.getPositionInfo());
    }

    @Test
    public void testExecFragmentFail(@Mocked GlobalTransactionMgr transactionMgr,
                                     @Mocked BackendServiceProxy backendServiceProxy,
                                     @Mocked Future<InternalService.PExecPlanFragmentResult> execFuture,
                                     @Mocked Future<InternalService.PRollbackResult> abortFuture) throws Exception {

        new Expectations() {
            {
                transactionMgr.beginTransaction(anyLong, (List<Long>) any, anyString,
                        (TransactionState.TxnCoordinator) any, (TransactionState.LoadJobSourceType) any, anyLong);
                minTimes = 0;
                result = 105L;

                backendServiceProxy.execPlanFragmentsAsync((TNetworkAddress) any, (TExecPlanFragmentParamsList) any,
                        anyBoolean);
                minTimes = 0;
                result = execFuture;

                backendServiceProxy.rollback((TNetworkAddress) any, (Types.PUniqueId) any, (Types.PUniqueId) any);
                minTimes = 0;
                result = abortFuture;

                execFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = beginFailResult;

                abortFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = abortOKResult;

                Env.getCurrentGlobalTransactionMgr();
                minTimes = 0;
                result = transactionMgr;

                BackendServiceProxy.getInstance();
                minTimes = 0;
                result = backendServiceProxy;
            }
        };

        CanalSyncDataConsumer consumer = new CanalSyncDataConsumer(
                syncJob, connector, getLock, false);
        CanalSyncDataReceiver receiver = new CanalSyncDataReceiver(
                syncJob, connector, "test", "mysql_db.mysql_tbl", consumer, 8192, getLock);
        CanalSyncChannel channel = new CanalSyncChannel(
                channelId, syncJob, database, table, Lists.newArrayList("a", "b"), "mysql_db", "mysql_tbl");

        Map<Long, CanalSyncChannel> idToChannels = Maps.newHashMap();
        idToChannels.put(channel.getId(), channel);
        consumer.setChannels(idToChannels);

        consumer.start();
        receiver.start();

        try {
            Thread.sleep(3000L);
        } finally {
            receiver.stop();
            consumer.stop();
        }

        Assert.assertEquals("position:N/A", consumer.getPositionInfo());
        LOG.info(consumer.getPositionInfo());
    }

    @Test
    public void testCommitTxnFail(@Mocked GlobalTransactionMgr transactionMgr,
                                  @Mocked BackendServiceProxy backendServiceProxy,
                                  @Mocked Future<InternalService.PExecPlanFragmentResult> execFuture,
                                  @Mocked Future<InternalService.PCommitResult> commitFuture,
                                  @Mocked Future<InternalService.PRollbackResult> abortFuture,
                                  @Mocked Future<InternalService.PSendDataResult> sendDataFuture) throws Exception {

        new Expectations() {
            {
                transactionMgr.beginTransaction(anyLong, (List<Long>) any, anyString,
                        (TransactionState.TxnCoordinator) any, (TransactionState.LoadJobSourceType) any, anyLong);
                minTimes = 0;
                result = 105L;

                backendServiceProxy.execPlanFragmentsAsync((TNetworkAddress) any, (TExecPlanFragmentParamsList) any,
                        anyBoolean);
                minTimes = 0;
                result = execFuture;

                backendServiceProxy.commit((TNetworkAddress) any, (Types.PUniqueId) any, (Types.PUniqueId) any);
                minTimes = 0;
                result = commitFuture;

                backendServiceProxy.rollback((TNetworkAddress) any, (Types.PUniqueId) any, (Types.PUniqueId) any);
                minTimes = 0;
                result = abortFuture;

                backendServiceProxy.sendData((TNetworkAddress) any, (Types.PUniqueId) any,
                        (Types.PUniqueId) any, (List<InternalService.PDataRow>) any);
                minTimes = 0;
                result = sendDataFuture;

                execFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = beginOkResult;

                commitFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = commitFailResult;

                abortFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = abortOKResult;

                sendDataFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = sendDataOKResult;

                Env.getCurrentGlobalTransactionMgr();
                minTimes = 0;
                result = transactionMgr;

                BackendServiceProxy.getInstance();
                minTimes = 0;
                result = backendServiceProxy;
            }
        };

        CanalSyncDataConsumer consumer = new CanalSyncDataConsumer(
                syncJob, connector, getLock, false);
        CanalSyncDataReceiver receiver = new CanalSyncDataReceiver(
                syncJob, connector, "test", "mysql_db.mysql_tbl", consumer, 8192, getLock);
        CanalSyncChannel channel = new CanalSyncChannel(
                channelId, syncJob, database, table, Lists.newArrayList("a", "b"), "mysql_db", "mysql_tbl");

        Map<Long, CanalSyncChannel> idToChannels = Maps.newHashMap();
        idToChannels.put(channel.getId(), channel);
        consumer.setChannels(idToChannels);

        consumer.start();
        receiver.start();

        try {
            Thread.sleep(3000L);
        } finally {
            receiver.stop();
            consumer.stop();
        }

        Assert.assertEquals("position:N/A", consumer.getPositionInfo());
        LOG.info(consumer.getPositionInfo());
    }
}
