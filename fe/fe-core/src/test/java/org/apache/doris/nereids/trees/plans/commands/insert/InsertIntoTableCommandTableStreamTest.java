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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.stream.AbstractTableStreamUpdate;
import org.apache.doris.catalog.stream.CloudOlapTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.rewrite.ResolveCloudTableStreamReadState;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class InsertIntoTableCommandTableStreamTest extends TestWithFeService {

    private final NereidsParser parser = new NereidsParser();

    @Override
    public void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
        Config.enable_feature_binlog = true;

        createDatabase("test_stream");
        connectContext.setDatabase("test_stream");

        String createBaseTable = "create table test_stream.tbl_stream_base (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\","
                + "\"enable_unique_key_merge_on_write\"=\"true\","
                + "\"binlog.enable\"=\"true\",\"binlog.format\"=\"ROW\","
                + "\"binlog.need_historical_value\"=\"true\")";
        createTable(createBaseTable);

        String createTargetTable = "create table test_stream.tbl_target (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")";
        createTable(createTargetTable);

        String createStream = "create stream if not exists test_stream.s1 on table test_stream.tbl_stream_base\n"
                + "properties('show_initial_rows' = 'true')";
        createTable(createStream);
    }

    @Test
    public void testInitPlanCollectsStreamUpdateInfosForHistoricalConsume() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        String sql = "insert into test_stream.tbl_target select * from test_stream.s1";
        LogicalPlan logicalPlan = parser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof InsertIntoTableCommand);

        connectContext.setStartTime();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        InsertIntoTableCommand command = (InsertIntoTableCommand) logicalPlan;
        AbstractInsertExecutor insertExecutor = command.initPlan(connectContext, executor, true);

        List<TableStreamUpdateInfo> streamUpdateInfos = insertExecutor.getStreamUpdateInfos();
        Assertions.assertNotNull(streamUpdateInfos);
        Assertions.assertEquals(1, streamUpdateInfos.size());

        TableStreamUpdateInfo info = streamUpdateInfos.get(0);
        Assertions.assertEquals(db.getId(), info.getDbId());
        Assertions.assertEquals(stream.getId(), info.getStreamId());

        Assertions.assertTrue(info.getUpdate() instanceof OlapTableStreamUpdate);
        OlapTableStreamUpdate update = (OlapTableStreamUpdate) info.getUpdate();
        Assertions.assertTrue(update.getPrev().isEmpty());

        List<AbstractInsertExecutor.InsertExecutorListener> listeners = Deencapsulation.getField(insertExecutor,
                "listeners");
        for (AbstractInsertExecutor.InsertExecutorListener listener : listeners) {
            listener.beforeComplete(insertExecutor, executor, -1);
        }

        TransactionState txnState = Env.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(),
                insertExecutor.getTxnId());
        Assertions.assertNotNull(txnState);
        Assertions.assertNotNull(txnState.getStreamUpdateInfos());
        Assertions.assertEquals(1, txnState.getStreamUpdateInfos().size());

        TableStreamUpdateInfo txnInfo = txnState.getStreamUpdateInfos().get(0);
        Assertions.assertEquals(info.getDbId(), txnInfo.getDbId());
        Assertions.assertEquals(info.getStreamId(), txnInfo.getStreamId());
        Assertions.assertTrue(txnInfo.getUpdate() instanceof OlapTableStreamUpdate);

        AbstractTableStreamUpdate txnUpdate = txnInfo.getUpdate();
        Assertions.assertEquals(update.getNext(), ((OlapTableStreamUpdate) txnUpdate).getNext());
    }

    @Test
    public void testUnprotectedUpdateAdvancesPartitionOffsetAndConsumptionTime() throws Exception {
        // (B1) When no historicalPartitionTSO is present, unprotectedUpdateStreamUpdate should
        // advance partitionOffset to the committed `next` TSO, record partitionConsumptionTime,
        // and leave hasHistoricalData() false.
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        // Create a fresh stream without showing initial rows so historicalPartitionTSO is empty.
        createTable("create stream if not exists test_stream.s_no_init on table test_stream.tbl_stream_base\n"
                + "properties('show_initial_rows' = 'false')");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s_no_init");

        Map<Long, Long> prev = new HashMap<>();
        Map<Long, Long> next = new HashMap<>();
        long offset = 0;
        for (Partition partition : baseTable.getPartitions()) {
            long pid = partition.getId();
            long previousOffset = stream.getStreamUpdate(pid).first;
            Assertions.assertNotNull(previousOffset);
            Assertions.assertFalse(stream.hasHistoricalData(pid));
            prev.put(pid, previousOffset);
            // Construct a strictly larger TSO so we can detect the advance.
            next.put(pid, previousOffset + 4242 + offset);
            offset++;
        }
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(prev, next);

        long ts = 12345L;
        baseTable.writeLock();
        try {
            stream.unprotectedUpdateStreamUpdate(update, ts);
        } finally {
            baseTable.writeUnlock();
        }

        for (Map.Entry<Long, Long> entry : next.entrySet()) {
            long pid = entry.getKey();
            Assertions.assertEquals(entry.getValue(), stream.getStreamUpdate(pid).first,
                    "partitionOffset must be advanced to next TSO");
            Assertions.assertFalse(stream.hasHistoricalData(pid),
                    "no historical offset should be present after advance without prior history");
        }
        // partitionConsumptionTime is private; verify via reflection on the same field name.
        @SuppressWarnings("unchecked")
        Map<Long, Long> consumptionTime = (Map<Long, Long>) Deencapsulation.getField(stream,
                "partitionConsumptionTime");
        for (Long pid : next.keySet()) {
            Assertions.assertEquals(Long.valueOf(ts), consumptionTime.get(pid),
                    "partitionConsumptionTime must be recorded with the commit ts");
        }
    }

    @Test
    public void testUnprotectedUpdateClearsHistoryAndAdvancesToNextOffset() throws Exception {
        // (B2) When historicalPartitionTSO is present, the commit must:
        //   - remove the entry from historicalPartitionTSO (history consumed)
        //   - advance partitionOffset to update.next
        //   - set partitionConsumptionTime = ts.
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        createTable("create stream if not exists test_stream.s_history on table test_stream.tbl_stream_base\n"
                + "properties('show_initial_rows' = 'false')");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s_history");

        @SuppressWarnings("unchecked")
        Map<Long, Long> historicalPartitionTSO = (Map<Long, Long>) Deencapsulation.getField(stream,
                "historicalPartitionTSO");

        Map<Long, Long> prev = new HashMap<>();
        Map<Long, Long> next = new HashMap<>();
        long seed = 0;
        for (Partition partition : baseTable.getPartitions()) {
            long pid = partition.getId();
            long histTso = 8000L + seed;
            long nextTso = 9999L + seed;
            historicalPartitionTSO.put(pid, histTso);
            prev.put(pid, histTso);
            next.put(pid, nextTso);
            seed++;
        }
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(prev, next);

        long ts = 67890L;
        baseTable.writeLock();
        try {
            stream.unprotectedUpdateStreamUpdate(update, ts);
        } finally {
            baseTable.writeUnlock();
        }

        @SuppressWarnings("unchecked")
        Map<Long, Long> consumptionTime = (Map<Long, Long>) Deencapsulation.getField(stream,
                "partitionConsumptionTime");
        for (Map.Entry<Long, Long> entry : next.entrySet()) {
            long pid = entry.getKey();
            Assertions.assertFalse(stream.hasHistoricalData(pid),
                    "historicalPartitionTSO must be cleared after commit");
            Assertions.assertFalse(historicalPartitionTSO.containsKey(pid),
                    "historicalPartitionTSO must be cleared after commit");
            Assertions.assertEquals(entry.getValue(), stream.getStreamUpdate(pid).first,
                    "partitionOffset must be advanced to update.next after history consumed");
            Assertions.assertEquals(Long.valueOf(ts), consumptionTime.get(pid),
                    "partitionConsumptionTime must be recorded with the commit ts");
        }
    }

    @Test
    public void testInsertProducedStreamUpdateNextEqualsPartitionTsoAndPrevFromHistory() throws Exception {
        // (C) End-to-end FE-side contract: the OlapTableStreamUpdate produced by the insert
        // path's planner must carry, for each selected partition,
        //   next == partition.getTso() (the dedicated commit-tso field), and
        //   prev == -historicalPartitionTSO[pid] (negated to mark a history offset) when present,
        //   else partitionOffset[pid].
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        // s1 was created with show_initial_rows=true. Seed historicalPartitionTSO for one
        // partition to also cover the history branch.
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");
        @SuppressWarnings("unchecked")
        Map<Long, Long> historicalPartitionTSO = (Map<Long, Long>) Deencapsulation.getField(stream,
                "historicalPartitionTSO");

        // Bump partition versions so partition.getTso() differs from any initial seed and
        // is a meaningful upper bound. Pass tso explicitly (third arg) to simulate a
        // transactional commit advancing the dedicated commit-tso field;
        // without it, partition.tso stays at the -1 sentinel.
        for (Partition partition : baseTable.getPartitions()) {
            long newVer = 5000L + partition.getId() % 1000;
            partition.setVisibleVersionAndTime(newVer, newVer, newVer);
            partition.setNextVersion(newVer + 1);
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersion(newVer);
                    }
                }
            }
        }

        // Pick the first partition to fall on the history path.
        Partition historyPartition = baseTable.getPartitions().iterator().next();
        long historyPid = historyPartition.getId();
        long historyTso = 6789L;
        historicalPartitionTSO.put(historyPid, historyTso);

        String sql = "insert into test_stream.tbl_target select * from test_stream.s1";
        LogicalPlan logicalPlan = parser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof InsertIntoTableCommand);

        connectContext.setStartTime();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        InsertIntoTableCommand command = (InsertIntoTableCommand) logicalPlan;
        AbstractInsertExecutor insertExecutor = command.initPlan(connectContext, executor, true);

        List<TableStreamUpdateInfo> streamUpdateInfos = insertExecutor.getStreamUpdateInfos();
        Assertions.assertEquals(1, streamUpdateInfos.size());
        OlapTableStreamUpdate update = (OlapTableStreamUpdate) streamUpdateInfos.get(0).getUpdate();
        Map<Long, Long> producedNext = update.getNext();
        Map<Long, Long> producedPrev = update.getPrev();
        Assertions.assertFalse(producedNext.isEmpty());

        for (Map.Entry<Long, Long> entry : producedNext.entrySet()) {
            long pid = entry.getKey();
            // next always equals partition commit tso, regardless of history.
            Assertions.assertEquals(baseTable.getPartition(pid).getTso(), entry.getValue(),
                    "next must equal partition.tso (commit tso)");
        }
        // history partition's prev is the historical TSO snapshot encoded as negative
        // (see StreamConsumptionInfoExtractor).
        Assertions.assertEquals(Long.valueOf(-historyTso), producedPrev.get(historyPid),
                "prev of history partition must equal negated historicalPartitionTSO");
    }

    @Test
    public void testEmptyCloudReadStateClearsLocalOffsetProjection() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");
        long partitionId = baseTable.getPartition("p1").getId();
        OlapTableStreamWrapper wrapper = new OlapTableStreamWrapper(
                stream, baseTable, List.of(partitionId));

        Assertions.assertFalse(wrapper.getOutputUpdateMap().isEmpty());
        wrapper.installCloudReadStates(Map.of());
        Assertions.assertTrue(wrapper.hasCloudReadStates());
        Assertions.assertTrue(wrapper.getOutputUpdateMap().isEmpty());
    }

    @Test
    public void testCloudPartitionLimitRejectsBeforeExecution() {
        Cloud.TableStreamIdentityPB identity = Cloud.TableStreamIdentityPB.newBuilder()
                .setBaseDbId(1)
                .setBaseTableId(2)
                .setStreamDbId(3)
                .setStreamId(4)
                .build();
        Map<Long, Cloud.TableStreamPartitionUpdatePB> partitionUpdates = new HashMap<>();
        partitionUpdates.put(10L, Cloud.TableStreamPartitionUpdatePB.newBuilder()
                .setPartitionId(10)
                .setExpectedState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN)
                .setNextOffsetTso(100)
                .build());
        partitionUpdates.put(11L, Cloud.TableStreamPartitionUpdatePB.newBuilder()
                .setPartitionId(11)
                .setExpectedState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN)
                .setNextOffsetTso(101)
                .build());
        List<TableStreamUpdateInfo> infos = List.of(new TableStreamUpdateInfo(
                3L, 4L, new CloudOlapTableStreamUpdate(identity, partitionUpdates)));

        int previousLimit = Config.cloud_table_stream_max_partitions_per_insert;
        try {
            Config.cloud_table_stream_max_partitions_per_insert = 1;
            org.apache.doris.nereids.exceptions.AnalysisException exception = Assertions.assertThrows(
                    org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> InsertIntoTableCommand.checkCloudTableStreamPartitionLimit(infos));
            Assertions.assertTrue(exception.getMessage().contains("Use stream PARTITION"));
        } finally {
            Config.cloud_table_stream_max_partitions_per_insert = previousLimit;
        }
    }

    @Test
    public void testCloudTableStreamRejectsUnsupportedInsertTargets() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        OlapInsertExecutor normalExecutor = Mockito.mock(OlapInsertExecutor.class);

        InsertIntoTableCommand.checkCloudTableStreamTarget(ctx, normalExecutor, targetTable);

        Mockito.when(ctx.isTxnModel()).thenReturn(true);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> InsertIntoTableCommand.checkCloudTableStreamTarget(ctx, normalExecutor, targetTable));

        Mockito.when(ctx.isTxnModel()).thenReturn(false);
        Mockito.when(ctx.isGroupCommit()).thenReturn(true);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> InsertIntoTableCommand.checkCloudTableStreamTarget(ctx, normalExecutor, targetTable));

        Mockito.when(ctx.isGroupCommit()).thenReturn(false);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> InsertIntoTableCommand.checkCloudTableStreamTarget(ctx,
                        Mockito.mock(AbstractInsertExecutor.class), targetTable));
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> InsertIntoTableCommand.checkCloudTableStreamTarget(ctx, normalExecutor,
                        Mockito.mock(TableIf.class)));
    }

    @Test
    public void testCloudPartitionSelectionAndOffsetUpdateUseSameSnapshot() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");
        long partitionId = baseTable.getPartition("p1").getId();
        Cloud.TableStreamIdentityPB identity = Cloud.TableStreamIdentityPB.newBuilder()
                .setBaseDbId(db.getId())
                .setBaseTableId(baseTable.getId())
                .setStreamDbId(db.getId())
                .setStreamId(stream.getId())
                .build();
        Cloud.GetTableStreamReadStateResponse response = Cloud.GetTableStreamReadStateResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                .addBindings(Cloud.TableStreamReadBindingResultPB.newBuilder()
                        .setIdentity(identity)
                        .addPartitionStates(Cloud.TableStreamPartitionReadStatePB.newBuilder()
                                .setPartitionId(partitionId)
                                .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                                .setOffsetTso(100)
                                .setEndTso(130)
                                .setVisibleVersion(8)))
                .build();

        String sql = "insert into test_stream.tbl_target "
                + "select * from test_stream.s1 partition (p1)";
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(sql);
        connectContext.setStartTime();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        command.initPlan(connectContext, new StmtExecutor(connectContext, sql), false);
        Plan analyzedPlan = command.getLineagePlan().orElseThrow();

        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try {
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.getTableStreamReadState(Mockito.any())).thenReturn(response);
                Mockito.doThrow(new RpcException("", "UNIMPLEMENTED: Method not found"))
                        .doNothing()
                        .when(proxy).requireTableStreamControlPlaneCapability();

                org.apache.doris.nereids.exceptions.AnalysisException unsupported = Assertions.assertThrows(
                        org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> new ResolveCloudTableStreamReadState().rewriteRoot(analyzedPlan, null));
                Assertions.assertTrue(unsupported.getMessage().contains("UNIMPLEMENTED"));

                new ResolveCloudTableStreamReadState().rewriteRoot(analyzedPlan, null);
                List<TableStreamUpdateInfo> streamUpdateInfos = StreamConsumptionInfoExtractor.extract(analyzedPlan);

                Assertions.assertEquals(1, streamUpdateInfos.size());
                Assertions.assertTrue(streamUpdateInfos.get(0).getUpdate() instanceof CloudOlapTableStreamUpdate);
                CloudOlapTableStreamUpdate update = (CloudOlapTableStreamUpdate) streamUpdateInfos.get(0).getUpdate();
                Assertions.assertEquals(identity, update.getIdentity());
                Assertions.assertEquals(1, update.getPartitionUpdates().size());
                Cloud.TableStreamPartitionUpdatePB partitionUpdate = update.getPartitionUpdates().get(partitionId);
                Assertions.assertNotNull(partitionUpdate);
                Assertions.assertEquals(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED,
                        partitionUpdate.getExpectedState());
                Assertions.assertEquals(100, partitionUpdate.getExpectedOffsetTso());
                Assertions.assertEquals(130, partitionUpdate.getNextOffsetTso());

                ArgumentCaptor<Cloud.GetTableStreamReadStateRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamReadStateRequest.class);
                Mockito.verify(proxy, Mockito.times(2)).requireTableStreamControlPlaneCapability();
                Mockito.verify(proxy).getTableStreamReadState(requestCaptor.capture());
                Assertions.assertEquals(1, requestCaptor.getValue().getBindingsCount());
                Assertions.assertEquals(List.of(partitionId),
                        requestCaptor.getValue().getBindings(0).getPartitionIdsList());
            }
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
        }
    }

    @Test
    public void testCloudCteAndOuterStreamScansUseSingleReadStateRpc() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        long p1 = baseTable.getPartition("p1").getId();
        long p2 = baseTable.getPartition("p2").getId();
        String sql = "with cte as (select k1, k2 from test_stream.s1) "
                + "select k1, k2 from cte where k1 < 100 union all "
                + "select k1, k2 from cte where k1 >= 100 and k1 < 200";

        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        boolean previousEnableCteMaterialize = connectContext.getSessionVariable().enableCTEMaterialize;
        try {
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            connectContext.getSessionVariable().enableCTEMaterialize = false;
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.getTableStreamReadState(Mockito.any())).thenAnswer(invocation -> {
                    Cloud.GetTableStreamReadStateRequest request = invocation.getArgument(0);
                    Cloud.GetTableStreamReadStateResponse.Builder response =
                            Cloud.GetTableStreamReadStateResponse.newBuilder()
                                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                            .setCode(Cloud.MetaServiceCode.OK));
                    for (Cloud.TableStreamPartitionSetPB binding : request.getBindingsList()) {
                        Cloud.TableStreamReadBindingResultPB.Builder result =
                                Cloud.TableStreamReadBindingResultPB.newBuilder()
                                        .setIdentity(binding.getIdentity());
                        for (long partitionId : binding.getPartitionIdsList()) {
                            result.addPartitionStates(Cloud.TableStreamPartitionReadStatePB.newBuilder()
                                    .setPartitionId(partitionId)
                                    .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                                    .setOffsetTso(100)
                                    .setEndTso(130)
                                    .setVisibleVersion(8));
                        }
                        response.addBindings(result);
                    }
                    return response.build();
                });

                connectContext.setStartTime();
                UUID uuid = UUID.randomUUID();
                connectContext.setQueryId(
                        new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                PlanChecker.from(connectContext).analyze(sql).rewrite();

                ArgumentCaptor<Cloud.GetTableStreamReadStateRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamReadStateRequest.class);
                Mockito.verify(proxy, Mockito.times(1)).getTableStreamReadState(requestCaptor.capture());
                Assertions.assertEquals(1, requestCaptor.getValue().getBindingsCount());
                Assertions.assertEquals(Set.of(p1, p2),
                        new HashSet<>(requestCaptor.getValue().getBindings(0).getPartitionIdsList()));
            }
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
            connectContext.getSessionVariable().enableCTEMaterialize = previousEnableCteMaterialize;
        }
    }

    @Test
    public void testCloudSnapshotPartitionsAreNotAdvancedByIncrementalScan() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        long p1 = baseTable.getPartition("p1").getId();
        long p2 = baseTable.getPartition("p2").getId();
        String sql = "select k1, k2 from test_stream.s1 where k1 < 100 union all "
                + "select k1, k2 from test_stream.s1@snapshot() where k1 >= 100 and k1 < 200";

        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try {
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.getTableStreamReadState(Mockito.any())).thenAnswer(invocation -> {
                    Cloud.GetTableStreamReadStateRequest request = invocation.getArgument(0);
                    Cloud.GetTableStreamReadStateResponse.Builder response =
                            Cloud.GetTableStreamReadStateResponse.newBuilder()
                                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                            .setCode(Cloud.MetaServiceCode.OK));
                    for (Cloud.TableStreamPartitionSetPB binding : request.getBindingsList()) {
                        Cloud.TableStreamReadBindingResultPB.Builder result =
                                Cloud.TableStreamReadBindingResultPB.newBuilder()
                                        .setIdentity(binding.getIdentity());
                        for (long partitionId : binding.getPartitionIdsList()) {
                            result.addPartitionStates(Cloud.TableStreamPartitionReadStatePB.newBuilder()
                                    .setPartitionId(partitionId)
                                    .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                                    .setOffsetTso(100)
                                    .setEndTso(130)
                                    .setVisibleVersion(8));
                        }
                        response.addBindings(result);
                    }
                    return response.build();
                });

                connectContext.setStartTime();
                UUID uuid = UUID.randomUUID();
                connectContext.setQueryId(
                        new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                PlanChecker checker = PlanChecker.from(connectContext).analyze(sql);
                Plan analyzedPlan = checker.getPlan();
                checker.rewrite();

                List<TableStreamUpdateInfo> updates = StreamConsumptionInfoExtractor.extract(analyzedPlan);
                Assertions.assertEquals(1, updates.size());
                CloudOlapTableStreamUpdate update = (CloudOlapTableStreamUpdate) updates.get(0).getUpdate();
                Assertions.assertEquals(Set.of(p1), update.getPartitionUpdates().keySet());

                ArgumentCaptor<Cloud.GetTableStreamReadStateRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamReadStateRequest.class);
                Mockito.verify(proxy).getTableStreamReadState(requestCaptor.capture());
                Assertions.assertEquals(1, requestCaptor.getValue().getBindingsCount());
                Assertions.assertEquals(Set.of(p1, p2),
                        new HashSet<>(requestCaptor.getValue().getBindings(0).getPartitionIdsList()));
            }
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
        }
    }

    @Test
    public void testCloudEliminatedStreamScanDoesNotAdvanceOffset() throws Exception {
        String sql = "select k1, k2 from test_stream.s1 where false";
        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try {
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);

                connectContext.setStartTime();
                UUID uuid = UUID.randomUUID();
                connectContext.setQueryId(
                        new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                PlanChecker checker = PlanChecker.from(connectContext).analyze(sql);
                Plan analyzedPlan = checker.getPlan();
                checker.rewrite();

                Assertions.assertTrue(StreamConsumptionInfoExtractor.extract(analyzedPlan).isEmpty());
                Mockito.verify(proxy, Mockito.never()).getTableStreamReadState(Mockito.any());
            }
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
        }
    }
}
