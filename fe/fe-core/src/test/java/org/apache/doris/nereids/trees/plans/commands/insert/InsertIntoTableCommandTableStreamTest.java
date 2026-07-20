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
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.CloudTableStreamReadStateHook;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
        createTable("create stream if not exists test_stream.s2 on table test_stream.tbl_stream_base\n"
                + "properties('show_initial_rows' = 'false')");

        String createCloudMvEmptyTable = "create table test_stream.tbl_cloud_mv_empty (\n"
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
        createTable(createCloudMvEmptyTable);

        String createCloudMvEmptyStream = "create stream test_stream.s_cloud_mv_empty "
                + "on table test_stream.tbl_cloud_mv_empty\n"
                + "properties('show_initial_rows' = 'false')";
        createTable(createCloudMvEmptyStream);
    }

    @Override
    protected void runBeforeEach() {
        // Switching Cloud mode can make the mock heartbeat mark this fixture's Backend dead.
        for (Backend backend : Env.getCurrentSystemInfo().getAllClusterBackends(false)) {
            backend.setAlive(true);
        }
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
    public void testEmptyInsertStartsTransactionForStreamOffsetUpdate() throws Exception {
        String sql = "insert into test_stream.tbl_target "
                + "select * from test_stream.s1 where false";
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(sql);

        resetQueryContext();
        AbstractInsertExecutor insertExecutor = command.initPlan(
                connectContext, new StmtExecutor(connectContext, sql), true);
        try {
            Assertions.assertTrue(insertExecutor.isEmptyInsert());
            Assertions.assertFalse(insertExecutor.getStreamUpdateInfos().isEmpty());
            Assertions.assertNotEquals(AbstractInsertExecutor.INVALID_TXN_ID, insertExecutor.getTxnId());
        } finally {
            insertExecutor.onFail(new RuntimeException("test cleanup"));
            resetQueryContext();
        }
    }

    @Test
    public void testOrdinaryEmptyInsertStillSkipsTransaction() throws Exception {
        String sql = "insert into test_stream.tbl_target "
                + "select * from test_stream.tbl_stream_base where false";
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(sql);

        resetQueryContext();
        AbstractInsertExecutor insertExecutor = command.initPlan(
                connectContext, new StmtExecutor(connectContext, sql), true);

        Assertions.assertTrue(insertExecutor.isEmptyInsert());
        Assertions.assertTrue(insertExecutor.getStreamUpdateInfos().isEmpty());
        Assertions.assertFalse(insertExecutor.requiresTransaction());
        Assertions.assertEquals(AbstractInsertExecutor.INVALID_TXN_ID, insertExecutor.getTxnId());
    }

    @Test
    public void testEmptyInsertCommitsStreamOffsetUpdate() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        createTable("create stream if not exists test_stream.s_empty_insert_commit "
                + "on table test_stream.tbl_stream_base properties('show_initial_rows' = 'false')");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s_empty_insert_commit");
        Map<Long, Long> originalTso = new HashMap<>();
        Map<Long, Long> expectedTso = new HashMap<>();

        for (Partition partition : baseTable.getPartitions()) {
            originalTso.put(partition.getId(), partition.getTso());
            long nextTso = partition.getTso() + 1000;
            expectedTso.put(partition.getId(), nextTso);
            partition.setVisibleVersionAndTime(
                    partition.getVisibleVersion(), partition.getVisibleVersionTime(), nextTso);
        }

        String sql = "insert into test_stream.tbl_target "
                + "select * from test_stream.s_empty_insert_commit where false";
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(sql);
        resetQueryContext();
        connectContext.getState().reset();
        connectContext.resetReturnRows();
        try {
            command.run(connectContext, new StmtExecutor(connectContext, sql));

            Assertions.assertEquals(MysqlStateType.OK, connectContext.getState().getStateType());
            Assertions.assertEquals(0L, connectContext.getReturnRows());
            for (Map.Entry<Long, Long> entry : expectedTso.entrySet()) {
                Assertions.assertEquals(entry.getValue(), stream.getStreamUpdate(entry.getKey()).first);
            }
        } finally {
            for (Partition partition : baseTable.getPartitions()) {
                partition.setVisibleVersionAndTime(partition.getVisibleVersion(),
                        partition.getVisibleVersionTime(), originalTso.get(partition.getId()));
            }
            resetQueryContext();
        }
    }

    @Test
    public void testInitPlanCollectsUpdatesForTwoStreams() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTableStream firstStream = (OlapTableStream) db.getTableOrMetaException("s1");
        OlapTableStream secondStream = (OlapTableStream) db.getTableOrMetaException("s2");
        String sql = "insert into test_stream.tbl_target "
                + "select * from test_stream.s1 partition(p1) "
                + "union all select * from test_stream.s2 partition(p2)";
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(sql);

        resetQueryContext();
        AbstractInsertExecutor insertExecutor = command.initPlan(
                connectContext, new StmtExecutor(connectContext, sql), true);
        try {
            List<TableStreamUpdateInfo> streamUpdateInfos = insertExecutor.getStreamUpdateInfos();
            Assertions.assertEquals(2, streamUpdateInfos.size());
            Assertions.assertEquals(Set.of(firstStream.getId(), secondStream.getId()),
                    streamUpdateInfos.stream().map(TableStreamUpdateInfo::getStreamId).collect(Collectors.toSet()));
            Assertions.assertTrue(streamUpdateInfos.stream().allMatch(info -> info.getDbId() == db.getId()));
            Assertions.assertTrue(streamUpdateInfos.stream()
                    .allMatch(info -> info.getUpdate() instanceof OlapTableStreamUpdate));
            Map<Long, TableStreamUpdateInfo> updatesByStream = streamUpdateInfos.stream()
                    .collect(Collectors.toMap(TableStreamUpdateInfo::getStreamId, info -> info));
            Assertions.assertEquals(Set.of(baseTable.getPartition("p1").getId()),
                    ((OlapTableStreamUpdate) updatesByStream.get(firstStream.getId()).getUpdate())
                            .getNext().keySet());
            Assertions.assertEquals(Set.of(baseTable.getPartition("p2").getId()),
                    ((OlapTableStreamUpdate) updatesByStream.get(secondStream.getId()).getUpdate())
                            .getNext().keySet());
        } finally {
            insertExecutor.onFail(new RuntimeException("test cleanup"));
            resetQueryContext();
        }
    }

    @Test
    public void testUserInsertRejectsIvmInternalTableStream() throws Exception {
        connectContext.getStatementContext().setIvmRewriteContext(Optional.empty());
        String streamName = IvmUtil.streamName(12345L, "tbl_stream_base");
        createTable("create stream if not exists test_stream." + streamName
                + " on table test_stream.tbl_stream_base\n"
                + "properties('show_initial_rows' = 'true')");
        String sql = "insert into test_stream.tbl_target select * from test_stream." + streamName;
        LogicalPlan logicalPlan = parser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof InsertIntoTableCommand);

        connectContext.setStartTime();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        InsertIntoTableCommand command = (InsertIntoTableCommand) logicalPlan;
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> command.initPlan(connectContext, executor, true));
        Assertions.assertTrue(ex.getMessage().contains("IVM internal table stream cannot be used in INSERT INTO"),
                "unexpected message: " + ex.getMessage());
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
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE, true)) {
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
    public void testCloudPartitionLimitAppliedDuringInitPlan() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        long p1 = baseTable.getPartition("p1").getId();
        long p2 = baseTable.getPartition("p2").getId();

        int previousLimit = Config.cloud_table_stream_max_partitions_per_insert;
        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        SystemInfoService previousSystemInfo = Env.getCurrentSystemInfo();
        CloudSystemInfoService cloudSystemInfo = new CloudSystemInfoService();
        for (Backend backend : previousSystemInfo.getAllClusterBackends(false)) {
            cloudSystemInfo.addBackend(backend);
        }
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        AbstractInsertExecutor p1Executor = null;
        try {
            Config.cloud_table_stream_max_partitions_per_insert = 1;
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", cloudSystemInfo);
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.getTableStreamOffset(Mockito.any()))
                        .thenAnswer(invocation -> buildReadStateResponse(invocation.getArgument(0)));

                String allPartitionsSql = "insert into test_stream.tbl_target select * from test_stream.s1";
                InsertIntoTableCommand allPartitionsCommand =
                        (InsertIntoTableCommand) parser.parseSingle(allPartitionsSql);
                resetQueryContext();
                org.apache.doris.nereids.exceptions.AnalysisException exception = Assertions.assertThrows(
                        org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> allPartitionsCommand.initPlan(
                                connectContext, new StmtExecutor(connectContext, allPartitionsSql), true));
                Assertions.assertTrue(exception.getMessage().contains("Use stream PARTITION"));

                String p1Sql = "insert into test_stream.tbl_target "
                        + "select * from test_stream.s1 partition (p1)";
                InsertIntoTableCommand p1Command = (InsertIntoTableCommand) parser.parseSingle(p1Sql);
                resetQueryContext();
                p1Executor = p1Command.initPlan(
                        connectContext, new StmtExecutor(connectContext, p1Sql), true);
                List<TableStreamUpdateInfo> p1Updates = p1Executor.getStreamUpdateInfos();
                Assertions.assertEquals(1, p1Updates.size());
                CloudOlapTableStreamUpdate p1Update =
                        (CloudOlapTableStreamUpdate) p1Updates.get(0).getUpdate();
                Assertions.assertEquals(Set.of(p1), p1Update.getPartitionUpdates().keySet());

                ArgumentCaptor<Cloud.GetTableStreamOffsetRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamOffsetRequest.class);
                Mockito.verify(proxy, Mockito.times(2)).getTableStreamOffset(requestCaptor.capture());
                Assertions.assertEquals(Set.of(p1, p2),
                        new HashSet<>(requestCaptor.getAllValues().get(0)
                                .getBindings(0).getPartitionIdsList()));
                Assertions.assertEquals(List.of(p1), requestCaptor.getAllValues().get(1)
                        .getBindings(0).getPartitionIdsList());
            }
        } finally {
            if (p1Executor != null) {
                p1Executor.onFail(new RuntimeException("test cleanup"));
            }
            Config.cloud_table_stream_max_partitions_per_insert = previousLimit;
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", previousSystemInfo);
            resetQueryContext();
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
        Cloud.GetTableStreamOffsetResponse response = Cloud.GetTableStreamOffsetResponse.newBuilder()
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

        String sql = "select * from test_stream.s1 partition (p1)";
        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try {
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.getTableStreamOffset(Mockito.any())).thenReturn(response);

                connectContext.setStartTime();
                UUID uuid = UUID.randomUUID();
                connectContext.setQueryId(
                        new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                PlanChecker checker = PlanChecker.from(connectContext).analyze(sql);
                Assertions.assertTrue(checker.getCascadesContext().getStatementContext().getPlannerHooks()
                        .contains(CloudTableStreamReadStateHook.INSTANCE));
                NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
                Mockito.when(planner.getCascadesContext()).thenReturn(checker.getCascadesContext());
                CloudTableStreamReadStateHook.INSTANCE.afterAnalyze(planner);
                Plan analyzedPlan = checker.getPlan();
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

                ArgumentCaptor<Cloud.GetTableStreamOffsetRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamOffsetRequest.class);
                Mockito.verify(proxy).getTableStreamOffset(requestCaptor.capture());
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
    public void testCloudReadStatePartialResponseDoesNotInstallStateAndCanRetry() throws Exception {
        String sql = "insert into test_stream.tbl_target "
                + "select * from test_stream.s1 union all select * from test_stream.s2";
        InsertIntoTableCommand command = (InsertIntoTableCommand) parser.parseSingle(sql);
        resetQueryContext();
        command.initPlan(connectContext, new StmtExecutor(connectContext, sql), false);
        Plan analyzedPlan = command.getLineagePlan().orElseThrow();
        Set<OlapTableStreamWrapper> wrappers = analyzedPlan
                .collectToList(LogicalOlapTableStreamScan.class::isInstance).stream()
                .map(scan -> ((LogicalOlapTableStreamScan) scan).getTable())
                .collect(Collectors.toSet());
        Assertions.assertEquals(2, wrappers.size());
        Assertions.assertTrue(wrappers.stream().noneMatch(OlapTableStreamWrapper::hasCloudReadStates));

        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try {
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.getTableStreamOffset(Mockito.any())).thenAnswer(invocation -> {
                    Cloud.GetTableStreamOffsetRequest request = invocation.getArgument(0);
                    Cloud.GetTableStreamOffsetResponse complete = buildReadStateResponse(request);
                    return Cloud.GetTableStreamOffsetResponse.newBuilder(complete)
                            .removeBindings(complete.getBindingsCount() - 1)
                            .build();
                }).thenAnswer(invocation -> buildReadStateResponse(invocation.getArgument(0)));

                // Read-state installation is a post-analysis invariant, so exercise the hook that
                // owns this lifecycle instead of the removed generic rewrite rule.
                org.apache.doris.nereids.exceptions.AnalysisException exception = Assertions.assertThrows(
                        org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> Deencapsulation.invoke(
                                CloudTableStreamReadStateHook.class, "resolve", analyzedPlan));
                Assertions.assertTrue(exception.getMessage()
                        .contains("did not return all Cloud Table Stream bindings"));
                Assertions.assertTrue(wrappers.stream().noneMatch(OlapTableStreamWrapper::hasCloudReadStates));

                Deencapsulation.invoke(CloudTableStreamReadStateHook.class, "resolve", analyzedPlan);
                Assertions.assertTrue(wrappers.stream().allMatch(OlapTableStreamWrapper::hasCloudReadStates));
                ArgumentCaptor<Cloud.GetTableStreamOffsetRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamOffsetRequest.class);
                Mockito.verify(proxy, Mockito.times(2)).getTableStreamOffset(requestCaptor.capture());
                for (Cloud.GetTableStreamOffsetRequest request : requestCaptor.getAllValues()) {
                    Assertions.assertEquals(2, request.getBindingsCount());
                    Assertions.assertTrue(request.getBindingsList().stream()
                            .allMatch(binding -> !binding.getPartitionIdsList().isEmpty()));
                }
            }
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
        }
    }

    @Test
    public void testCloudCteAndOuterStreamScansUseSingleReadStateRpc() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_cloud_mv_empty");
        long p1 = baseTable.getPartition("p1").getId();
        long p2 = baseTable.getPartition("p2").getId();
        String sql = "with cte as (select k1, k2 from test_stream.s_cloud_mv_empty) "
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
                Mockito.when(proxy.getTableStreamOffset(Mockito.any())).thenAnswer(invocation -> {
                    Cloud.GetTableStreamOffsetRequest request = invocation.getArgument(0);
                    Cloud.GetTableStreamOffsetResponse.Builder response =
                            Cloud.GetTableStreamOffsetResponse.newBuilder()
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
                Deencapsulation.invoke(CloudTableStreamReadStateHook.class, "resolve", checker.getPlan());
                checker.getCascadesContext().getStatementContext().setForceRecordTmpPlan(true);
                checker.rewrite();

                List<Plan> tmpPlans = checker.getCascadesContext().getStatementContext()
                        .getTmpPlanForMvRewrite();
                Assertions.assertFalse(tmpPlans.isEmpty());
                Assertions.assertTrue(tmpPlans.stream().allMatch(tmpPlan -> tmpPlan
                        .collectToList(LogicalOlapTableStreamScan.class::isInstance).isEmpty()));
                Assertions.assertTrue(checker.getCascadesContext().getRewritePlan()
                        .collectToList(LogicalOlapTableStreamScan.class::isInstance).isEmpty());

                checker.getCascadesContext().getStatementContext().setNeedPreMvRewrite(true);
                checker.preMvRewrite();
                Assertions.assertTrue(checker.getCascadesContext().getStatementContext().isPreMvRewritten());

                ArgumentCaptor<Cloud.GetTableStreamOffsetRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamOffsetRequest.class);
                Mockito.verify(proxy, Mockito.times(1)).getTableStreamOffset(requestCaptor.capture());
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
        String sql = "select k1, k2 from test_stream.s1 partition (p1) where k1 < 100 union all "
                + "select k1, k2 from test_stream.s1@snapshot() partition (p2) "
                + "where k1 >= 100 and k1 < 200";

        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try {
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.getTableStreamOffset(Mockito.any())).thenAnswer(invocation -> {
                    Cloud.GetTableStreamOffsetRequest request = invocation.getArgument(0);
                    Cloud.GetTableStreamOffsetResponse.Builder response =
                            Cloud.GetTableStreamOffsetResponse.newBuilder()
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
                Deencapsulation.invoke(CloudTableStreamReadStateHook.class, "resolve", analyzedPlan);
                checker.rewrite();

                List<TableStreamUpdateInfo> updates = StreamConsumptionInfoExtractor.extract(analyzedPlan);
                Assertions.assertEquals(1, updates.size());
                CloudOlapTableStreamUpdate update = (CloudOlapTableStreamUpdate) updates.get(0).getUpdate();
                Assertions.assertEquals(Set.of(p1), update.getPartitionUpdates().keySet());

                ArgumentCaptor<Cloud.GetTableStreamOffsetRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamOffsetRequest.class);
                Mockito.verify(proxy).getTableStreamOffset(requestCaptor.capture());
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
    public void testCloudPredicateEliminationDoesNotShrinkOffsetRange() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        long p1 = baseTable.getPartition("p1").getId();
        long p2 = baseTable.getPartition("p2").getId();
        String sql = "select k1, k2 from test_stream.s1 where false";
        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try {
            Config.cloud_unique_id = "cloud_table_stream_ut";
            Config.meta_service_endpoint = "127.0.0.1:20121";
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.getTableStreamOffset(Mockito.any())).thenAnswer(invocation -> {
                    Cloud.GetTableStreamOffsetRequest request = invocation.getArgument(0);
                    Cloud.GetTableStreamOffsetResponse.Builder response =
                            Cloud.GetTableStreamOffsetResponse.newBuilder()
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
                Deencapsulation.invoke(CloudTableStreamReadStateHook.class, "resolve", analyzedPlan);
                checker.rewrite();

                List<TableStreamUpdateInfo> updates = StreamConsumptionInfoExtractor.extract(analyzedPlan);
                Assertions.assertEquals(1, updates.size());
                CloudOlapTableStreamUpdate update = (CloudOlapTableStreamUpdate) updates.get(0).getUpdate();
                Assertions.assertEquals(Set.of(p1, p2), update.getPartitionUpdates().keySet());

                ArgumentCaptor<Cloud.GetTableStreamOffsetRequest> requestCaptor =
                        ArgumentCaptor.forClass(Cloud.GetTableStreamOffsetRequest.class);
                Mockito.verify(proxy).getTableStreamOffset(requestCaptor.capture());
                Assertions.assertEquals(Set.of(p1, p2),
                        new HashSet<>(requestCaptor.getValue().getBindings(0).getPartitionIdsList()));
            }
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
        }
    }

    private void resetQueryContext() {
        connectContext.setStartTime();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
    }

    private Cloud.GetTableStreamOffsetResponse buildReadStateResponse(
            Cloud.GetTableStreamOffsetRequest request) {
        Cloud.GetTableStreamOffsetResponse.Builder response = Cloud.GetTableStreamOffsetResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK));
        for (Cloud.TableStreamPartitionSetPB binding : request.getBindingsList()) {
            Cloud.TableStreamReadBindingResultPB.Builder result = Cloud.TableStreamReadBindingResultPB.newBuilder()
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
    }
}
