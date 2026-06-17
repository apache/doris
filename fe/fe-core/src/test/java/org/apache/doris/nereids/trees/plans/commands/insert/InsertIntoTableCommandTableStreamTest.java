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
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.stream.AbstractTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        //   prev == historicalPartitionTSO[pid] when present, else partitionOffset[pid].
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
        // history partition's prev must equal the historical TSO snapshot.
        Assertions.assertEquals(Long.valueOf(historyTso), producedPrev.get(historyPid),
                "prev of history partition must equal historicalPartitionTSO");
    }
}
