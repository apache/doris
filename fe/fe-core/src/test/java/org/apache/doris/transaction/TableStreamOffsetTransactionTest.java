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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStream.PartitionStreamState;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate.PartitionUpdate;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableStreamOffsetTransactionTest extends TestWithFeService {

    @Override
    public void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
        Config.enable_feature_changelog = true;

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
                + "properties(\"replication_num\"=\"1\",\"changelog.enable\"=\"true\","
                + "\"changelog.need_historical_value\"=\"true\")";
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
    public void testHistoricalConsumeOffsetCheckAndUpdate() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        List<Long> partitionIds = new ArrayList<>(baseTable.getPartitionIds());
        Map<Long, Long> historicalPartitionTSO = new HashMap<>();
        for (Long partitionId : partitionIds) {
            historicalPartitionTSO.put(partitionId, 1000L + partitionId);
        }
        // committed offset must be advanced to the historical TSO snapshot captured at creation
        Map<Long, Long> expectedPartitionOffset = new HashMap<>(historicalPartitionTSO);
        Map<Long, PartitionStreamState> partitionStates = new HashMap<>();
        historicalPartitionTSO.forEach((pid, tso) ->
                partitionStates.put(pid, PartitionStreamState.restored(null, -1, tso, -1L, -1L)));
        Deencapsulation.setField(stream, "partitionStates", partitionStates);

        // historical consume: prev is the historical TSO snapshot encoded as negative
        // (see StreamConsumptionInfoExtractor), next points to the historical TSO snapshot
        Map<Long, PartitionUpdate> updates = Maps.newHashMapWithExpectedSize(historicalPartitionTSO.size());
        historicalPartitionTSO.forEach((pid, tso) ->
                updates.put(pid, new PartitionUpdate(-tso, PartitionStreamState.initial().atOffset(tso))));
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(updates);
        Assertions.assertTrue(update.getPartitionUpdates().keySet().containsAll(partitionIds));

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
        long txnId = transactionMgr.beginTransaction(db.getId(), Collections.singletonList(targetTable.getId()),
                "ut_table_stream_hist_ok_" + System.nanoTime(), coordinator,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        TransactionState transactionState = transactionMgr.getTransactionState(db.getId(), txnId);
        transactionState.setStreamUpdateInfos(Collections.singletonList(
                new TableStreamUpdateInfo(db.getId(), stream.getId(), update)));

        DatabaseTransactionMgr dbTxnMgr = transactionMgr.getDatabaseTransactionMgr(db.getId());
        Deencapsulation.invoke(dbTxnMgr, "checkStreamOffset", transactionState);

        long commitTime = System.currentTimeMillis();
        Deencapsulation.invoke(dbTxnMgr, "updateStreamOffset", transactionState, commitTime, false);

        for (Long pid : partitionIds) {
            PartitionStreamState state = partitionStates.get(pid);
            Assertions.assertNull(state.getHistoricalTso());
            Assertions.assertEquals(expectedPartitionOffset.get(pid), state.getOffset());
            Assertions.assertEquals(commitTime, state.getConsumptionTime());
        }
    }

    @Test
    public void testHistoricalConsumeConcurrentCommitPrevMissing() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        List<Long> partitionIds = new ArrayList<>(baseTable.getPartitionIds());
        Map<Long, PartitionUpdate> updates = Maps.newHashMapWithExpectedSize(partitionIds.size());
        for (Long partitionId : partitionIds) {
            Pair<Long, Long> streamUpdate = stream.getStreamUpdate(partitionId);
            updates.put(partitionId, new PartitionUpdate(
                    streamUpdate.first, PartitionStreamState.initial().atOffset(streamUpdate.second)));
        }
        OlapTableStreamUpdate updateAtReadTime = new OlapTableStreamUpdate(updates);

        long committedTime = System.currentTimeMillis();
        stream.unprotectedUpdateStreamUpdate(updateAtReadTime, committedTime);

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
        long txnId = transactionMgr.beginTransaction(db.getId(), Collections.singletonList(targetTable.getId()),
                "ut_table_stream_hist_conflict_" + System.nanoTime(), coordinator,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        TransactionState transactionState = transactionMgr.getTransactionState(db.getId(), txnId);
        transactionState.setStreamUpdateInfos(Collections.singletonList(
                new TableStreamUpdateInfo(db.getId(), stream.getId(), updateAtReadTime)));

        DatabaseTransactionMgr dbTxnMgr = transactionMgr.getDatabaseTransactionMgr(db.getId());
        TransactionCommitFailedException exception = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> Deencapsulation.invoke(dbTxnMgr, "checkStreamOffset", transactionState));
        Assertions.assertTrue(exception.getMessage().contains("previous version missing"));
    }

    @Test
    public void testTransactionLogCarriesPendingCountsAndOffsetTogether() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable source = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable target = (OlapTable) db.getTableOrMetaException("tbl_target");
        long pid = source.getPartitionIds().get(0);
        OlapTableStream stream = new OlapTableStream(Env.getCurrentEnv().getNextId(), "counts_log_stream", source);
        db.registerTable(stream);
        source.getPartition(pid).getBaseIndex().setRowCount(5L);
        source.getPartition(pid).getIndex(source.getChangelogIndexMeta().getIndexId()).setRowCount(10L);
        stream.unprotectedPublishPartitionState(pid,
                PartitionStreamState.restored(100L, 1L, null, -1L, -1L).withRowCounts(source, pid));
        PartitionStreamState previous = stream.getPartitionState(pid);
        GlobalTransactionMgr mgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        long id = mgr.beginTransaction(db.getId(), List.of(target.getId()), "counts_log_transaction",
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0, "localfe",
                        System.currentTimeMillis()), TransactionState.LoadJobSourceType.FRONTEND,
                Config.stream_load_default_timeout_second);
        TransactionState txn = mgr.getTransactionState(db.getId(), id);
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(
                Map.of(pid, new PartitionUpdate(100L, PartitionStreamState.initial().atOffset(200L))));
        txn.setStreamUpdateInfos(List.of(new TableStreamUpdateInfo(db.getId(), stream.getId(), update)));
        source.getPartition(pid).getBaseIndex().setRowCount(50L);
        source.getPartition(pid).getIndex(source.getChangelogIndexMeta().getIndexId()).setRowCount(100L);
        DatabaseTransactionMgr dbTxnMgr = mgr.getDatabaseTransactionMgr(db.getId());
        stream.writeLock();
        try {
            Deencapsulation.invoke(dbTxnMgr, "checkStreamOffset", txn);
            Assertions.assertSame(previous, stream.getPartitionState(pid));
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            txn.write(new DataOutputStream(bytes));
            TransactionState journaled = TransactionState.read(new DataInputStream(
                    new ByteArrayInputStream(bytes.toByteArray())));
            PartitionStreamState pending = ((OlapTableStreamUpdate) journaled.getStreamUpdateInfos()
                    .get(0).getUpdate()).getPartitionUpdates().get(pid).getNextState();
            Assertions.assertEquals(200L, pending.getOffset());
            Assertions.assertEquals(50L, pending.getBaseRowCount());
            Assertions.assertEquals(100L, pending.getChangelogRowCount());
            source.getPartition(pid).getBaseIndex().setRowCount(70L);
            source.getPartition(pid).getIndex(source.getChangelogIndexMeta().getIndexId()).setRowCount(130L);
            Deencapsulation.invoke(dbTxnMgr, "updateStreamOffset", journaled, 2L, true);
            PartitionStreamState recovered = stream.getPartitionState(pid);
            Assertions.assertEquals(200L, recovered.getOffset());
            Assertions.assertEquals(50L, recovered.getBaseRowCount());
            Assertions.assertEquals(100L, recovered.getChangelogRowCount());
            Assertions.assertFalse(recovered.matches(source, pid, 200L));
        } finally {
            stream.writeUnlock();
        }
    }

    @Test
    public void testAbortDoesNotPublishPendingTuple() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable source = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable target = (OlapTable) db.getTableOrMetaException("tbl_target");
        long pid = source.getPartitionIds().get(0);
        OlapTableStream stream = new OlapTableStream(Env.getCurrentEnv().getNextId(), "counts_abort_stream", source);
        db.registerTable(stream);
        stream.unprotectedPublishPartitionState(pid,
                PartitionStreamState.restored(100L, 1L, null, -1L, -1L).withRowCounts(source, pid));
        PartitionStreamState previous = stream.getPartitionState(pid);
        GlobalTransactionMgr mgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        long id = mgr.beginTransaction(db.getId(), List.of(target.getId()), "counts_abort_transaction",
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0, "localfe",
                        System.currentTimeMillis()), TransactionState.LoadJobSourceType.FRONTEND,
                Config.stream_load_default_timeout_second);
        TransactionState txn = mgr.getTransactionState(db.getId(), id);
        txn.setStreamUpdateInfos(List.of(new TableStreamUpdateInfo(db.getId(), stream.getId(),
                new OlapTableStreamUpdate(
                        Map.of(pid, new PartitionUpdate(100L, PartitionStreamState.initial().atOffset(200L)))))));
        stream.writeLock();
        try {
            Deencapsulation.invoke(mgr.getDatabaseTransactionMgr(db.getId()), "checkStreamOffset", txn);
        } finally {
            stream.writeUnlock();
        }
        mgr.abortTransaction(db.getId(), id, "abort pending count tuple");
        Assertions.assertEquals(TransactionStatus.ABORTED, txn.getTransactionStatus());
        Assertions.assertSame(previous, stream.getPartitionState(pid));
    }

}
