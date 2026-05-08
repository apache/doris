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
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

        createDatabase("test_stream");
        connectContext.setDatabase("test_stream");

        String createBaseTable = "create table test_stream.tbl_stream_base (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")";
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
                + "properties('type' = 'default', 'show_initial_rows' = 'true')";
        createTable(createStream);
    }

    @Test
    public void testHistoricalConsumeOffsetCheckAndUpdate() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        List<Long> partitionIds = new ArrayList<>(baseTable.getPartitionIds());
        Map<Long, Long> historicalPartitionOffset = new HashMap<>();
        Map<Long, Long> partitionOffset = new HashMap<>();
        for (Long partitionId : partitionIds) {
            historicalPartitionOffset.put(partitionId, 100L);
            partitionOffset.put(partitionId, 0L);
        }
        Deencapsulation.setField(stream, "historicalPartitionOffset", historicalPartitionOffset);
        Deencapsulation.setField(stream, "partitionOffset", partitionOffset);

        OlapTableStreamUpdate update = new OlapTableStreamUpdate(new HashMap<>(),
                new HashMap<>(historicalPartitionOffset));
        Assertions.assertTrue(update.getNext().keySet().containsAll(partitionIds));

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
        Deencapsulation.invoke(dbTxnMgr, "updateStreamOffset", transactionState, commitTime);

        Map<Long, Long> updatedHistoricalPartitionOffset = Deencapsulation.getField(stream, "historicalPartitionOffset");
        Map<Long, Long> updatedPartitionOffset = Deencapsulation.getField(stream, "partitionOffset");
        Map<Long, Long> partitionConsumptionTime = Deencapsulation.getField(stream, "partitionConsumptionTime");

        for (Long pid : partitionIds) {
            Assertions.assertFalse(updatedHistoricalPartitionOffset.containsKey(pid));
            Assertions.assertEquals(update.getNext().get(pid), updatedPartitionOffset.get(pid));
            Assertions.assertEquals(commitTime, partitionConsumptionTime.get(pid));
        }
    }

    @Test
    public void testHistoricalConsumeConcurrentCommitPrevMissing() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        List<Long> partitionIds = new ArrayList<>(baseTable.getPartitionIds());
        Map<Long, Long> prev = Maps.newHashMap();
        Map<Long, Long> next = Maps.newHashMap();
        for (Long partitionId : partitionIds) {
            Pair<Long, Long> streamUpdate = stream.getStreamUpdate(partitionId);
            if (streamUpdate.first != null) {
                prev.put(partitionId, streamUpdate.first);
            }
            next.put(partitionId, streamUpdate.second);
        }
        OlapTableStreamUpdate updateAtReadTime = new OlapTableStreamUpdate(prev, next);

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
}
