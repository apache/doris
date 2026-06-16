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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.TableStreamCleanupInfo;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TableStreamManagerCleanupTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
        createDatabase("test_stream_cleanup");
        connectContext.setDatabase("test_stream_cleanup");
    }

    @Test
    public void testCleanupRemovedPartitionOffsets() throws Exception {
        StreamContext context = createStreamContext("cleanup_normal");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);

        alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName() + " drop partition p2");
        Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets();

        assertPartitionState(context.stream, keptPartitionId, removedPartitionId, true);
    }

    @Test
    public void testCleanupSkipsDisabledStream() throws Exception {
        StreamContext context = createStreamContext("cleanup_disabled");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);

        alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName() + " drop partition p2");
        context.stream.writeLock();
        try {
            context.stream.setDisabled(true);
        } finally {
            context.stream.writeUnlock();
        }
        Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets();

        assertPartitionState(context.stream, keptPartitionId, removedPartitionId, false);
    }

    @Test
    public void testCleanupSkipsStaleStream() throws Exception {
        StreamContext context = createStreamContext("cleanup_stale");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);

        alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName() + " drop partition p2");
        context.stream.writeLock();
        try {
            context.stream.setStale(true);
            context.stream.setStaleReason("ut");
        } finally {
            context.stream.writeUnlock();
        }
        Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets();

        assertPartitionState(context.stream, keptPartitionId, removedPartitionId, false);
    }

    @Test
    public void testReplayPrunePartitionOffsetsDirectly() throws Exception {
        StreamContext context = createStreamContext("replay_prune");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);

        context.stream.writeLock();
        try {
            context.stream.setDisabled(true);
        } finally {
            context.stream.writeUnlock();
        }
        Env.getCurrentEnv().getTableStreamManager().replayTableStreamCleanup(
                new TableStreamCleanupInfo(Collections.singletonList(
                        new TableStreamCleanupInfo.PartitionOffsetPruneEntry(
                                context.stream.getDatabase().getId(), context.stream.getId(),
                                Collections.singleton(removedPartitionId)))));

        assertPartitionState(context.stream, keptPartitionId, removedPartitionId, true);
    }

    @Test
    public void testReplayRemoveStaleDbAndStream() throws Exception {
        StreamContext context = createStreamContext("replay_remove");
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_cleanup");
        long dbId = db.getId();
        long streamId = context.stream.getId();

        Assertions.assertTrue(
                Env.getCurrentEnv().getTableStreamManager().getTableStreamIds(db).contains(streamId));

        Env.getCurrentEnv().getTableStreamManager().replayTableStreamCleanup(
                new TableStreamCleanupInfo(Collections.emptyList(), Collections.emptyList(),
                        Collections.singletonList(Pair.of(dbId, streamId))));

        Assertions.assertFalse(
                Env.getCurrentEnv().getTableStreamManager().getTableStreamIds(db).contains(streamId));
    }

    private StreamContext createStreamContext(String suffix) throws Exception {
        String tableName = "tbl_" + suffix;
        String streamName = "s_" + suffix;
        createTable("create table test_stream_cleanup." + tableName + " (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\",\"binlog.enable\"=\"true\","
                + "\"binlog.format\"=\"ROW\","
                + "\"binlog.need_historical_value\"=\"true\")");
        createTable("create stream test_stream_cleanup." + streamName + " on table test_stream_cleanup." + tableName
                + " properties('type' = 'append_only', 'show_initial_rows' = 'true')");

        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_cleanup");
        return new StreamContext((OlapTable) db.getTableOrMetaException(tableName),
                (OlapTableStream) db.getTableOrMetaException(streamName));
    }

    private void setPartitionState(OlapTableStream stream, long keptPartitionId, long removedPartitionId) {
        Map<Long, Long> partitionOffset = new HashMap<>();
        partitionOffset.put(keptPartitionId, 11L);
        partitionOffset.put(removedPartitionId, 22L);
        Map<Long, Long> partitionConsumptionTime = new HashMap<>();
        partitionConsumptionTime.put(keptPartitionId, 111L);
        partitionConsumptionTime.put(removedPartitionId, 222L);
        Map<Long, Long> historicalPartitionOffset = new HashMap<>();
        historicalPartitionOffset.put(keptPartitionId, 101L);
        historicalPartitionOffset.put(removedPartitionId, 202L);
        Map<Long, Long> historicalPartitionTSO = new HashMap<>();
        historicalPartitionTSO.put(keptPartitionId, 1001L);
        historicalPartitionTSO.put(removedPartitionId, 2002L);
        Deencapsulation.setField(stream, "partitionOffset", partitionOffset);
        Deencapsulation.setField(stream, "partitionConsumptionTime", partitionConsumptionTime);
        Deencapsulation.setField(stream, "historicalPartitionOffset", historicalPartitionOffset);
        Deencapsulation.setField(stream, "historicalPartitionTSO", historicalPartitionTSO);
    }

    private void assertPartitionState(OlapTableStream stream, long keptPartitionId, long removedPartitionId,
            boolean removedExpected) {
        Map<Long, Long> partitionOffset = Deencapsulation.getField(stream, "partitionOffset");
        Map<Long, Long> partitionConsumptionTime = Deencapsulation.getField(stream, "partitionConsumptionTime");
        Map<Long, Long> historicalPartitionOffset = Deencapsulation.getField(stream, "historicalPartitionOffset");
        Map<Long, Long> historicalPartitionTSO = Deencapsulation.getField(stream, "historicalPartitionTSO");

        Assertions.assertTrue(partitionOffset.containsKey(keptPartitionId));
        Assertions.assertTrue(partitionConsumptionTime.containsKey(keptPartitionId));
        Assertions.assertTrue(historicalPartitionOffset.containsKey(keptPartitionId));
        Assertions.assertTrue(historicalPartitionTSO.containsKey(keptPartitionId));
        Assertions.assertEquals(!removedExpected, partitionOffset.containsKey(removedPartitionId));
        Assertions.assertEquals(!removedExpected, partitionConsumptionTime.containsKey(removedPartitionId));
        Assertions.assertEquals(!removedExpected, historicalPartitionOffset.containsKey(removedPartitionId));
        Assertions.assertEquals(!removedExpected, historicalPartitionTSO.containsKey(removedPartitionId));
    }

    private static class StreamContext {
        private final OlapTable baseTable;
        private final OlapTableStream stream;

        private StreamContext(OlapTable baseTable, OlapTableStream stream) {
            this.baseTable = baseTable;
            this.stream = stream;
        }
    }
}
