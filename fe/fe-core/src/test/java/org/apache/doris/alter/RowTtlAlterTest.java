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

package org.apache.doris.alter;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.RowTtlConfig;
import org.apache.doris.catalog.TabletBinlogConfig;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RowTtlAlterTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("row_ttl_alter");
    }

    private OlapTable createSourceTable(String name) throws Exception {
        createTable("CREATE TABLE row_ttl_alter." + name
                + " (k INT, event_time DATETIMEV2(6)) DUPLICATE KEY(k)"
                + " PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (2), PARTITION p2 VALUES LESS THAN (3))"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1',"
                + " 'function_column.enable_row_ttl'='true', 'function_column.ttl_col'='event_time',"
                + " 'function_column.ttl'='1 day', 'function_column.ttl_time_zone'='+00:00')");
        return (OlapTable) database().getTableOrDdlException(name);
    }

    private Database database() throws DdlException {
        return Env.getCurrentInternalCatalog().getDbOrDdlException("row_ttl_alter");
    }

    private static class RecordingHandler extends SchemaChangeHandler {
        private final long expectedQueryDuration;
        private final long expectedGcDuration;
        private final int failAfter;
        private int updated;
        private Runnable afterUpdate = () -> {};

        RecordingHandler(long expectedQueryDuration, long expectedGcDuration, int failAfter) {
            this.expectedQueryDuration = expectedQueryDuration;
            this.expectedGcDuration = expectedGcDuration;
            this.failAfter = failAfter;
        }

        @Override
        protected void updatePartitionProperties(Database db, OlapTable table, Partition partition,
                long storagePolicyId, int isInMemory, TabletBinlogConfig binlogConfig, String compactionPolicy,
                Map<String, Long> timeSeriesCompactionConfig, int skipWriteIndexOnLoad, int disableAutoCompaction,
                int verticalCompactionNumColumnsPerGroup, RowTtlConfig config) throws UserException {
            Assertions.assertEquals(expectedQueryDuration, table.getRowTtlDurationMicros());
            Assertions.assertEquals(expectedGcDuration, table.getRowTtlConfig().getDurationUs());
            Assertions.assertEquals(config.getConfigVersion(), table.getRowTtlConfig().getConfigVersion());
            // Persisted properties carry the in-flight policy across FE restart and into new replicas.
            OlapTable restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
            Assertions.assertEquals(expectedQueryDuration, restored.getRowTtlDurationMicros());
            Assertions.assertEquals(expectedGcDuration, restored.getRowTtlConfig().getDurationUs());
            Assertions.assertEquals(config.getConfigVersion(), restored.getRowTtlConfig().getConfigVersion());
            if (updated == failAfter) {
                throw new DdlException("injected partition update failure");
            }
            updated++;
            afterUpdate.run();
        }
    }

    @Test
    void testExtensionRejectedBeforePublication() throws Exception {
        OlapTable table = createSourceTable("extension");
        long day = 86_400_000_000L;
        RecordingHandler handler = new RecordingHandler(day, day, -1);
        DdlException error = Assertions.assertThrows(DdlException.class,
                () -> handler.updateRowTtl(database(), table, "2 day"));
        Assertions.assertTrue(error.getMessage().contains("Increasing function_column.ttl is not supported"));
        Assertions.assertEquals(0, handler.updated);
        Assertions.assertEquals(day, table.getRowTtlDurationMicros());
        Assertions.assertEquals(0, table.getRowTtlConfig().getConfigVersion());
    }

    @Test
    void testShorterPolicyPublishedBeforeGcAndRetry() throws Exception {
        OlapTable table = createSourceTable("shorter");
        RecordingHandler failed = new RecordingHandler(0, 0, 1);
        Assertions.assertThrows(DdlException.class, () -> failed.updateRowTtl(database(), table, "0"));
        Assertions.assertEquals(0, table.getRowTtlDurationMicros());
        Assertions.assertEquals(1, table.getRowTtlConfig().getConfigVersion());
        RecordingHandler retry = new RecordingHandler(0, 0, -1);
        retry.updateRowTtl(database(), table, "0");
        Assertions.assertEquals(2, retry.updated);
        Assertions.assertEquals(2, table.getRowTtlConfig().getConfigVersion());
    }

    @Test
    void testClonedReplicaMustAcknowledgeNewGcPolicy() throws Exception {
        OlapTable table = createSourceTable("clone_during_update");
        RecordingHandler handler = new RecordingHandler(0, 0, -1);
        handler.afterUpdate = () -> {
            table.writeLock();
            try {
                table.getPartitions().iterator().next().getBaseIndex().getTablets().get(0).addReplica(
                        new LocalReplica(Env.getCurrentEnv().getNextId(), 99999, ReplicaState.CLONE, 1, 0));
            } finally {
                table.writeUnlock();
            }
        };
        DdlException error = Assertions.assertThrows(DdlException.class,
                () -> handler.updateRowTtl(database(), table, "0"));
        Assertions.assertTrue(error.getMessage().contains("Table changed during row TTL update"));
        Assertions.assertEquals(0, table.getRowTtlDurationMicros());
        new RecordingHandler(0, 0, -1).updateRowTtl(database(), table, "0");
        Assertions.assertEquals(0, table.getRowTtlDurationMicros());
    }

    @Test
    void testDirectExpirationRejectsTableDuration() throws Exception {
        createTable("CREATE TABLE row_ttl_alter.direct_expiration (k INT) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1',"
                + " 'function_column.enable_row_ttl'='true')");
        OlapTable table = (OlapTable) database().getTableOrDdlException("direct_expiration");
        UserException error = Assertions.assertThrows(UserException.class,
                () -> new SchemaChangeHandler().updateRowTtl(database(), table, "1 day"));
        Assertions.assertTrue(error.getMessage().contains("source-column row TTL table"));
    }
}
