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

package org.apache.doris.statistics.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * DROP TABLE, DROP DATABASE and REPLACE TABLE remove the statistics record of a table, and their replay runs
 * the very same code again on every frontend. Those paths must not write an OP_DELETE_TABLE_STATS entry of
 * their own: a frontend which replays the journal must not write to it at all (BDBJE treats a replica side
 * write as fatal), and on the live path such an entry would make the removal durable before the entry of the
 * enclosing operation, so a crash in between would restore the table without its statistics.
 * <p>
 * The edit log of these tests rejects every statistics deletion, so a removal which journals its own entry
 * fails the test with that reason instead of passing quietly.
 */
public class StatisticsRecordReplayTest extends TestWithFeService {

    private static final String DB_NAME = "test_stats_record_replay";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        createDatabase(DB_NAME);
        connectContext.setDatabase(DB_NAME);
    }

    private EditLog rejectingEditLog(Env env) {
        EditLog spyEditLog = Mockito.spy(env.getEditLog());
        Mockito.doThrow(new AssertionError("a replay must not write OP_DELETE_TABLE_STATS"))
                .when(spyEditLog).logDeleteTableStats(Mockito.any());
        return spyEditLog;
    }

    @Test
    public void testDropTableReplayDoesNotJournalTheStatsDeletion() throws Exception {
        createTable("create table " + DB_NAME + ".t_drop_stats (k int) distributed by hash(k) buckets 1 "
                + "properties('replication_num'='1')");
        Env env = Env.getCurrentEnv();
        Database db = env.getInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTable table = (OlapTable) db.getTableNullable("t_drop_stats");
        long tableId = table.getId();
        // Isolate the assertion from the record being absent anyway: the record exists before the replay.
        env.getAnalysisManager().replayUpdateTableStatsStatus(new TableStatsMeta(table));
        Assertions.assertNotNull(env.getAnalysisManager().findTableStatsStatus(tableId));

        EditLog editLog = env.getEditLog();
        env.setEditLog(rejectingEditLog(env));
        try {
            // What a follower does while it replays OP_DROP_TABLE.
            env.replayDropTable(db, tableId, true, 0L);
        } finally {
            env.setEditLog(editLog);
        }

        // The record is removed, and the entry of the enclosing DROP is what makes that transition durable
        // and what removes the record on the other frontends.
        Assertions.assertNull(env.getAnalysisManager().findTableStatsStatus(tableId));
    }

    @Test
    public void testReplaceTableReplayDoesNotJournalTheStatsDeletion() throws Exception {
        createTable("create table " + DB_NAME + ".t_replace_orig (k int) distributed by hash(k) buckets 1 "
                + "properties('replication_num'='1')");
        createTable("create table " + DB_NAME + ".t_replace_new (k int) distributed by hash(k) buckets 1 "
                + "properties('replication_num'='1')");
        Env env = Env.getCurrentEnv();
        Database db = env.getInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTable origTable = (OlapTable) db.getTableNullable("t_replace_orig");
        OlapTable newTable = (OlapTable) db.getTableNullable("t_replace_new");
        env.getAnalysisManager().replayUpdateTableStatsStatus(new TableStatsMeta(origTable));
        Assertions.assertNotNull(env.getAnalysisManager().findTableStatsStatus(origTable.getId()));

        EditLog editLog = env.getEditLog();
        env.setEditLog(rejectingEditLog(env));
        try {
            // What a follower does while it replays OP_REPLACE_TABLE. A non swap replacement erases the
            // replaced table, which is the branch that removes the statistics record.
            env.getAlterInstance().replayReplaceTable(new ReplaceTableOperationLog(db.getId(), origTable.getId(),
                    origTable.getName(), newTable.getId(), newTable.getName(), false, true));
        } finally {
            env.setEditLog(editLog);
        }

        Assertions.assertNull(env.getAnalysisManager().findTableStatsStatus(origTable.getId()));
    }
}
