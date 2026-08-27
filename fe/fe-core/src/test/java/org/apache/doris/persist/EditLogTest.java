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

package org.apache.doris.persist;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.DistributionMappingConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.bdbje.Timestamp;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class EditLogTest {
    private String originalEditLogType;
    private int originalEditLogRollNum;
    private int originalCloudEditLogRollIntervalSecond;
    private String originalDeployMode;
    private String originalCloudUniqueId;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUpEditLogRollConfig() {
        originalEditLogType = Config.edit_log_type;
        originalEditLogRollNum = Config.edit_log_roll_num;
        originalCloudEditLogRollIntervalSecond = Config.cloud_edit_log_roll_interval_second;
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;

        Config.edit_log_type = "local";
        Config.edit_log_roll_num = Integer.MAX_VALUE;
        Config.cloud_edit_log_roll_interval_second = 3600;
        Config.cloud_unique_id = "";
    }

    @After
    public void restoreEditLogRollConfig() {
        Config.edit_log_type = originalEditLogType;
        Config.edit_log_roll_num = originalEditLogRollNum;
        Config.cloud_edit_log_roll_interval_second = originalCloudEditLogRollIntervalSecond;
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
    }

    @Test
    public void testTimestampUsesQueueWhenBatchEditLogDisabled() {
        boolean original = Config.enable_batch_editlog;
        try {
            Config.enable_batch_editlog = false;
            Assert.assertTrue(EditLog.shouldUseQueue(OperationType.OP_TIMESTAMP));
            Assert.assertTrue(EditLog.requiresDirectJournalWrite(OperationType.OP_TIMESTAMP));
            Assert.assertTrue(EditLog.shouldUseQueue(OperationType.OP_ADD_CONSTRAINT));
            Assert.assertFalse(EditLog.requiresDirectJournalWrite(OperationType.OP_ADD_CONSTRAINT));

            Config.enable_batch_editlog = true;
            Assert.assertFalse(EditLog.shouldUseQueue(OperationType.OP_TIMESTAMP));
            Assert.assertTrue(EditLog.shouldUseQueue(OperationType.OP_ADD_CONSTRAINT));
        } finally {
            Config.enable_batch_editlog = original;
        }
    }

    @Test
    public void testReplayMappingConstraintWaitsForTableWriteLock() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogManager = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CountDownLatch writeLockAttempted = new CountDownLatch(1);
        OlapTable table = new OlapTable() {
            @Override
            public void writeLock() {
                writeLockAttempted.countDown();
                super.writeLock();
            }
        };
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        NereidsSqlCacheManager sqlCacheManager = Mockito.mock(NereidsSqlCacheManager.class);
        TableNameInfo tableNameInfo = new TableNameInfo("internal", "db", "tbl");
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("d1"), List.of("k1"));
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogManager);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getSqlCacheManager()).thenReturn(sqlCacheManager);
        Mockito.when(catalogManager.getCatalog("internal")).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(database);
        Mockito.when(database.getTableNullable("tbl")).thenReturn(table);

        table.readLock();
        boolean readLocked = true;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> replay = executor.submit(() -> Deencapsulation.invoke(
                    EditLog.class, "replayConstraint", env, tableNameInfo, mapping, true));
            Assert.assertTrue(writeLockAttempted.await(10, TimeUnit.SECONDS));
            Assert.assertFalse(replay.isDone());

            table.readUnlock();
            readLocked = false;
            replay.get(10, TimeUnit.SECONDS);
        } finally {
            if (readLocked) {
                table.readUnlock();
            }
            executor.shutdownNow();
        }

        Mockito.verify(constraintManager).addConstraint(
                tableNameInfo, mapping.getName(), mapping, true);
        Mockito.verify(sqlCacheManager).invalidateAboutTableAndFencePublication(table);
    }

    @Test
    public void testQueuedTimestampSplitsJournalBatchInFifoOrder() throws Exception {
        Journal journal = Mockito.mock(Journal.class);
        AtomicLong nextLogId = new AtomicLong(10);
        List<String> writes = new ArrayList<>();
        Mockito.when(journal.write(Mockito.any(JournalBatch.class))).thenAnswer(invocation -> {
            JournalBatch batch = invocation.getArgument(0);
            List<JournalBatch.Entity> entities = batch.getJournalEntities();
            writes.add("batch:" + entities.get(0).getOpCode());
            return nextLogId.getAndAdd(entities.size());
        });
        Mockito.when(journal.write(
                        Mockito.eq(OperationType.OP_TIMESTAMP), Mockito.any()))
                .thenAnswer(invocation -> {
                    writes.add("timestamp");
                    return nextLogId.getAndIncrement();
                });

        List<EditLog.EditLogItem> requests = List.of(
                new EditLog.EditLogItem(OperationType.OP_ADD_CONSTRAINT, new Text("add")),
                new EditLog.EditLogItem(OperationType.OP_TIMESTAMP, new Text("timestamp")),
                new EditLog.EditLogItem(OperationType.OP_DROP_CONSTRAINT, new Text("drop")));
        List<long[]> logIdNumPairs = EditLog.writeJournalBatch(journal, requests);

        Assert.assertEquals(List.of(
                "batch:" + OperationType.OP_ADD_CONSTRAINT,
                "timestamp",
                "batch:" + OperationType.OP_DROP_CONSTRAINT), writes);
        Assert.assertArrayEquals(new long[]{10, 1}, logIdNumPairs.get(0));
        Assert.assertArrayEquals(new long[]{11, 1}, logIdNumPairs.get(1));
        Assert.assertArrayEquals(new long[]{12, 1}, logIdNumPairs.get(2));
    }

    @Test
    public void testAtomicRequestStaysInSingleJournalBatch() throws Exception {
        int originalMaxItemNum = Config.batch_edit_log_max_item_num;
        long originalMaxByteSize = Config.batch_edit_log_max_byte_size;
        try {
            Config.batch_edit_log_max_item_num = 1;
            Config.batch_edit_log_max_byte_size = 1;

            Journal journal = Mockito.mock(Journal.class);
            AtomicLong nextLogId = new AtomicLong(10);
            List<List<Short>> writes = new ArrayList<>();
            Mockito.when(journal.write(Mockito.any(JournalBatch.class))).thenAnswer(invocation -> {
                JournalBatch batch = invocation.getArgument(0);
                List<Short> opCodes = new ArrayList<>();
                for (JournalBatch.Entity entity : batch.getJournalEntities()) {
                    opCodes.add(entity.getOpCode());
                }
                writes.add(opCodes);
                return nextLogId.getAndAdd(batch.getJournalEntities().size());
            });

            List<EditLog.EditLogItem> requests = List.of(
                    new EditLog.EditLogItem(OperationType.OP_ADD_CONSTRAINT, new Text("before")),
                    new EditLog.EditLogItem(List.of(
                            new EditLog.EditLogOperation(
                                    OperationType.OP_DROP_CONSTRAINT, new Text("drop")),
                            new EditLog.EditLogOperation(
                                    OperationType.OP_ADD_META_ID_MAPPINGS, new Text("cursor")))),
                    new EditLog.EditLogItem(OperationType.OP_ADD_CONSTRAINT, new Text("after")));

            List<long[]> logIdNumPairs = EditLog.writeJournalBatch(journal, requests);

            Assert.assertEquals(List.of(
                    List.of(OperationType.OP_ADD_CONSTRAINT),
                    List.of(OperationType.OP_DROP_CONSTRAINT, OperationType.OP_ADD_META_ID_MAPPINGS),
                    List.of(OperationType.OP_ADD_CONSTRAINT)), writes);
            Assert.assertArrayEquals(new long[]{10, 1}, logIdNumPairs.get(0));
            Assert.assertArrayEquals(new long[]{11, 2, 1}, logIdNumPairs.get(1));
            Assert.assertArrayEquals(new long[]{13, 1}, logIdNumPairs.get(2));
        } finally {
            Config.batch_edit_log_max_item_num = originalMaxItemNum;
            Config.batch_edit_log_max_byte_size = originalMaxByteSize;
        }
    }

    @Test
    public void testAtomicRequestReturnsFinalJournalId() throws Exception {
        File imageDir = temporaryFolder.newFolder("atomic_edit_log");
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLog editLog = new EditLog("test");
            editLog.open();
            try {
                EditLog.EditLogItem item = editLog.submitAtomicEdits(List.of(
                        new EditLog.EditLogOperation(
                                OperationType.OP_DROP_CONSTRAINT, new Text("drop")),
                        new EditLog.EditLogOperation(
                                OperationType.OP_ADD_META_ID_MAPPINGS, new Text("cursor"))));

                Assert.assertEquals(2L, item.await());
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testCloudModeTimeBasedEditLogRoll() throws Exception {
        Config.deploy_mode = "cloud";

        File imageDir = temporaryFolder.newFolder("time_based_roll");
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLog editLog = new EditLog("test");
            editLog.open();
            try {
                Deencapsulation.setField(editLog, "lastEditLogRollTimeMs",
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

                editLog.logTimestamp(new Timestamp());

                Assert.assertTrue(new File(imageDir, "edits.2").exists());
                long txId = Deencapsulation.getField(editLog, "txId");
                Assert.assertEquals(0L, txId);
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testNonCloudModeDoesNotRollEditLogByTime() throws Exception {
        Config.deploy_mode = "share_nothing";

        File imageDir = temporaryFolder.newFolder("non_cloud_time_based_roll");
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLog editLog = new EditLog("test");
            editLog.open();
            try {
                Deencapsulation.setField(editLog, "lastEditLogRollTimeMs",
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

                editLog.logTimestamp(new Timestamp());

                Assert.assertFalse(new File(imageDir, "edits.2").exists());

                Config.edit_log_roll_num = 2;
                editLog.logTimestamp(new Timestamp());

                Assert.assertTrue(new File(imageDir, "edits.3").exists());
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testRollEditLogResetsCloudRollTime() throws Exception {
        Config.deploy_mode = "cloud";

        File imageDir = temporaryFolder.newFolder("reset_time_after_roll");
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLog editLog = new EditLog("test");
            editLog.open();
            try {
                editLog.logTimestamp(new Timestamp());
                Deencapsulation.setField(editLog, "lastEditLogRollTimeMs",
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

                editLog.rollEditLog();
                editLog.logTimestamp(new Timestamp());

                Assert.assertTrue(new File(imageDir, "edits.2").exists());
                Assert.assertFalse(new File(imageDir, "edits.3").exists());
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testNonPositiveCloudEditLogRollIntervalDisablesTimeBasedRoll() throws Exception {
        Config.deploy_mode = "cloud";
        int[] disabledIntervals = {0, -1};
        for (int i = 0; i < disabledIntervals.length; i++) {
            Config.cloud_edit_log_roll_interval_second = disabledIntervals[i];
            File imageDir = temporaryFolder.newFolder("disabled_time_based_roll_" + i);
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(env);
                EditLog editLog = new EditLog("test");
                editLog.open();
                try {
                    Deencapsulation.setField(editLog, "lastEditLogRollTimeMs",
                            System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

                    editLog.logTimestamp(new Timestamp());

                    Assert.assertFalse(new File(imageDir, "edits.2").exists());

                    Config.edit_log_roll_num = 2;
                    editLog.logTimestamp(new Timestamp());

                    Assert.assertTrue(new File(imageDir, "edits.3").exists());
                    Config.edit_log_roll_num = Integer.MAX_VALUE;
                } finally {
                    editLog.close();
                }
            }
        }
    }
}
