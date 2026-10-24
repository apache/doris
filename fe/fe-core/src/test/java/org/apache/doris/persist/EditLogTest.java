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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.journal.bdbje.Timestamp;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class EditLogTest {
    private String meta = "editLogTestDir/";
    private String originalEditLogType;
    private int originalEditLogRollNum;
    private int originalCloudEditLogRollIntervalSecond;
    private String originalDeployMode;
    private String originalCloudUniqueId;
    private boolean originalEnableBatchEditlog;

    @TempDir
    public Path temporaryFolder;

    @BeforeEach
    public void setUpEditLogRollConfig() {
        originalEditLogType = Config.edit_log_type;
        originalEditLogRollNum = Config.edit_log_roll_num;
        originalCloudEditLogRollIntervalSecond = Config.cloud_edit_log_roll_interval_second;
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalEnableBatchEditlog = Config.enable_batch_editlog;

        Config.edit_log_type = "local";
        Config.edit_log_roll_num = Integer.MAX_VALUE;
        Config.cloud_edit_log_roll_interval_second = 3600;
        Config.cloud_unique_id = "";
    }

    @AfterEach
    public void restoreEditLogRollConfig() {
        Config.edit_log_type = originalEditLogType;
        Config.edit_log_roll_num = originalEditLogRollNum;
        Config.cloud_edit_log_roll_interval_second = originalCloudEditLogRollIntervalSecond;
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
        Config.enable_batch_editlog = originalEnableBatchEditlog;
    }

    public void mkdir() {
        File dir = new File(meta);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void addFiles(int image, int edit) {
        File imageFile = new File(meta + "image." + image);
        try {
            imageFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 1; i <= edit; i++) {
            File editFile = new File(meta + "edits." + i);
            try {
                editFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File current = new File(meta + "edits");
        try {
            current.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File version = new File(meta + "VERSION");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2015\n";
            String line2 = "clusterId=966271669";
            FileWriter fw = new FileWriter(version);
            fw.write(line1);
            fw.write(line2);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteDir() {
        File dir = new File(meta);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    @Test
    public void testWriteLog() throws IOException {

    }

    @Test
    public void test() {

    }

    @Test
    public void testCloudModeTimeBasedEditLogRoll() throws Exception {
        Config.deploy_mode = "cloud";

        File imageDir = Files.createDirectories(temporaryFolder.resolve("time_based_roll")).toFile();
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

                Assertions.assertTrue(new File(imageDir, "edits.2").exists());
                long txId = Deencapsulation.getField(editLog, "txId");
                Assertions.assertEquals(0L, txId);
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testNonCloudModeDoesNotRollEditLogByTime() throws Exception {
        Config.deploy_mode = "share_nothing";

        File imageDir = Files.createDirectories(temporaryFolder.resolve("non_cloud_time_based_roll")).toFile();
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

                Assertions.assertFalse(new File(imageDir, "edits.2").exists());

                Config.edit_log_roll_num = 2;
                editLog.logTimestamp(new Timestamp());

                Assertions.assertTrue(new File(imageDir, "edits.3").exists());
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testRollEditLogResetsCloudRollTime() throws Exception {
        Config.deploy_mode = "cloud";

        File imageDir = Files.createDirectories(temporaryFolder.resolve("reset_time_after_roll")).toFile();
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

                Assertions.assertTrue(new File(imageDir, "edits.2").exists());
                Assertions.assertFalse(new File(imageDir, "edits.3").exists());
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
            File imageDir = Files.createDirectories(temporaryFolder.resolve("disabled_time_based_roll_" + i)).toFile();
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

                    Assertions.assertFalse(new File(imageDir, "edits.2").exists());

                    Config.edit_log_roll_num = 2;
                    editLog.logTimestamp(new Timestamp());

                    Assertions.assertTrue(new File(imageDir, "edits.3").exists());
                    Config.edit_log_roll_num = Integer.MAX_VALUE;
                } finally {
                    editLog.close();
                }
            }
        }
    }

    // A queued edit is written by the flusher thread later. The caller releases its lock as soon as the
    // edit is queued, and other threads (PublishVersionDaemon, finishTransaction) keep changing the same
    // TransactionState while the edit waits. The journal must get the state of the moment it was queued.
    @Test
    public void testSubmitEditWritesStateAtSubmission() throws Exception {
        assertQueuedEditWritesStateAtSubmission(true);
    }

    @Test
    public void testLogEditWithQueueWritesStateAtSubmission() throws Exception {
        assertQueuedEditWritesStateAtSubmission(false);
    }

    private void assertQueuedEditWritesStateAtSubmission(boolean submitWithoutWaiting) throws Exception {
        Config.enable_batch_editlog = true;
        File imageDir = Files.createDirectories(temporaryFolder.resolve("queued_edit_" + submitWithoutWaiting))
                .toFile();
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLog editLog = new EditLog("test");

            // The first write parks the flusher until the test releases it, so the edit under test has
            // to wait in the queue.
            CountDownLatch flusherParked = new CountDownLatch(1);
            CountDownLatch releaseFlusher = new CountDownLatch(1);
            List<byte[]> writtenEntities = Collections.synchronizedList(new ArrayList<>());
            AtomicLong nextJournalId = new AtomicLong(1);
            Journal journal = Mockito.mock(Journal.class);
            Mockito.when(journal.write(Mockito.any(JournalBatch.class))).thenAnswer(invocation -> {
                JournalBatch batch = invocation.getArgument(0);
                for (JournalBatch.Entity entity : batch.getJournalEntities()) {
                    writtenEntities.add(entity.getBinaryData().clone());
                }
                if (flusherParked.getCount() > 0) {
                    flusherParked.countDown();
                    releaseFlusher.await();
                }
                return nextJournalId.getAndAdd(batch.getJournalEntities().size());
            });
            Deencapsulation.setField(editLog, "journal", journal);
            try {
                EditLogItem parked = editLog.submitEdit(OperationType.OP_SAVE_NEXTID, new Text("1"));
                Assertions.assertTrue(flusherParked.await(30, TimeUnit.SECONDS));

                TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(2000L),
                        3000L, "label_queued_edit", new TUniqueId(1L, 2L), LoadJobSourceType.BACKEND_STREAMING,
                        new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", System.currentTimeMillis()),
                        -1L, 60 * 1000L);
                transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
                EditLogItem queued = null;
                Thread logger = null;
                if (submitWithoutWaiting) {
                    queued = editLog.submitEdit(OperationType.OP_UPSERT_TRANSACTION_STATE, transactionState);
                } else {
                    logger = new Thread(() -> editLog.logEditWithQueue(
                            OperationType.OP_UPSERT_TRANSACTION_STATE, transactionState));
                    logger.start();
                    BlockingQueue<EditLogItem> queue = Deencapsulation.getField(editLog, "logEditQueue");
                    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
                    while (queue.isEmpty()) {
                        Assertions.assertTrue(System.currentTimeMillis() < deadline);
                        Thread.sleep(10);
                    }
                }

                // What the publish of the same transaction does while its commit edit waits in the queue.
                Map<Long, Map<Long, Long>> deltaRows = Maps.newHashMap();
                deltaRows.put(2000L, Maps.newHashMap());
                deltaRows.get(2000L).put(4000L, 10L);
                transactionState.setTableIdToTabletDeltaRows(deltaRows);
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);

                releaseFlusher.countDown();
                parked.await();
                if (submitWithoutWaiting) {
                    queued.await();
                } else {
                    logger.join(TimeUnit.SECONDS.toMillis(30));
                    Assertions.assertFalse(logger.isAlive());
                }

                Assertions.assertEquals(2, writtenEntities.size());
                MetaContext metaContext = new MetaContext();
                metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
                metaContext.setThreadLocalInfo();
                try {
                    JournalEntity entity = new JournalEntity();
                    entity.readFields(new DataInputStream(new ByteArrayInputStream(writtenEntities.get(1))));
                    Assertions.assertEquals(OperationType.OP_UPSERT_TRANSACTION_STATE, entity.getOpCode());
                    TransactionState persisted = (TransactionState) entity.getData();
                    Assertions.assertEquals(TransactionStatus.COMMITTED, persisted.getTransactionStatus());
                    Assertions.assertTrue(persisted.getTableIdToTabletDeltaRows().isEmpty());
                } finally {
                    MetaContext.remove();
                }
            } finally {
                releaseFlusher.countDown();
                editLog.close();
            }
        }
    }
}
