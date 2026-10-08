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
import org.apache.doris.cloud.persist.CloudMetaSyncPoint;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.bdbje.BDBJEJournal;
import org.apache.doris.journal.bdbje.Timestamp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EditLogTest {
    private String meta = "editLogTestDir/";
    private String originalEditLogType;
    private int originalEditLogRollNum;
    private int originalCloudEditLogRollIntervalSecond;
    private String originalDeployMode;
    private String originalCloudUniqueId;
    private boolean originalEnableBatchEditlog;
    private int originalBatchMaxItems;
    private long originalBatchMaxBytes;

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
        originalBatchMaxItems = Config.batch_edit_log_max_item_num;
        originalBatchMaxBytes = Config.batch_edit_log_max_byte_size;

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
        Config.batch_edit_log_max_item_num = originalBatchMaxItems;
        Config.batch_edit_log_max_byte_size = originalBatchMaxBytes;
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
    public void testSubmitWithoutBatching() throws Exception {
        checkQueuedSubmit(false);
    }

    @Test
    public void testSubmitWithBatching() throws Exception {
        checkQueuedSubmit(true);
    }

    private void checkQueuedSubmit(boolean batching) throws Exception {
        Config.edit_log_type = "bdb";
        Config.deploy_mode = "share_nothing";
        Config.enable_batch_editlog = batching;
        Config.batch_edit_log_max_item_num = 10;
        Config.batch_edit_log_max_byte_size = Long.MAX_VALUE;
        Config.edit_log_roll_num = 4;
        CountDownLatch firstWriting = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondWriting = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        CountDownLatch rolled = new CountDownLatch(1);
        AtomicLong nextId = new AtomicLong(101);
        List<Short> operations = new ArrayList<>();
        List<Integer> batchSizes = new ArrayList<>();
        Answer<Long> write = invocation -> {
            int count = 1;
            if (invocation.getArguments().length == 1) {
                JournalBatch batch = invocation.getArgument(0);
                count = batch.getJournalEntities().size();
                batchSizes.add(count);
                batch.getJournalEntities().forEach(entity -> operations.add(entity.getOpCode()));
            } else {
                operations.add(invocation.getArgument(0));
            }
            long id = nextId.getAndAdd(count);
            // Only explicit release may complete a write; finally releases both gates on test failure.
            if (id == 101) {
                firstWriting.countDown();
                releaseFirst.await();
            } else if (id == 102) {
                secondWriting.countDown();
                releaseSecond.await();
            }
            return id;
        };
        try (MockedConstruction<BDBJEJournal> journals = Mockito.mockConstruction(BDBJEJournal.class,
                (journal, context) -> {
                    Mockito.when(journal.write(Mockito.anyShort(), Mockito.any(Writable.class))).thenAnswer(write);
                    Mockito.when(journal.write(Mockito.any(JournalBatch.class))).thenAnswer(write);
                    Mockito.doAnswer(invocation -> {
                        rolled.countDown();
                        return null;
                    }).when(journal).rollJournal();
                })) {
            EditLog editLog = new EditLog("queued-submit");
            ExecutorService workers = Executors.newFixedThreadPool(3);
            ReentrantReadWriteLock metadataLock = new ReentrantReadWriteLock();
            try {
                EditLog.EditLogItem first = workers.submit(() -> editLog.submitEdit(
                        OperationType.OP_SAVE_NEXTID, new Text("1"))).get(5, TimeUnit.SECONDS);
                Assertions.assertTrue(firstWriting.await(5, TimeUnit.SECONDS));
                EditLog.EditLogItem second = workers.submit(() -> {
                    metadataLock.writeLock().lock();
                    try {
                        return editLog.submitEdit(OperationType.OP_SAVE_TRANSACTION_ID, new Text("2"));
                    } finally {
                        metadataLock.writeLock().unlock();
                    }
                }).get(5, TimeUnit.SECONDS);
                CountDownLatch awaiting = new CountDownLatch(1);
                Future<Long> persisted = workers.submit(() -> {
                    awaiting.countDown();
                    return second.await();
                });
                Assertions.assertTrue(awaiting.await(5, TimeUnit.SECONDS));
                Assertions.assertTrue(workers.submit(() -> {
                    metadataLock.readLock().lock();
                    try {
                        return true;
                    } finally {
                        metadataLock.readLock().unlock();
                    }
                }).get(5, TimeUnit.SECONDS));
                Assertions.assertThrows(TimeoutException.class, () -> persisted.get(100, TimeUnit.MILLISECONDS));
                EditLog.EditLogItem third = workers.submit(() -> editLog.submitEdit(
                        OperationType.OP_META_VERSION, new Text("3"))).get(5, TimeUnit.SECONDS);
                CountDownLatch syncing = new CountDownLatch(1);
                Future<Long> synchronous = workers.submit(() -> {
                    syncing.countDown();
                    return Deencapsulation.<Long>invoke(editLog, "logEdit", OperationType.OP_SAVE_NEXTID, new Text("4"));
                });
                Assertions.assertTrue(syncing.await(5, TimeUnit.SECONDS));
                Assertions.assertThrows(TimeoutException.class, () -> synchronous.get(100, TimeUnit.MILLISECONDS));
                Assertions.assertFalse(first.finished);
                Assertions.assertFalse(second.finished);
                Assertions.assertFalse(third.finished);
                // Verifying a synchronized journal method here would block on the paused writer's monitor.
                Assertions.assertEquals(1L, rolled.getCount());
                releaseFirst.countDown();
                Assertions.assertTrue(secondWriting.await(5, TimeUnit.SECONDS));
                Assertions.assertTrue(first.finished);
                Assertions.assertEquals(101L, first.await());
                Assertions.assertFalse(second.finished);
                Assertions.assertFalse(third.finished);
                Assertions.assertFalse(persisted.isDone());
                releaseSecond.countDown();
                Assertions.assertEquals(102L, persisted.get(5, TimeUnit.SECONDS).longValue());
                Assertions.assertEquals(104L, synchronous.get(5, TimeUnit.SECONDS).longValue());
                Assertions.assertTrue(third.finished);
                Assertions.assertEquals(103L, third.await());
                Assertions.assertTrue(rolled.await(5, TimeUnit.SECONDS));
                Assertions.assertEquals(Arrays.asList(OperationType.OP_SAVE_NEXTID,
                        OperationType.OP_SAVE_TRANSACTION_ID, OperationType.OP_META_VERSION,
                        OperationType.OP_SAVE_NEXTID), operations);
                BDBJEJournal journal = journals.constructed().get(0);
                Mockito.verify(journal, Mockito.times(1)).rollJournal();
                if (batching) {
                    Assertions.assertTrue(batchSizes.stream().anyMatch(size -> size >= 2));
                    Mockito.verify(journal, Mockito.never()).write(Mockito.anyShort(), Mockito.any(Writable.class));
                } else {
                    Mockito.verify(journal, Mockito.times(4)).write(Mockito.anyShort(), Mockito.any(Writable.class));
                    Mockito.verify(journal, Mockito.never()).write(Mockito.any(JournalBatch.class));
                }
            } finally {
                // Release writes before cleanup: interrupting a thread in await triggers System.exit.
                releaseFirst.countDown();
                releaseSecond.countDown();
                try {
                    // Drain pending requests with a queue barrier, including on assertion failures.
                    workers.submit(() -> editLog.logEditWithQueue(OperationType.OP_SAVE_NEXTID,
                            new Text("cleanup"))).get(5, TimeUnit.SECONDS);
                } finally {
                    workers.shutdown();
                    Assertions.assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
                    editLog.close();
                }
            }
        }
    }

    @Test
    public void testNonBatchMetaSyncPointRemainsDirectUnderEditLogMonitor() throws Exception {
        Config.edit_log_type = "bdb";
        Config.deploy_mode = "share_nothing";
        Config.enable_batch_editlog = false;
        List<Thread> writers = new ArrayList<>();
        AtomicLong nextId = new AtomicLong(101);
        CloudMetaSyncPoint syncPoint = new CloudMetaSyncPoint(1L, "sync-point", 1L);
        try (MockedConstruction<BDBJEJournal> journals = Mockito.mockConstruction(BDBJEJournal.class,
                (journal, context) -> Mockito.when(journal.write(Mockito.eq(OperationType.OP_META_SYNC_POINT),
                        Mockito.same(syncPoint))).thenAnswer(invocation -> {
                            writers.add(Thread.currentThread());
                            return nextId.getAndIncrement();
                        }))) {
            EditLog editLog = Mockito.spy(new EditLog("direct-meta-sync-point"));
            try {
                // Fail before queueing: the non-batch flusher needs the monitor held by the caller.
                Mockito.doThrow(new AssertionError("Metadata sync points must not wait on the queue"))
                        .when(editLog).logEditWithQueue(Mockito.eq(OperationType.OP_META_SYNC_POINT),
                                Mockito.same(syncPoint));
                synchronized (editLog) {
                    Assertions.assertEquals(101L, editLog.logMetaSyncPoint(syncPoint));
                    EditLog.EditLogItem item = editLog.submitEdit(OperationType.OP_META_SYNC_POINT, syncPoint);
                    Assertions.assertTrue(item.finished);
                    Assertions.assertEquals(102L, item.await());
                    Assertions.assertEquals(Arrays.asList(Thread.currentThread(), Thread.currentThread()), writers);
                }
                Mockito.verify(journals.constructed().get(0), Mockito.times(2))
                        .write(OperationType.OP_META_SYNC_POINT, syncPoint);
                Mockito.verify(journals.constructed().get(0), Mockito.never()).write(Mockito.any(JournalBatch.class));
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testTimestampRemainsDirectInBothModes() throws Exception {
        Config.edit_log_type = "bdb";
        for (boolean batching : new boolean[] {false, true}) {
            Config.enable_batch_editlog = batching;
            List<Thread> writers = new ArrayList<>();
            try (MockedConstruction<BDBJEJournal> journals = Mockito.mockConstruction(BDBJEJournal.class,
                    (journal, context) -> Mockito.when(journal.write(Mockito.eq(OperationType.OP_TIMESTAMP),
                            Mockito.any(Timestamp.class))).thenAnswer(invocation -> {
                                writers.add(Thread.currentThread());
                                return 101L;
                            }))) {
                EditLog editLog = new EditLog("direct-timestamp");
                try {
                    EditLog.EditLogItem item = editLog.submitEdit(OperationType.OP_TIMESTAMP, new Timestamp());
                    Assertions.assertTrue(item.finished);
                    Assertions.assertEquals(101L, item.await());
                    editLog.logTimestamp(new Timestamp());
                    Assertions.assertEquals(Arrays.asList(Thread.currentThread(), Thread.currentThread()), writers);
                    Mockito.verify(journals.constructed().get(0), Mockito.never()).write(Mockito.any(JournalBatch.class));
                } finally {
                    editLog.close();
                }
            }
        }
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
}
