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

package org.apache.doris.journal.bdbje;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sleepycat.je.Database;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class BDBJEJournalTest { // CHECKSTYLE IGNORE THIS LINE: BDBJE should use uppercase
    private static final Logger LOG = LogManager.getLogger(BDBJEJournalTest.class);
    private static List<File> tmpDirs = new ArrayList<>();

    public static File createTmpDir() throws Exception {
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        Path mockDir = Paths.get(dorisHome, "fe", "mocked");
        if (!Files.exists(mockDir)) {
            Files.createDirectories(mockDir);
        }
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dorisHome));
        File dir = Files.createTempDirectory(Paths.get(dorisHome, "fe", "mocked"), "BDBJEJournalTest").toFile();
        if (LOG.isDebugEnabled()) {
            LOG.debug("createTmpDir path {}", dir.getAbsolutePath());
        }
        tmpDirs.add(dir);
        return dir;
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        for (File dir : tmpDirs) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("deleteTmpDir path {}", dir.getAbsolutePath());
            }
            FileUtils.deleteDirectory(dir);
        }
    }

    private int findValidPort() {
        int port = 0;
        for (int i = 0; i < 65535; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                port = socket.getLocalPort();
                try (DatagramSocket datagramSocket = new DatagramSocket(port)) {
                    datagramSocket.setReuseAddress(true);
                    break;
                } catch (SocketException e) {
                    LOG.info("The port {} is invalid and try another port", port);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not find a free TCP/IP port");
            }
        }
        return port;
    }

    @RepeatedTest(1)
    public void testNormal() throws Exception {
        int port = findValidPort();
        Preconditions.checkArgument(((port > 0) && (port < 65535)));
        String nodeName = Env.genFeNodeName("127.0.0.1", port, false);
        long replayedJournalId = 0;
        File tmpDir = createTmpDir();
        Env mockEnv = Mockito.mock(Env.class);
        HostInfo selfNode = new HostInfo("127.0.0.1", port);
        Mockito.when(mockEnv.getBdbDir()).thenReturn(tmpDir.getAbsolutePath());
        Mockito.when(mockEnv.getSelfNode()).thenReturn(selfNode);
        Mockito.when(mockEnv.getHelperNode()).thenReturn(selfNode);
        Mockito.when(mockEnv.isElectable()).thenReturn(true);
        Mockito.when(mockEnv.getReplayedJournalId()).thenReturn(replayedJournalId);

        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnvStatic.when(Env::getServingEnv).thenReturn(mockEnv);

            LOG.info("BdbDir:{}, selfNode:{}, nodeName:{}", Env.getServingEnv().getBdbDir(),
                    Env.getServingEnv().getBdbDir(), nodeName);
            Assertions.assertEquals(tmpDir.getAbsolutePath(), Env.getServingEnv().getBdbDir());
            BDBJEJournal journal = new BDBJEJournal(nodeName);
            journal.open();
            // BDBEnvrinment need several seconds election from unknown to master
            for (int i = 0; i < 10; i++) {
                if (journal.getBDBEnvironment().getReplicatedEnvironment().getState()
                        .equals(ReplicatedEnvironment.State.MASTER)) {
                    break;
                }
                Thread.sleep(1000);
            }
            Assertions.assertEquals(ReplicatedEnvironment.State.MASTER,
                    journal.getBDBEnvironment().getReplicatedEnvironment().getState());

            journal.rollJournal();
            for (int i = 0; i < 10; i++) {
                journal.write(OperationType.OP_TIMESTAMP, new Timestamp());
            }

            Assertions.assertEquals(10, journal.getMaxJournalId());
            Assertions.assertEquals(10, journal.getJournalNum());
            Assertions.assertEquals(1, journal.getMinJournalId());
            Assertions.assertEquals(0, journal.getFinalizedJournalId());

            if (LOG.isDebugEnabled()) {
                LOG.debug("journal.getDatabaseNames(): {}", journal.getDatabaseNames());
            }
            Assertions.assertEquals(1, journal.getDatabaseNames().size());
            Assertions.assertEquals(1, journal.getDatabaseNames().get(0));

            JournalEntity journalEntity = journal.read(1);
            Assertions.assertEquals(OperationType.OP_TIMESTAMP, journalEntity.getOpCode());

            for (int i = 10; i < 50; i++) {
                if (i % 10 == 0) {
                    journal.rollJournal();
                }
                journal.write(OperationType.OP_TIMESTAMP, new Timestamp());
            }

            Assertions.assertEquals(50, journal.getMaxJournalId());
            Assertions.assertEquals(10, journal.getJournalNum());
            Assertions.assertEquals(1, journal.getMinJournalId());
            Assertions.assertEquals(40, journal.getFinalizedJournalId());

            if (LOG.isDebugEnabled()) {
                LOG.debug("journal.getDatabaseNames(): {}", journal.getDatabaseNames());
            }
            Assertions.assertEquals(5, journal.getDatabaseNames().size());
            Assertions.assertEquals(41, journal.getDatabaseNames().get(4));

            JournalCursor cursor = journal.read(1, 51);
            Assertions.assertNotNull(cursor);
            for (int i = 0; i < 50; i++) {
                Pair<Long, JournalEntity> kv = cursor.next();
                Assertions.assertNotNull(kv);
                JournalEntity entity = kv.second;
                Assertions.assertEquals(OperationType.OP_TIMESTAMP, entity.getOpCode());
            }

            Assertions.assertNull(cursor.next());

            journal.close();
            Assertions.assertNull(journal.getBDBEnvironment());

            journal.open();
            Assertions.assertNotNull(journal.getBDBEnvironment());
            // BDBEnvrinment need several seconds election from unknown to master
            for (int i = 0; i < 10; i++) {
                if (journal.getBDBEnvironment().getReplicatedEnvironment().getState()
                        .equals(ReplicatedEnvironment.State.MASTER)) {
                    break;
                }
                Thread.sleep(1000);
            }

            Assertions.assertEquals(ReplicatedEnvironment.State.MASTER,
                    journal.getBDBEnvironment().getReplicatedEnvironment().getState());
            journal.deleteJournals(21);
            if (LOG.isDebugEnabled()) {
                LOG.debug("journal.getDatabaseNames(): {}", journal.getDatabaseNames());
            }
            Assertions.assertEquals(3, journal.getDatabaseNames().size());
            Assertions.assertEquals(21, journal.getDatabaseNames().get(0));
            journal.close();
        }
    }

    @RepeatedTest(1)
    public void testRecoveryJournalIdNoEffectWithoutMetadataRecovery() throws Exception {
        int port = findValidPort();
        Preconditions.checkArgument(((port > 0) && (port < 65535)));
        String nodeName = Env.genFeNodeName("127.0.0.1", port, false);
        long replayedJournalId = 0;
        File tmpDir = createTmpDir();
        Env mockEnv = Mockito.mock(Env.class);
        HostInfo selfNode = new HostInfo("127.0.0.1", port);
        Mockito.when(mockEnv.getBdbDir()).thenReturn(tmpDir.getAbsolutePath());
        Mockito.when(mockEnv.getSelfNode()).thenReturn(selfNode);
        Mockito.when(mockEnv.getHelperNode()).thenReturn(selfNode);
        Mockito.when(mockEnv.isElectable()).thenReturn(true);
        Mockito.when(mockEnv.getReplayedJournalId()).thenReturn(replayedJournalId);

        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnvStatic.when(Env::getServingEnv).thenReturn(mockEnv);

            String oldRecovery = System.getProperty(FeConstants.RECOVERY_JOURNAL_ID_KEY);
            String oldMetadataRecovery = System.getProperty(FeConstants.METADATA_FAILURE_RECOVERY_KEY);
            BDBJEJournal journal = new BDBJEJournal(nodeName);
            try {
                System.clearProperty(FeConstants.METADATA_FAILURE_RECOVERY_KEY);
                System.setProperty(FeConstants.RECOVERY_JOURNAL_ID_KEY, "5");

                journal.open();
                for (int i = 0; i < 10; i++) {
                    if (journal.getBDBEnvironment().getReplicatedEnvironment().getState()
                            .equals(ReplicatedEnvironment.State.MASTER)) {
                        break;
                    }
                    Thread.sleep(1000);
                }
                Assertions.assertEquals(ReplicatedEnvironment.State.MASTER,
                        journal.getBDBEnvironment().getReplicatedEnvironment().getState());
                for (int i = 0; i < 10; i++) {
                    journal.write(OperationType.OP_TIMESTAMP, new Timestamp());
                }
                Assertions.assertEquals(10, journal.getMaxJournalId());
                journal.close();

                journal.open();
                for (int i = 0; i < 10; i++) {
                    if (journal.getBDBEnvironment().getReplicatedEnvironment().getState()
                            .equals(ReplicatedEnvironment.State.MASTER)) {
                        break;
                    }
                    Thread.sleep(1000);
                }
                Assertions.assertEquals(ReplicatedEnvironment.State.MASTER,
                        journal.getBDBEnvironment().getReplicatedEnvironment().getState());
                Assertions.assertEquals(10, journal.getMaxJournalId());
            } finally {
                if (journal.getBDBEnvironment() != null) {
                    journal.close();
                }
                if (oldRecovery == null) {
                    System.clearProperty(FeConstants.RECOVERY_JOURNAL_ID_KEY);
                } else {
                    System.setProperty(FeConstants.RECOVERY_JOURNAL_ID_KEY, oldRecovery);
                }
                if (oldMetadataRecovery == null) {
                    System.clearProperty(FeConstants.METADATA_FAILURE_RECOVERY_KEY);
                } else {
                    System.setProperty(FeConstants.METADATA_FAILURE_RECOVERY_KEY, oldMetadataRecovery);
                }
            }
        }
    }

    @RepeatedTest(1)
    public void testJournalBatch() throws Exception {
        int port = findValidPort();
        Preconditions.checkArgument(((port > 0) && (port < 65535)));
        String nodeName = Env.genFeNodeName("127.0.0.1", port, false);
        long replayedJournalId = 0;
        File tmpDir = createTmpDir();
        Env mockEnv = Mockito.mock(Env.class);
        HostInfo selfNode = new HostInfo("127.0.0.1", port);
        Mockito.when(mockEnv.getBdbDir()).thenReturn(tmpDir.getAbsolutePath());
        Mockito.when(mockEnv.getSelfNode()).thenReturn(selfNode);
        Mockito.when(mockEnv.getHelperNode()).thenReturn(selfNode);
        Mockito.when(mockEnv.isElectable()).thenReturn(true);
        Mockito.when(mockEnv.getReplayedJournalId()).thenReturn(replayedJournalId);

        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnvStatic.when(Env::getServingEnv).thenReturn(mockEnv);

            LOG.info("BdbDir:{}, selfNode:{}, nodeName:{}", Env.getServingEnv().getBdbDir(),
                    Env.getServingEnv().getBdbDir(), nodeName);
            Assertions.assertEquals(tmpDir.getAbsolutePath(), Env.getServingEnv().getBdbDir());
            BDBJEJournal journal = new BDBJEJournal(nodeName);
            journal.open();
            // BDBEnvironment need several seconds election from unknown to master
            for (int i = 0; i < 10; i++) {
                if (journal.getBDBEnvironment().getReplicatedEnvironment().getState()
                        .equals(ReplicatedEnvironment.State.MASTER)) {
                    break;
                }
                Thread.sleep(1000);
            }
            Assertions.assertEquals(ReplicatedEnvironment.State.MASTER,
                    journal.getBDBEnvironment().getReplicatedEnvironment().getState());

            journal.rollJournal();
            JournalBatch batch = new JournalBatch(10);
            for (int i = 0; i < 10; i++) {
                String data = "JournalBatch item " + i;
                Writable writable = new Writable() {
                    @Override
                    public void write(DataOutput out) throws IOException {
                        Text.writeString(out, data);
                    }
                };
                // OP_START_ROLLUP is deprecated, and safe to write any data.
                batch.addJournal(OperationType.OP_START_ROLLUP, writable);
            }
            long journalId = journal.write(batch);
            Assertions.assertEquals(1, journalId);

            Assertions.assertEquals(10, journal.getMaxJournalId());
            Assertions.assertEquals(10, journal.getJournalNum());
            Assertions.assertEquals(1, journal.getMinJournalId());
            Assertions.assertEquals(0, journal.getFinalizedJournalId());

            if (LOG.isDebugEnabled()) {
                LOG.debug("journal.getDatabaseNames(): {}", journal.getDatabaseNames());
            }
            Assertions.assertEquals(1, journal.getDatabaseNames().size());
            Assertions.assertEquals(1, journal.getDatabaseNames().get(0));

            JournalEntity journalEntity = journal.read(1);
            Assertions.assertEquals(OperationType.OP_START_ROLLUP, journalEntity.getOpCode());

            batch = new JournalBatch(10);
            for (int i = 0; i < 10; i++) {
                String data = "JournalBatch 2 item " + i;
                Writable writable = new Writable() {
                    @Override
                    public void write(DataOutput out) throws IOException {
                        Text.writeString(out, data);
                    }
                };
                batch.addJournal(OperationType.OP_START_ROLLUP, writable);
            }
            journalId = journal.write(batch);
            Assertions.assertEquals(11, journalId);

            journal.close();
        }
    }

    // This journal exits the frontend by itself when it can not complete a write. Such an exit must go through
    // EditLog.exitOnWriteFailure, which records the failure so that the shutdown hook does not wait for running
    // queries. Each case below is a real failure of the journal; only the exit itself is replaced.
    @Test
    public void testWriteFailuresExitThroughEditLog() throws Exception {
        String nodeName = Env.genFeNodeName("127.0.0.1", 9010, false);
        Env mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getBdbDir()).thenReturn(createTmpDir().getAbsolutePath());
        Mockito.when(mockEnv.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        // A database that never accepts a put, and that holds more journals than the journal ids handed out.
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.put(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(OperationStatus.KEYEXIST);
        Mockito.when(database.count()).thenReturn(5L);
        Mockito.when(database.getDatabaseName()).thenReturn("1");
        RuntimeException exit = new RuntimeException("exit on write failure");

        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<EditLog> mockedEditLog = Mockito.mockStatic(EditLog.class)) {
            mockedEnvStatic.when(Env::getServingEnv).thenReturn(mockEnv);
            mockedEditLog.when(() -> EditLog.exitOnWriteFailure(Mockito.any())).thenThrow(exit);
            BDBJEJournal journal = new BDBJEJournal(nodeName);
            Deencapsulation.setField(journal, "currentJournalDB", database);

            // A direct write gives up after its retries.
            Writable writable = out -> Text.writeString(out, "data");
            Assertions.assertSame(exit, Assertions.assertThrows(RuntimeException.class,
                    () -> journal.write(OperationType.OP_START_ROLLUP, writable)));
            mockedEditLog.verify(() -> EditLog.exitOnWriteFailure(Mockito.any()), Mockito.times(1));

            // A roll finds journals that this journal did not write.
            Assertions.assertSame(exit, Assertions.assertThrows(RuntimeException.class, journal::rollJournal));
            mockedEditLog.verify(() -> EditLog.exitOnWriteFailure(Mockito.any()), Mockito.times(2));
        }
    }
}
