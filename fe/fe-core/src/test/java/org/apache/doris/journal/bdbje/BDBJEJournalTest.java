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
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.OperationType;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

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
        new MockUp<Env>() {
            HostInfo selfNode = new HostInfo("127.0.0.1", port);
            @Mock
            public String getBdbDir() {
                return tmpDir.getAbsolutePath();
            }

            @Mock
            public HostInfo getSelfNode() {
                return this.selfNode;
            }

            @Mock
            public HostInfo getHelperNode() {
                return this.selfNode;
            }

            @Mock
            public boolean isElectable() {
                return true;
            }

            @Mock
            public long getReplayedJournalId() {
                return replayedJournalId;
            }
        };

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
            String data = "OperationType.OP_TIMESTAMP";
            Writable writable = new Writable() {
                @Override
                public void write(DataOutput out) throws IOException {
                    Text.writeString(out, data);
                }
            };
            journal.write(OperationType.OP_TIMESTAMP, writable);
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
            String data = "OperationType.OP_TIMESTAMP";
            Writable writable = new Writable() {
                @Override
                public void write(DataOutput out) throws IOException {
                    Text.writeString(out, data);
                }
            };
            journal.write(OperationType.OP_TIMESTAMP, writable);
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

        Assertions.assertEquals(null, cursor.next());

        journal.close();
        Assertions.assertEquals(null, journal.getBDBEnvironment());

        journal.open();
        Assertions.assertTrue(journal.getBDBEnvironment() != null);
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

    @RepeatedTest(1)
    public void testJournalBatch() throws Exception {
        int port = findValidPort();
        Preconditions.checkArgument(((port > 0) && (port < 65535)));
        String nodeName = Env.genFeNodeName("127.0.0.1", port, false);
        long replayedJournalId = 0;
        File tmpDir = createTmpDir();
        new MockUp<Env>() {
            HostInfo selfNode = new HostInfo("127.0.0.1", port);
            @Mock
            public String getBdbDir() {
                return tmpDir.getAbsolutePath();
            }

            @Mock
            public HostInfo getSelfNode() {
                return this.selfNode;
            }

            @Mock
            public HostInfo getHelperNode() {
                return this.selfNode;
            }

            @Mock
            public boolean isElectable() {
                return true;
            }

            @Mock
            public long getReplayedJournalId() {
                return replayedJournalId;
            }
        };

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
            // CREATE_MTMV_JOB is deprecated, and safe to write any data.
            batch.addJournal(OperationType.OP_CREATE_MTMV_JOB, writable);
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
        Assertions.assertEquals(OperationType.OP_CREATE_MTMV_JOB, journalEntity.getOpCode());

        batch = new JournalBatch(10);
        for (int i = 0; i < 10; i++) {
            String data = "JournalBatch 2 item " + i;
            Writable writable = new Writable() {
                @Override
                public void write(DataOutput out) throws IOException {
                    Text.writeString(out, data);
                }
            };
            batch.addJournal(OperationType.OP_CREATE_MTMV_JOB, writable);
        }
        journalId = journal.write(batch);
        Assertions.assertEquals(11, journalId);

        journal.close();
    }
}
