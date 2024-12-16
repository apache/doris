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
import org.apache.doris.common.jmockit.Deencapsulation;
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

public class BDBDebuggerTest {
    private static final Logger LOG = LogManager.getLogger(BDBDebuggerTest.class);
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
            Timestamp ts = new Timestamp();
            journal.write(OperationType.OP_TIMESTAMP, ts);
        }
        JournalEntity journalEntity = journal.read(1);
        Assertions.assertEquals(OperationType.OP_TIMESTAMP, journalEntity.getOpCode());
        journal.close();

        Deencapsulation.invoke(BDBDebugger.get(), "initDebugEnv", tmpDir.getAbsolutePath());
        // BDBDebugger.BDBDebugEnv bdbDebugEnv = new BDBDebugger.BDBDebugEnv(tmpDir.getAbsolutePath());
        // bdbDebugEnv.init();
        BDBDebugger.BDBDebugEnv bdbDebugEnv = BDBDebugger.get().getEnv();

        LOG.info("{}|{}|{}", bdbDebugEnv.listDbNames(), bdbDebugEnv.getJournalIds("1"),
                bdbDebugEnv.getJournalNumber("1"));
        Assertions.assertEquals(2, bdbDebugEnv.listDbNames().size());
        Assertions.assertEquals(10, bdbDebugEnv.getJournalIds("1").size());
        Assertions.assertEquals(10, bdbDebugEnv.getJournalNumber("1"));
        BDBDebugger.JournalEntityWrapper entityWrapper = bdbDebugEnv.getJournalEntity("1", 5L);
        Assertions.assertEquals(5, entityWrapper.journalId);
        Assertions.assertEquals(OperationType.OP_TIMESTAMP, entityWrapper.entity.getOpCode());
        bdbDebugEnv.close();
    }
}
