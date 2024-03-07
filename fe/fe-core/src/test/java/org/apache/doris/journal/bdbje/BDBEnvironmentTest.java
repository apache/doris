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
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.Frontend;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BDBEnvironmentTest {
    private static final Logger LOG = LogManager.getLogger(BDBEnvironmentTest.class);
    private static List<String> tmpDirs = new ArrayList<>();

    public static String createTmpDir() throws Exception {
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dorisHome));
        Path mockDir = Paths.get(dorisHome, "fe", "mocked");
        if (!Files.exists(mockDir)) {
            Files.createDirectories(mockDir);
        }
        UUID uuid = UUID.randomUUID();
        File dir = Files.createDirectories(Paths.get(dorisHome, "fe", "mocked", "BDBEnvironmentTest-" + uuid.toString())).toFile();
        if (LOG.isDebugEnabled()) {
            LOG.debug("createTmpDir path {}", dir.getAbsolutePath());
        }
        tmpDirs.add(dir.getAbsolutePath());
        return dir.getAbsolutePath();
    }

    @BeforeAll
    public static void startUp() throws Exception {
        Config.bdbje_file_logging_level = "ALL";
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        for (String dir : tmpDirs) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("deleteTmpDir path {}", dir);
            }
            FileUtils.deleteDirectory(new File(dir));
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
        Preconditions.checkArgument(((port > 0) && (port < 65536)));
        return port;
    }

    private byte[] randomBytes() {
        byte[] byteArray = new byte[32];
        new SecureRandom().nextBytes(byteArray);
        return byteArray;
    }

    // @Test
    @RepeatedTest(1)
    public void testSetup() throws Exception {
        int port = findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;
        if (LOG.isDebugEnabled()) {
            LOG.debug("selfNodeName:{}, selfNodeHostPort:{}", selfNodeName, selfNodeHostPort);
        }

        BDBEnvironment bdbEnvironment = new BDBEnvironment(true, false);
        bdbEnvironment.setup(new File(createTmpDir()), selfNodeName, selfNodeHostPort, selfNodeHostPort);

        String dbName = "testEnvironment";
        Database db = bdbEnvironment.openDatabase(dbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());

        Assertions.assertEquals(OperationStatus.SUCCESS, db.put(null, key, value));

        DatabaseEntry readValue = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, db.get(null, key, readValue, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));

        // Remove database
        bdbEnvironment.removeDatabase(dbName);
        // Rmove database twice will get DatabaseNotFoundException
        bdbEnvironment.removeDatabase(dbName);
        Exception exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            db.put(null, key, value);
        });

        String expectedMessage = "Database was closed.";
        String actualMessage = exception.getMessage();
        if (LOG.isDebugEnabled()) {
            LOG.debug("exception:", exception);
        }
        Assertions.assertTrue(actualMessage.contains(expectedMessage));

        Database epochDb = bdbEnvironment.getEpochDB();
        Assertions.assertEquals(OperationStatus.SUCCESS, epochDb.put(null, key, value));
        DatabaseEntry readValue2 = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, epochDb.get(null, key, readValue2, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue2.getData()));

        new MockUp<Env>() {
            int i = 0;
            @Mock
            public List<Frontend> getFrontends(FrontendNodeType nodeType) {
                ArrayList<Frontend> frontends = new ArrayList<Frontend>();
                if (i == 0) {
                    i++;
                    return frontends;
                }
                Frontend frontend = new Frontend(FrontendNodeType.FOLLOWER, selfNodeName,
                        "127.0.0.1", port);
                frontend.setIsAlive(true);
                frontends.add(frontend);
                return frontends;
            }
        };

        ReplicationGroupAdmin replicationGroupAdmin = bdbEnvironment.getReplicationGroupAdmin();
        Assertions.assertNull(replicationGroupAdmin);
        replicationGroupAdmin = bdbEnvironment.getReplicationGroupAdmin();
        Assertions.assertNotNull(replicationGroupAdmin);

        bdbEnvironment.close();
        exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            db.put(null, key, value);
        });
        expectedMessage = "Environment is closed.";
        actualMessage = exception.getMessage();
        if (LOG.isDebugEnabled()) {
            LOG.debug("exception:", exception);
        }
        Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }

    // @Test
    @RepeatedTest(1)
    public void testSetupTwice() throws Exception {
        int port = findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;
        File homeFile = new File(createTmpDir());
        BDBEnvironment bdbEnvironment = new BDBEnvironment(true, false);
        bdbEnvironment.setup(homeFile, selfNodeName, selfNodeHostPort, selfNodeHostPort);

        bdbEnvironment.setup(homeFile, selfNodeName, selfNodeHostPort, selfNodeHostPort);
        bdbEnvironment.close();
    }

    // @Test
    @RepeatedTest(1)
    public void testMetadataRecovery() throws Exception {
        int port = findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;

        File recoveryFile = new File(createTmpDir());
        BDBEnvironment bdbEnvironment = new BDBEnvironment(true, false);
        bdbEnvironment.setup(recoveryFile, selfNodeName, selfNodeHostPort, selfNodeHostPort);

        String dbName = "testMetadataRecovery";
        Database db = bdbEnvironment.openDatabase(dbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());

        Assertions.assertEquals(OperationStatus.SUCCESS, db.put(null, key, value));

        DatabaseEntry readValue = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, db.get(null, key, readValue, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));
        bdbEnvironment.close();

        // recovery mode
        BDBEnvironment bdbEnvironment2 = new BDBEnvironment(true, true);
        bdbEnvironment2.setup(recoveryFile, selfNodeName, selfNodeHostPort, selfNodeHostPort);
        Database db2 = bdbEnvironment2.openDatabase(dbName);

        DatabaseEntry readValue2 = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, db2.get(null, key, readValue2, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue2.getData()));
        bdbEnvironment2.close();
    }

    // @Test
    @RepeatedTest(1)
    public void testOpenReplicatedEnvironmentTwice() throws Exception {
        int port = findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;

        File homeFile = new File(createTmpDir());
        BDBEnvironment bdbEnvironment = new BDBEnvironment(true, false);
        bdbEnvironment.setup(homeFile, selfNodeName, selfNodeHostPort, selfNodeHostPort);

        String dbName = "testMetadataRecovery";
        Database db = bdbEnvironment.openDatabase(dbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());

        Assertions.assertEquals(OperationStatus.SUCCESS, db.put(null, key, value));

        DatabaseEntry readValue = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, db.get(null, key, readValue, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));
        bdbEnvironment.close();

        bdbEnvironment.openReplicatedEnvironment(homeFile);
        bdbEnvironment.openReplicatedEnvironment(homeFile);
        Database db2 = bdbEnvironment.openDatabase(dbName);
        DatabaseEntry readValue2 = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, db2.get(null, key, readValue2, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue2.getData()));
        bdbEnvironment.close();
    }

    /**
     * Test build a BDBEnvironment cluster (1 master + 2 follower + 1 observer)
     * @throws Exception
     */
    // @Test
    @RepeatedTest(1)
    public void testCluster() throws Exception {
        int masterPort = findValidPort();
        String masterNodeName = Env.genFeNodeName("127.0.0.1", masterPort, false);
        String masterNodeHostPort = "127.0.0.1:" + masterPort;
        if (LOG.isDebugEnabled()) {
            LOG.debug("masterNodeName:{}, masterNodeHostPort:{}", masterNodeName, masterNodeHostPort);
        }

        BDBEnvironment masterEnvironment = new BDBEnvironment(true, false);
        File masterDir = new File(createTmpDir());
        masterEnvironment.setup(masterDir, masterNodeName, masterNodeHostPort, masterNodeHostPort);

        List<BDBEnvironment> followerEnvironments = new ArrayList<>();
        List<File> followerDirs = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            int followerPort = findValidPort();
            String followerNodeName = Env.genFeNodeName("127.0.0.1", followerPort, false);
            String followerNodeHostPort = "127.0.0.1:" + followerPort;
            if (LOG.isDebugEnabled()) {
                LOG.debug("followerNodeName{}:{}, followerNodeHostPort{}:{}", i, i, followerNodeName, followerNodeHostPort);
            }

            BDBEnvironment followerEnvironment = new BDBEnvironment(true, false);
            File followerDir = new File(createTmpDir());
            followerDirs.add(followerDir);
            followerEnvironment.setup(followerDir, followerNodeName, followerNodeHostPort, masterNodeHostPort);
            followerEnvironments.add(followerEnvironment);
        }

        int observerPort = findValidPort();
        String observerNodeName = Env.genFeNodeName("127.0.0.1", observerPort, false);
        String observerNodeHostPort = "127.0.0.1:" + observerPort;
        if (LOG.isDebugEnabled()) {
            LOG.debug("observerNodeName:{}, observerNodeHostPort:{}", observerNodeName, observerNodeHostPort);
        }

        BDBEnvironment observerEnvironment = new BDBEnvironment(false, false);
        File observerDir = new File(createTmpDir());
        observerEnvironment.setup(observerDir, observerNodeName, observerNodeHostPort, masterNodeHostPort);

        String dbName = "1234";
        Database masterDb = masterEnvironment.openDatabase(dbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());
        Assertions.assertEquals(OperationStatus.SUCCESS, masterDb.put(null, key, value));

        for (BDBEnvironment followerEnvironment : followerEnvironments) {
            Assertions.assertEquals(1, followerEnvironment.getDatabaseNames().size());
            Database followerDb = followerEnvironment.openDatabase(dbName);
            DatabaseEntry readValue = new DatabaseEntry();
            Assertions.assertEquals(OperationStatus.SUCCESS, followerDb.get(null, key, readValue, LockMode.READ_COMMITTED));
            Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));
        }

        Assertions.assertEquals(1, observerEnvironment.getDatabaseNames().size());
        Database observerDb = observerEnvironment.openDatabase(dbName);
        DatabaseEntry readValue = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, observerDb.get(null, key, readValue, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));

        observerEnvironment.close();
        followerEnvironments.stream().forEach(environment -> {
            environment.close(); });
        masterEnvironment.close();

        masterEnvironment.openReplicatedEnvironment(masterDir);
        for (int i = 0; i < 2; i++) {
            followerEnvironments.get(i).openReplicatedEnvironment(followerDirs.get(i));
        }
        observerEnvironment.openReplicatedEnvironment(observerDir);

        observerEnvironment.close();
        followerEnvironments.stream().forEach(environment -> {
            environment.close(); });
        masterEnvironment.close();
    }

    class NodeInfo {
        public String name;
        public String hostPort;
        public String dir;

        NodeInfo(String name, String hostPort, String dir) {
            this.name = name;
            this.hostPort = hostPort;
            this.dir = dir;
        }
    }

    private Pair<BDBEnvironment, NodeInfo> findMaster(List<Pair<BDBEnvironment, NodeInfo>> followersInfo)
            throws Exception {
        NodeInfo masterNode = null;
        BDBEnvironment masterEnvironment = null;
        boolean electionSuccess = true;
        for (int i = 0; i < 10; i++) {
            electionSuccess = true;
            for (Pair<BDBEnvironment, NodeInfo> entryPair : followersInfo) {
                ReplicatedEnvironment env = entryPair.first.getReplicatedEnvironment();
                if (env == null) {
                    continue;
                }
                if (env.getState().equals(ReplicatedEnvironment.State.MASTER)) {
                    masterEnvironment = entryPair.first;
                    masterNode = entryPair.second;
                }
                if (!env.getState().equals(ReplicatedEnvironment.State.MASTER)
                        && !env.getState().equals(ReplicatedEnvironment.State.REPLICA)) {
                    electionSuccess = false;
                }
            }
            if (!electionSuccess) {
                Thread.sleep(1000);
            }
        }
        Assertions.assertTrue(electionSuccess);
        Assertions.assertNotNull(masterNode);
        Assertions.assertNotNull(masterEnvironment);
        return Pair.of(masterEnvironment, masterNode);
    }

    // @Test
    @RepeatedTest(1)
    public void testRollbackException() throws Exception {
        LOG.info("start");
        List<Pair<BDBEnvironment, NodeInfo>> followersInfo = new ArrayList<>();

        int masterPort = findValidPort();
        String masterNodeName = "fe1";
        String masterNodeHostPort = "127.0.0.1:" + masterPort;

        BDBEnvironment masterEnvironment = new BDBEnvironment(true, false);
        String masterDir = createTmpDir();
        masterEnvironment.setup(new File(masterDir), masterNodeName, masterNodeHostPort, masterNodeHostPort);
        followersInfo.add(Pair.of(masterEnvironment, new NodeInfo(masterNodeName, masterNodeHostPort, masterDir)));

        for (int i = 2; i <= 3; i++) {
            int nodePort = findValidPort();
            String nodeName = "fe" + i;
            String nodeHostPort = "127.0.0.1:" + nodePort;

            BDBEnvironment followerEnvironment = new BDBEnvironment(true, false);
            String nodeDir = createTmpDir();
            followerEnvironment.setup(new File(nodeDir), nodeName, nodeHostPort, masterNodeHostPort);
            followersInfo.add(Pair.of(followerEnvironment, new NodeInfo(nodeName, nodeHostPort, nodeDir)));
        }

        Pair<BDBEnvironment, NodeInfo> masterPair = findMaster(followersInfo);
        String beginDbName = String.valueOf(0L);
        Database masterDb = masterPair.first.openDatabase(beginDbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());
        Assertions.assertEquals(OperationStatus.SUCCESS, masterDb.put(null, key, value));
        Assertions.assertEquals(1, masterEnvironment.getDatabaseNames().size());
        LOG.info("master is {} | {}", masterPair.second.name, masterPair.second.dir);

        for (Pair<BDBEnvironment, NodeInfo> entryPair : followersInfo) {
            if (entryPair.second.dir.equals(masterPair.second.dir)) {
                LOG.info("skip {}", entryPair.second.name);
                return;
            }

            Assertions.assertEquals(1, entryPair.first.getDatabaseNames().size());
            Database followerDb = entryPair.first.openDatabase(beginDbName);
            DatabaseEntry readValue = new DatabaseEntry();
            Assertions.assertEquals(OperationStatus.SUCCESS, followerDb.get(null, key, readValue, LockMode.READ_COMMITTED));
            Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));
            followerDb.close();
        }

        masterDb.close();
        masterEnvironment.getEpochDB().close();

        followersInfo.stream().forEach(entryPair -> {
            entryPair.first.close();
            LOG.info("close {} | {}", entryPair.second.name, entryPair.second.dir);
        });

        // all follower closed
        for (Pair<BDBEnvironment, NodeInfo> entryPair : followersInfo) {
            String followerCopyDir = entryPair.second.dir + "_copy";
            LOG.info("Copy from {} to {}", entryPair.second.dir, followerCopyDir);
            FileUtils.copyDirectory(new File(entryPair.second.dir), new File(followerCopyDir));
        }

        followersInfo.stream().forEach(entryPair -> {
            entryPair.first.openReplicatedEnvironment(new File(entryPair.second.dir));
            LOG.info("open {} | {}", entryPair.second.name, entryPair.second.dir);
        });

        masterPair = findMaster(followersInfo);

        masterDb = masterPair.first.openDatabase(String.valueOf(1L));
        for (int i = 0; i < 2 * Config.txn_rollback_limit + 10; i++) {
            // for (int i = 0; i < 10; i++) {
            OperationStatus status = masterDb.put(null, new DatabaseEntry(randomBytes()), new DatabaseEntry(randomBytes()));
            Assertions.assertEquals(OperationStatus.SUCCESS, status);
        }
        Assertions.assertEquals(2, masterPair.first.getDatabaseNames().size());
        Assertions.assertEquals(0, masterPair.first.getDatabaseNames().get(0));
        Assertions.assertEquals(1, masterPair.first.getDatabaseNames().get(1));

        followersInfo.stream().forEach(entryPair -> {
            entryPair.first.close();
            LOG.info("close {} | {}", entryPair.second.name, entryPair.second.dir);
        });

        // Restore follower's (not new master) bdbje dir
        for (Pair<BDBEnvironment, NodeInfo> entryPair : followersInfo) {
            if (entryPair.second.dir.equals(masterDir)) {
                String masterCopyDir = entryPair.second.dir + "_copy";
                FileUtils.deleteDirectory(new File(masterCopyDir));
                continue;
            }
            LOG.info("Delete followerDir {} ", entryPair.second.dir);
            FileUtils.deleteDirectory(new File(entryPair.second.dir));
            // FileUtils.moveDirectory(new File(entryPair.second.dir), new File(entryPair.second.dir + "_copy2"));
            String followerCopyDir = entryPair.second.dir + "_copy";
            LOG.info("Move {} to {}", followerCopyDir, entryPair.second.dir);
            FileUtils.moveDirectory(new File(followerCopyDir), new File(entryPair.second.dir));
        }

        Thread.sleep(1000);
        for (Pair<BDBEnvironment, NodeInfo> entryPair : followersInfo) {
            if (entryPair.second.dir.equals(masterPair.second.dir)) {
                LOG.info("skip open {} | {}", entryPair.second.name, entryPair.second.dir);
                continue;
            }
            entryPair.first.openReplicatedEnvironment(new File(entryPair.second.dir));
            LOG.info("open {} | {}", entryPair.second.name, entryPair.second.dir);
        }

        BDBEnvironment newMasterEnvironment = null;
        boolean found = false;
        for (int i = 0; i < 300; i++) {
            for (Pair<BDBEnvironment, NodeInfo> entryPair : followersInfo) {
                if (entryPair.second.dir.equals(masterPair.second.dir)) {
                    continue;
                }

                LOG.info("name:{} state:{} dir:{}", entryPair.first.getReplicatedEnvironment().getNodeName(),
                        entryPair.first.getReplicatedEnvironment().getState(),
                        entryPair.second.dir);
                if (entryPair.first.getReplicatedEnvironment().getState().equals(ReplicatedEnvironment.State.MASTER)) {
                    newMasterEnvironment = entryPair.first;
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
            Thread.sleep(1000);
        }
        Assertions.assertNotNull(newMasterEnvironment);

        masterDb = newMasterEnvironment.openDatabase(beginDbName);
        Assertions.assertEquals(OperationStatus.SUCCESS, masterDb.put(null, new DatabaseEntry(randomBytes()), new DatabaseEntry(randomBytes())));
        Assertions.assertEquals(1, newMasterEnvironment.getDatabaseNames().size());
        // // old master
        masterEnvironment.openReplicatedEnvironment(new File(masterDir));
        followersInfo.stream().forEach(entryPair -> {
            entryPair.first.close();
            LOG.info("close {} | {}", entryPair.second.name, entryPair.second.dir);
        });
        LOG.info("end");
    }

    @RepeatedTest(1)
    public void testGetSyncPolicy() throws Exception {
        Assertions.assertEquals(Durability.SyncPolicy.NO_SYNC,
                Deencapsulation.invoke(BDBEnvironment.class, "getSyncPolicy", "NO_SYNC"));

        Assertions.assertEquals(Durability.SyncPolicy.SYNC,
                Deencapsulation.invoke(BDBEnvironment.class, "getSyncPolicy", "SYNC"));

        Assertions.assertEquals(Durability.SyncPolicy.WRITE_NO_SYNC,
                Deencapsulation.invoke(BDBEnvironment.class, "getSyncPolicy", "default"));
    }

    @RepeatedTest(1)
    public void testGetAckPolicy() throws Exception {
        Assertions.assertEquals(Durability.ReplicaAckPolicy.ALL,
                Deencapsulation.invoke(BDBEnvironment.class, "getAckPolicy", "ALL"));

        Assertions.assertEquals(Durability.ReplicaAckPolicy.NONE,
                Deencapsulation.invoke(BDBEnvironment.class, "getAckPolicy", "NONE"));

        Assertions.assertEquals(Durability.ReplicaAckPolicy.SIMPLE_MAJORITY,
                Deencapsulation.invoke(BDBEnvironment.class, "getAckPolicy", "default"));
    }

    @RepeatedTest(1)
    public void testReadTxnIsNotMatched() throws Exception {
        List<Pair<BDBEnvironment, NodeInfo>> followersInfo = new ArrayList<>();

        int masterPort = findValidPort();
        String masterNodeName = "fe1";
        String masterNodeHostPort = "127.0.0.1:" + masterPort;

        BDBEnvironment masterEnvironment = new BDBEnvironment(true, false);
        String masterDir = createTmpDir();
        masterEnvironment.setup(new File(masterDir), masterNodeName, masterNodeHostPort, masterNodeHostPort);
        followersInfo.add(Pair.of(masterEnvironment, new NodeInfo(masterNodeName, masterNodeHostPort, masterDir)));

        for (int i = 2; i <= 3; i++) {
            int nodePort = findValidPort();
            String nodeName = "fe" + i;
            String nodeHostPort = "127.0.0.1:" + nodePort;

            BDBEnvironment followerEnvironment = new BDBEnvironment(true, false);
            String nodeDir = createTmpDir();
            followerEnvironment.setup(new File(nodeDir), nodeName, nodeHostPort, masterNodeHostPort);
            followersInfo.add(Pair.of(followerEnvironment, new NodeInfo(nodeName, nodeHostPort, nodeDir)));
        }

        Pair<BDBEnvironment, NodeInfo> masterPair = findMaster(followersInfo);
        String beginDbName = String.valueOf(0L);
        Database masterDb = masterPair.first.openDatabase(beginDbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());
        Assertions.assertEquals(OperationStatus.SUCCESS, masterDb.put(null, key, value));
        Assertions.assertEquals(1, masterEnvironment.getDatabaseNames().size());
        LOG.info("master is {} | {}", masterPair.second.name, masterPair.second.dir);

        for (Pair<BDBEnvironment, NodeInfo> entryPair : followersInfo) {
            if (entryPair.second.dir.equals(masterPair.second.dir)) {
                LOG.info("skip {}", entryPair.second.name);
                continue;
            }

            Assertions.assertEquals(1, entryPair.first.getDatabaseNames().size());
            Database followerDb = entryPair.first.openDatabase(beginDbName);
            DatabaseEntry readValue = new DatabaseEntry();
            Assertions.assertEquals(OperationStatus.SUCCESS, followerDb.get(null, key, readValue, LockMode.READ_COMMITTED));
            Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));
        }

        Field envImplField = ReplicatedEnvironment.class.getDeclaredField("repEnvironmentImpl");
        envImplField.setAccessible(true);
        RepImpl impl = (RepImpl) envImplField.get(masterPair.first.getReplicatedEnvironment());
        Assertions.assertNotNull(impl);

        new Expectations(impl) {{
                // Below method will replicate log item to followers.
                impl.registerVLSN(withNotNull());
                // Below method will wait until the logs are replicated.
                impl.postLogCommitHook(withNotNull(), withNotNull());
                result = new InsufficientAcksException("mocked");
            }};

        long count = masterDb.count();
        final Database oldMasterDb = masterDb;
        Assertions.assertThrows(InsufficientAcksException.class, () -> {
            // Since this key is not replicated to any replicas, it should not be read.
            DatabaseEntry k = new DatabaseEntry(new byte[]{1, 2, 3});
            DatabaseEntry v = new DatabaseEntry(new byte[]{4, 5, 6});
            oldMasterDb.put(null, k, v);
        });

        LOG.info("close old master {} | {}", masterPair.second.name, masterPair.second.dir);
        masterDb.close();
        masterEnvironment.getEpochDB().close();
        masterEnvironment.close();

        for (Pair<BDBEnvironment, NodeInfo> entryPair : followersInfo) {
            if (entryPair.second.dir.equals(masterPair.second.dir)) {
                LOG.info("skip {}", entryPair.second.name);
                continue;
            }
            LOG.info("close follower {} | {}", entryPair.second.name, entryPair.second.dir);
            entryPair.first.close();
        }

        masterPair.first.openReplicatedEnvironment(new File(masterPair.second.dir));
        masterDb = masterPair.first.openDatabase(beginDbName);
        LOG.info("open {} | {}", masterPair.second.name, masterPair.second.dir);

        // The local commit txn is readable!!!
        Assertions.assertEquals(count + 1, masterDb.count());

        key = new DatabaseEntry(new byte[]{1, 2, 3});
        DatabaseEntry readValue = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, masterDb.get(null, key, readValue, LockMode.READ_COMMITTED));
    }
}
