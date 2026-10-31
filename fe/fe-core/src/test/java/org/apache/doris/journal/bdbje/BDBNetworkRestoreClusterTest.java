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
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Strings;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.logging.Level;

/*
 * Reproduces a corruption of the bdb directory of an observer during a rolling upgrade, with a real bdbje
 * replication group. For example, a cluster with 3 followers and 1 observer:
 *
 *   OLD_MASTER (electable, master)   F1 (electable)   F2 (electable)   OBSERVER (secondary)
 *
 * The old master is restarted for upgrade, F1 or F2 becomes the new master, and the observer must roll back
 * the txns which it received from the old master but which were not committed on a majority.
 *
 * 1. All nodes are in sync.
 * 2. The old master sends some txns which are only received by the observer, not by a majority of electable nodes.
 *    (The observer never acks, so it is not counted in the majority.)
 * 3. The cleaner of the observer cleans log files and the observer makes a checkpoint after these txns,
 *    then the old master is stopped for upgrade. The observer keeps RUNNING and keeps querying for a master.
 *    NOTE: bdbje treats the rollback as "past a checkpoint" only if the CheckpointEnd has cleaned files to delete
 *    (CheckpointEnd.getCleanedFilesToDelete()), a checkpoint without cleaned files leads to hard recovery instead.
 * 4. F1 and F2 elect a new master, which does not have the txns of step 2, and writes new txns.
 * 5. The RUNNING observer syncs up with the new master, it must roll back past its checkpoint, so its replica thread
 *    invalidates the environment with InsufficientLogException
 *    ("matchpointVLSN ... precedes a checkpoint end, needs network restore").
 * 6. The replayer thread and http/rpc threads of the observer FE call BDBJEJournal.getMaxJournalId() on the
 *    invalidated environment concurrently, all of them get InsufficientLogException.
 *    Before the fix, each of them ran NetworkRestore on the same directory concurrently, e.g. one thread
 *    restored from F1 while another restored from F2, they deleted and renamed the log files of each other
 *    (e.g. "Rename from xxx.jdb.tmp to xxx.jdb failed", "Missing file: xxx.jdb"), and the next recovery failed
 *    with LOG_FILE_NOT_FOUND.
 *    Expected: NO NetworkRestore runs in the running process, exactly one thread exits the process and the others
 *    are blocked on the restore lock.
 * 7. After the FE process restarts, opening the environment still throws InsufficientLogException, and
 *    BDBEnvironment.setup() runs NetworkRestore exactly once with the environment closed, then the observer has the
 *    same data as the new master.
 */
public class BDBNetworkRestoreClusterTest {
    private static final Logger LOG = LogManager.getLogger(BDBNetworkRestoreClusterTest.class);
    private static final String DB_NAME = "1";
    private static final int UNCOMMITTED_TXN_NUM = 5;
    private static final int NEW_MASTER_TXN_NUM = 3;
    private static final int LOG_FILE_MAX = 1_000_000;
    private static final int CHURN_ROUNDS = 6;
    private static final int CHURN_KEYS = 50;
    private static final int CHURN_VALUE_SIZE = 20 * 1024;
    private static final long WAIT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);
    // threads of the observer FE which call getMaxJournalId(), e.g. the replayer thread:
    //   Env$4.runOneCycle -> replayJournal -> getMaxJournalId -> BDBJEJournal.getDatabaseNames
    // and http/rpc/metric threads, e.g. HaController, FrontendServiceImpl, MetricRepo
    private static final String[] FE_THREAD_NAMES = {"replayer", "qtp-1", "qtp-2", "thrift-1", "thrift-2",
            "metric-1", "qtp-3", "thrift-3"};

    // messages in je.info of the observer, see ReplicaFeederSyncup, NetworkRestore and NetworkBackup of bdbje
    private static final String JE_INFO_NEEDS_RESTORE = "precedes a checkpoint end, needs network restore";
    private static final String JE_INFO_STARTED_RESTORE = "Started network restore";
    private static final String[] JE_INFO_CORRUPTIONS = {"Missing file:", "Rename from:", "LOG_FILE_NOT_FOUND",
            "LOG_CHECKSUM", "UNEXPECTED_STATE"};
    // hold the reference, java.util.logging only keeps weak references to loggers
    private static final java.util.logging.Logger JE_LOGGER =
            java.util.logging.Logger.getLogger("com.sleepycat.je");

    private final List<File> tmpDirs = new ArrayList<>();
    private final List<ReplicatedEnvironment> openedEnvs = Collections.synchronizedList(new ArrayList<>());
    private final IntConsumer originExitHook = BDBEnvironment.exitHook;
    private final List<MockedConstruction<?>> mockedConstructions = new ArrayList<>();

    private static class ExitException extends RuntimeException {
        ExitException(int status) {
            super("exit with status " + status);
        }
    }

    @AfterEach
    public void tearDown() throws IOException {
        BDBEnvironment.exitHook = originExitHook;
        for (MockedConstruction<?> mockedConstruction : mockedConstructions) {
            mockedConstruction.close();
        }
        mockedConstructions.clear();
        synchronized (openedEnvs) {
            for (ReplicatedEnvironment env : openedEnvs) {
                try {
                    env.close();
                } catch (Exception e) {
                    LOG.warn("failed to close environment", e);
                }
            }
            openedEnvs.clear();
        }
        for (File dir : tmpDirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    private File createTmpDir() throws IOException {
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        Path mockDir = Files.createDirectories(Paths.get(dorisHome, "fe", "mocked"));
        File dir = Files.createTempDirectory(mockDir, "BDBNetworkRestoreClusterTest").toFile();
        tmpDirs.add(dir);
        return dir;
    }

    private static int findValidPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static DatabaseEntry entry(String s) {
        return new DatabaseEntry(s.getBytes());
    }

    private ReplicatedEnvironment openNode(File dir, String name, String hostPort, String helpers, boolean electable) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC, ReplicaAckPolicy.SIMPLE_MAJORITY));
        // small log files so that the cleaner has files to clean, and clean them only when the test asks to
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, String.valueOf(LOG_FILE_MAX));
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, "90");
        // je.info in the environment directory is checked by the test
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, "INFO");

        ReplicationConfig repConfig = new ReplicationConfig(BDBEnvironment.PALO_JOURNAL_GROUP, name, hostPort);
        repConfig.setHelperHosts(helpers);
        repConfig.setConfigParam(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "10 s");
        repConfig.setConsistencyPolicy(new NoConsistencyRequiredPolicy());
        if (!electable) {
            repConfig.setNodeType(NodeType.SECONDARY);
        }
        ReplicatedEnvironment env = new ReplicatedEnvironment(dir, repConfig, envConfig);
        openedEnvs.add(env);
        return env;
    }

    private void closeNode(ReplicatedEnvironment env, Database... dbs) {
        for (Database db : dbs) {
            db.close();
        }
        env.close();
        openedEnvs.remove(env);
    }

    private static Database openDb(ReplicatedEnvironment env, boolean readOnly) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(!readOnly);
        dbConfig.setReadOnly(readOnly);
        return env.openDatabase(null, DB_NAME, dbConfig);
    }

    private static void waitForState(ReplicatedEnvironment env, ReplicatedEnvironment.State state)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && env.getState() != state) {
            Thread.sleep(100);
        }
        Assertions.assertEquals(state, env.getState());
    }

    private static void waitForKey(Database db, String key) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (db.get(null, entry(key), new DatabaseEntry(), LockMode.READ_COMMITTED) == OperationStatus.SUCCESS) {
                return;
            }
            Thread.sleep(100);
        }
        Assertions.fail("key " + key + " is not replicated");
    }

    // je.info, je.info.0, je.info.1 ... in the environment directory, oldest first
    private static String readJeInfo(File envDir) throws IOException {
        File[] files = envDir.listFiles((dir, name) -> name.startsWith("je.info") && !name.endsWith(".lck"));
        if (files == null) {
            return "";
        }
        // java.util.logging.FileHandler rotates je.info.0 to je.info.1, so a larger index means older
        Arrays.sort(files, (a, b) -> b.getName().compareTo(a.getName()));
        StringBuilder sb = new StringBuilder();
        for (File file : files) {
            sb.append(new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    private static int countOf(String text, String sub) {
        int count = 0;
        for (int idx = text.indexOf(sub); idx >= 0; idx = text.indexOf(sub, idx + sub.length())) {
            count++;
        }
        return count;
    }

    private static boolean hasKey(Database db, String key) {
        return db.get(null, entry(key), new DatabaseEntry(), LockMode.READ_COMMITTED) == OperationStatus.SUCCESS;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Object networkRestoreLock() throws Exception {
        Field field = BDBEnvironment.class.getDeclaredField("NETWORK_RESTORE_LOCK");
        field.setAccessible(true);
        return field.get(null);
    }

    private static boolean isBlockedOn(ThreadMXBean mxBean, Thread thread, Object lock) {
        ThreadInfo info = mxBean.getThreadInfo(thread.getId());
        return info != null && info.getThreadState() == Thread.State.BLOCKED && info.getLockInfo() != null
                && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(lock);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testObserverInvalidatedAtRuntimeAfterMasterChanged() throws Exception {
        File dirOldMaster = createTmpDir();
        File dirF1 = createTmpDir();
        File dirF2 = createTmpDir();
        File dirObserver = createTmpDir();
        String hostPortOldMaster = "127.0.0.1:" + findValidPort();
        String hostPortF1 = "127.0.0.1:" + findValidPort();
        String hostPortF2 = "127.0.0.1:" + findValidPort();
        int observerPort = findValidPort();
        String hostPortObserver = "127.0.0.1:" + observerPort;
        String nameOldMaster = "old_master";
        String nameF1 = "follower_1";
        String nameF2 = "follower_2";
        String nameObserver = "observer";
        JE_LOGGER.setLevel(Level.INFO);

        // the observer FE: Env is only used to construct BDBJEJournal
        HostInfo observerNode = new HostInfo("127.0.0.1", observerPort);
        Env mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getBdbDir()).thenReturn(dirObserver.getAbsolutePath());
        Mockito.when(mockEnv.getSelfNode()).thenReturn(observerNode);

        // count the real NetworkRestore runs, and check they only run with the restore lock held
        Object restoreLock = networkRestoreLock();
        List<String> restoreThreads = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<BDBEnvironment> restartedEnv = new AtomicReference<>();
        MockedConstruction<NetworkRestore> restoreSpy = Mockito.mockConstruction(NetworkRestore.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS), (mock, context) ->
                        Mockito.doAnswer(inv -> {
                            restoreThreads.add(Thread.currentThread().getName());
                            Assertions.assertTrue(Thread.holdsLock(restoreLock),
                                    "NetworkRestore must run with the restore lock held");
                            BDBEnvironment env = restartedEnv.get();
                            Assertions.assertNotNull(env, "NetworkRestore must only run in setup() after restart");
                            Assertions.assertNull(getField(env, "replicatedEnvironment"),
                                    "NetworkRestore must run with the environment closed");
                            // the real NetworkRestore
                            return inv.callRealMethod();
                        }).when(mock).execute(Mockito.any(InsufficientLogException.class),
                                Mockito.any(NetworkRestoreConfig.class)));
        mockedConstructions.add(restoreSpy);

        // ---------- 1. all nodes are in sync ----------
        ReplicatedEnvironment envOldMaster = openNode(dirOldMaster, nameOldMaster, hostPortOldMaster,
                hostPortOldMaster, true);
        waitForState(envOldMaster, ReplicatedEnvironment.State.MASTER);
        ReplicatedEnvironment envF1 = openNode(dirF1, nameF1, hostPortF1, hostPortOldMaster, true);
        waitForState(envF1, ReplicatedEnvironment.State.REPLICA);
        ReplicatedEnvironment envF2 = openNode(dirF2, nameF2, hostPortF2, hostPortOldMaster, true);
        waitForState(envF2, ReplicatedEnvironment.State.REPLICA);
        ReplicatedEnvironment envObserver = openNode(dirObserver, nameObserver, hostPortObserver,
                hostPortOldMaster, false);
        waitForState(envObserver, ReplicatedEnvironment.State.REPLICA);

        // the master FE creates epochDB, BDBEnvironment.setup() of a non-master node requires it
        DatabaseConfig epochDbConfig = new DatabaseConfig();
        epochDbConfig.setTransactional(true);
        epochDbConfig.setAllowCreate(true);
        envOldMaster.openDatabase(null, "epochDB", epochDbConfig).close();

        Database dbOldMaster = openDb(envOldMaster, false);
        // overwrite the same keys several times, so that the old log files are mostly obsolete and can be cleaned
        DatabaseEntry bigValue = new DatabaseEntry(new byte[CHURN_VALUE_SIZE]);
        for (int round = 0; round < CHURN_ROUNDS; round++) {
            Transaction txn = envOldMaster.beginTransaction(null, null);
            for (int k = 0; k < CHURN_KEYS; k++) {
                Assertions.assertEquals(OperationStatus.SUCCESS,
                        dbOldMaster.put(txn, entry("churn-" + k), bigValue));
            }
            txn.commit();
        }
        for (int i = 0; i < 10; i++) {
            Assertions.assertEquals(OperationStatus.SUCCESS,
                    dbOldMaster.put(null, entry("committed-" + i), entry("v")));
        }
        Database dbF1 = openDb(envF1, true);
        Database dbF2 = openDb(envF2, true);
        Database dbObserver = openDb(envObserver, true);
        waitForKey(dbF1, "committed-9");
        waitForKey(dbF2, "committed-9");
        waitForKey(dbObserver, "committed-9");

        // ---------- 2. txns only received by the observer, not by a majority ----------
        closeNode(envF1, dbF1);
        closeNode(envF2, dbF2);
        TransactionConfig noAck = new TransactionConfig()
                .setDurability(new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC, ReplicaAckPolicy.NONE));
        for (int i = 0; i < UNCOMMITTED_TXN_NUM; i++) {
            Transaction txn = envOldMaster.beginTransaction(null, noAck);
            Assertions.assertEquals(OperationStatus.SUCCESS,
                    dbOldMaster.put(txn, entry("uncommitted-" + i), entry("v")));
            txn.commit();
        }
        waitForKey(dbObserver, "uncommitted-" + (UNCOMMITTED_TXN_NUM - 1));
        dbObserver.close();

        // ---------- 3. the observer cleans files and makes a checkpoint, the old master stops ----------
        int cleanedFiles = 0;
        int cleaned;
        while ((cleaned = envObserver.cleanLog()) > 0) {
            cleanedFiles += cleaned;
        }
        LOG.info("cleaned {} log files on the observer", cleanedFiles);
        Assertions.assertTrue(cleanedFiles > 0, "the cleaner of the observer should clean some files");
        envObserver.checkpoint(new CheckpointConfig().setForce(true));
        closeNode(envOldMaster, dbOldMaster);
        // the observer keeps running without a master
        Assertions.assertTrue(envObserver.isValid());

        // ---------- 4. F1 and F2 elect a new master (2 of 3 electable nodes is a majority) ----------
        String helpers = hostPortF1 + "," + hostPortF2;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ReplicatedEnvironment newMaster;
        String newMasterHostPort;
        try {
            Future<ReplicatedEnvironment> f1 = executor.submit(() -> openNode(dirF1, nameF1, hostPortF1, helpers,
                    true));
            Future<ReplicatedEnvironment> f2 = executor.submit(() -> openNode(dirF2, nameF2, hostPortF2, helpers,
                    true));
            envF1 = f1.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            envF2 = f2.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } finally {
            executor.shutdownNow();
        }
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && !envF1.getState().isMaster()
                && !envF2.getState().isMaster()) {
            Thread.sleep(100);
        }
        if (envF1.getState().isMaster()) {
            newMaster = envF1;
            newMasterHostPort = hostPortF1;
        } else {
            Assertions.assertTrue(envF2.getState().isMaster(), "F1 or F2 should be elected as the new master");
            newMaster = envF2;
            newMasterHostPort = hostPortF2;
        }
        LOG.info("new master: {}", newMaster.getNodeName());
        Database dbNewMaster = openDb(newMaster, false);
        Assertions.assertFalse(hasKey(dbNewMaster, "uncommitted-0"));
        for (int i = 0; i < NEW_MASTER_TXN_NUM; i++) {
            Assertions.assertEquals(OperationStatus.SUCCESS,
                    dbNewMaster.put(null, entry("new-master-" + i), entry("v")));
        }

        // ---------- 5. the RUNNING observer is invalidated by its replica thread ----------
        deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && envObserver.isValid()) {
            Thread.sleep(100);
        }
        Assertions.assertFalse(envObserver.isValid(),
                "the observer should be invalidated after syncing up with the new master");
        // every caller of the invalidated environment gets InsufficientLogException
        Assertions.assertThrows(InsufficientLogException.class, envObserver::getDatabaseNames);
        // the observer is invalidated for the same reason as the real case, not e.g. because it is too far behind
        String jeInfo = readJeInfo(dirObserver);
        Assertions.assertTrue(jeInfo.contains(JE_INFO_NEEDS_RESTORE),
                "je.info of the observer should contain: " + JE_INFO_NEEDS_RESTORE);
        Assertions.assertEquals(0, countOf(jeInfo, JE_INFO_STARTED_RESTORE));

        // ---------- 6. FE threads hit the invalidated environment concurrently ----------
        // the environment of the running observer FE, used by BDBJEJournal as in a real FE process
        BDBEnvironment runningEnv = new BDBEnvironment(false, false);
        setField(runningEnv, "replicatedEnvironment", envObserver);
        BDBJEJournal journal;
        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnvStatic.when(Env::getServingEnv).thenReturn(mockEnv);
            journal = new BDBJEJournal(nameObserver);
        }
        setField(journal, "bdbEnvironment", runningEnv);

        // System.exit() never returns, simulate it by blocking until the test releases it
        AtomicInteger exitCount = new AtomicInteger(0);
        AtomicInteger exitWithLock = new AtomicInteger(0);
        CountDownLatch processExited = new CountDownLatch(1);
        BDBEnvironment.exitHook = status -> {
            exitCount.incrementAndGet();
            if (Thread.holdsLock(restoreLock)) {
                exitWithLock.incrementAndGet();
            }
            try {
                processExited.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new ExitException(status);
        };

        List<Thread> threads = new ArrayList<>();
        AtomicInteger unexpectedError = new AtomicInteger(0);
        CountDownLatch start = new CountDownLatch(1);
        for (String name : FE_THREAD_NAMES) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    journal.getMaxJournalId();
                    unexpectedError.incrementAndGet();
                } catch (ExitException e) {
                    // expected
                } catch (Throwable e) {
                    LOG.warn("unexpected error of thread {}", Thread.currentThread().getName(), e);
                    unexpectedError.incrementAndGet();
                }
            }, name);
            t.setDaemon(true);
            t.start();
            threads.add(t);
        }
        start.countDown();

        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        try {
            deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
            long blocked = 0;
            while (System.currentTimeMillis() < deadline) {
                blocked = threads.stream().filter(t -> isBlockedOn(mxBean, t, restoreLock)).count();
                if (exitCount.get() == 1 && blocked == FE_THREAD_NAMES.length - 1) {
                    break;
                }
                Thread.sleep(50);
            }
            Assertions.assertEquals(0, restoreThreads.size(),
                    "must not run NetworkRestore in a running process, but ran by " + restoreThreads);
            Assertions.assertEquals(1, exitCount.get(), "only one thread should exit while the process is alive");
            Assertions.assertEquals(1, exitWithLock.get(), "the exiting thread should hold the restore lock");
            Assertions.assertEquals(FE_THREAD_NAMES.length - 1, blocked,
                    "other threads should be blocked on the restore lock until the process exits");
        } finally {
            // the process exits, release the threads even if the assertions above failed
            processExited.countDown();
            for (Thread t : threads) {
                t.join(WAIT_TIMEOUT_MS);
            }
        }
        for (Thread t : threads) {
            Assertions.assertFalse(t.isAlive());
        }
        Assertions.assertEquals(0, unexpectedError.get());
        Assertions.assertEquals(0, restoreThreads.size());
        // before the fix, each restore in the running process wrote "Started network restore" to je.info of the
        // invalidated environment, followed by "Missing file: xxx.jdb" / "Rename from: xxx.jdb.tmp ..." when they
        // ran concurrently
        jeInfo = readJeInfo(dirObserver);
        Assertions.assertEquals(0, countOf(jeInfo, JE_INFO_STARTED_RESTORE),
                "must not run NetworkRestore in a running process");
        for (String corruption : JE_INFO_CORRUPTIONS) {
            Assertions.assertFalse(jeInfo.contains(corruption), "je.info of the observer contains: " + corruption);
        }

        // the process exited, release the environment so that it can be reopened in this jvm
        runningEnv.close();
        openedEnvs.remove(envObserver);
        BDBEnvironment.exitHook = status -> {
            throw new ExitException(status);
        };

        // ---------- 7. restart: the cause still exists, setup() restores once and opens the observer ----------
        Assertions.assertThrows(InsufficientLogException.class,
                () -> openNode(dirObserver, nameObserver, hostPortObserver, newMasterHostPort, false),
                "opening the observer without network restore should still throw InsufficientLogException");

        BDBEnvironment restarted = new BDBEnvironment(false, false);
        restartedEnv.set(restarted);
        restarted.setup(dirObserver, nameObserver, hostPortObserver, newMasterHostPort);
        try {
            Assertions.assertEquals(1, restoreThreads.size(), "setup() should run NetworkRestore exactly once");
            ReplicatedEnvironment restoredObserver = restarted.getReplicatedEnvironment();
            Assertions.assertNotNull(restoredObserver);
            Assertions.assertTrue(restoredObserver.isValid());
            Database restoredDb = restarted.openDatabase(DB_NAME);
            waitForKey(restoredDb, "new-master-" + (NEW_MASTER_TXN_NUM - 1));
            Assertions.assertTrue(hasKey(restoredDb, "committed-9"));
            for (int i = 0; i < UNCOMMITTED_TXN_NUM; i++) {
                Assertions.assertFalse(hasKey(restoredDb, "uncommitted-" + i),
                        "txns not committed on the new master must be rolled back");
            }
            // FE threads work on the restored environment
            setField(journal, "bdbEnvironment", restarted);
            Assertions.assertEquals(Collections.singletonList(Long.valueOf(DB_NAME)), journal.getDatabaseNames());
            // the recovery after restore does not hit missing or broken log files.
            // NOTE: the restore count is checked by the mock above, not by je.info: after restart the restore runs
            // with an internal read only environment created by InsufficientLogException, which has no je.info.
            jeInfo = readJeInfo(dirObserver);
            for (String corruption : JE_INFO_CORRUPTIONS) {
                Assertions.assertFalse(jeInfo.contains(corruption),
                        "je.info of the observer contains: " + corruption);
            }
        } finally {
            restarted.close();
            closeNode(newMaster, dbNewMaster);
        }
    }
}
