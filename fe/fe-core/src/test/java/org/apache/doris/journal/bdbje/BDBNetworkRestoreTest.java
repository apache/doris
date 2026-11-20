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
import org.apache.doris.ha.HAProtocol;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Strings;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

/*
 * Simulates a corruption of the bdb directory: e.g. after the master changed during a rolling upgrade, the bdb
 * environment of an observer was invalidated with InsufficientLogException. Several threads (e.g. the replayer
 * thread and an http thread) got the exception and ran NetworkRestore on the same directory concurrently,
 * which corrupted the log files.
 *
 * Expected behavior:
 * 1. at runtime, no thread runs NetworkRestore, the first thread exits the process, others are blocked;
 * 2. after restart, setup() runs NetworkRestore once with the environment closed, then opens the environment;
 * 3. if setup() still can not open the environment, it exits instead of returning a null environment.
 */
public class BDBNetworkRestoreTest {
    private static final List<File> tmpDirs = new ArrayList<>();
    private static final IntConsumer ORIGIN_EXIT_HOOK = BDBEnvironment.exitHook;

    // thrown by the mocked exit hook instead of exiting the jvm
    private static class ExitException extends RuntimeException {
        ExitException(int status) {
            super("exit with status " + status);
        }
    }

    @AfterEach
    public void tearDown() {
        BDBEnvironment.exitHook = ORIGIN_EXIT_HOOK;
    }

    @AfterAll
    public static void cleanUp() throws IOException {
        for (File dir : tmpDirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    private static File createTmpDir() throws IOException {
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        Path mockDir = Files.createDirectories(Paths.get(dorisHome, "fe", "mocked"));
        File dir = Files.createTempDirectory(mockDir, "BDBNetworkRestoreTest").toFile();
        tmpDirs.add(dir);
        return dir;
    }

    private static int findValidPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    // every NetworkRestore created while the returned construction is open is a mock, which counts the restores
    // and runs the given check instead of restoring the log files
    private static MockedConstruction<NetworkRestore> mockNetworkRestore(AtomicInteger restoreCount,
            Runnable check) {
        return Mockito.mockConstruction(NetworkRestore.class, (mock, context) ->
                Mockito.doAnswer(inv -> {
                    restoreCount.incrementAndGet();
                    check.run();
                    return null;
                }).when(mock).execute(Mockito.any(InsufficientLogException.class),
                        Mockito.any(NetworkRestoreConfig.class)));
    }

    private static Object networkRestoreLock() throws Exception {
        Field field = BDBEnvironment.class.getDeclaredField("NETWORK_RESTORE_LOCK");
        field.setAccessible(true);
        return field.get(null);
    }

    private static ReplicatedEnvironment getReplicatedEnvironmentField(BDBEnvironment env) {
        try {
            Field field = BDBEnvironment.class.getDeclaredField("replicatedEnvironment");
            field.setAccessible(true);
            return (ReplicatedEnvironment) field.get(env);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean isBlockedOn(ThreadMXBean mxBean, Thread thread, Object lock) {
        ThreadInfo info = mxBean.getThreadInfo(thread.getId());
        return info != null && info.getThreadState() == Thread.State.BLOCKED && info.getLockInfo() != null
                && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(lock);
    }

    // Scenario: the replayer thread and http threads hit the invalidated environment at the
    // same time. None of them may run NetworkRestore, and only one exits while the process is alive.
    @Test
    public void testConcurrentInsufficientLogAtRuntimeExitOnceWithoutRestore() throws Exception {
        int port = findValidPort();
        File bdbDir = createTmpDir();
        HostInfo selfNode = new HostInfo("127.0.0.1", port);

        // the invalidated environment throws the same exception to every caller
        BDBEnvironment invalidatedEnv = Mockito.mock(BDBEnvironment.class);
        Mockito.when(invalidatedEnv.getDatabaseNames())
                .thenThrow(new InsufficientLogException("mocked: log files at this node are obsolete"));

        // System.exit() never returns, simulate it by blocking until the test releases it
        Object restoreLock = networkRestoreLock();
        AtomicInteger exitCount = new AtomicInteger(0);
        List<Integer> exitStatus = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch processExited = new CountDownLatch(1);
        BDBEnvironment.exitHook = status -> {
            exitCount.incrementAndGet();
            exitStatus.add(status);
            try {
                processExited.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new ExitException(status);
        };

        Env mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getBdbDir()).thenReturn(bdbDir.getAbsolutePath());
        Mockito.when(mockEnv.getSelfNode()).thenReturn(selfNode);
        BDBJEJournal journal;
        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnvStatic.when(Env::getServingEnv).thenReturn(mockEnv);
            journal = new BDBJEJournal(Env.genFeNodeName("127.0.0.1", port, false));
        }
        Field envField = BDBJEJournal.class.getDeclaredField("bdbEnvironment");
        envField.setAccessible(true);
        envField.set(journal, invalidatedEnv);

        AtomicInteger restoreCount = new AtomicInteger(0);
        try (MockedConstruction<NetworkRestore> ignored = mockNetworkRestore(restoreCount, () -> { })) {
            // getMaxJournalId() is called by http apis, e.g. /rest/v2/manager/node/frontends
            String[] threadNames = {"replayer", "qtp-1", "qtp-2", "qtp-3", "qtp-4", "thrift-1", "thrift-2", "qtp-5"};
            List<Thread> threads = new ArrayList<>();
            AtomicInteger unexpectedError = new AtomicInteger(0);
            CountDownLatch start = new CountDownLatch(1);
            for (String name : threadNames) {
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        journal.getMaxJournalId();
                        unexpectedError.incrementAndGet();
                    } catch (ExitException e) {
                        // expected
                    } catch (Throwable e) {
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
                // wait until one thread is "exiting" and all others are blocked on the restore lock
                long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
                long blocked = 0;
                while (System.currentTimeMillis() < deadline) {
                    blocked = threads.stream().filter(t -> isBlockedOn(mxBean, t, restoreLock)).count();
                    if (exitCount.get() == 1 && blocked == threadNames.length - 1) {
                        break;
                    }
                    Thread.sleep(50);
                }
                Assertions.assertEquals(0, restoreCount.get(), "must not run NetworkRestore in a running process");
                Assertions.assertEquals(1, exitCount.get(), "only one thread should exit while the process is alive");
                Assertions.assertEquals(threadNames.length - 1, blocked,
                        "other threads should be blocked on the restore lock until exit");
                Assertions.assertEquals(-1, exitStatus.get(0));
            } finally {
                // let the blocked threads finish, even if the assertions above failed
                processExited.countDown();
                for (Thread t : threads) {
                    t.join(TimeUnit.SECONDS.toMillis(30));
                }
            }
            for (Thread t : threads) {
                Assertions.assertFalse(t.isAlive());
            }
            Assertions.assertEquals(0, unexpectedError.get());
            Assertions.assertEquals(0, restoreCount.get());
        }
    }

    // Scenario after restart: opening the environment throws InsufficientLogException again because the cause
    // still exists. setup() runs NetworkRestore exactly once, with the environment closed and the lock held,
    // then opens the environment successfully.
    @Test
    public void testRestoreOnceWhenOpeningEnvironmentAfterRestart() throws Exception {
        BDBEnvironment.exitHook = status -> {
            throw new ExitException(status);
        };
        int port = findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;
        Object lock = networkRestoreLock();
        BDBEnvironment bdbEnvironment = new BDBEnvironment(true, false);

        List<String> events = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger restoreCount = new AtomicInteger(0);
        // setHaProtocol() is called in the same try block as opening the environment in setup(),
        // throwing from it takes the same path as the environment constructor throwing.
        AtomicInteger openCount = new AtomicInteger(0);
        Env mockEnv = Mockito.mock(Env.class);
        Mockito.doAnswer(inv -> {
            if (openCount.incrementAndGet() == 1) {
                events.add("open-insufficient-log");
                throw new InsufficientLogException("mocked: log files at this node are obsolete");
            }
            events.add("open-success");
            return null;
        }).when(mockEnv).setHaProtocol(Mockito.any(HAProtocol.class));

        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                MockedConstruction<NetworkRestore> ignored = mockNetworkRestore(restoreCount, () -> {
                    Assertions.assertTrue(Thread.holdsLock(lock),
                            "NetworkRestore must run with the restore lock held");
                    Assertions.assertNull(getReplicatedEnvironmentField(bdbEnvironment),
                            "NetworkRestore must run with the environment closed");
                    events.add("restore");
                })) {
            mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);
            bdbEnvironment.setup(createTmpDir(), selfNodeName, selfNodeHostPort, selfNodeHostPort);
        }
        try {
            Assertions.assertEquals(1, restoreCount.get());
            Assertions.assertEquals(2, openCount.get());
            Assertions.assertEquals(Arrays.asList("open-insufficient-log", "restore", "open-success"), events);

            // the environment is usable after restore
            Database epochDb = bdbEnvironment.getEpochDB();
            Assertions.assertNotNull(epochDb);
            DatabaseEntry key = new DatabaseEntry("key".getBytes());
            DatabaseEntry value = new DatabaseEntry("value".getBytes());
            Assertions.assertEquals(OperationStatus.SUCCESS, epochDb.put(null, key, value));
            DatabaseEntry readValue = new DatabaseEntry();
            Assertions.assertEquals(OperationStatus.SUCCESS,
                    epochDb.get(null, key, readValue, LockMode.READ_COMMITTED));
            Assertions.assertEquals("value", new String(readValue.getData()));
        } finally {
            bdbEnvironment.close();
        }
    }

    // If the environment still can not be opened after all retries, setup() must exit the process
    // instead of returning with a null environment.
    @Test
    public void testExitWhenEnvironmentCanNotBeOpenedAfterRestore() throws Exception {
        AtomicInteger exitCount = new AtomicInteger(0);
        BDBEnvironment.exitHook = status -> {
            exitCount.incrementAndGet();
            throw new ExitException(status);
        };
        int port = findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;
        Env mockEnv = Mockito.mock(Env.class);
        Mockito.doThrow(new InsufficientLogException("mocked: log files at this node are obsolete"))
                .when(mockEnv).setHaProtocol(Mockito.any(HAProtocol.class));

        BDBEnvironment bdbEnvironment = new BDBEnvironment(true, false);
        File dir = createTmpDir();
        AtomicInteger restoreCount = new AtomicInteger(0);
        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                MockedConstruction<NetworkRestore> ignored = mockNetworkRestore(restoreCount, () -> { })) {
            mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);
            Assertions.assertThrows(ExitException.class,
                    () -> bdbEnvironment.setup(dir, selfNodeName, selfNodeHostPort, selfNodeHostPort));
        }
        Assertions.assertEquals(3, restoreCount.get());
        Assertions.assertEquals(1, exitCount.get());
        Assertions.assertNull(bdbEnvironment.getReplicatedEnvironment());
    }
}
