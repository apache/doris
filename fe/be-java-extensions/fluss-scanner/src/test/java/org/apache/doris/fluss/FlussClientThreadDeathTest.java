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

package org.apache.doris.fluss;

import org.apache.fluss.utils.FatalExitExceptionHandler;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A fluss client thread that dies must not take the process with it. Fluss's own handler for these
 * threads, and the base class of its remote log download threads, call {@code System.exit}, and inside
 * BE that runs the C++ global destructors under a BE that is still serving; the plugin ships classes
 * of the same names that only log.
 *
 * <p>Should one of fluss's come back - a patched class dropped, or found after fluss-client's - these
 * tests do not fail with an assertion: the JVM running them exits, and surefire reports a forked VM
 * that terminated without saying goodbye.
 */
public class FlussClientThreadDeathTest {

    @Test
    public void clientThreadThatDiesOfAnUncaughtErrorLeavesTheProcessRunning() throws Exception {
        // The factory behind fluss's admin refresh, remote file download and token renewal threads.
        Thread thread = new ExecutorThreadFactory("doris-fatal-exit-test").newThread(() -> {
            throw new OutOfMemoryError("simulated: Java heap space");
        });
        Assertions.assertSame(FatalExitExceptionHandler.INSTANCE, thread.getUncaughtExceptionHandler(),
                "fluss no longer hands its threads this handler; the patch may not be needed any more");

        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(60));
        Assertions.assertFalse(thread.isAlive(), "the thread was meant to die");
    }

    @Test
    public void failedFutureFlussAssertsNeverFailsLeavesTheProcessRunning() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        FutureUtils.assertNoException(future);
        future.completeExceptionally(new IllegalStateException("simulated"));
        Assertions.assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void downloadThreadThatDiesOfAnErrorLeavesTheProcessRunning() throws Exception {
        Assertions.assertSame(ShutdownableThread.class,
                Class.forName("org.apache.fluss.client.table.scanner.log.RemoteLogDownloader$DownloadRemoteLogThread")
                        .getSuperclass(),
                "fluss no longer runs its remote log download on this class; the patch may not be needed any more");
        ShutdownableThread thread = new ShutdownableThread("doris-fatal-exit-test") {
            @Override
            public void doWork() {
                throw new OutOfMemoryError("simulated: Java heap space");
            }
        };

        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(60));
        Assertions.assertFalse(thread.isAlive(), "the thread was meant to die");
        // What closing its log scanner does with it afterwards: must return, not wait on the dead thread.
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(60), thread::shutdown);
    }

    /**
     * The heap was full as the thread started, and logging that it starts is the first thing it
     * allocates. The thread is lost all the same; the close of its log scanner, which shuts it down on a
     * scan thread, must still return instead of waiting for a thread that never said it stopped.
     */
    @Test
    public void downloadThreadThatDiesAsItStartsLetsItsScannerClose() throws Exception {
        ShutdownableThread thread = new ShutdownableThread("doris-fatal-exit-test") {
            @Override
            public void doWork() throws InterruptedException {
                pause(1, TimeUnit.DAYS);
            }
        };
        Field log = ShutdownableThread.class.getDeclaredField("log");
        log.setAccessible(true);
        log.set(thread, runningOutOfHeapOn("Starting", (Logger) log.get(thread)));

        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(60));
        Assertions.assertFalse(thread.isAlive(), "the thread was meant to die");
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(60), thread::shutdown);
    }

    /**
     * Under a full heap the JVM can end a thread without running its {@code finally} blocks: a compiled
     * frame whose scalar-replaced objects cannot be reallocated on deoptimization is unwound past them. The
     * thread then never counts itself shut down, and closing its log scanner must return all the same.
     */
    @Test
    public void downloadThreadThatEndsWithoutSayingSoLetsItsScannerClose() throws Exception {
        ShutdownableThread thread = new ShutdownableThread("doris-fatal-exit-test") {
            @Override
            public void doWork() {
            }

            @Override
            public void run() {
                // What the JVM leaves when it unwinds ShutdownableThread#run past its finally: a thread
                // that started and ended, and a shutdownComplete nobody counted down.
            }
        };
        Field started = ShutdownableThread.class.getDeclaredField("isStarted");
        started.setAccessible(true);
        started.set(thread, true);

        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(60));
        Assertions.assertFalse(thread.isAlive(), "the thread was meant to end");
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(60), thread::shutdown);
    }

    /** {@code logger}, except that logging {@code message} at info runs out of heap. */
    private static Logger runningOutOfHeapOn(String message, Logger logger) {
        return (Logger) Proxy.newProxyInstance(Logger.class.getClassLoader(), new Class<?>[] {Logger.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("info") && args != null && message.equals(args[0])) {
                        throw new OutOfMemoryError("simulated: Java heap space");
                    }
                    try {
                        return method.invoke(logger, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
