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

package org.apache.fluss.utils.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Doris's copy of the base class of fluss's long-lived worker threads: a thread whose work throws an
 * {@link Error} ends here, where fluss's ends the process.
 *
 * <p>Fluss's version calls {@code System.exit(-1)} when {@link #doWork()} throws an {@code Error} - fluss
 * took the class from Kafka, which exits only on its own {@code FatalExitError}, and widened that to
 * every {@code Error}. In fluss-client the class runs {@code RemoteLogDownloader}'s thread, one per log
 * scanner, which spends its life waiting for a remote log segment to fetch, and the waiting allocates. A
 * scan that filled BE's JVM heap made that wait throw {@code OutOfMemoryError}, and the exit ran the C++
 * global destructors under a BE that was still serving: BE aborted in its compaction thread on a
 * destroyed mutex, as it did when {@link org.apache.fluss.utils.FatalExitExceptionHandler} exited. Here
 * the thread logs the error, counts itself shut down as fluss's does, and stops - what fluss does on
 * every other {@code Throwable}. Nothing restarts it, so the log scanner it served fetches no further
 * remote segment: a range of that scanner that still needs one waits for it indefinitely.
 *
 * <p>Found ahead of fluss-client's copy because the jar it ships in names it in
 * {@code Doris-Shadows-Classes} (see this module's pom). Apart from that one branch of {@link #run()} it
 * is fluss's class member for member, so the fluss classes compiled against that one link to this one.
 */
public abstract class ShutdownableThread extends Thread {

    protected final Logger log;

    private final boolean isInterruptible;

    private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    private volatile boolean isStarted = false;

    public ShutdownableThread(String name) {
        this(name, true);
    }

    public ShutdownableThread(String name, boolean isInterruptible) {
        super(name);
        this.isInterruptible = isInterruptible;
        this.log = LoggerFactory.getLogger(getClass());
        setDaemon(false);
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    public boolean isShutdownInitiated() {
        return shutdownInitiated.getCount() == 0;
    }

    /**
     * Asks the thread to stop after its current unit of work, interrupting it if it was made
     * interruptible; returns whether this call was the one that asked.
     */
    public boolean initiateShutdown() {
        synchronized (this) {
            if (isRunning()) {
                log.info("Shutting down");
                shutdownInitiated.countDown();
                if (isInterruptible) {
                    interrupt();
                }
                return true;
            }
            return false;
        }
    }

    /** After calling {@link #initiateShutdown()}, waits for the thread to finish its work. */
    public void awaitShutdown() throws InterruptedException {
        if (!isShutdownInitiated()) {
            throw new IllegalStateException("initiateShutdown() was not called before awaitShutdown()");
        }
        if (isStarted) {
            shutdownComplete.await();
        }
        log.info("Shutdown completed");
    }

    /** One unit of the thread's work, run over and over until the thread is shut down. */
    public abstract void doWork() throws Exception;

    @Override
    public void run() {
        isStarted = true;
        log.info("Starting");
        try {
            while (isRunning()) {
                doWork();
            }
        } catch (Error e) {
            shutdownInitiated.countDown();
            shutdownComplete.countDown();
            // Fluss's version calls System.exit(-1) here; see the class comment.
            log.error("Fluss client thread '{}' died of an error. Fluss would stop the process here; inside BE "
                    + "the thread is lost and the process carries on.", getName(), e);
        } catch (Throwable e) {
            if (isRunning()) {
                log.error("Error due to", e);
            }
        } finally {
            shutdownComplete.countDown();
        }
        log.info("Stopped");
    }

    /**
     * Waits for {@code timeout}, or until shutdown is initiated, whichever comes first: a pause that a
     * shutdown cuts short.
     */
    protected void pause(long timeout, TimeUnit unit) throws InterruptedException {
        if (shutdownInitiated.await(timeout, unit)) {
            log.trace("shutdownInitiated latch count reached zero. Shutdown called.");
        }
    }

    public boolean isRunning() {
        return !isShutdownInitiated();
    }
}
