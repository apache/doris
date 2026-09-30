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

import org.apache.fluss.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Closes the fluss connections scanners are done with, off the thread that was scanning.
 *
 * <p>Closing a {@link Connection} shuts its netty client down with netty's default graceful period,
 * which returns only after two quiet seconds, and fluss offers no shorter one. Every range opens a
 * connection of its own, so closing it inline put those two seconds on every range: most of what a
 * union read with a small log tail cost, and the whole of what a count over a tiny table did. Nothing
 * a scan returns depends on the connection being gone, so a scanner hands it here and returns.
 *
 * <p><b>Bounded, because the wait was also a brake.</b> A connection that is closing still holds its
 * client threads (three netty threads each) until the quiet period passes. While scanners closed
 * inline, no more connections could be closing than there were scanners. Off the scanning thread
 * nothing holds a scan of thousands of small ranges back from finishing ranges far faster than their
 * connections close, so at most {@link #MAX_CLOSING} close here at a time; past that a scanner closes
 * its own connection the way every scanner used to, and the brake is back.
 */
final class FlussConnectionCloser {

    private static final Logger LOG = LoggerFactory.getLogger(FlussConnectionCloser.class);

    /** Connections closing in the background at once; each is one thread here and three of its own. */
    static final int MAX_CLOSING = 256;

    /** Scanners closing inline again is worth a line in the log, not one per connection. */
    private static final long SATURATION_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    private static final AtomicLong LAST_SATURATION_LOG_NANOS =
            new AtomicLong(System.nanoTime() - SATURATION_LOG_INTERVAL_NANOS);

    /**
     * One thread per closing connection and no queue: a queued connection would hold its threads
     * while waiting for a turn, which is the pile-up the bound exists to prevent. A connection that
     * finds every thread busy is closed by the thread that brought it.
     */
    private static final ExecutorService CLOSER = new ThreadPoolExecutor(
            0, MAX_CLOSING, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(),
            runnable -> {
                Thread thread = new Thread(runnable,
                        "fluss-connection-closer-" + THREAD_COUNTER.incrementAndGet());
                // Never what keeps BE's JVM alive, and never worth waiting for at shutdown.
                thread.setDaemon(true);
                // Shutting the client down can still load fluss classes; like JniScanner does around
                // open and close, run it under the loader of the plugin that can see them.
                thread.setContextClassLoader(FlussConnectionCloser.class.getClassLoader());
                return thread;
            },
            (close, executor) -> closeOnCallingThread(close));

    private FlussConnectionCloser() {
    }

    /**
     * Every closer thread is busy, so the thread that brought the connection closes it. That costs
     * its scan two seconds a range again, which would otherwise show up nowhere: BE publishes a
     * scanner's counters before it closes it.
     */
    private static void closeOnCallingThread(Runnable close) {
        long now = System.nanoTime();
        long last = LAST_SATURATION_LOG_NANOS.get();
        if (now - last >= SATURATION_LOG_INTERVAL_NANOS && LAST_SATURATION_LOG_NANOS.compareAndSet(last, now)) {
            LOG.info("{} fluss connections are closing in the background; scanners close their own "
                    + "connections, waiting about two seconds for each, until some of those are done",
                    MAX_CLOSING);
        }
        close.run();
    }

    /**
     * Closes {@code connection} without making the caller wait for it, unless {@link #MAX_CLOSING}
     * are closing already. A failure to close is logged and nothing else: the scan the connection
     * served has already returned its rows, and a query must not fail over the cleanup of one of
     * its connections.
     */
    static void close(Connection connection) {
        CLOSER.execute(() -> {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("Failed to close a fluss connection", e);
            }
        });
    }
}
