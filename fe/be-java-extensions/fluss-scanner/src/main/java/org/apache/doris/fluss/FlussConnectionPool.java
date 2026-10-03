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
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Fluss connections kept open from one scan range to the next, so that a range borrows a connection
 * instead of opening one of its own.
 *
 * <p>A connection is a netty client with its own event loop threads, a metadata cache, and - once a
 * range has read a kv snapshot or a remote log segment through it - a pool of download threads. Opening
 * one per range cost every range the connect, and a close that waits out netty's two-second graceful
 * shutdown. {@link FlussConnectionCloser} took that close off the scanning thread, but only up to the
 * number of closes it runs at once: back-to-back queries over many small ranges, or one query over a
 * partitioned table with hundreds of them, still left ranges closing their own connections, two
 * seconds each. A range read to its end now gives its connection back instead; only one that failed or
 * was closed early (a LIMIT, a cancel) has its connection closed, for the reasons {@link #discard} gives.
 *
 * <p><b>One range per connection at a time.</b> A fluss connection is thread-safe and could serve every
 * range at once, but then its download threads ({@code client.remote-file.download-thread-num}, three by
 * default) would be shared by every primary-key range copying its kv snapshot at the same moment, where
 * each such range used to have three of its own. Lending a connection to one range at a time keeps every
 * range's client resources what they were; what changes is that a connection outlives its range and
 * serves the next one. The pool never holds more connections than ranges were read at once.
 *
 * <p><b>Keyed by the whole client configuration.</b> A connection goes back to the ranges that would have
 * opened it with the same settings, so ranges of catalogs with different servers, credentials or client
 * options never share one. The key holds whatever the configuration holds, credentials included, and is
 * never logged.
 *
 * <p><b>Idle connections are closed after {@link #IDLE_TIMEOUT_NANOS}.</b> What one burst of ranges
 * opened is there for the next burst and closed, through the closer, once nothing has borrowed it for
 * that long - a BE that stops reading fluss does not keep the threads.
 */
final class FlussConnectionPool {

    private static final Logger LOG = LoggerFactory.getLogger(FlussConnectionPool.class);

    /** How long a connection nobody borrows is kept. */
    static final long IDLE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);

    /** How often idle connections are looked at: one lives at most this much past its timeout. */
    private static final long REAP_INTERVAL_SECONDS = 10;

    static final FlussConnectionPool INSTANCE =
            new FlussConnectionPool(ConnectionFactory::createConnection, System::nanoTime);

    static {
        ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "fluss-connection-reaper");
            // Never what keeps BE's JVM alive.
            thread.setDaemon(true);
            // The closer may hand a close back to this thread, and closing can load fluss classes.
            thread.setContextClassLoader(FlussConnectionPool.class.getClassLoader());
            return thread;
        });
        reaper.scheduleWithFixedDelay(() -> sweep(INSTANCE), REAP_INTERVAL_SECONDS, REAP_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }

    private final Function<Configuration, Connection> factory;
    private final LongSupplier nanoTime;

    /**
     * Idle connections by the configuration they were opened with, the most recently returned last. A
     * configuration with no idle connection has no entry, which is what {@link #borrow} relies on.
     */
    private final Map<Map<String, String>, ArrayDeque<Idle>> idle = new HashMap<>();

    FlussConnectionPool(Function<Configuration, Connection> factory, LongSupplier nanoTime) {
        this.factory = factory;
        this.nanoTime = nanoTime;
    }

    /**
     * A connection opened with {@code config}: the one given back most recently if any is idle, else a
     * new one. The most recent rather than the oldest, so that after a burst the connections it no longer
     * needs are the ones left idle long enough to be closed.
     */
    Lease borrow(Configuration config) {
        Map<String, String> key = config.toMap();
        synchronized (idle) {
            ArrayDeque<Idle> connections = idle.get(key);
            if (connections != null) {
                Connection connection = connections.pollLast().connection;
                if (connections.isEmpty()) {
                    idle.remove(key);
                }
                return new Lease(key, connection, false);
            }
        }
        // Outside the lock: opening a connection talks to the cluster.
        return new Lease(key, factory.apply(config), true);
    }

    /** Takes back the connection of a range that was read to its end, for the next range to borrow. */
    void giveBack(Lease lease) {
        long now = nanoTime.getAsLong();
        synchronized (idle) {
            idle.computeIfAbsent(lease.key, key -> new ArrayDeque<>()).addLast(new Idle(lease.connection, now));
        }
    }

    /**
     * Closes, without waiting for it, the connection of a range that failed or was closed before its end,
     * instead of lending it again. Such a connection may be broken by what failed it: an
     * {@code OutOfMemoryError} on one of its netty threads ends that thread's event loop for good. The
     * most recently returned connection is lent first, so one like that, given back, would be lent to
     * every range that came next. A primary-key range closed before its kv snapshot arrived hands its
     * connection here only once the snapshot copy running on the connection's download threads is over
     * ({@code FlussJniScanner#closeInternal}); closed under the copy, the connection would strand it.
     */
    void discard(Lease lease) {
        FlussConnectionCloser.close(lease.connection);
    }

    /** Closes, without waiting for them, the connections nobody has borrowed for {@code idleNanos}. */
    void closeIdleLongerThan(long idleNanos) {
        long now = nanoTime.getAsLong();
        List<Connection> expired = new ArrayList<>();
        synchronized (idle) {
            Iterator<ArrayDeque<Idle>> keys = idle.values().iterator();
            while (keys.hasNext()) {
                ArrayDeque<Idle> connections = keys.next();
                // A deque is in the order its connections came back, so the expired ones are at its head.
                while (!connections.isEmpty() && now - connections.peekFirst().since >= idleNanos) {
                    expired.add(connections.pollFirst().connection);
                }
                if (connections.isEmpty()) {
                    keys.remove();
                }
            }
        }
        if (!expired.isEmpty()) {
            LOG.info("Closing {} fluss connections nobody has borrowed for {} seconds", expired.size(),
                    TimeUnit.NANOSECONDS.toSeconds(idleNanos));
        }
        // Outside the lock: a close the closer hands back to this thread takes two seconds.
        for (Connection connection : expired) {
            FlussConnectionCloser.close(connection);
        }
    }

    /**
     * One run of the reaper. Nothing may escape it: a scheduled task that throws is never run again, and
     * idle connections would then keep their threads for the life of the process. That holds for an
     * {@code Error} as much as for an exception - above all the {@code OutOfMemoryError} of a scan that
     * filled BE's JVM heap, which strikes whatever allocates while the heap is full. Caught as an
     * exception only, one such error ended the sweeps of a BE for good, and the 128 connections idle at
     * that moment stayed open, threads and sockets, until the BE restarted.
     */
    static void sweep(FlussConnectionPool pool) {
        try {
            pool.closeIdleLongerThan(IDLE_TIMEOUT_NANOS);
        } catch (Throwable t) {
            try {
                LOG.warn("Failed to close idle fluss connections", t);
            } catch (Throwable logFailure) {
                // Logging allocates too, and the heap may still be full; the next sweep comes regardless.
            }
        }
    }

    int idleCount() {
        synchronized (idle) {
            int count = 0;
            for (ArrayDeque<Idle> connections : idle.values()) {
                count += connections.size();
            }
            return count;
        }
    }

    /** A connection lent to one range, and what it has to be given back under. */
    static final class Lease {
        private final Map<String, String> key;
        private final Connection connection;
        private final boolean opened;

        private Lease(Map<String, String> key, Connection connection, boolean opened) {
            this.key = key;
            this.connection = connection;
            this.opened = opened;
        }

        Connection connection() {
            return connection;
        }

        /** Whether the range had to open this connection, rather than borrow one that was idle. */
        boolean opened() {
            return opened;
        }
    }

    private static final class Idle {
        private final Connection connection;
        private final long since;

        private Idle(Connection connection, long since) {
            this.connection = connection;
            this.since = since;
        }
    }
}
