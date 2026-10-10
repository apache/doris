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

import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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
 * serves the next one. So the pool never holds more connections for a client configuration than ranges
 * with that configuration have been read at once.
 *
 * <p><b>Keyed by the whole client configuration.</b> A connection goes back to the ranges that would have
 * opened it with the same settings, so ranges of catalogs with different servers, credentials or client
 * options never share one. The key holds whatever the configuration holds, credentials included, and is
 * never logged. The bound above is per configuration, not for the pool as a whole: what is idle under
 * each configuration adds up until it is closed. One catalog alone has two, since its {@code PK_FULL}
 * ranges leave the log read preference at fluss's default and its other ranges set it
 * ({@code FlussJniScanner#clientConfig}).
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

    static final FlussConnectionPool INSTANCE = new FlussConnectionPool(ConnectionFactory::createConnection,
            System::nanoTime, FlussConnectionCloser::close, FlussConnectionPool::startReaper);

    private final Function<Configuration, Connection> factory;
    private final LongSupplier nanoTime;
    /** Closes, without waiting for it, a connection the pool lets go of: {@link FlussConnectionCloser#close}. */
    private final Consumer<Connection> closer;
    /** Starts the thread that runs the sweep it is given on this pool: {@link #startReaper}. */
    private final Consumer<Runnable> reaperStarter;
    /** Set while the reaper is being started and once it has; see {@link #startReaperOnce}. */
    private final AtomicBoolean reaperStarted = new AtomicBoolean();

    /**
     * Idle connections by the configuration they were opened with, the most recently returned last. A
     * configuration with no idle connection has no entry, which is what {@link #borrow} and the reaper rely
     * on: {@link #giveBack} puts an entry in only once it holds its connection.
     */
    private final Map<Map<String, String>, Deque<Idle>> idle = new HashMap<>();

    FlussConnectionPool(Function<Configuration, Connection> factory, LongSupplier nanoTime,
            Consumer<Connection> closer, Consumer<Runnable> reaperStarter) {
        this.factory = factory;
        this.nanoTime = nanoTime;
        this.closer = closer;
        this.reaperStarter = reaperStarter;
    }

    /**
     * A connection opened with {@code config}: the one given back most recently if any is idle, else a
     * new one. The most recent rather than the oldest, so that after a burst the connections it no longer
     * needs are the ones left idle long enough to be closed.
     */
    Lease borrow(Configuration config) {
        Map<String, String> key = config.toMap();
        synchronized (idle) {
            Deque<Idle> connections = idle.get(key);
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

    /**
     * Takes back the connection of a range that was read to its end, for the next range to borrow.
     *
     * <p>An {@code OutOfMemoryError} here may cost this connection, never the ones the pool holds: wherever
     * it strikes, the pool is left whole. An entry goes into {@link #idle} already holding its connection -
     * put in empty and filled after, an error in between would leave an entry that every later
     * {@link #borrow} and every sweep take a connection from and find none. And an entry is a
     * {@link LinkedList}, which allocates a node before it links it in: an {@code ArrayDeque} stores an
     * element first and grows its array after, and an error in that growth leaves it looking empty while it
     * holds every connection - the next one given back overwrites the oldest, and the rest are out of reach
     * of {@link #borrow} and of the reaper for good.
     */
    void giveBack(Lease lease) {
        Idle returned = new Idle(lease.connection, nanoTime.getAsLong());
        synchronized (idle) {
            Deque<Idle> connections = idle.get(lease.key);
            if (connections == null) {
                Deque<Idle> first = new LinkedList<>();
                first.addLast(returned);
                idle.put(lease.key, first);
            } else {
                connections.addLast(returned);
            }
        }
        startReaperOnce();
    }

    /**
     * Starts the reaper with the first connection given back, and with a later one again if starting it
     * failed. Not while the class initializes: starting a thread fails with an {@code OutOfMemoryError}
     * while BE's JVM heap is full or no native thread is to be had, and an error in a static initializer
     * leaves the class unusable in its classloader, which BE keeps for the life of the process - every
     * fluss read of the BE would fail with {@code NoClassDefFoundError} until it restarted. Until the reaper
     * runs, ranges go on borrowing and giving back; only idle connections wait for it.
     */
    private void startReaperOnce() {
        if (reaperStarted.get() || !reaperStarted.compareAndSet(false, true)) {
            return;
        }
        try {
            reaperStarter.accept(() -> sweep(this));
        } catch (Throwable startFailure) {
            reaperStarted.set(false);
            try {
                LOG.warn("Failed to start the thread that closes idle fluss connections; the next connection"
                        + " given back tries again", startFailure);
            } catch (Throwable logFailure) {
                // Logging allocates too, and the heap may still be full; the next connection given back
                // tries again regardless.
            }
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
        closer.accept(lease.connection);
    }

    /**
     * Closes, without waiting for them, the connections nobody has borrowed for {@code idleNanos}.
     *
     * <p>One at a time: a connection leaves the pool only to be handed to the closer at once. Nothing else
     * holds it then, so an {@code OutOfMemoryError} between taking it and handing it over would leave it
     * open, threads and sockets, for the life of the process - and while BE's JVM heap is full, the error
     * strikes whatever allocates. Taken out together into a list and handed over after a log line, every
     * expired connection was out of the pool while the list grew, the line was logged and each close was
     * submitted, which is where nearly all of a sweep's allocation is, and an error there lost all of them.
     */
    void closeIdleLongerThan(long idleNanos) {
        long now = nanoTime.getAsLong();
        int closed = 0;
        for (Connection connection = takeExpired(now, idleNanos); connection != null;
                connection = takeExpired(now, idleNanos)) {
            // Outside the lock: a close the closer hands back to this thread takes two seconds.
            closer.accept(connection);
            closed++;
        }
        if (closed > 0) {
            LOG.info("Closing {} fluss connections nobody has borrowed for {} seconds", closed,
                    TimeUnit.NANOSECONDS.toSeconds(idleNanos));
        }
    }

    /**
     * Takes one connection idle for {@code idleNanos} at {@code now} out of the pool, or returns null if
     * none is. Nothing may allocate between unlinking it and returning it ({@link #closeIdleLongerThan}):
     * the list only unlinks it, and the iterator removes an emptied entry by the hash the map stored
     * rather than hashing the configuration again.
     */
    private Connection takeExpired(long now, long idleNanos) {
        synchronized (idle) {
            Iterator<Deque<Idle>> keys = idle.values().iterator();
            while (keys.hasNext()) {
                Deque<Idle> connections = keys.next();
                // An entry is in the order its connections came back, so an expired one is at its head.
                if (now - connections.peekFirst().since >= idleNanos) {
                    Connection connection = connections.pollFirst().connection;
                    if (connections.isEmpty()) {
                        keys.remove();
                    }
                    return connection;
                }
            }
            return null;
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

    /**
     * Runs {@code sweep} every {@link #REAP_INTERVAL_SECONDS} on a daemon thread of its own. A new executor
     * on every call, never one kept from a call that failed: that one already holds the sweep, queued
     * before the thread to run it failed to start, and a later start of its thread would run it twice.
     */
    private static void startReaper(Runnable sweep) {
        ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "fluss-connection-reaper");
            // Never what keeps BE's JVM alive.
            thread.setDaemon(true);
            // The closer may hand a close back to this thread, and closing can load fluss classes.
            thread.setContextClassLoader(FlussConnectionPool.class.getClassLoader());
            return thread;
        });
        reaper.scheduleWithFixedDelay(sweep, REAP_INTERVAL_SECONDS, REAP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    int idleCount() {
        synchronized (idle) {
            int count = 0;
            for (Deque<Idle> connections : idle.values()) {
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
