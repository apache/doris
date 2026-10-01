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
import org.apache.fluss.config.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * What the pool promises a scan range: it borrows the connection a range before it gave back, never one
 * another range is still reading through, never one opened with other settings; and what nobody borrows
 * is closed once it has been idle long enough. The connections are stubs and the clock is the test's, so
 * none of this waits for a cluster or for a minute to pass.
 */
public class FlussConnectionPoolTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    /** Far longer than a close handed to the closer takes to start. */
    private static final long PATIENCE_SECONDS = 60;

    private final AtomicLong now = new AtomicLong();
    /** One per connection opened, counted down when it is closed. Opened outside the pool's lock. */
    private final List<CountDownLatch> closes = Collections.synchronizedList(new ArrayList<>());
    private final FlussConnectionPool pool = new FlussConnectionPool(config -> open(), now::get);

    @Test
    public void rangeBorrowsTheConnectionTheRangeBeforeItGaveBack() {
        FlussConnectionPool.Lease first = pool.borrow(config("server-a:9123"));
        Assertions.assertTrue(first.opened(), "the first range found nothing idle");
        pool.giveBack(first);

        FlussConnectionPool.Lease second = pool.borrow(config("server-a:9123"));
        Assertions.assertSame(first.connection(), second.connection());
        Assertions.assertFalse(second.opened(), "the second range opened a connection of its own");
        Assertions.assertEquals(1, closes.size(), "connections opened");
    }

    /**
     * A connection serves one range at a time: its download threads are what a primary-key range copies
     * its kv snapshot with, and ranges reading at the same time must not queue for them.
     */
    @Test
    public void rangesReadingAtTheSameTimeEachHaveAConnectionOfTheirOwn() {
        FlussConnectionPool.Lease first = pool.borrow(config("server-a:9123"));
        FlussConnectionPool.Lease second = pool.borrow(config("server-a:9123"));
        Assertions.assertNotSame(first.connection(), second.connection());
        Assertions.assertTrue(second.opened());

        pool.giveBack(first);
        pool.giveBack(second);
        Assertions.assertEquals(2, pool.idleCount());
        // The one given back last is lent first, so that after a burst the ones it no longer needs are
        // the ones that stay idle long enough to be closed.
        Assertions.assertSame(second.connection(), pool.borrow(config("server-a:9123")).connection());
    }

    /** Servers, credentials and client options are part of what a connection is. */
    @Test
    public void rangesWithOtherSettingsNeverBorrowTheConnection() {
        FlussConnectionPool.Lease first = pool.borrow(config("server-a:9123"));
        pool.giveBack(first);

        Configuration otherServer = config("server-b:9123");
        Assertions.assertTrue(pool.borrow(otherServer).opened());
        Configuration otherOption = config("server-a:9123");
        otherOption.setString("client.scanner.log.read-preference", "REMOTE_FIRST");
        Assertions.assertTrue(pool.borrow(otherOption).opened());

        Assertions.assertSame(first.connection(), pool.borrow(config("server-a:9123")).connection());
    }

    /**
     * Ranges borrow and give back from every scanner thread at once. None may ever hold a connection
     * another one holds, and the pool opens no more than were held at once.
     */
    @Test
    public void connectionIsNeverLentToTwoRangesAtOnce() throws Exception {
        int threads = 8;
        int rounds = 2000;
        Set<Connection> held = ConcurrentHashMap.newKeySet();
        AtomicReference<String> clash = new AtomicReference<>();
        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService ranges = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> done = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                done.add(ranges.submit(() -> {
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        FlussConnectionPool.Lease lease = pool.borrow(config("server-a:9123"));
                        if (!held.add(lease.connection())) {
                            clash.set("a connection was lent to a second range while the first held it");
                        }
                        Thread.yield();
                        held.remove(lease.connection());
                        pool.giveBack(lease);
                    }
                    return null;
                }));
            }
            for (Future<?> range : done) {
                range.get(PATIENCE_SECONDS, TimeUnit.SECONDS);
            }
        } finally {
            ranges.shutdownNow();
        }
        Assertions.assertNull(clash.get(), clash.get());
        Assertions.assertTrue(closes.size() <= threads,
                closes.size() + " connections opened for " + threads + " ranges reading at once");
        Assertions.assertEquals(closes.size(), pool.idleCount());
    }

    /** A connection given up on is closed, not kept for the next range. */
    @Test
    public void discardedConnectionIsClosedAndNotLent() throws Exception {
        FlussConnectionPool.Lease lease = pool.borrow(config("server-a:9123"));
        pool.discard(lease);
        Assertions.assertTrue(closes.get(0).await(PATIENCE_SECONDS, TimeUnit.SECONDS), "never closed");
        Assertions.assertEquals(0, pool.idleCount());
        Assertions.assertTrue(pool.borrow(config("server-a:9123")).opened());
    }

    @Test
    public void connectionIdleForTheTimeoutIsClosed() throws Exception {
        FlussConnectionPool.Lease lease = pool.borrow(config("server-a:9123"));
        pool.giveBack(lease);

        now.addAndGet(FlussConnectionPool.IDLE_TIMEOUT_NANOS - 1);
        pool.closeIdleLongerThan(FlussConnectionPool.IDLE_TIMEOUT_NANOS);
        Assertions.assertEquals(1, pool.idleCount(), "closed before its timeout");
        Assertions.assertEquals(1, closes.get(0).getCount(), "closed before its timeout");

        now.incrementAndGet();
        pool.closeIdleLongerThan(FlussConnectionPool.IDLE_TIMEOUT_NANOS);
        Assertions.assertEquals(0, pool.idleCount());
        Assertions.assertTrue(closes.get(0).await(PATIENCE_SECONDS, TimeUnit.SECONDS), "never closed");
        Assertions.assertTrue(pool.borrow(config("server-a:9123")).opened(), "a closed connection was lent");
    }

    /** Idle means idle since the last range gave it back, not since it was opened. */
    @Test
    public void connectionInUseIsNotIdle() {
        FlussConnectionPool.Lease lease = pool.borrow(config("server-a:9123"));
        pool.giveBack(lease);
        now.addAndGet(50 * SECOND);
        pool.giveBack(pool.borrow(config("server-a:9123")));

        now.addAndGet(FlussConnectionPool.IDLE_TIMEOUT_NANOS - 10 * SECOND);
        pool.closeIdleLongerThan(FlussConnectionPool.IDLE_TIMEOUT_NANOS);
        Assertions.assertEquals(1, pool.idleCount(), "closed while it had been idle for less than the timeout");
        Assertions.assertEquals(1, closes.get(0).getCount());
    }

    private Connection open() {
        CountDownLatch latch = new CountDownLatch(1);
        closes.add(latch);
        return new StubConnection(latch::countDown);
    }

    private static Configuration config(String bootstrapServers) {
        Configuration config = new Configuration();
        config.setString("bootstrap.servers", bootstrapServers);
        return config;
    }
}
