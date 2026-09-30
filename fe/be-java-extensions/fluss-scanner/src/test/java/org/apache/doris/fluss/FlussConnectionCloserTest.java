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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.MultiTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * What the closer promises a scanner: it does not wait for its connection to close, and connections do
 * not pile up behind it. Every step here waits on a latch rather than on the clock.
 */
public class FlussConnectionCloserTest {

    /** Far longer than any step takes; reached only when a close never starts. */
    private static final long PATIENCE_SECONDS = 60;

    @Test
    public void theCallerDoesNotWaitForItsConnectionToClose() throws Exception {
        Thread caller = Thread.currentThread();
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch mayFinish = new CountDownLatch(1);
        AtomicReference<Thread> closedBy = new AtomicReference<>();
        try {
            // The connection's close() stays blocked until the end of this test, so close() below
            // can only return if something else is running it.
            FlussConnectionCloser.close(new StubConnection(() -> {
                closedBy.set(Thread.currentThread());
                closing.countDown();
                if (Thread.currentThread() != caller) {
                    mayFinish.await();
                }
            }));
            Assertions.assertTrue(closing.await(PATIENCE_SECONDS, TimeUnit.SECONDS),
                    "the connection was never closed");
            Assertions.assertNotSame(caller, closedBy.get(),
                    "the connection was closed on the thread that handed it over");
        } finally {
            mayFinish.countDown();
        }
    }

    @Test
    public void onceTooManyAreClosingTheCallerClosesItsOwn() throws Exception {
        Thread caller = Thread.currentThread();
        CountDownLatch mayFinish = new CountDownLatch(1);
        try {
            // Hold one closer thread per connection until the closer has none left. Connections of
            // other tests may be closing as well, so that can happen before MAX_CLOSING of these are
            // in - but never after.
            boolean closedByCaller = false;
            for (int i = 0; i <= FlussConnectionCloser.MAX_CLOSING && !closedByCaller; i++) {
                CountDownLatch closing = new CountDownLatch(1);
                AtomicReference<Thread> closedBy = new AtomicReference<>();
                FlussConnectionCloser.close(new StubConnection(() -> {
                    closedBy.set(Thread.currentThread());
                    closing.countDown();
                    if (Thread.currentThread() != caller) {
                        mayFinish.await();
                    }
                }));
                Assertions.assertTrue(closing.await(PATIENCE_SECONDS, TimeUnit.SECONDS),
                        "connection " + i + " was never closed");
                closedByCaller = closedBy.get() == caller;
            }
            Assertions.assertTrue(closedByCaller,
                    "more than " + FlussConnectionCloser.MAX_CLOSING + " connections were closing at once");

            // A connection that fails to close must not fail the scan that is done with it, whichever
            // thread ends up closing it.
            Assertions.assertDoesNotThrow(() -> FlussConnectionCloser.close(new StubConnection(() -> {
                throw new IllegalStateException("this connection refuses to close");
            })));
        } finally {
            mayFinish.countDown();
        }
    }

    private interface CloseAction {
        void run() throws Exception;
    }

    /** A connection nobody uses for anything but closing it. */
    private static final class StubConnection implements Connection {
        private final CloseAction onClose;

        StubConnection(CloseAction onClose) {
            this.onClose = onClose;
        }

        @Override
        public Configuration getConfiguration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Admin getAdmin() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table getTable(TablePath tablePath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MultiTable getMultiTable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws Exception {
            onClose.run();
        }
    }
}
