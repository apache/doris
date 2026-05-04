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

package org.apache.doris.connector.jdbc.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the ClassLoader reference counting mechanism in
 * {@link JdbcConnectorClient}.
 */
class JdbcConnectorClientClassLoaderTest {

    @Test
    void testRefCountedClassLoaderStartsAtOne() {
        JdbcConnectorClient.RefCountedClassLoader entry =
                new JdbcConnectorClient.RefCountedClassLoader(getClass().getClassLoader());
        Assertions.assertEquals(1, entry.refCount.get(),
                "Initial ref count should be 1");
    }

    @Test
    void testRefCountedClassLoaderIncrementDecrement() {
        JdbcConnectorClient.RefCountedClassLoader entry =
                new JdbcConnectorClient.RefCountedClassLoader(getClass().getClassLoader());
        entry.refCount.incrementAndGet();
        Assertions.assertEquals(2, entry.refCount.get(),
                "Ref count should be 2 after increment");
        entry.refCount.decrementAndGet();
        Assertions.assertEquals(1, entry.refCount.get(),
                "Ref count should be 1 after decrement");
        entry.refCount.decrementAndGet();
        Assertions.assertEquals(0, entry.refCount.get(),
                "Ref count should be 0 after second decrement");
    }

    @Test
    void testRefCountedClassLoaderStoresLoader() {
        ClassLoader loader = getClass().getClassLoader();
        JdbcConnectorClient.RefCountedClassLoader entry =
                new JdbcConnectorClient.RefCountedClassLoader(loader);
        Assertions.assertSame(loader, entry.loader,
                "Loader reference must be the one passed to constructor");
    }

    @Test
    void testConcurrentRefCountOperations() throws InterruptedException {
        JdbcConnectorClient.RefCountedClassLoader entry =
                new JdbcConnectorClient.RefCountedClassLoader(getClass().getClassLoader());
        AtomicInteger errors = new AtomicInteger(0);

        int threadCount = 20;
        Thread[] threads = new Thread[threadCount];
        // Each thread increments, then decrements — net effect is zero
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    entry.refCount.incrementAndGet();
                    Thread.yield();
                    entry.refCount.decrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        Assertions.assertEquals(0, errors.get(), "No thread errors expected");
        Assertions.assertEquals(1, entry.refCount.get(),
                "Ref count should return to 1 after concurrent inc/dec");
    }
}
