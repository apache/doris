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

package org.apache.doris.connector.iceberg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Verifies the worker-pool split-brain guard {@link IcebergConnector#pinPoolThreadsToClassLoader}. WHY it
 * exists: iceberg fans its parallel data-manifest WRITE onto its own shared worker pool, and iceberg-aws builds
 * the S3 client lazily on whichever worker thread first writes a manifest — resolving
 * {@code ApacheHttpClientConfigurations} via {@code DynMethods} off that thread's context classloader. Pinning
 * only the engine commit thread is not enough; EVERY worker thread must carry the plugin loader or the write
 * ClassCasts the parent (fe-core) copy against the child-loaded one. This test pins that contract: after the
 * helper runs, every distinct thread in the pool reports the target loader as its TCCL.
 */
public class IcebergConnectorWorkerPoolPinTest {

    private static ClassLoader isolatedLoader() {
        return new URLClassLoader(new URL[0], IcebergConnectorWorkerPoolPinTest.class.getClassLoader());
    }

    @Test
    public void pinsEveryThreadOfThePool() throws Exception {
        int poolSize = 4;
        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        ClassLoader target = isolatedLoader();
        try {
            boolean allReached = IcebergConnector.pinPoolThreadsToClassLoader(pool, poolSize, target, 10);
            Assertions.assertTrue(allReached, "every thread of the fixed pool must be reached by a primer");

            // Sample EACH thread's TCCL: a second barrier forces all poolSize distinct threads to run, so the
            // observed set covers the whole pool — the manifest-write path may land on any of them.
            Set<ClassLoader> observed = ConcurrentHashMap.newKeySet();
            CountDownLatch sampled = new CountDownLatch(poolSize);
            CountDownLatch hold = new CountDownLatch(1);
            for (int i = 0; i < poolSize; i++) {
                pool.submit(() -> {
                    observed.add(Thread.currentThread().getContextClassLoader());
                    sampled.countDown();
                    hold.await();
                    return null;
                });
            }
            Assertions.assertTrue(sampled.await(10, TimeUnit.SECONDS), "all pool threads must run the sampler");
            hold.countDown();

            Assertions.assertEquals(Collections.singleton(target), observed,
                    "every worker thread must carry the pinned plugin loader as its TCCL (none left on 'app')");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void repinsAThreadAnEarlierUnpinnedUseAlreadyCreated() throws Exception {
        // A worker thread created/used before the pin keeps whatever TCCL it had (a pool never resets it between
        // tasks); the helper must SET it, not rely on creation-time inheritance. Prime the single thread with a
        // foreign loader first, then assert the helper overwrites it.
        ExecutorService pool = Executors.newFixedThreadPool(1);
        ClassLoader stale = isolatedLoader();
        ClassLoader target = isolatedLoader();
        try {
            pool.submit(() -> Thread.currentThread().setContextClassLoader(stale)).get();

            Assertions.assertTrue(IcebergConnector.pinPoolThreadsToClassLoader(pool, 1, target, 10));

            ClassLoader[] after = new ClassLoader[1];
            pool.submit(() -> {
                after[0] = Thread.currentThread().getContextClassLoader();
                return null;
            }).get();
            Assertions.assertSame(target, after[0], "the helper must repin an already-poisoned worker thread");
        } finally {
            pool.shutdownNow();
        }
    }
}
