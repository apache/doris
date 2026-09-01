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

package org.apache.doris.connector.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class StripedPhaseGateTest {
    private static final long TIMEOUT_SECONDS = 10L;

    @Test
    public void readersCanOverlap() throws Exception {
        StripedPhaseGate gate = new StripedPhaseGate();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch readersEntered = new CountDownLatch(2);
        CountDownLatch releaseReaders = new CountDownLatch(1);
        try {
            Future<Boolean> first = executor.submit(() -> gate.readBoolean(() -> {
                readersEntered.countDown();
                await(releaseReaders);
                return true;
            }));
            Future<Boolean> second = executor.submit(() -> gate.readBoolean(() -> {
                readersEntered.countDown();
                await(releaseReaders);
                return true;
            }));

            await(readersEntered);
            releaseReaders.countDown();
            Assertions.assertTrue(first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assertions.assertTrue(second.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            releaseReaders.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void writerWaitsForCurrentReadersAndBlocksNewReaders() throws Exception {
        StripedPhaseGate gate = new StripedPhaseGate();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch firstReaderEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstReader = new CountDownLatch(1);
        CountDownLatch writerEntered = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        try {
            Future<?> firstReader = executor.submit(() -> gate.read(() -> {
                firstReaderEntered.countDown();
                await(releaseFirstReader);
                return null;
            }));
            await(firstReaderEntered);
            Future<?> writer = executor.submit(() -> gate.write(() -> {
                writerEntered.countDown();
                await(releaseWriter);
            }));

            Assertions.assertThrows(
                    TimeoutException.class, () -> writer.get(100, TimeUnit.MILLISECONDS));
            releaseFirstReader.countDown();
            await(writerEntered);

            Future<Boolean> secondReader = executor.submit(() -> gate.readBoolean(() -> true));
            Assertions.assertThrows(
                    TimeoutException.class, () -> secondReader.get(100, TimeUnit.MILLISECONDS));
            releaseWriter.countDown();

            firstReader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            writer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assertions.assertTrue(secondReader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            releaseFirstReader.countDown();
            releaseWriter.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void writerCompletesUnderContinuousReaders() throws Exception {
        StripedPhaseGate gate = new StripedPhaseGate();
        ExecutorService executor = Executors.newFixedThreadPool(9);
        AtomicBoolean run = new AtomicBoolean(true);
        CountDownLatch readersStarted = new CountDownLatch(8);
        List<Future<?>> readers = new ArrayList<>();
        try {
            for (int index = 0; index < 8; index++) {
                readers.add(executor.submit(() -> {
                    readersStarted.countDown();
                    while (run.get()) {
                        gate.readBoolean(() -> true);
                    }
                }));
            }
            await(readersStarted);

            Future<?> writer = executor.submit(() -> gate.write(() -> {
            }));
            writer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            run.set(false);
            for (Future<?> reader : readers) {
                reader.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            executor.shutdownNow();
        }
    }

    @Test
    public void exceptionReleasesReaderAndWriterPhases() {
        StripedPhaseGate gate = new StripedPhaseGate();

        Assertions.assertThrows(IllegalStateException.class, () -> gate.read(() -> {
            throw new IllegalStateException("read");
        }));
        gate.write(() -> {
        });

        Assertions.assertThrows(IllegalStateException.class, () -> gate.write(() -> {
            throw new IllegalStateException("write");
        }));
        Assertions.assertTrue(gate.readBoolean(() -> true));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for test latch", e);
        }
    }
}
