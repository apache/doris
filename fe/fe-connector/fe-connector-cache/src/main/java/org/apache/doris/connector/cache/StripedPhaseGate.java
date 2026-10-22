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

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Writer-preferred phase gate for short cache-publication critical sections.
 *
 * <p>Read phases are reentrant so one guarded publication can update a second cache in the same catalog.
 * Read-to-write upgrade is forbidden. In particular,
 * synchronous removal callbacks must run after the read phase. Reader actions must also avoid remote I/O or other
 * unbounded work because a waiting writer and newly arriving readers spin until the current reader phase drains.
 */
final class StripedPhaseGate {
    // Sixteen counters spread across distinct 64-byte cache lines avoid one shared reader counter under FE workers.
    private static final int READER_STRIPE_COUNT = 16;
    private static final int READER_STRIDE = 16;

    private final Object writerLock = new Object();
    private final AtomicIntegerArray activeReaders =
            new AtomicIntegerArray(READER_STRIPE_COUNT * READER_STRIDE);
    private volatile boolean writerActive;
    private final ThreadLocal<Integer> readDepth = new ThreadLocal<>();

    boolean readBoolean(BooleanSupplier action) {
        int readerIndex = enterRead();
        try {
            return action.getAsBoolean();
        } finally {
            exitRead(readerIndex);
        }
    }

    <T> T read(Supplier<T> action) {
        int readerIndex = enterRead();
        try {
            return action.get();
        } finally {
            exitRead(readerIndex);
        }
    }

    void write(Runnable action) {
        write(() -> {
            action.run();
            return null;
        });
    }

    <T> T write(Supplier<T> action) {
        if (readDepth.get() != null) {
            throw new IllegalStateException("A cache publication action cannot invalidate its own scope");
        }
        synchronized (writerLock) {
            writerActive = true;
            try {
                awaitReaders();
                return action.get();
            } finally {
                writerActive = false;
            }
        }
    }

    private int enterRead() {
        Integer depth = readDepth.get();
        if (depth != null) {
            readDepth.set(depth + 1);
            return -1;
        }
        int readerIndex = readerIndex();
        while (true) {
            while (writerActive) {
                Thread.onSpinWait();
            }
            activeReaders.incrementAndGet(readerIndex);
            if (!writerActive) {
                readDepth.set(1);
                return readerIndex;
            }
            activeReaders.decrementAndGet(readerIndex);
        }
    }

    private void exitRead(int readerIndex) {
        if (readerIndex < 0) {
            readDepth.set(readDepth.get() - 1);
        } else {
            readDepth.remove();
            activeReaders.decrementAndGet(readerIndex);
        }
    }

    private void awaitReaders() {
        while (hasReaders()) {
            Thread.onSpinWait();
        }
    }

    private boolean hasReaders() {
        for (int stripe = 0; stripe < READER_STRIPE_COUNT; stripe++) {
            if (activeReaders.get(stripe * READER_STRIDE) != 0) {
                return true;
            }
        }
        return false;
    }

    private int readerIndex() {
        return ((int) Thread.currentThread().getId() & (READER_STRIPE_COUNT - 1))
                * READER_STRIDE;
    }
}
