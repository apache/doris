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

package org.apache.doris.datasource;

import org.apache.doris.common.UserException;
import org.apache.doris.spi.Split;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SplitSink} implementation for synchronous split retrieval.
 *
 * <p>This sink buffers every produced split in memory and allows callers to block until the
 * producer finishes. It is used when a scan node wants to reuse the producer protocol but still
 * needs a materialized {@code List<Split>} result.
 */
public class CollectingSplitSink implements SplitSink {
    private final List<Split> splits = new ArrayList<>();
    private final CountDownLatch completion = new CountDownLatch(1);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private UserException exception;

    @Override
    public void addBatch(List<Split> splits) {
        synchronized (this.splits) {
            this.splits.addAll(splits);
        }
    }

    @Override
    public boolean needMore() {
        return !completed.get();
    }

    @Override
    public void finish() {
        complete(null);
    }

    @Override
    public void fail(UserException e) {
        complete(e);
    }

    /**
     * Blocks until the producer calls {@link #finish()} or {@link #fail(UserException)}.
     */
    public void awaitFinished() throws UserException {
        try {
            completion.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UserException("Interrupted while waiting for split planning to finish", e);
        }
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Returns a snapshot of all collected splits.
     */
    public List<Split> getSplits() {
        synchronized (splits) {
            return new ArrayList<>(splits);
        }
    }

    private void complete(UserException e) {
        if (completed.compareAndSet(false, true)) {
            exception = e;
            completion.countDown();
            return;
        }
        if (e != null && exception != null) {
            exception.addSuppressed(e);
        }
    }
}
