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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.common.util.FairTaskExecutor;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;

import org.apache.iceberg.ManifestFile;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Bounded completion-order reads. Every admitted read owns its generation until its worker exits. */
final class NativeManifestPrefetcher implements Iterable<NativeManifestPrefetcher.Result>, Closeable {
    static final class Result {
        final ManifestFile manifest;
        ManifestCacheValue value;
        Throwable failure;
        IcebergScanNode.AsyncPlanningTask task;

        Result(ManifestFile manifest) {
            this.manifest = manifest;
        }
    }

    private final Iterator<ManifestFile> manifests;
    private final Executor executor;
    private final SplitAssignment assignment;
    private final boolean ownsExecutor;
    private final Callable<Closeable> retain;
    private final Function<ManifestFile, ManifestCacheValue> loader;
    private final BlockingQueue<Result> completed = new LinkedBlockingQueue<>();
    private final Set<IcebergScanNode.AsyncPlanningTask> pending = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private int inFlight;

    NativeManifestPrefetcher(Iterable<ManifestFile> manifests, Executor executor,
            SplitAssignment assignment, boolean ownsExecutor, int width,
            Callable<Closeable> retain, Function<ManifestFile, ManifestCacheValue> loader) {
        this.manifests = manifests.iterator();
        this.executor = executor;
        this.assignment = assignment;
        this.ownsExecutor = ownsExecutor;
        this.retain = retain;
        this.loader = loader;
        assignment.addCloseable(this);
        try {
            for (int i = 0; i < width && this.manifests.hasNext(); i++) {
                submitNext();
            }
        } catch (RuntimeException | Error e) {
            close();
            throw e;
        }
    }

    private void submitNext() {
        if (closed.get() || assignment.isStop()) {
            throw new CancellationException("Iceberg manifest prefetch was cancelled");
        }
        Result result = new Result(manifests.next());
        Closeable generation;
        try {
            generation = retain.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        result.task = new IcebergScanNode.AsyncPlanningTask(executor, assignment, generation, () -> {
            try {
                if (closed.get() || assignment.isStop()) {
                    throw new CancellationException("Iceberg manifest prefetch was cancelled");
                }
                result.value = loader.apply(result.manifest);
            } catch (Throwable failure) {
                result.failure = failure;
            }
        }, () -> {
            pending.remove(result.task);
            completed.add(result);
        });
        inFlight++;
        pending.add(result.task);
        assignment.addCloseable(result.task);
        result.task.submit();
    }

    @Override
    public Iterator<Result> iterator() {
        return new Iterator<Result>() {
            @Override
            public boolean hasNext() {
                return inFlight != 0;
            }

            @Override
            public Result next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Result result;
                try {
                    result = completed.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException("Iceberg manifest prefetch was interrupted");
                }
                if (closed.get() || assignment.isStop() || result.value == null) {
                    if (result.failure != null) {
                        throw new RuntimeException(result.failure);
                    }
                    throw new CancellationException("Iceberg manifest prefetch was cancelled");
                }
                inFlight--;
                if (manifests.hasNext()) {
                    submitNext();
                }
                return result;
            }
        };
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        assignment.removeCloseable(this);
        for (IcebergScanNode.AsyncPlanningTask task : pending) {
            task.close();
        }
        completed.add(new Result(null));
        if (ownsExecutor) {
            ((FairTaskExecutor.Group) executor).close();
        }
    }
}
