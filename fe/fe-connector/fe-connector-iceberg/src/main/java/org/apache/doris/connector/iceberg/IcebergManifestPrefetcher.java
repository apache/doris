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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Completion-ordered, single-consumer manifest prefetch. close() may run on the query-cancellation thread.
 * Only the immutable manifest-list iterator and cached-only probes run under the short registry lock;
 * remote loading, future waits and cancellation all run outside it. At most window results are retained.
 */
final class IcebergManifestPrefetcher implements CloseableIterator<IcebergManifestPrefetcher.LoadedManifest> {
    static final class LoadedManifest {
        final ManifestFile manifest;
        final ManifestCacheValue files;

        LoadedManifest(ManifestFile manifest, ManifestCacheValue files) {
            this.manifest = manifest;
            this.files = files;
        }
    }

    private static final class Pending {
        final ManifestFile manifest;
        final ManifestCacheValue cached;
        RunnableFuture<ManifestCacheValue> future;

        Pending(ManifestFile manifest, ManifestCacheValue cached, RunnableFuture<ManifestCacheValue> future) {
            this.manifest = manifest;
            this.cached = cached;
            this.future = future;
        }
    }

    private static final Pending CLOSED = new Pending(null, null, null);

    private final CloseableIterator<ManifestFile> manifests;
    private final int window;
    private final Function<ManifestFile, ManifestCacheValue> cached;
    private final Function<ManifestFile, ManifestCacheValue> loader;
    private final Function<ManifestFile, CompletionStage<ManifestCacheValue>> sharedLoader;
    private final ConnectorTaskScheduler scheduler;
    private final BooleanSupplier allowPrefetch;
    private final Deque<Pending> pending = new ArrayDeque<>();
    // Entries stay in pending until consumed, so completed results also count against the window.
    // Completion callbacks never acquire the registry lock. Reserve one slot for the close signal.
    private final BlockingQueue<Pending> ready;
    private volatile boolean closed;
    private boolean coldManifestConsumed;
    private final ConnectorContext context;
    private final IcebergManifestReadContext readContext;

    IcebergManifestPrefetcher(CloseableIterator<ManifestFile> manifests, int window,
            Function<ManifestFile, ManifestCacheValue> cached,
            Function<ManifestFile, ManifestCacheValue> loader, ConnectorContext context, Semaphore readPermits) {
        this(manifests, window, cached, loader, context, readPermits, () -> true);
    }

    IcebergManifestPrefetcher(CloseableIterator<ManifestFile> manifests, int window,
            Function<ManifestFile, ManifestCacheValue> cached,
            Function<ManifestFile, ManifestCacheValue> loader, ConnectorContext context, Semaphore readPermits,
            BooleanSupplier allowPrefetch) {
        this(manifests, window, cached, loader, context, readPermits, allowPrefetch, null);
    }

    IcebergManifestPrefetcher(CloseableIterator<ManifestFile> manifests, int window,
            Function<ManifestFile, ManifestCacheValue> cached,
            Function<ManifestFile, ManifestCacheValue> loader, ConnectorContext context, Semaphore readPermits,
            BooleanSupplier allowPrefetch, ConnectorTaskScheduler scheduler) {
        this(manifests, window, cached, loader, context, readPermits, allowPrefetch, scheduler,
                manifest -> CompletableFuture.completedFuture(loader.apply(manifest)));
    }

    IcebergManifestPrefetcher(CloseableIterator<ManifestFile> manifests, int window,
            Function<ManifestFile, ManifestCacheValue> cached,
            Function<ManifestFile, ManifestCacheValue> loader, ConnectorContext context, Semaphore readPermits,
            BooleanSupplier allowPrefetch, ConnectorTaskScheduler scheduler,
            Function<ManifestFile, CompletionStage<ManifestCacheValue>> sharedLoader) {
        this(manifests, window, cached, loader, context, readPermits, allowPrefetch, scheduler,
                sharedLoader, () -> () -> { });
    }

    IcebergManifestPrefetcher(CloseableIterator<ManifestFile> manifests, int window,
            Function<ManifestFile, ManifestCacheValue> cached,
            Function<ManifestFile, ManifestCacheValue> loader, ConnectorContext context, Semaphore readPermits,
            BooleanSupplier allowPrefetch, ConnectorTaskScheduler scheduler,
            Function<ManifestFile, CompletionStage<ManifestCacheValue>> sharedLoader,
            Supplier<AutoCloseable> retainLoadOwner) {
        this.context = context;
        this.readContext = new IcebergManifestReadContext(context, readPermits,
                () -> closed || (scheduler != null && scheduler.isClosed()), retainLoadOwner);
        this.sharedLoader = sharedLoader;
        this.scheduler = scheduler;
        this.manifests = manifests;
        this.window = window;
        this.ready = new ArrayBlockingQueue<>(window + 1);
        this.cached = cached;
        this.loader = loader;
        this.allowPrefetch = allowPrefetch;
    }

    @Override
    public boolean hasNext() {
        synchronized (pending) {
            return !closed && (!pending.isEmpty() || manifests.hasNext());
        }
    }

    @Override
    public LoadedManifest next() {
        Pending head = null;
        List<ScheduledLoad> submissions = null;
        try {
            synchronized (pending) {
                if (closed) {
                    throw new CancellationException("Iceberg manifest source is closed");
                }
                if (pending.isEmpty()) {
                    if (!manifests.hasNext()) {
                        throw new NoSuchElementException();
                    }
                    ManifestFile manifest = manifests.next();
                    ManifestCacheValue value = cached.apply(manifest);
                    if (value != null) {
                        // Warm cache: no futures, submissions, or speculative iterator traversal.
                        return new LoadedManifest(manifest, value);
                    }
                    if (window == 1 || !coldManifestConsumed || !allowPrefetch.getAsBoolean()) {
                        // A required serial read has no executor handoff. Expand only when useful work
                        // needs more metadata, without making progress depend on backend requests.
                        head = new Pending(manifest, null, null);
                    } else {
                        submissions = new ArrayList<>(window);
                        pending.addLast(submit(manifest, null, submissions));
                    }
                }
                if (head == null) {
                    // If a matching file was found, pause further speculative submissions until the
                    // backend asks for another batch. Already-started shared reads may still complete.
                    while (allowPrefetch.getAsBoolean() && pending.size() < window && manifests.hasNext()) {
                        ManifestFile manifest = manifests.next();
                        if (submissions == null) {
                            submissions = new ArrayList<>(window);
                        }
                        pending.addLast(submit(manifest, cached.apply(manifest), submissions));
                    }
                }
            }
            // The offline scheduler may execute inline. Even that path must never do IO under
            // the pending lock: close() must be able to cancel and wake the consumer at any time.
            if (submissions != null) {
                submissions.forEach(ScheduledLoad::admit);
            }
            if (head == null) {
                head = awaitReady();
            }
            ManifestCacheValue value;
            if (head.future == null && head.cached == null) {
                value = load(head.manifest);
            } else if (head.cached != null) {
                value = head.cached;
            } else {
                // Readiness is published only after the load completes. get() supplies the
                // completion barrier and propagates failures, including Errors.
                value = head.future.get();
            }
            synchronized (pending) {
                if (closed) {
                    throw new CancellationException("Iceberg manifest source is closed");
                }
                if (head.future != null || head.cached != null) {
                    pending.remove(head);
                }
                coldManifestConsumed = true;
            }
            return new LoadedManifest(head.manifest, value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw fail(new RuntimeException("Interrupted while loading Iceberg manifest", e));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw fail(new RuntimeException("Failed to load Iceberg manifest " + head.manifest.path(), cause));
        } catch (RuntimeException e) {
            throw fail(e);
        }
    }

    private Pending awaitReady() throws InterruptedException {
        Pending help;
        synchronized (pending) {
            if (closed) {
                throw new CancellationException("Iceberg manifest source is closed");
            }
            Pending completed = ready.poll();
            if (completed != null) {
                return completed;
            }
            help = pending.getFirst();
        }
        // If every worker is occupied by a parent, run our own queued work to guarantee progress.
        // If a worker already owns it, run() is a no-op: wait for ANY completion, not this future.
        if (scheduler == null) {
            help.future.run();
        }
        Pending completed = ready.take();
        if (closed) {
            throw new CancellationException("Iceberg manifest source is closed");
        }
        return completed;
    }

    private Pending submit(ManifestFile manifest, ManifestCacheValue value, List<ScheduledLoad> submissions) {
        Pending task = new Pending(manifest, value, null);
        if (value != null) {
            ready.add(task);
            return task;
        }
        if (scheduler != null) {
            ScheduledLoad load = new ScheduledLoad(task);
            task.future = load;
            submissions.add(load);
            return task;
        }
        Callable<ManifestCacheValue> load = () -> {
            try {
                return load(manifest);
            } finally {
                if (!closed) {
                    // A close racing this publication can leave at most window completions plus
                    // its wakeup signal. add() must never block a metadata worker on consumption.
                    ready.add(task);
                }
            }
        };
        task.future = context == null ? new FutureTask<>(load) : context.submitMetadataTask(load);
        return task;
    }

    /** A rejected admission keeps only this window entry; it never blocks a scheduler worker. */
    private final class ScheduledLoad extends CompletableFuture<ManifestCacheValue>
            implements RunnableFuture<ManifestCacheValue> {
        private final Pending pendingLoad;
        private final Runnable onSpace = this::admit;
        private final AtomicBoolean started = new AtomicBoolean();
        // A shared load must not retain a cancelled scan until remote IO eventually completes.
        private final AtomicReference<ScheduledLoad> completionTarget = new AtomicReference<>(this);

        private ScheduledLoad(Pending pendingLoad) {
            this.pendingLoad = pendingLoad;
            whenComplete((value, failure) -> {
                completionTarget.set(null);
                scheduler.removeSpaceListener(onSpace);
                if (!closed) {
                    ready.add(pendingLoad);
                }
            });
        }

        private void admit() {
            if (isDone()) {
                return;
            }
            try {
                scheduler.tryExecute(this, this::completeExceptionally, onSpace);
            } catch (RuntimeException e) {
                completeExceptionally(e);
            }
            // Cancellation can race callback registration in tryExecute.
            if (isDone()) {
                scheduler.removeSpaceListener(onSpace);
            }
        }

        @Override
        public void run() {
            if (isDone() || !started.compareAndSet(false, true)) {
                return;
            }
            try {
                CompletionStage<ManifestCacheValue> shared = executeLoad(pendingLoad.manifest, sharedLoader);
                AtomicReference<ScheduledLoad> target = completionTarget;
                shared.whenComplete((value, failure) -> completeSharedLoad(target, value, failure));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                completeExceptionally(e);
            } catch (Throwable failure) {
                completeExceptionally(failure);
            }
        }
    }

    private static void completeSharedLoad(AtomicReference<ScheduledLoad> target,
            ManifestCacheValue value, Throwable failure) {
        ScheduledLoad load = target.getAndSet(null);
        if (load != null) {
            if (failure == null) {
                load.complete(value);
            } else {
                load.completeExceptionally(failure);
            }
        }
    }

    private ManifestCacheValue load(ManifestFile manifest) throws InterruptedException {
        return executeLoad(manifest, loader);
    }

    private <T> T executeLoad(ManifestFile manifest, Function<ManifestFile, T> action) throws InterruptedException {
        return readContext.read(manifest, action);
    }

    private RuntimeException fail(RuntimeException failure) {
        try {
            close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
        return failure;
    }

    @Override
    public void close() throws IOException {
        List<Pending> unused;
        synchronized (pending) {
            if (closed) {
                return;
            }
            closed = true;
            unused = new ArrayList<>(pending);
            pending.clear();
            ready.clear();
            ready.add(CLOSED);
        }
        for (Pending task : unused) {
            if (task.future != null) {
                // MetaCache single-flights this immutable load across queries. Interrupting its elected
                // loader would also fail unrelated waiters. Cancel queued work, but let an ongoing shared
                // read finish; closed is checked before any subsequent manifest read starts.
                task.future.cancel(false);
            }
        }
        manifests.close();
    }
}
