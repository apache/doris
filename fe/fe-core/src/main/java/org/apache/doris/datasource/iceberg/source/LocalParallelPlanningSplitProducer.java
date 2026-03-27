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

import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.spi.Split;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Local FE async planning producer that reuses current Iceberg planning flow.
 */
public class LocalParallelPlanningSplitProducer implements PlanningSplitProducer {
    private static final Logger LOG = LogManager.getLogger(LocalParallelPlanningSplitProducer.class);

    /**
     * Runtime context required by local split planning.
     */
    public interface PlanningContext {
        boolean isBatchMode();

        int numApproximateSplits();

        TableScan createTableScan() throws UserException;

        CloseableIterable<FileScanTask> planFileScanTask(TableScan scan);

        Split createSplit(FileScanTask task);

        void recordManifestCacheProfile();

        Optional<NotSupportedException> checkNotSupportedException(Exception e);

        ExecutionAuthenticator getExecutionAuthenticator();

        Executor getScheduleExecutor();
    }

    private final PlanningContext context;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<Void>> runningTask = new AtomicReference<>();

    public LocalParallelPlanningSplitProducer(PlanningContext context) {
        this.context = Preconditions.checkNotNull(context, "planningContext is null");
    }

    @Override
    public boolean isBatchMode() {
        return context.isBatchMode();
    }

    @Override
    public int numApproximateSplits() {
        return context.numApproximateSplits();
    }

    @Override
    public void start(int numBackends, SplitSink sink) throws UserException {
        stopped.set(false);
        final TableScan scan;
        try {
            scan = context.getExecutionAuthenticator().execute(() -> context.createTableScan());
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            AtomicReference<CloseableIterable<FileScanTask>> taskRef = new AtomicReference<>();
            try {
                context.getExecutionAuthenticator().execute(() -> {
                    CloseableIterable<FileScanTask> fileScanTasks = context.planFileScanTask(scan);
                    taskRef.set(fileScanTasks);
                    CloseableIterator<FileScanTask> iterator = fileScanTasks.iterator();
                    while (!stopped.get() && sink.needMore() && iterator.hasNext()) {
                        sink.addBatch(Lists.newArrayList(context.createSplit(iterator.next())));
                    }
                    return null;
                });
                sink.finish();
                context.recordManifestCacheProfile();
            } catch (Exception e) {
                if (stopped.get()) {
                    LOG.debug("Iceberg local split planning is stopped");
                    return;
                }
                Optional<NotSupportedException> opt = context.checkNotSupportedException(e);
                if (opt.isPresent()) {
                    sink.fail(new UserException(opt.get().getMessage(), opt.get()));
                } else {
                    sink.fail(new UserException(e.getMessage(), e));
                }
            } finally {
                closeTaskIterable(taskRef.get());
            }
        }, context.getScheduleExecutor());
        runningTask.set(future);
    }

    @Override
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture<Void> task = runningTask.get();
        if (task != null) {
            task.cancel(true);
        }
    }

    private void closeTaskIterable(CloseableIterable<FileScanTask> tasks) {
        if (tasks == null) {
            return;
        }
        try {
            tasks.close();
        } catch (IOException e) {
            LOG.warn("close file scan task iterable failed: {}", e.getMessage(), e);
        }
    }
}
