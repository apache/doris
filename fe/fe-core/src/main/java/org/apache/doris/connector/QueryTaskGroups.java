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

package org.apache.doris.connector;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.FairTaskExecutor;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;

import java.io.Closeable;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import java.util.function.Function;

/** Captured by every session of one statement before off-thread planning can begin. */
final class QueryTaskGroups implements Closeable {
    static final String SCOPE_KEY = "engine:query-task-groups";
    private final Map<ConnectorTaskScheduler.Stage, Adapter> groups =
            new EnumMap<>(ConnectorTaskScheduler.Stage.class);
    private final Function<ConnectorTaskScheduler.Stage, FairTaskExecutor> executors;
    private boolean closed;

    QueryTaskGroups() {
        this(stage -> Env.getCurrentEnv().getExtMetaCacheMgr().getQueryTaskExecutor(stage));
    }

    QueryTaskGroups(Function<ConnectorTaskScheduler.Stage, FairTaskExecutor> executors) {
        this.executors = executors;
    }

    synchronized ConnectorTaskScheduler get(ConnectorTaskScheduler.Stage stage) {
        if (closed) {
            // This captured query has ended; callers must not treat cancellation as a retryable rejection.
            throw new CancellationException("Query task groups are closed");
        }
        return groups.computeIfAbsent(stage, key -> new Adapter(
                executors.apply(key).newGroup(Config.external_scan_task_queue_capacity)));
    }

    @Override
    public void close() {
        Adapter[] snapshot;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            snapshot = groups.values().toArray(new Adapter[0]);
        }
        // Never invoke task completion (which may re-enter query cleanup) under the registry lock.
        for (Adapter group : snapshot) {
            group.delegate.close();
        }
    }

    private static final class Adapter implements ConnectorTaskScheduler {
        private final FairTaskExecutor.Group delegate;

        private Adapter(FairTaskExecutor.Group delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean tryExecute(Runnable task, Consumer<Throwable> onFailure, Runnable onSpace) {
            return delegate.tryExecute(task, onFailure, onSpace);
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public boolean tryExecuteContinuation(Runnable task, Consumer<Throwable> onFailure, Runnable onSpace) {
            return delegate.tryExecuteContinuation(task, onFailure, onSpace);
        }

        @Override
        public void removeSpaceListener(Runnable onSpace) {
            delegate.removeSpaceListener(onSpace);
        }

        @Override
        public void execute(Runnable task) {
            delegate.execute(task);
        }
    }
}
