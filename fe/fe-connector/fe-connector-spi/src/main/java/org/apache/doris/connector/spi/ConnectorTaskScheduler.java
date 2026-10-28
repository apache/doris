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

package org.apache.doris.connector.spi;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * A query's bounded task queue. The engine owns its lifetime; connectors must not close it.
 * Admission never waits. On a full queue the caller retains its task and retries after onSpace runs.
 * The callback may race the return from tryExecute, so callers must coalesce retries.
 * Accepted tasks run once or receive onFailure; failures include cancellation before execution.
 * Tasks own their authentication and context classloader. Callbacks must be short and nonblocking.
 */
public interface ConnectorTaskScheduler extends Executor {
    enum Stage {
        SPLIT,
        METADATA
    }

    boolean tryExecute(Runnable task, Consumer<Throwable> onFailure, Runnable onSpace);

    /** Query completion closes the group; synchronous permit waiters can observe the same lifetime. */
    default boolean isClosed() {
        return false;
    }

    /**
     * Requeue the currently running task as the caller's final action before returning. The scheduler
     * may retain the current worker instead of waking another one, but must preserve normal queue order,
     * admission, and failure semantics. Calls outside that task fall back to ordinary admission.
     */
    default boolean tryExecuteContinuation(Runnable task, Consumer<Throwable> onFailure, Runnable onSpace) {
        return tryExecute(task, onFailure, onSpace);
    }

    void removeSpaceListener(Runnable onSpace);

    /** Offline sessions have no shared worker pool, and execute directly. */
    ConnectorTaskScheduler DIRECT = new ConnectorTaskScheduler() {
        @Override
        public boolean tryExecute(Runnable task, Consumer<Throwable> onFailure, Runnable onSpace) {
            try {
                task.run();
            } catch (Throwable failure) {
                onFailure.accept(failure);
            }
            return true;
        }

        @Override
        public void removeSpaceListener(Runnable onSpace) {
        }

        @Override
        public void execute(Runnable task) {
            task.run();
        }
    };
}
