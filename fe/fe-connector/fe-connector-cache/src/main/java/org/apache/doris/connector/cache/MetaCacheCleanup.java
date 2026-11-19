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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Defers physical removal and resource release until an enclosing guarded cache mutation has finished. */
public final class MetaCacheCleanup {
    private static final ThreadLocal<Deque<Runnable>> PENDING = new ThreadLocal<>();
    private static final ThreadLocal<BiConsumer<String, Runnable>> LOCAL_RESOURCE_RECLAIMER = new ThreadLocal<>();
    // Standalone connector users retain synchronous cleanup unless their host installs a reclaimer.
    private static volatile BiConsumer<String, Runnable> resourceReclaimer = (description, cleanup) -> cleanup.run();

    private MetaCacheCleanup() {
    }

    /**
     * Logical invalidation still completes inside the action. Detached registrations and removal callbacks
     * are cleaned synchronously after the outermost action returns, including when that action fails.
     */
    public static void runDeferred(Runnable action) {
        if (PENDING.get() != null) {
            action.run();
            return;
        }
        Deque<Runnable> pending = new ArrayDeque<>();
        PENDING.set(pending);
        Throwable failure = null;
        try {
            action.run();
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        try {
            Runnable cleanup;
            while ((cleanup = pending.pollFirst()) != null) {
                try {
                    cleanup.run();
                } catch (RuntimeException | Error e) {
                    if (failure == null) {
                        failure = e;
                    } else if (failure != e) {
                        failure.addSuppressed(e);
                    }
                }
            }
        } finally {
            PENDING.remove();
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure != null) {
            throw (Error) failure;
        }
    }

    /** Installs the host's non-blocking, ownership-retaining resource reclaimer at FE startup. */
    public static void configureResourceReclaimer(BiConsumer<String, Runnable> reclaimer) {
        resourceReclaimer = Objects.requireNonNull(reclaimer, "reclaimer");
    }

    /**
     * Releases only a captured resource whose last user has finished. Logical invalidation and callbacks that
     * modify shared metadata must still run synchronously through execute/runDeferred.
     */
    public static void reclaimResource(String description, Runnable cleanup) {
        BiConsumer<String, Runnable> local = LOCAL_RESOURCE_RECLAIMER.get();
        (local == null ? resourceReclaimer : local).accept(description, Objects.requireNonNull(cleanup, "cleanup"));
    }

    /** Groups dependent resources during retirement, without deferring any logical invalidation callback. */
    public static void runWithResourceReclaimer(BiConsumer<String, Runnable> reclaimer, Runnable action) {
        BiConsumer<String, Runnable> previous = LOCAL_RESOURCE_RECLAIMER.get();
        LOCAL_RESOURCE_RECLAIMER.set(Objects.requireNonNull(reclaimer, "reclaimer"));
        try {
            action.run();
        } finally {
            if (previous == null) {
                LOCAL_RESOURCE_RECLAIMER.remove();
            } else {
                LOCAL_RESOURCE_RECLAIMER.set(previous);
            }
        }
    }

    static void execute(Runnable cleanup) {
        Deque<Runnable> pending = PENDING.get();
        if (pending == null) {
            cleanup.run();
        } else {
            pending.addLast(cleanup);
        }
    }
}
