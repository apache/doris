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

package org.apache.doris.catalog.authorizer.ranger.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.ranger.plugin.service.RangerAuthContextListener;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * What talking to one Ranger service of type {@code hive} costs, in one object: the plugin polling it, the
 * audit handler buffering what it decided, and the task draining that buffer.
 *
 * <p>Bundled because the three are one lifetime - the handler is configured out of the plugin and the task
 * exists to drain the handler - and because that lifetime is not a binding's. A catalog bound to this source
 * is detached and re-attached by a plain {@code ALTER CATALOG}, and a stack torn down and rebuilt between
 * those two has no policies until its next download completes. So {@link RangerHiveAccessControllerFactory}
 * keeps one stack per Ranger service and hands it to every controller reading that service, while a controller
 * built directly - a test, or an embedding that owns its own - starts and stops one of its own.
 */
class RangerHiveAuditStack {

    /**
     * Drains the audit buffers. One thread per load of this class - which is one per plugin directory, since
     * the loader is child-first - and every stack schedules its own task on the timer of the directory it was
     * loaded from.
     *
     * <p>Built here rather than through the engine's thread pool manager, which a plugin outside fe-core
     * cannot reach. What that costs is the pool's entry in the FE thread-pool metrics
     * ({@code doris_fe_thread_pool} with name {@code ranger-hive-audit-log-flusher-timer}), which no plugin
     * loaded from its own directory can register into; for a fixed single-thread timer those gauges never
     * moved.
     */
    private static final ScheduledThreadPoolExecutor LOG_FLUSH_TIMER = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("ranger-hive-audit-log-flusher-timer-%d")
                    .build());

    private final RangerHivePlugin plugin;
    private final RangerHiveAuditHandler auditHandler;
    private final ScheduledFuture<?> flushFuture;

    private RangerHiveAuditStack(RangerHivePlugin plugin, RangerHiveAuditHandler auditHandler,
            ScheduledFuture<?> flushFuture) {
        this.plugin = plugin;
        this.auditHandler = auditHandler;
        this.flushFuture = flushFuture;
    }

    /** Starts polling {@code serviceName} and draining what the policies it downloads decide. */
    static RangerHiveAuditStack startFor(String serviceName, RangerAuthContextListener authContextListener) {
        RangerHivePlugin plugin = new RangerHivePlugin(serviceName, authContextListener);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler(plugin.getConfig());
        ScheduledFuture<?> flushFuture = LOG_FLUSH_TIMER.scheduleAtFixedRate(
                new RangerHiveAuditLogFlusher(auditHandler), 10, 20L, TimeUnit.SECONDS);
        return new RangerHiveAuditStack(plugin, auditHandler, flushFuture);
    }

    static RangerHiveAuditStack startFor(String serviceName) {
        return startFor(serviceName, null);
    }

    RangerHivePlugin getPlugin() {
        return plugin;
    }

    RangerHiveAuditHandler getAuditHandler() {
        return auditHandler;
    }

    ScheduledFuture<?> getFlushFuture() {
        return flushFuture;
    }
}
