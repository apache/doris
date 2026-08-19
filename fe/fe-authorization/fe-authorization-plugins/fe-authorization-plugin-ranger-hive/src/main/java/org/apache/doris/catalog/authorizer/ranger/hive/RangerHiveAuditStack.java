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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
 * is detached and re-attached by a plain {@code ALTER CATALOG}, and tearing a stack down between those two
 * costs a {@code cleanup()} on the DDL thread and two synchronous REST calls to the Ranger admin on the way
 * back up. So {@link RangerHiveAccessControllerFactory} keeps one stack per Ranger service, hands it to
 * every controller reading that service, and stops it only once nothing has read it for a while - while a
 * controller built directly, a test or an embedding that owns its own, starts and stops one of its own.
 */
class RangerHiveAuditStack {
    private static final Logger LOG = LogManager.getLogger(RangerHiveAuditStack.class);

    /**
     * Drains the audit buffers, and stops the stacks nothing reads any more. One pool per load of this class
     * - which is one per plugin directory, since the loader is child-first - and every stack schedules its
     * own task on the timer of the directory it was loaded from.
     *
     * <p>Two threads rather than one: {@link #stop()} joins the Ranger policy refresher without a timeout, so
     * against an unreachable admin a stop runs for the whole REST timeout, and on a single-threaded pool that
     * is the whole plugin directory's auditing not being written out for that long.
     *
     * <p>Built here rather than through the engine's thread pool manager, which a plugin outside fe-core
     * cannot reach. What that costs is the pool's entry in the FE thread-pool metrics
     * ({@code doris_fe_thread_pool} with name {@code ranger-hive-audit-log-flusher-timer}), which no plugin
     * loaded from its own directory can register into; for a fixed-size timer those gauges never moved.
     */
    private static final ScheduledThreadPoolExecutor LOG_FLUSH_TIMER = new ScheduledThreadPoolExecutor(2,
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
        try {
            RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler(plugin.getConfig());
            ScheduledFuture<?> flushFuture = LOG_FLUSH_TIMER.scheduleAtFixedRate(
                    new RangerHiveAuditLogFlusher(auditHandler), 10, 20L, TimeUnit.SECONDS);
            return new RangerHiveAuditStack(plugin, auditHandler, flushFuture);
        } catch (RuntimeException | Error e) {
            // The plugin is already polling when its constructor returns - a policy refresher thread and a
            // policy download timer - and a stack that is not returned is one nothing holds a reference to.
            // The handler reads ranger.plugin.hive.* through Hadoop's Configuration.getInt, so a malformed
            // integer property leaves one of these behind per attempt rather than per service name, and
            // nothing can ever stop it.
            try {
                plugin.cleanup();
            } catch (Throwable suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    static RangerHiveAuditStack startFor(String serviceName) {
        return startFor(serviceName, null);
    }

    /**
     * Stops polling the Ranger service, after writing out what this stack decided and has not flushed yet.
     *
     * <p>Never with a lock held that anything else waits on: {@code cleanup()} interrupts the policy
     * refresher and joins it without a timeout, so against an unreachable Ranger admin it takes the whole
     * REST timeout to return.
     */
    void stop() {
        flushFuture.cancel(false);
        // flushAudit atomically drains the handler. This preserves events produced before the stop without
        // racing the periodic flusher or re-emitting events it has already sent.
        try {
            auditHandler.flushAudit();
        } catch (Throwable e) {
            LOG.warn("Failed to flush Ranger Hive audit events while stopping the audit stack", e);
        }
        try {
            plugin.cleanup();
        } catch (Throwable e) {
            LOG.warn("Failed to clean up Ranger Hive plugin", e);
        }
    }

    /**
     * Runs {@code task} once, after {@code delay}, on the timer this class already owns.
     *
     * <p>Here rather than on a timer of the factory's because this is the one thread a stack's lifetime is
     * allowed to cost, and because stopping a stack must not happen on the thread that let go of it - see
     * {@link #stop()}.
     */
    static ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        return LOG_FLUSH_TIMER.schedule(task, delay, unit);
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
