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

package org.apache.doris.qe;

import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.service.ExecuteEnv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The end of the graceful shutdown of this FE.
 *
 * <p>Once {@link #begin} runs, after the queries running at shutdown have finished, new statements on the client
 * connections and Arrow Flight SQL sessions of this FE are refused, so clients reconnect to another FE.
 *
 * <p>A master that hands its mastership over first fences its master-only work with {@link #fenceMasterWork}:
 * the work already running ends, and no new work starts.
 */
public final class ShutdownGate {
    private static final Logger LOG = LogManager.getLogger(ShutdownGate.class);

    public static final String REJECT_MESSAGE = "frontend is shutting down, reconnect";

    // How long the master waits for the master-only work already running (daemon cycles, backend report handling)
    // to end before it hands over. Work still running then makes it shut down as master, without a handover.
    public static final long FENCE_WAIT_MS = 3000L;
    // How long the master waits for the journal entries already submitted to become durable before it hands over.
    public static final long JOURNAL_FLUSH_WAIT_MS = 2000L;
    // The transferMaster timeout. A follower that is not current by then is not worth waiting for: the master
    // shuts down as master instead, and the followers elect a new one as after a crash.
    public static final int TRANSFER_TIMEOUT_SECOND = 10;
    // How long, at most, the old master stays up after the handover, serving nothing, until the other followers
    // are replicas of the new master. Its replication node answers their lookups for the new master meanwhile;
    // without it they need many seconds to find the new master, which cannot commit until a majority has.
    public static final long REJOIN_WAIT_MS = 5000L;
    // How long, at least, the old master stays up after the handover. The other FEs learn the new master from
    // the journal within about a second; until then their forwards to this FE fail at once instead of waiting
    // for a connection to an address that is gone.
    public static final long MIN_LINGER_MS = 2000L;
    // How far, in journal ids, a follower's replay may trail the master's for the follower to take mastership.
    public static final long MAX_TRANSFER_JOURNAL_LAG = 1000L;

    private static volatile boolean shuttingDown = false;
    // Set once the master stops its master-only work before it hands mastership over. From then on that work
    // must not start: what it changes in memory or tells the backends could be lost with the journal entries the
    // transfer blocks, while the new master never learns of it.
    private static volatile boolean masterWorkFenced = false;
    // The threads running a unit of master-only work, named in the log when a handover cannot wait them out.
    private static final Set<Thread> runningMasterWork = ConcurrentHashMap.newKeySet();

    private ShutdownGate() {
    }

    /** Starts refusing new statements and closes the idle MySQL connections of this FE. */
    public static void begin() {
        shuttingDown = true;
        try {
            LOG.info("refuse new statements during shutdown, closed {} idle MySQL connections",
                    closeIdleMysqlConnections());
        } catch (Throwable t) {
            LOG.warn("failed to close idle MySQL connections during shutdown", t);
        }
    }

    /**
     * Registers one unit of master-only work, such as a master daemon cycle or a backend report, before it runs.
     * Returns false once the master work is fenced; the caller then skips the work and must not call
     * {@link #exitMasterWork()}.
     */
    public static boolean enterMasterWork() {
        runningMasterWork.add(Thread.currentThread());
        if (masterWorkFenced) {
            runningMasterWork.remove(Thread.currentThread());
            return false;
        }
        return true;
    }

    public static void exitMasterWork() {
        runningMasterWork.remove(Thread.currentThread());
    }

    /** Whether master-only work that is not tracked, such as scheduled jobs and transaction commits, must not start. */
    public static boolean isMasterWorkFenced() {
        return masterWorkFenced;
    }

    /**
     * Stops new master-only work and waits up to timeoutMs for the work already running to end. Returns whether it
     * ended. The fence stays in place either way: this FE is shutting down.
     */
    public static boolean fenceMasterWork(long timeoutMs) throws InterruptedException {
        masterWorkFenced = true;
        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        while (!runningMasterWork.isEmpty()) {
            if (System.currentTimeMillis() >= deadlineMs) {
                List<String> names = new ArrayList<>();
                runningMasterWork.forEach(thread -> names.add(thread.getName()));
                LOG.warn("master work still runs after {} ms in threads {}", timeoutMs, names);
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }

    static void resetMasterWorkFence() {
        masterWorkFenced = false;
    }

    public static boolean rejectsNewStatements() {
        return shuttingDown;
    }

    public static boolean rejectsMysqlCommand(MysqlCommand command) {
        return rejectsMysqlCommand(command, shuttingDown);
    }

    /** Every command is refused during the shutdown except COM_QUIT, which still closes the connection cleanly. */
    static boolean rejectsMysqlCommand(MysqlCommand command, boolean shuttingDown) {
        return shuttingDown && command != MysqlCommand.COM_QUIT;
    }

    /**
     * A connection is closed at the start of the shutdown when it waits for its client's next command. The
     * dispatcher sets the command before it checks the gate, so a command that arrives concurrently is either
     * refused or seen here as running, never cut off.
     */
    static boolean closesIdleConnection(boolean isProxy, ConnectContext.ConnectType type, MysqlCommand command) {
        // An Arrow Flight SQL session is idle between its commands while its client still fetches results, so it
        // is never closed here: it gets UNAVAILABLE on its next statement instead.
        return !isProxy && type == ConnectContext.ConnectType.MYSQL && command == MysqlCommand.COM_SLEEP;
    }

    private static int closeIdleMysqlConnections() {
        List<ConnectContext> contexts = new ArrayList<>(
                ExecuteEnv.getInstance().getScheduler().getConnectPoolMgr().getConnectionMap().values());
        int closed = 0;
        for (ConnectContext ctx : contexts) {
            if (closesIdleConnection(ctx.isProxy(), ctx.getConnectType(), ctx.getCommand())) {
                ctx.kill(true);
                closed++;
            }
        }
        return closed;
    }
}
