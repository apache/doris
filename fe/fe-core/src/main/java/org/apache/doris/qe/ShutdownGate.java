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

import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.thrift.TMasterOpResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks the end of the graceful shutdown of this FE and decides which work it still accepts.
 * Once {@link #begin} runs, after the query drain, new statements from MySQL connections and Arrow Flight SQL
 * sessions are refused, so clients reconnect to another FE. A master that is about to hand its mastership over
 * also refuses the statements other FEs forward to it; those FEs hold them and send them to the new master.
 */
public final class ShutdownGate {
    private static final Logger LOG = LogManager.getLogger(ShutdownGate.class);

    public static final String REJECT_MESSAGE = "frontend is shutting down, reconnect";
    public static final String FORWARD_REJECT_MESSAGE = "master frontend is shutting down, retry on the next master";

    private static volatile boolean shuttingDown = false;
    private static volatile boolean refusingForwards = false;
    private static final AtomicInteger inFlightForwards = new AtomicInteger();

    private ShutdownGate() {
    }

    /**
     * Starts refusing new statements and closes the idle MySQL connections of this FE. With refuseForwards, it
     * also refuses statements forwarded by other FEs.
     */
    public static void begin(boolean refuseForwards) {
        refusingForwards = refuseForwards;
        shuttingDown = true;
        if (!Config.enable_reject_new_statements_on_shutdown) {
            return;
        }
        try {
            LOG.info("refuse new statements during shutdown, closed {} idle MySQL connections",
                    closeIdleMysqlConnections());
        } catch (Throwable t) {
            LOG.warn("failed to close idle MySQL connections during shutdown", t);
        }
    }

    /**
     * Registers a forwarded statement before it runs. Returns false when forwards are refused; the caller then
     * answers with {@link #refusedForwardResult()} and must not call {@link #exitForward()}.
     */
    public static boolean enterForward() {
        inFlightForwards.incrementAndGet();
        if (refusingForwards) {
            inFlightForwards.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * How long the last drain before a handover waits for statements in flight. Forwarded statements are held
     * meanwhile, so the wait takes a sixth of master_change_wait_timeout_second, and at least one second.
     */
    public static long finalWaitMs(int holdSecond) {
        return Math.max(1000L, holdSecond * 1000L / 6);
    }

    /**
     * The timeout of the handover itself: the configured one, cut so that the last drain and the handover end
     * within master_change_wait_timeout_second, with room left for the new master to start.
     */
    public static int transferTimeoutSecond(int configuredSecond, int holdSecond) {
        int timeout = Math.max(1, configuredSecond);
        if (holdSecond <= 0) {
            return timeout;
        }
        long budgetMs = (holdSecond * 1000L - finalWaitMs(holdSecond)) / 2;
        return (int) Math.max(1, Math.min(timeout, budgetMs / 1000));
    }

    static void setRefusingForwards(boolean refuse) {
        refusingForwards = refuse;
    }

    public static void exitForward() {
        inFlightForwards.decrementAndGet();
    }

    public static int inFlightForwards() {
        return inFlightForwards.get();
    }

    public static boolean rejectsNewStatements() {
        return rejectsNewStatements(shuttingDown, Config.enable_reject_new_statements_on_shutdown);
    }

    public static boolean rejectsMysqlCommand(MysqlCommand command) {
        return rejectsMysqlCommand(command, shuttingDown, Config.enable_reject_new_statements_on_shutdown);
    }

    static boolean rejectsNewStatements(boolean shuttingDown, boolean enabled) {
        return shuttingDown && enabled;
    }

    /** Every command is refused during the shutdown except COM_QUIT, which still closes the connection cleanly. */
    static boolean rejectsMysqlCommand(MysqlCommand command, boolean shuttingDown, boolean enabled) {
        return rejectsNewStatements(shuttingDown, enabled) && command != MysqlCommand.COM_QUIT;
    }

    /**
     * A connection is closed at the start of the shutdown when it waits for its client's next command. The
     * dispatcher sets the command before it checks the gate, so a command that arrives concurrently is either
     * refused or seen here as running, never cut off.
     */
    static boolean closesIdleConnection(boolean isProxy, MysqlCommand command) {
        return !isProxy && command == MysqlCommand.COM_SLEEP;
    }

    /**
     * The answer to a statement another FE forwards here while this master hands its mastership over: error 1053.
     * The statement did not run, so there is no journal to wait for, and the forwarding FE sends it again.
     */
    public static TMasterOpResult refusedForwardResult() {
        QueryState state = new QueryState();
        state.setError(ErrorCode.ERR_SERVER_SHUTDOWN, FORWARD_REJECT_MESSAGE);
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        state.toResponsePacket().writeTo(serializer);
        TMasterOpResult result = new TMasterOpResult();
        result.setMaxJournalId(0);
        result.setPacket(serializer.toByteBuffer());
        result.setStatus(state.toString());
        result.setStatusCode(ErrorCode.ERR_SERVER_SHUTDOWN.getCode());
        result.setErrMessage(FORWARD_REJECT_MESSAGE);
        return result;
    }

    /** Whether a forwarded request was refused by a departing master, so it never ran and can be sent again. */
    public static boolean isRefusedForward(TMasterOpResult result) {
        return result != null && result.isSetStatusCode()
                && result.getStatusCode() == ErrorCode.ERR_SERVER_SHUTDOWN.getCode()
                && FORWARD_REJECT_MESSAGE.equals(result.getErrMessage());
    }

    private static int closeIdleMysqlConnections() {
        List<ConnectContext> contexts = new ArrayList<>(
                ExecuteEnv.getInstance().getScheduler().getConnectPoolMgr().getConnectionMap().values());
        int closed = 0;
        for (ConnectContext ctx : contexts) {
            if (closesIdleConnection(ctx.isProxy(), ctx.getCommand())) {
                ctx.kill(true);
                closed++;
            }
        }
        return closed;
    }
}
