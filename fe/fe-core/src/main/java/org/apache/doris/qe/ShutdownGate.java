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

    // How long an FE holds a statement it forwards to the master while the master changes, before it fails it.
    // A graceful handover fits well inside it: the last drain, the transfer and the new master's start.
    public static final long MASTER_CHANGE_HOLD_MS = 30_000L;
    // The last drain before a handover. Forwarded statements are held meanwhile, so it takes a sixth of the hold.
    public static final long FINAL_DRAIN_MS = MASTER_CHANGE_HOLD_MS / 6;
    // The transferMaster timeout: half of what the hold leaves after the last drain, so the new master has the
    // other half to start before the forwarding FEs give up.
    public static final int TRANSFER_TIMEOUT_SECOND = (int) ((MASTER_CHANGE_HOLD_MS - FINAL_DRAIN_MS) / 2 / 1000);
    // How long the old master stays up after the handover. Forwards that still reach it are refused at once and
    // sent again to the new master, instead of timing out against an address that is gone.
    public static final long SETTLE_MS = 2000L;
    // How far, in journal ids, a follower's replay may trail the master's for the follower to take mastership.
    public static final long MAX_TRANSFER_JOURNAL_LAG = 1000L;

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
