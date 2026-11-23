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
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class FEOpExecutorHoldTest {

    private static TMasterOpResult accepted() {
        TMasterOpResult result = new TMasterOpResult();
        result.setMaxJournalId(7);
        result.setStatusCode(0);
        return result;
    }

    // Answers each forward with the next scripted outcome: a result, or an exception to throw.
    private static class ScriptedExecutor extends FEOpExecutor {
        private final Deque<Object> outcomes;
        private final boolean followsMaster;
        private final Runnable onRefreshTarget;
        int sends = 0;
        int refreshes = 0;

        ScriptedExecutor(boolean followsMaster, Runnable onRefreshTarget, Object... outcomes) {
            super(new TNetworkAddress("127.0.0.1", 1), new OriginStatement("select 1", 0), new ConnectContext(),
                    true);
            this.followsMaster = followsMaster;
            this.onRefreshTarget = onRefreshTarget;
            this.outcomes = new ArrayDeque<>(Arrays.asList(outcomes));
        }

        @Override
        protected boolean followsMaster() {
            return followsMaster;
        }

        @Override
        protected void refreshTarget() {
            refreshes++;
            onRefreshTarget.run();
        }

        @Override
        protected TMasterOpResult forwardOnce(TMasterOpRequest params) throws Exception {
            sends++;
            Object outcome = outcomes.isEmpty() ? accepted() : outcomes.poll();
            if (outcome instanceof Exception) {
                throw (Exception) outcome;
            }
            return (TMasterOpResult) outcome;
        }
    }

    private static FEOpExecutor.NotSentException notReachable() {
        return new FEOpExecutor.NotSentException(new Exception("Failed to get master client."), false);
    }

    @Test
    public void testResendsARefusedStatementToTheNextMaster() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(true, () -> { },
                ShutdownGate.refusedForwardResult(), notReachable(), accepted());
        TMasterOpResult result = executor.forward(new TMasterOpRequest());
        Assertions.assertEquals(7, result.getMaxJournalId());
        Assertions.assertEquals(3, executor.sends);
        Assertions.assertEquals(2, executor.refreshes);
    }

    @Test
    public void testFixedTargetIsNotHeld() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(false, () -> { }, ShutdownGate.refusedForwardResult());
        Assertions.assertTrue(ShutdownGate.isRefusedForward(executor.forward(new TMasterOpRequest())));
        Assertions.assertEquals(1, executor.sends);

        ScriptedExecutor unreachable = new ScriptedExecutor(false, () -> { }, notReachable());
        try {
            unreachable.forward(new TMasterOpRequest());
            Assertions.fail("a fixed target must not be held");
        } catch (Exception e) {
            Assertions.assertEquals("Failed to get master client.", e.getMessage());
        }
        Assertions.assertEquals(1, unreachable.sends);
    }

    @Test
    public void testStatementCancelledWhileHeldIsNotResent() throws Exception {
        ScriptedExecutor[] holder = new ScriptedExecutor[1];
        holder[0] = new ScriptedExecutor(true, () -> {
            try {
                // No query id yet, so cancel() only marks the executor cancelled.
                holder[0].cancel();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ShutdownGate.refusedForwardResult(), accepted());
        Assertions.assertTrue(ShutdownGate.isRefusedForward(holder[0].forward(new TMasterOpRequest())));
        Assertions.assertEquals(1, holder[0].sends);
    }

    @Test
    public void testKilledConnectionIsNotResent() throws Exception {
        ScriptedExecutor[] holder = new ScriptedExecutor[1];
        holder[0] = new ScriptedExecutor(true, () -> holder[0].ctx.setKilled(), notReachable(), accepted());
        try {
            holder[0].forward(new TMasterOpRequest());
            Assertions.fail("a killed statement must not be sent again");
        } catch (Exception e) {
            Assertions.assertEquals("Failed to get master client.", e.getMessage());
        }
        Assertions.assertEquals(1, holder[0].sends);
    }

    @Test
    public void testNotReadyIsNotHeldWithIgnoreMetaCheck() throws Exception {
        boolean saved = Config.ignore_meta_check;
        Config.ignore_meta_check = true;
        try {
            ScriptedExecutor executor = new ScriptedExecutor(true, () -> { },
                    new FEOpExecutor.NotSentException(new Exception("not ready"), true), accepted());
            try {
                executor.forward(new TMasterOpRequest());
                Assertions.fail("a not-ready FE with ignore_meta_check must fail at once");
            } catch (Exception e) {
                Assertions.assertEquals("not ready", e.getMessage());
            }
            Assertions.assertEquals(1, executor.sends);
        } finally {
            Config.ignore_meta_check = saved;
        }
    }

    @Test
    public void testNotReadyIsHeldUntilReady() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(true, () -> { },
                ShutdownGate.refusedForwardResult(),
                new FEOpExecutor.NotSentException(new Exception("not ready"), true), accepted());
        Assertions.assertEquals(7, executor.forward(new TMasterOpRequest()).getMaxJournalId());
        Assertions.assertEquals(3, executor.sends);
    }
}
