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
import org.apache.doris.thrift.TMasterOpResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ShutdownGateTest {

    @Test
    public void testAcceptsEverythingBeforeShutdown() {
        for (MysqlCommand command : MysqlCommand.values()) {
            Assertions.assertFalse(ShutdownGate.rejectsMysqlCommand(command, false));
        }
    }

    @Test
    public void testRejectsNewCommandsButQuitDuringShutdown() {
        Assertions.assertTrue(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_QUERY, true));
        Assertions.assertTrue(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_STMT_EXECUTE, true));
        Assertions.assertTrue(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_PING, true));
        Assertions.assertFalse(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_QUIT, true));
    }

    @Test
    public void testRefusedForwardCarriesShutdownError() {
        TMasterOpResult result = ShutdownGate.refusedForwardResult();
        Assertions.assertEquals(ErrorCode.ERR_SERVER_SHUTDOWN.getCode(), result.getStatusCode());
        Assertions.assertEquals(0, result.getMaxJournalId());
        ByteBuffer packet = result.bufferForPacket();
        Assertions.assertEquals((byte) 0xff, packet.get(0));
        Assertions.assertEquals(1053, (packet.get(1) & 0xff) | ((packet.get(2) & 0xff) << 8));
        byte[] bytes = new byte[packet.remaining()];
        packet.duplicate().get(bytes);
        String text = new String(bytes, StandardCharsets.UTF_8);
        Assertions.assertTrue(text.contains("08S01"));
        Assertions.assertTrue(text.contains(ShutdownGate.FORWARD_REJECT_MESSAGE));
    }

    @Test
    public void testRecognisesOnlyTheRefusedForward() {
        Assertions.assertTrue(ShutdownGate.isRefusedForward(ShutdownGate.refusedForwardResult()));
        TMasterOpResult other = ShutdownGate.refusedForwardResult();
        other.setErrMessage("Server shutdown in progress");
        Assertions.assertFalse(ShutdownGate.isRefusedForward(other));
        TMasterOpResult ok = new TMasterOpResult();
        ok.setStatusCode(0);
        Assertions.assertFalse(ShutdownGate.isRefusedForward(ok));
        Assertions.assertFalse(ShutdownGate.isRefusedForward(null));
    }

    @Test
    public void testCountsForwardsUntilTheyAreRefused() {
        int before = ShutdownGate.inFlightForwards();
        try {
            Assertions.assertTrue(ShutdownGate.enterForward());
            Assertions.assertEquals(before + 1, ShutdownGate.inFlightForwards());
            ShutdownGate.setRefusingForwards(true);
            Assertions.assertFalse(ShutdownGate.enterForward());
            Assertions.assertEquals(before + 1, ShutdownGate.inFlightForwards());
            ShutdownGate.exitForward();
            Assertions.assertEquals(before, ShutdownGate.inFlightForwards());
        } finally {
            ShutdownGate.setRefusingForwards(false);
        }
    }

    @Test
    public void testFinalDrainAndTransferStayWithinTheHoldWindow() {
        Assertions.assertEquals(5000L, ShutdownGate.FINAL_DRAIN_MS);
        // (30 s - 5 s - 3 s - 2 s) / 2 = 10 s.
        Assertions.assertEquals(10, ShutdownGate.TRANSFER_TIMEOUT_SECOND);
        Assertions.assertTrue(ShutdownGate.FINAL_DRAIN_MS + ShutdownGate.FENCE_WAIT_MS
                + ShutdownGate.JOURNAL_FLUSH_WAIT_MS + ShutdownGate.TRANSFER_TIMEOUT_SECOND * 1000L
                + ShutdownGate.SETTLE_MS < ShutdownGate.MASTER_CHANGE_HOLD_MS);
    }

    @Test
    public void testFenceStopsNewMasterWorkAndWaitsForRunningWork() throws Exception {
        try {
            Assertions.assertTrue(ShutdownGate.enterMasterWork());
            // One unit still runs, so the fence does not report the master work as stopped.
            Assertions.assertFalse(ShutdownGate.fenceMasterWork(50));
            Assertions.assertTrue(ShutdownGate.isMasterWorkFenced());
            Assertions.assertFalse(ShutdownGate.enterMasterWork());
            ShutdownGate.exitMasterWork();
            Assertions.assertTrue(ShutdownGate.fenceMasterWork(50));
        } finally {
            ShutdownGate.resetMasterWorkFence();
        }
    }

    @Test
    public void testClosesOnlyIdleClientConnections() {
        ConnectContext.ConnectType mysql = ConnectContext.ConnectType.MYSQL;
        Assertions.assertTrue(ShutdownGate.closesIdleConnection(false, mysql, MysqlCommand.COM_SLEEP));
        Assertions.assertFalse(ShutdownGate.closesIdleConnection(false, mysql, MysqlCommand.COM_QUERY));
        Assertions.assertFalse(ShutdownGate.closesIdleConnection(true, mysql, MysqlCommand.COM_SLEEP));
        // An Arrow Flight SQL session between commands may still be fetching results.
        Assertions.assertFalse(ShutdownGate.closesIdleConnection(false, ConnectContext.ConnectType.ARROW_FLIGHT_SQL,
                MysqlCommand.COM_SLEEP));
    }
}
