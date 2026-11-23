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
        Assertions.assertFalse(ShutdownGate.rejectsNewStatements(false, true));
        for (MysqlCommand command : MysqlCommand.values()) {
            Assertions.assertFalse(ShutdownGate.rejectsMysqlCommand(command, false, true));
        }
    }

    @Test
    public void testRejectsNewCommandsButQuitDuringShutdown() {
        Assertions.assertTrue(ShutdownGate.rejectsNewStatements(true, true));
        Assertions.assertTrue(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_QUERY, true, true));
        Assertions.assertTrue(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_STMT_EXECUTE, true, true));
        Assertions.assertTrue(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_PING, true, true));
        Assertions.assertFalse(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_QUIT, true, true));
    }

    @Test
    public void testDisabledByConfig() {
        Assertions.assertFalse(ShutdownGate.rejectsNewStatements(true, false));
        Assertions.assertFalse(ShutdownGate.rejectsMysqlCommand(MysqlCommand.COM_QUERY, true, false));
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
    public void testFinalWaitAndTransferStayWithinTheHoldWindow() {
        Assertions.assertEquals(5000L, ShutdownGate.finalWaitMs(30));
        Assertions.assertEquals(1000L, ShutdownGate.finalWaitMs(0));
        // (30 s - 5 s) / 2 = 12 s, below the configured 30 s.
        Assertions.assertEquals(12, ShutdownGate.transferTimeoutSecond(30, 30));
        Assertions.assertEquals(5, ShutdownGate.transferTimeoutSecond(5, 30));
        Assertions.assertTrue(ShutdownGate.finalWaitMs(30) + ShutdownGate.transferTimeoutSecond(30, 30) * 1000L
                < 30 * 1000L);
        // Without a hold window, the configured timeout applies.
        Assertions.assertEquals(30, ShutdownGate.transferTimeoutSecond(30, 0));
        Assertions.assertEquals(1, ShutdownGate.transferTimeoutSecond(30, 2));
    }

    @Test
    public void testClosesOnlyIdleClientConnections() {
        Assertions.assertTrue(ShutdownGate.closesIdleConnection(false, MysqlCommand.COM_SLEEP));
        Assertions.assertFalse(ShutdownGate.closesIdleConnection(false, MysqlCommand.COM_QUERY));
        Assertions.assertFalse(ShutdownGate.closesIdleConnection(true, MysqlCommand.COM_SLEEP));
    }
}
