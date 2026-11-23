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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    public void testFenceWaitFitsBeforeTheTransfer() {
        // The fence and the journal flush take a few seconds at most, well below the transfer timeout and the
        // BDBJE heartbeat timeout that a handover saves the followers from waiting for.
        Assertions.assertTrue(ShutdownGate.FENCE_WAIT_MS + ShutdownGate.JOURNAL_FLUSH_WAIT_MS
                <= ShutdownGate.TRANSFER_TIMEOUT_SECOND * 1000L);
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
