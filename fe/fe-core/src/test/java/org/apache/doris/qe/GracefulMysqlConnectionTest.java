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

import org.apache.doris.common.Status;
import org.apache.doris.mysql.MysqlCommand;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GracefulMysqlConnectionTest {
    @Test
    public void testIdleClosurePreventsDispatchWithoutCancellingExecutor() {
        TestContext context = new TestContext();
        context.setCommand(MysqlCommand.COM_SLEEP);
        Assertions.assertTrue(context.closeIdleMysqlConnection());
        Assertions.assertTrue(context.closed);
        Assertions.assertTrue(context.isKilled());
        Assertions.assertFalse(context.startMysqlCommand());
        Assertions.assertFalse(context.closeIdleMysqlConnection());
    }

    @Test
    public void testReceivedPacketPreventsIdleClosure() {
        TestContext context = new TestContext();
        context.setCommand(MysqlCommand.COM_SLEEP);
        Assertions.assertTrue(context.startMysqlCommand());
        Assertions.assertFalse(context.closeIdleMysqlConnection());
        Assertions.assertFalse(context.closed);
        Assertions.assertFalse(context.isKilled());
        context.setCommand(MysqlCommand.COM_SLEEP);
        Assertions.assertTrue(context.closeIdleMysqlConnection());
    }

    @Test
    public void testFlightSessionIsNotClosedAsIdleMysql() {
        TestContext context = new TestContext() {
            @Override
            public ConnectType getConnectType() {
                return ConnectType.ARROW_FLIGHT_SQL;
            }
        };
        context.setCommand(MysqlCommand.COM_SLEEP);
        Assertions.assertFalse(context.closeIdleMysqlConnection());
        Assertions.assertFalse(context.closed);
    }

    private static class TestContext extends ConnectContext {
        private boolean closed;

        @Override
        protected void closeChannel() {
            closed = true;
        }

        @Override
        public void cancelQuery(Status reason) {
            Assertions.fail("graceful idle closure must not cancel a query");
        }
    }
}
