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

package org.apache.doris.arrowflight.results;

import com.google.common.base.Ticker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class FlightSqlChannelTest {
    @Test
    public void testExpiredResultDoesNotHoldShutdownOpen() {
        ManualTicker ticker = new ManualTicker();
        FlightSqlChannel channel = new FlightSqlChannel(ticker);
        try {
            channel.addOKResult("expired", "select 1");
            Assertions.assertTrue(channel.hasPendingResults());
            ticker.advanceMinutes(11);
            Assertions.assertFalse(channel.hasPendingResults());
            Assertions.assertNull(channel.getResult("expired"));
            Assertions.assertEquals(0, channel.getAllocatedMemory());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testLiveResultStillHoldsShutdownOpen() {
        ManualTicker ticker = new ManualTicker();
        FlightSqlChannel channel = new FlightSqlChannel(ticker);
        try {
            channel.addOKResult("expired", "select 1");
            ticker.advanceMinutes(6);
            channel.addOKResult("live", "select 2");
            ticker.advanceMinutes(5);
            Assertions.assertTrue(channel.hasPendingResults());
            Assertions.assertNull(channel.getResult("expired"));
            Assertions.assertNotNull(channel.getResult("live"));
            channel.invalidate("live");
            Assertions.assertFalse(channel.hasPendingResults());
        } finally {
            channel.close();
        }
    }

    private static class ManualTicker extends Ticker {
        private long nanos;

        @Override
        public long read() {
            return nanos;
        }

        void advanceMinutes(long minutes) {
            nanos += TimeUnit.MINUTES.toNanos(minutes);
        }
    }
}
