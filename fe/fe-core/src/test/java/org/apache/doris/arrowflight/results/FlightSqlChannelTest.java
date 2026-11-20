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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Ticker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FlightSqlChannelTest {
    @Test
    public void testIdleMaintenanceReleasesExpiredBuffersWithoutAnotherCacheRead() {
        AtomicLong now = new AtomicLong();
        FlightSqlChannel channel = channel(now);
        try {
            channel.addOKResult("result", "SELECT 1");
            Assertions.assertTrue(channel.getAllocatedMemory() > 0);
            now.set(TimeUnit.MINUTES.toNanos(10));
            // Guava expiry is lazy; resultNum is deliberately a pure observation for the handoff gate.
            Assertions.assertEquals(1, channel.resultNum());
            channel.expireResults();
            Assertions.assertEquals(0, channel.resultNum());
            Assertions.assertEquals(0, channel.getAllocatedMemory());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testReadingAResultDoesNotExtendItsWriteDeadlineOrExpireANewerResult() {
        AtomicLong now = new AtomicLong();
        FlightSqlChannel channel = channel(now);
        try {
            channel.addOKResult("first", "SELECT 1");
            now.set(TimeUnit.MINUTES.toNanos(9));
            Assertions.assertNotNull(channel.getResult("first"));
            channel.addOKResult("second", "SELECT 2");
            now.set(TimeUnit.MINUTES.toNanos(10));
            channel.expireResults();
            Assertions.assertNull(channel.getResult("first"));
            Assertions.assertNotNull(channel.getResult("second"));
            Assertions.assertEquals(1, channel.resultNum());
            Assertions.assertTrue(channel.getAllocatedMemory() > 0);
            now.set(TimeUnit.MINUTES.toNanos(19));
            channel.expireResults();
            Assertions.assertEquals(0, channel.resultNum());
            Assertions.assertEquals(0, channel.getAllocatedMemory());
        } finally {
            channel.close();
        }
    }

    @Test
    public void testFailedResultConversionReleasesUnpublishedBuffers() {
        FlightSqlChannel channel = new FlightSqlChannel();
        try {
            channel.addOKResult("previous", "SELECT 1");
            long previousBytes = channel.getAllocatedMemory();
            ShowResultSet rows = new ShowResultSet(ShowResultSetMetaData.builder()
                    .addColumn(new Column("payload", ScalarType.createVarchar(20))).build(),
                    List.of(List.of("first", "unexpected column")));
            Assertions.assertThrows(IndexOutOfBoundsException.class, () -> channel.addResult("failed", "SHOW", rows));
            Assertions.assertNull(channel.getResult("failed"));
            Assertions.assertNotNull(channel.getResult("previous"));
            Assertions.assertEquals(previousBytes, channel.getAllocatedMemory());
        } finally {
            channel.close();
        }
    }

    private static FlightSqlChannel channel(AtomicLong now) {
        return new FlightSqlChannel(new Ticker() {
            @Override
            public long read() {
                return now.get();
            }
        });
    }
}
