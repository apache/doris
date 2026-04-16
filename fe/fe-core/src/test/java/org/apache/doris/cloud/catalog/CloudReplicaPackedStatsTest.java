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

package org.apache.doris.cloud.catalog;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

/**
 * Unit tests for the packedStatsState bit-packing logic in CloudReplica.
 * Bottom 60 bits = lastGetTabletStatsTime, top 4 bits = statsIntervalIndex.
 */
public class CloudReplicaPackedStatsTest {

    @Test
    public void testSetTimestampPreservesIntervalIndex() {
        CloudReplica replica = new CloudReplica();
        replica.setStatsIntervalIndex(7);
        replica.setLastGetTabletStatsTime(123456789L);
        Assertions.assertEquals(7, replica.getStatsIntervalIndex(),
                "Setting timestamp must not corrupt interval index");
        Assertions.assertEquals(123456789L, replica.getLastGetTabletStatsTime());
    }

    @Test
    public void testSetIntervalIndexPreservesTimestamp() {
        CloudReplica replica = new CloudReplica();
        replica.setLastGetTabletStatsTime(999888777L);
        replica.setStatsIntervalIndex(12);
        Assertions.assertEquals(999888777L, replica.getLastGetTabletStatsTime(),
                "Setting interval index must not corrupt timestamp");
        Assertions.assertEquals(12, replica.getStatsIntervalIndex());
    }

    @Test
    public void testMaxValidTimestamp() {
        CloudReplica replica = new CloudReplica();
        long maxTimestamp = 0x0FFFFFFFFFFFFFFFL;
        replica.setStatsIntervalIndex(15);
        replica.setLastGetTabletStatsTime(maxTimestamp);
        Assertions.assertEquals(maxTimestamp, replica.getLastGetTabletStatsTime());
        Assertions.assertEquals(15, replica.getStatsIntervalIndex());
    }

    @Test
    public void testMaxValidIntervalIndex() {
        CloudReplica replica = new CloudReplica();
        replica.setLastGetTabletStatsTime(42L);
        replica.setStatsIntervalIndex(15);
        Assertions.assertEquals(15, replica.getStatsIntervalIndex());
        Assertions.assertEquals(42L, replica.getLastGetTabletStatsTime());
    }

    @Test
    public void testZeroValues() {
        CloudReplica replica = new CloudReplica();
        replica.setLastGetTabletStatsTime(0);
        replica.setStatsIntervalIndex(0);
        Assertions.assertEquals(0, replica.getLastGetTabletStatsTime());
        Assertions.assertEquals(0, replica.getStatsIntervalIndex());
    }

    @Test
    public void testNegativeTimestampThrows() {
        CloudReplica replica = new CloudReplica();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> replica.setLastGetTabletStatsTime(-1L));
    }

    @Test
    public void testTimestampExceedingMaskThrows() {
        CloudReplica replica = new CloudReplica();
        long overMax = 0x0FFFFFFFFFFFFFFFL + 1;
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> replica.setLastGetTabletStatsTime(overMax));
    }

    @Test
    public void testNegativeIntervalIndexThrows() {
        CloudReplica replica = new CloudReplica();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> replica.setStatsIntervalIndex(-1));
    }

    @Test
    public void testIntervalIndexExceeding15Throws() {
        CloudReplica replica = new CloudReplica();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> replica.setStatsIntervalIndex(16));
    }

    @Test
    public void testRepeatedUpdatesPreserveInvariants() {
        CloudReplica replica = new CloudReplica();
        for (int idx = 0; idx <= 15; idx++) {
            long time = (long) idx * 100000L + 1;
            replica.setStatsIntervalIndex(idx);
            replica.setLastGetTabletStatsTime(time);
            Assertions.assertEquals(idx, replica.getStatsIntervalIndex(),
                    "Interval index corrupted at iteration " + idx);
            Assertions.assertEquals(time, replica.getLastGetTabletStatsTime(),
                    "Timestamp corrupted at iteration " + idx);
        }
    }

    @Test
    public void testDefaultPackedStateIsZero() {
        CloudReplica replica = new CloudReplica();
        Assertions.assertEquals(0, replica.getLastGetTabletStatsTime());
        Assertions.assertEquals(0, replica.getStatsIntervalIndex());
    }
}
