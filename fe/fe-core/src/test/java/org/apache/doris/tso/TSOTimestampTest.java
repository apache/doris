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

package org.apache.doris.tso;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Unit tests for TSOTimestamp class.
 */
public class TSOTimestampTest {

    @Test
    public void testConstructor() {
        // Test default constructor
        TSOTimestamp timestamp = new TSOTimestamp();
        Assert.assertEquals(0L, timestamp.getPhysicalTimestamp());
        Assert.assertEquals(0L, timestamp.getLogicalCounter());

        // Test constructor with parameters
        long physicalTime = 1625097600000L;
        long logicalCounter = 123L;
        timestamp = new TSOTimestamp(physicalTime, logicalCounter);
        Assert.assertEquals(physicalTime, timestamp.getPhysicalTimestamp());
        Assert.assertEquals(logicalCounter, timestamp.getLogicalCounter());
    }

    @Test
    public void testComposeAndExtractTimestamp() {
        long physicalTime = 1625097600000L;
        long logicalCounter = 123L;

        TSOTimestamp timestamp = new TSOTimestamp(physicalTime, logicalCounter);
        long composed = timestamp.composeTimestamp();

        // Verify extraction works correctly
        Assert.assertEquals(physicalTime, TSOTimestamp.extractPhysicalTime(composed));
        Assert.assertEquals(logicalCounter, TSOTimestamp.extractLogicalCounter(composed));
    }

    @Test
    public void testBitWidthLimitations() {
        // Test that values are properly masked to fit in their respective bit widths
        long largePhysicalTime = (1L << 46) + 1000L; // Larger than 46 bits
        long largeLogicalCounter = (1L << 18) + 50L;  // Larger than 18 bits

        TSOTimestamp timestamp = new TSOTimestamp(largePhysicalTime, largeLogicalCounter);
        long composed = timestamp.composeTimestamp();

        // Values should be masked to fit in their respective bit widths
        Assert.assertEquals(largePhysicalTime & ((1L << 46) - 1), TSOTimestamp.extractPhysicalTime(composed));
        Assert.assertEquals(largeLogicalCounter & ((1L << 18) - 1), TSOTimestamp.extractLogicalCounter(composed));
    }

    @Test
    public void testSetterAndGetters() {
        TSOTimestamp timestamp = new TSOTimestamp();

        long physicalTime = 1625097600000L;
        long logicalCounter = 456L;

        timestamp.setPhysicalTimestamp(physicalTime);
        timestamp.setLogicalCounter(logicalCounter);

        Assert.assertEquals(physicalTime, timestamp.getPhysicalTimestamp());
        Assert.assertEquals(logicalCounter, timestamp.getLogicalCounter());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorRejectNegativePhysicalTimestamp() {
        new TSOTimestamp(-1L, 0L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorRejectNegativeLogicalCounter() {
        new TSOTimestamp(0L, -1L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetterRejectNegativePhysicalTimestamp() {
        TSOTimestamp timestamp = new TSOTimestamp();
        timestamp.setPhysicalTimestamp(-1L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetterRejectNegativeLogicalCounter() {
        TSOTimestamp timestamp = new TSOTimestamp();
        timestamp.setLogicalCounter(-1L);
    }

    @Test
    public void testDecomposeIsConsistentWithExtract() {
        long physicalTime = 1625097600000L;
        long logicalCounter = 123L;
        long tso = TSOTimestamp.composeTimestamp(physicalTime, logicalCounter);

        TSOTimestamp decomposed = TSOTimestamp.decompose(tso);
        Assert.assertEquals(TSOTimestamp.extractPhysicalTime(tso), decomposed.getPhysicalTimestamp());
        Assert.assertEquals(TSOTimestamp.extractLogicalCounter(tso), decomposed.getLogicalCounter());
    }

    @Test
    public void testWritableRoundTrip() throws Exception {
        TSOTimestamp timestamp = new TSOTimestamp(1625097600000L, 123L);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        timestamp.write(dos);
        dos.flush();

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        DataInputStream dis = new DataInputStream(bis);
        TSOTimestamp restored = TSOTimestamp.read(dis);

        Assert.assertEquals(timestamp, restored);
        Assert.assertEquals(timestamp.hashCode(), restored.hashCode());
        Assert.assertEquals(0, timestamp.compareTo(restored));
    }

    @Test
    public void testCompareToOrdersByPhysicalThenLogical() {
        TSOTimestamp a = new TSOTimestamp(100L, 2L);
        TSOTimestamp b = new TSOTimestamp(100L, 3L);
        TSOTimestamp c = new TSOTimestamp(101L, 0L);
        Assert.assertTrue(a.compareTo(b) < 0);
        Assert.assertTrue(b.compareTo(c) < 0);
        Assert.assertTrue(a.compareTo(c) < 0);
    }

    @Test
    public void testMaxLogicalCounter() {
        // Test the maximum logical counter value
        Assert.assertEquals((1L << 18) - 1, TSOTimestamp.MAX_LOGICAL_COUNTER);
    }
}
