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

package org.apache.doris.mtmv.ivm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IvmStreamRefTest {

    @Test
    public void testDefaultConstructorAllZeros() {
        IvmStreamRef ref = new IvmStreamRef();
        Assertions.assertEquals(0, ref.getConsumedTso());
        Assertions.assertEquals(0, ref.getLatestTso());
    }

    @Test
    public void testConstructorWithConsumedTso() {
        IvmStreamRef ref = new IvmStreamRef(42L);
        Assertions.assertEquals(42L, ref.getConsumedTso());
        Assertions.assertEquals(0, ref.getLatestTso());
    }

    @Test
    public void testIsUpToDateWhenEqual() {
        IvmStreamRef ref = new IvmStreamRef(10L);
        ref.setLatestTso(10L);
        Assertions.assertTrue(ref.isUpToDate());
    }

    @Test
    public void testIsUpToDateWhenDifferent() {
        IvmStreamRef ref = new IvmStreamRef(5L);
        ref.setLatestTso(10L);
        Assertions.assertFalse(ref.isUpToDate());
    }

    @Test
    public void testIsUpToDateDefaultBothZero() {
        IvmStreamRef ref = new IvmStreamRef();
        Assertions.assertTrue(ref.isUpToDate());
    }

    @Test
    public void testSettersAndGetters() {
        IvmStreamRef ref = new IvmStreamRef();
        ref.setConsumedTso(100L);
        ref.setLatestTso(200L);
        Assertions.assertEquals(100L, ref.getConsumedTso());
        Assertions.assertEquals(200L, ref.getLatestTso());
        Assertions.assertFalse(ref.isUpToDate());
    }

    @Test
    public void testToStringContainsFields() {
        IvmStreamRef ref = new IvmStreamRef(7L);
        ref.setLatestTso(9L);
        String s = ref.toString();
        Assertions.assertTrue(s.contains("consumedTso=7"), "should contain consumedTso: " + s);
        Assertions.assertTrue(s.contains("latestTso=9"), "should contain latestTso: " + s);
    }
}
