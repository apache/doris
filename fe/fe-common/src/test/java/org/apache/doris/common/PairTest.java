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

package org.apache.doris.common;

import org.junit.Assert;
import org.junit.Test;

public class PairTest {

    @Test
    public void testToString() {
        Pair<String, Object> pairFirstNull = Pair.of(null, "world");
        Assert.assertEquals(":world", pairFirstNull.toString());

        Pair<String, Object> pairSecondNull = Pair.of("hello", null);
        Assert.assertEquals("hello:", pairSecondNull.toString());
    }

    @Test
    public void testEquals() {
        Pair<String, Object> firstPair = Pair.of(null, "world");
        Pair<String, Object> secondPair = null;

        Assert.assertTrue(firstPair.equals(firstPair));
        Assert.assertFalse(firstPair.equals(secondPair));

        secondPair = Pair.of(null, "world");
        Assert.assertTrue(firstPair.equals(secondPair));

        secondPair = Pair.of("hello", null);
        Assert.assertFalse(firstPair.equals(secondPair));

        firstPair = Pair.of("hello", "world");
        secondPair = Pair.of("hello", "world");
        Assert.assertTrue(firstPair.equals(secondPair));

        secondPair = Pair.of("world", "hello");
        Assert.assertFalse(firstPair.equals(secondPair));
    }
}
