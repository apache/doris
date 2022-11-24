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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.Bitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.SubsetIterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.HashSet;

public class BitSetTest {
    @Test
    void subsetIteratorTest() {
        BitSet bitSet = new BitSet();
        bitSet.set(0, 3);
        bitSet.set(64, 67);
        SubsetIterator subsetIterator = new SubsetIterator(bitSet);
        HashSet<BitSet> subsets = new HashSet<>();
        for (BitSet subset : subsetIterator) {
            Assertions.assertTrue(Bitmap.isSubset(subset, bitSet));
            subsets.add(subset);
        }
        Assertions.assertEquals(subsets.size(), Math.pow(2, 6) - 1);
    }
}
