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

package org.apache.doris.nereids.rules.joinreorder.hypergraph.bitmap;

import java.util.BitSet;

/**
 * This is helper class for some bitmap operation
 */
public class Bitmap {
    public static boolean isSubset(BitSet bitSet1, BitSet bitSet2) {
        BitSet bitSet = new BitSet();
        bitSet.or(bitSet1);
        bitSet.or(bitSet2);
        return bitSet.equals(bitSet2);
    }

    public static BitSet newBitmap(int... values) {
        BitSet bitSet = new BitSet();
        for (int v : values) {
            bitSet.set(v);
        }
        return bitSet;
    }

    public static BitSet unionBitmap(BitSet... bitSets) {
        BitSet u = new BitSet();
        for (BitSet bitSet : bitSets) {
            u.or(bitSet);
        }
        return u;
    }
}

