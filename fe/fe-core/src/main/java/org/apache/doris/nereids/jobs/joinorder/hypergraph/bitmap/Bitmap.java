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

package org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap;

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

    public static BitSet newBitmap(BitSet bitSet) {
        BitSet n = new BitSet();
        n.or(bitSet);
        return n;
    }

    public static BitSet newBitmap() {
        return new BitSet();
    }

    public static BitSet newBitmapUnion(BitSet... bitSets) {
        BitSet u = new BitSet();
        for (BitSet bitSet : bitSets) {
            u.or(bitSet);
        }
        return u;
    }

    // return bitSet1 - bitSet2
    public static BitSet newBitmapDiff(BitSet bitSet1, BitSet bitSet2) {
        BitSet u = new BitSet();
        u.or(bitSet1);
        u.andNot(bitSet2);
        return u;
    }

    public static BitSet newBitmapBetween(int start, int end) {
        BitSet bitSet = new BitSet();
        bitSet.set(start, end);
        return bitSet;
    }

    public static int nextSetBit(BitSet bitSet, int fromIndex) {
        return bitSet.nextSetBit(fromIndex);
    }

    public static boolean get(BitSet bitSet, int index) {
        return bitSet.get(index);
    }

    public static void set(BitSet bitSet, int index) {
        bitSet.set(index);
    }

    public static void unset(BitSet bitSet, int index) {
        bitSet.set(index, false);
    }

    public static void clear(BitSet bitSet) {
        bitSet.clear();
    }

    public static int getCardinality(BitSet bitSet) {
        return bitSet.cardinality();
    }

    public static BitSetIterator getIterator(BitSet bitSet) {
        return new BitSetIterator(bitSet);
    }

    public static ReverseBitSetIterator getReverseIterator(BitSet bitSet) {
        return new ReverseBitSetIterator(bitSet);
    }

    public static void or(BitSet bitSet1, BitSet bitSet2) {
        bitSet1.or(bitSet2);
    }

    public static boolean isOverlap(BitSet bitSet1, BitSet bitSet2) {
        return bitSet1.intersects(bitSet2);
    }

    public static void andNot(BitSet bitSet1, BitSet bitSet2) {
        bitSet1.andNot(bitSet2);
    }

    public static void and(BitSet bitSet1, BitSet bitSet2) {
        bitSet1.and(bitSet2);
    }

    public static SubsetIterator getSubsetIterator(BitSet bitSet) {
        return new SubsetIterator(bitSet);
    }
}

