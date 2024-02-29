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

import org.apache.doris.nereids.trees.plans.RelationId;

import java.util.BitSet;
import java.util.Collection;
import java.util.Set;

/**
 * This is helper class for some bitmap operation
 */
public class LongBitmap {
    private static final long MASK = 0xffffffffffffffffL;
    private static final int SIZE = Long.SIZE;

    public static boolean isSubset(long bitmap1, long bitmap2) {
        return (bitmap1 | bitmap2) == bitmap2;
    }

    public static long newBitmap(int... values) {
        long bitmap = 0;
        for (int v : values) {
            bitmap |= (1L << v);
        }
        return bitmap;
    }

    public static long newBitmap(int value) {
        return 1L << value;
    }

    public static long newBitmap() {
        return 0;
    }

    public static long newBitmap(Collection<Integer> values) {
        long res = 0;
        for (int v : values) {
            res = LongBitmap.set(res, v);
        }
        return res;
    }

    public static long clone(long bitmap) {
        return bitmap;
    }

    public static long newBitmapUnion(long... bitmaps) {
        long u = 0;
        for (long bitmap : bitmaps) {
            u |= bitmap;
        }
        return u;
    }

    public static long newBitmapUnion(long b1, long b2) {
        return b1 | b2;
    }

    // return bitSet1 - bitSet2
    public static long newBitmapDiff(long bitmap1, long bitmap2) {
        return bitmap1 & (~bitmap2);
    }

    //return bitset1 ∩ bitset2
    public static long newBitmapIntersect(long bitmap1, long bitmap2) {
        return bitmap1 & bitmap2;
    }

    public static long newBitmapBetween(int start, int end) {
        long bitmap = 0;
        for (int i = start; i < end; i++) {
            bitmap |= (1L << i);
        }
        return bitmap;
    }

    public static int nextSetBit(long bitmap, int fromIndex) {
        bitmap &= (MASK << fromIndex);
        bitmap = bitmap & (-bitmap);
        return Long.numberOfTrailingZeros(bitmap);
    }

    public static boolean get(long bitmap, int index) {
        return (bitmap & (1L << index)) != 0;
    }

    public static long set(long bitmap, int index) {
        return bitmap | (1L << index);
    }

    public static long unset(long bitmap, int index) {
        return bitmap & (~(1L << index));
    }

    public static long clear(long bitSet) {
        return 0;
    }

    public static int getCardinality(long bimap) {
        return Long.bitCount(bimap);
    }

    public static LongBitmapIterator getIterator(long bitmap) {
        return new LongBitmapIterator(bitmap);
    }

    public static LongBitmapReverseIterator getReverseIterator(long bitmap) {
        return new LongBitmapReverseIterator(bitmap);
    }

    public static long or(long bitmap1, long bitmap2) {
        return bitmap1 | bitmap2;
    }

    public static boolean isOverlap(long bitmap1, long bitmap2) {
        return (bitmap1 & bitmap2) != 0;
    }

    public static long andNot(long bitmap1, long bitmap2) {
        return (bitmap1 & ~(bitmap2));
    }

    public static long and(long bitmap1, long bitmap2) {
        return bitmap1 & bitmap2;
    }

    public static LongBitmapSubsetIterator getSubsetIterator(long bitmap) {
        return new LongBitmapSubsetIterator(bitmap);
    }

    public static long clearLowestBit(long bitmap) {
        return bitmap & (bitmap - 1);
    }

    public static int previousSetBit(long bitmap, int fromIndex) {
        long newBitmap = bitmap & (MASK >>> -(fromIndex + 1));
        return SIZE - Long.numberOfLeadingZeros(newBitmap) - 1;
    }

    public static int lowestOneIndex(long bitmap) {
        return Long.numberOfTrailingZeros(bitmap);
    }

    /**
     * use to calculate table bitmap
     * @param relationIdSet relationIds
     * @return bitmap
     */
    public static Long computeTableBitmap(Set<RelationId> relationIdSet) {
        Long totalBitmap = 0L;
        for (RelationId id : relationIdSet) {
            if (id == null) {
                continue;
            }
            totalBitmap = LongBitmap.set(totalBitmap, (id.asInt()));
        }
        return totalBitmap;
    }

    public static String toString(long bitmap) {
        long[] longs = {bitmap};
        BitSet bitSet = BitSet.valueOf(longs);
        return bitSet.toString();
    }
}
