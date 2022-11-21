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

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

/**
 * This is a class for iterating all true subset of a bitset, referenced in
 * https://groups.google.com/forum/#!msg/rec.games.chess/KnJvBnhgDKU/yCi5yBx18PQJ
 */
public class SubsetIterator implements Iterable<BitSet> {
    List<BitSet> subsets = new ArrayList<>();
    int cursor = 0;

    /**
     * Generate all subset for this bitSet
     *
     * @param bitSet The bitset that need to be generated
     */
    public SubsetIterator(BitSet bitSet) {
        long[] setVal = bitSet.toLongArray();
        int len = setVal.length;
        long[] baseVal = new long[len];
        subsets.add(new BitSet());
        for (int i = 0; i < len; i++) {
            long subVal = (-setVal[i]) & setVal[i];
            int size = subsets.size();
            while (subVal != 0) {
                baseVal[i] = subVal;
                for (int j = 0; j < size; j++) {
                    BitSet newSubset = BitSet.valueOf(baseVal);
                    newSubset.or(subsets.get(j));
                    subsets.add(newSubset);
                }
                subVal = (subVal - setVal[i]) & setVal[i];
            }
            baseVal[i] = 0;
        }
        // remove empty subset
        subsets.remove(0);
    }

    public void reset() {
        cursor = 0;
    }

    @NotNull
    @Override
    public Iterator<BitSet> iterator() {
        class Iter implements Iterator<BitSet> {
            @Override
            public boolean hasNext() {
                return (cursor < subsets.size());
            }

            @Override
            public BitSet next() {
                return subsets.get(cursor++);
            }

            @Override
            public void remove() {
            }
        }

        return new Iter();
    }
}
