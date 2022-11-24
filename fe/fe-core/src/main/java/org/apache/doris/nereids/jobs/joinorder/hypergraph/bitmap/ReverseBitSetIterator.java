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

import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.Iterator;

/**
 * This is an Iterator for iterating bitmap descending
 */
public class ReverseBitSetIterator implements Iterable<Integer> {
    int lastIndex = 0;
    int readNum = 0;
    BitSet bitSet;

    ReverseBitSetIterator(BitSet bitSet) {
        this.bitSet = bitSet;
        lastIndex = bitSet.size();
    }

    @NotNull
    @Override
    public Iterator<Integer> iterator() {
        class Iter implements Iterator<Integer> {
            @Override
            public boolean hasNext() {
                return (readNum < bitSet.cardinality());
            }

            @Override
            public Integer next() {
                lastIndex = bitSet.previousSetBit(lastIndex - 1);
                readNum += 1;
                return lastIndex;
            }

            @Override
            public void remove() {
            }
        }

        return new Iter();
    }
}
