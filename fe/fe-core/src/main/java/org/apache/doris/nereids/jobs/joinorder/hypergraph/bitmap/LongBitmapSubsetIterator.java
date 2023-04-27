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

import java.util.Iterator;

/**
 * This is a class for iterating all true subset of a bitset, referenced in
 * https://groups.google.com/forum/#!msg/rec.games.chess/KnJvBnhgDKU/yCi5yBx18PQJ
 */
public class LongBitmapSubsetIterator implements Iterable<Long> {
    private long bitmap;
    private long state;

    /**
     * Generate all subset for this bitSet
     *
     * @param bitmap The bitset that need to be generated
     */
    public LongBitmapSubsetIterator(long bitmap) {
        this.bitmap = bitmap;
        this.state = (-bitmap) & bitmap;
    }

    public void reset() {
        state = (-bitmap) & bitmap;
    }

    @NotNull
    @Override
    public Iterator<Long> iterator() {
        class Iter implements Iterator<Long> {
            @Override
            public boolean hasNext() {
                return state != 0;
            }

            @Override
            public Long next() {
                Long subset = state;
                state = (state - bitmap) & bitmap;
                return subset;
            }

            @Override
            public void remove() {
            }
        }

        return new Iter();
    }
}
