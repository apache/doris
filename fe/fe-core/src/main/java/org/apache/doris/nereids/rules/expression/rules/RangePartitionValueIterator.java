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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/** RangePartitionValueIterator */
public abstract class RangePartitionValueIterator<C extends Comparable, L extends Literal>
        implements Iterator<L> {
    private final C startInclusive;
    private final C end;
    private final boolean endExclusive;
    private C current;

    private final Function<C, L> toLiteral;

    public RangePartitionValueIterator(C startInclusive, C end, boolean endExclusive, Function<C, L> toLiteral) {
        this.startInclusive = startInclusive;
        this.end = end;
        this.endExclusive = endExclusive;
        this.current = this.startInclusive;
        this.toLiteral = toLiteral;
    }

    @Override
    public boolean hasNext() {
        if (endExclusive) {
            return current.compareTo(end) < 0;
        } else {
            return current.compareTo(end) <= 0;
        }
    }

    @Override
    public L next() {
        if (hasNext()) {
            C value = current;
            current = doGetNext(current);
            return toLiteral.apply(value);
        }
        throw new NoSuchElementException();
    }

    protected abstract C doGetNext(C current);
}
