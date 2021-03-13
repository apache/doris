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
package org.apache.doris.analysis.ConditionExpr;

import org.apache.doris.catalog.Type;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public interface ValueSet
{
    static ValueSet none(Type type)
    {
        if (Utils.isOrderable(type)) {
            return SortedRangeSet.none(type);
        }
        return AllOrNoneValueSet.none(type);
    }

    static ValueSet all(Type type)
    {
        if (Utils.isOrderable(type)) {
            return SortedRangeSet.all(type);
        }
        return AllOrNoneValueSet.all(type);
    }

    static ValueSet of(Type type, Object first, Object... rest)
    {
        if (Utils.isOrderable(type)) {
            return SortedRangeSet.of(type, first, rest);
        }
        throw new IllegalArgumentException("Cannot create discrete ValueSet with non-comparable type: " + type);
    }

    static ValueSet copyOf(Type type, Collection<Object> values)
    {
        if (Utils.isOrderable(type)) {
            return SortedRangeSet.copyOf(type, values.stream()
                    .map(value -> Range.equal(type, value))
                    .collect(toList()));
        }
        throw new IllegalArgumentException("Cannot create discrete ValueSet with non-comparable type: " + type);
    }

    ValuesProcessor getValuesProcessor();

    static ValueSet ofRanges(Range first, Range... rest)
    {
        return SortedRangeSet.of(first, rest);
    }

    static ValueSet copyOfRanges(Type type, Collection<Range> ranges)
    {
        return SortedRangeSet.copyOf(type, ranges);
    }

    Type getType();

    boolean isNone();

    boolean isAll();

    boolean isSingleValue();

    Object getSingleValue();

    boolean isDiscreteSet();

    List<Object> getDiscreteSet();

    boolean containsValue(Object value);

    /**
     * @return range predicates for orderable Types
     */
    default Ranges getRanges()
    {
        throw new UnsupportedOperationException();
    }

    ValueSet intersect(ValueSet other);

    ValueSet union(ValueSet other);

    default ValueSet union(Collection<ValueSet> valueSets)
    {
        ValueSet current = this;
        for (ValueSet valueSet : valueSets) {
            current = current.union(valueSet);
        }
        return current;
    }

    ValueSet complement();

    default boolean overlaps(ValueSet other)
    {
        return !this.intersect(other).isNone();
    }

    default ValueSet subtract(ValueSet other)
    {
        return this.intersect(other.complement());
    }

    default boolean contains(ValueSet other)
    {
        return this.union(other).equals(this);
    }

    @Override
    String toString();
}
