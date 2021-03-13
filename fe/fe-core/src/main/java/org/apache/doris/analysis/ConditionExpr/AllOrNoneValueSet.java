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

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Set that either includes all values, or excludes all values.
 */
public class AllOrNoneValueSet
        implements ValueSet
{
    private final Type type;
    private final boolean all;

    public AllOrNoneValueSet(Type type, boolean all)
    {
        this.type = requireNonNull(type, "type is null");
        this.all = all;
    }

    static AllOrNoneValueSet all(Type type)
    {
        return new AllOrNoneValueSet(type, true);
    }

    static AllOrNoneValueSet none(Type type)
    {
        return new AllOrNoneValueSet(type, false);
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public boolean isNone()
    {
        return !all;
    }

    @Override
    public boolean isAll()
    {
        return all;
    }

    @Override
    public boolean isSingleValue()
    {
        return false;
    }

    @Override
    public Object getSingleValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDiscreteSet()
    {
        return false;
    }

    @Override
    public List<Object> getDiscreteSet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value)
    {
        requireNonNull(value, "value is null");
        return all;
    }

    @Override
    public ValueSet intersect(ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all && otherValueSet.all);
    }

    @Override
    public ValueSet union(ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all || otherValueSet.all);
    }

    @Override
    public ValueSet complement()
    {
        return new AllOrNoneValueSet(type, !all);
    }

    @Override
    public String toString()
    {
        return "[" + (all ? "ALL" : "NONE") + "]";
    }


    @Override
    public int hashCode()
    {
        return Objects.hash(type, all);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AllOrNoneValueSet other = (AllOrNoneValueSet) obj;
        return Objects.equals(this.type, other.type)
                && this.all == other.all;
    }

    private AllOrNoneValueSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalArgumentException(format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof AllOrNoneValueSet)) {
            throw new IllegalArgumentException(format("ValueSet is not a AllOrNoneValueSet: %s", other.getClass()));
        }
        return (AllOrNoneValueSet) other;
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor()
        {
            @Override
            public <T> T transform(Function<Ranges, T> rangesFunction, Function<AllOrNone, T> allOrNoneFunction)
            {
                return rangesFunction.apply(getRanges());
            }
        };
    }
}
