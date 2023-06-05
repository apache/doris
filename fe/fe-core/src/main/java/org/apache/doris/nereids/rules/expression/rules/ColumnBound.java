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

import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

/** ColumnBound */
public class ColumnBound implements Comparable<ColumnBound> {
    private final Literal value;

    private ColumnBound(Literal value) {
        this.value = value;
    }

    @Override
    public int compareTo(ColumnBound o) {
        return value.toLegacyLiteral().compareTo(o.value.toLegacyLiteral());
    }

    public static ColumnBound of(Literal expr) {
        return new ColumnBound(expr);
    }

    public Literal getValue() {
        return value;
    }

    // <
    public static Range<ColumnBound> lessThen(Literal value) {
        return Range.lessThan(new ColumnBound(value));
    }

    // <=
    public static Range<ColumnBound> atMost(Literal value) {
        return Range.atMost(new ColumnBound(value));
    }

    // >
    public static Range<ColumnBound> greaterThan(Literal value) {
        return Range.greaterThan(new ColumnBound(value));
    }

    // >=
    public static Range<ColumnBound> atLeast(Literal value) {
        return Range.atLeast(new ColumnBound(value));
    }

    public static Range<ColumnBound> all() {
        return Range.all();
    }

    public static ColumnRange empty() {
        return ColumnRange.empty();
    }

    public static Range<ColumnBound> singleton(Literal value) {
        return Range.singleton(new ColumnBound(value));
    }

    public static Range<ColumnBound> between(Literal lower, Literal upper) {
        return Range.range(new ColumnBound(lower), BoundType.CLOSED, new ColumnBound(upper), BoundType.CLOSED);
    }

    public static Range<ColumnBound> range(Literal lower, BoundType lowerType, Literal upper, BoundType upperType) {
        return Range.range(new ColumnBound(lower), lowerType, new ColumnBound(upper), upperType);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("value", PartitionKey.toString(ImmutableList.of(value.toLegacyLiteral())))
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnBound that = (ColumnBound) o;
        return Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
