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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Pair.java
// and modified by Doris

package org.apache.doris.common;

import com.google.gson.annotations.SerializedName;

import java.util.Comparator;
import java.util.Objects;

/**
 * The equivalent of a {@link Pair} but with three elements: left, middle, and right.
 * <p>
 * Notice: When using Triple for persistence, users need to guarantee that L, M, and R can be serialized through Gson
 */
public class Triple<L, M, R> {
    public static TripleComparator<Triple<?, ?, Comparable>> TRIPLE_VALUE_COMPARATOR = new TripleComparator<>();

    @SerializedName(value = "left")
    public L left;
    @SerializedName(value = "middle")
    public M middle;
    @SerializedName(value = "right")
    public R right;

    private Triple(L left, M middle, R right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }

    public static <P, K extends P> Triple<K, K, K> ofSame(K same) {
        return new Triple<>(same, same, same);
    }

    public static <L, M, R> Triple<L, M, R> of(L left, M middle, R right) {
        return new Triple<>(left, middle, right);
    }

    public L getLeft() {
        return left;
    }

    public M getMiddle() {
        return middle;
    }

    public R getRight() {
        return right;
    }

    /**
     * A triple is equal if all three parts are equal().
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof Triple) {
            Triple<L, M, R> other = (Triple<L, M, R>) o;

            boolean leftEqual = Objects.isNull(left) ? other.left == null : left.equals(other.left);
            boolean middleEqual = Objects.isNull(middle) ? other.middle == null : middle.equals(other.middle);
            boolean rightEqual = Objects.isNull(right) ? other.right == null : right.equals(other.right);

            return leftEqual && middleEqual && rightEqual;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, middle, right);
    }

    @Override
    public String toString() {
        String leftStr = Objects.nonNull(left) ? left.toString() : "";
        String middleStr = Objects.nonNull(middle) ? middle.toString() : "";
        String rightStr = Objects.nonNull(right) ? right.toString() : "";
        return leftStr + ":" + middleStr + ":" + rightStr;
    }

    public static class TripleComparator<T extends Triple<?, ?, ? extends Comparable>> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            return o1.right.compareTo(o2.right);
        }
    }
}
