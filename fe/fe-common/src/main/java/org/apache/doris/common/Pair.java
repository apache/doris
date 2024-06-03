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
 * The equivalent of C++'s std::pair<>.
 * <p>
 * Notice: When using Pair for persistence, users need to guarantee that F and S can be serialized through Gson
 */
public class Pair<F, S> {
    public static PairComparator<Pair<?, Comparable>> PAIR_VALUE_COMPARATOR = new PairComparator<>();

    @SerializedName(value = "first")
    public F first;
    @SerializedName(value = "second")
    public S second;

    private Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public static <P, K extends P> Pair<K, K> ofSame(K same) {
        return new Pair<>(same, same);
    }

    public static <F, S> Pair<F, S> of(F first, S second) {
        return new Pair<>(first, second);
    }

    public F key() {
        return first;
    }

    public S value() {
        return second;
    }

    /**
     * A pair is equal if both parts are equal().
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof Pair) {
            Pair<F, S> other = (Pair<F, S>) o;

            boolean firstEqual = Objects.isNull(first) ? null == other.first : first.equals(other.first);
            boolean secondEqual = Objects.isNull(second) ? null == other.second : second.equals(other.second);
            return firstEqual && secondEqual;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        String firstStr = Objects.nonNull(first) ? first.toString() : "";
        String secondStr = Objects.nonNull(second) ? second.toString() : "";
        return firstStr + ":" + secondStr;
    }

    public static class PairComparator<T extends Pair<?, ? extends Comparable>> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            return o1.second.compareTo(o2.second);
        }
    }
}
