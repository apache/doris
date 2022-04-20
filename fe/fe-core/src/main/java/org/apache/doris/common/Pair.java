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

/**
 * The equivalent of C++'s std::pair<>.
 *
 * Notice: When using Pair for persistence, users need to guarantee that F and S can be serialized through Gson
 */
public class Pair<F, S> {
    public static PairComparator<Pair<?, Comparable>> PAIR_VALUE_COMPARATOR = new PairComparator<>();

    @SerializedName(value = "first")
    public F first;
    @SerializedName(value = "second")
    public S second;

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    static public <F, S> Pair<F, S> create(F first, S second) {
        return new Pair<F, S>(first, second);
    }

    @Override
    /**
     * A pair is equal if both parts are equal().
     */
    public boolean equals(Object o) {
        if (o instanceof Pair) {
            Pair<F, S> other = (Pair<F, S>) o;
            return this.first.equals(other.first) && this.second.equals(other.second);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hashFirst = first != null ? first.hashCode() : 0;
        int hashSecond = second != null ? second.hashCode() : 0;

        return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    @Override
    public String toString() {
        return first.toString() + ":" + second.toString();
    }

    public static class PairComparator<T extends Pair<?, Comparable>> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            return o1.second.compareTo(o2.second);
        }
    }
}
