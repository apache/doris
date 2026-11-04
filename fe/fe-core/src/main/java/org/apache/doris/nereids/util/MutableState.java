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

package org.apache.doris.nereids.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** MutableState */
public interface MutableState {
    String KEY_GROUP = "group";
    String KEY_FRAGMENT = "fragment";
    String KEY_PARENT = "parent";
    String KEY_RF_JUMP = "rf-jump";
    String KEY_PUSH_TOPN_TO_AGG = "pushTopnToAgg";
    /** hbo fingerprint attached at planning time for explain printing */
    String KEY_HBO_FP = "hbo-fingerprint";
    /**
     * hbo constant agnostic fingerprint attached at planning time for explain printing; filter, join
     * and aggregation nodes all carry both forms (literal carrying + shape), scans only the shape
     */
    String KEY_HBO_FP_NO_LITERAL = "hbo-fingerprint-no-literal";

    /** the literal carrying key of a join / aggregation root (see KEY_HBO_FP_NO_LITERAL) */
    String KEY_HBO_FP_WITH_LITERAL = "hbo-fingerprint-with-literal";
    /** hbo simplified struct info canonical string attached at planning time for explain printing */
    String KEY_HBO_STRUCT = "hbo-struct";
    /**
     * hbo struct info of the same node in the constant agnostic form, built by the group traversal
     * (never by folding the literal carrying form of {@link #KEY_HBO_STRUCT} as text)
     */
    String KEY_HBO_STRUCT_NO_LITERAL = "hbo-struct-no-literal";

    /** the literal carrying struct info of a join / aggregation root */
    String KEY_HBO_STRUCT_WITH_LITERAL = "hbo-struct-with-literal";
    /**
     * why no struct info was attached to this node, when it was refused by a limit instead of being
     * unsupported (e.g. its sub tree reads more scans than {@code hbo_max_scans_per_group}); printed
     * by the explain annotation so a missing fingerprint can be told from a broken one
     */
    String KEY_HBO_STRUCT_SKIP = "hbo-struct-skip";
    /** canonical string of the join equality conditions (join nodes, for HBO SET EXPANSION) */
    String KEY_HBO_COND = "hbo-cond";
    /** fingerprint of the join equality conditions (join nodes, the expansion injection key) */
    String KEY_HBO_COND_FP = "hbo-cond-fp";
    /** applied injected join expansion, e.g. {@code exp=200x} */
    String KEY_HBO_EXPANSION = "hbo-expansion";

    <T> Optional<T> get(String key);

    MutableState set(String key, Object value);

    /**
     * A holder with the same entries as this one. {@code set} of a multi state mutates the holder it is
     * called on, so a holder must not be shared between two plans: they would observe each other's later
     * writes (a rebuilt node and the node it replaces, for instance).
     */
    MutableState copy();

    /** EmptyMutableState */
    class EmptyMutableState implements MutableState {
        public static final EmptyMutableState INSTANCE = new EmptyMutableState();

        private EmptyMutableState() {}

        @Override
        public MutableState copy() {
            return this;
        }

        @Override
        public <T> Optional<T> get(String key) {
            return Optional.empty();
        }

        @Override
        public MutableState set(String key, Object value) {
            MultiMutableState state = new MultiMutableState();
            state.set(key, value);
            return state;
        }

        @Override
        public String toString() {
            return "[]";
        }
    }

    /** MultiMutableState */
    class MultiMutableState implements MutableState {
        private final Map<String, Object> states = new LinkedHashMap<>();

        @Override
        public <T> Optional<T> get(String key) {
            return (Optional<T>) Optional.ofNullable(states.get(key));
        }

        @Override
        public MutableState set(String key, Object value) {
            states.put(key, value);
            return this;
        }

        @Override
        public MutableState copy() {
            MultiMutableState copied = new MultiMutableState();
            copied.states.putAll(states);
            return copied;
        }

        @Override
        public String toString() {
            return states.entrySet()
                    .stream()
                    .map(kv -> kv.getKey() + ": " + (kv.getValue() == null ? "null" : kv.getValue()))
                    .collect(Collectors.joining(", ", "[", "]"));
        }
    }
}
