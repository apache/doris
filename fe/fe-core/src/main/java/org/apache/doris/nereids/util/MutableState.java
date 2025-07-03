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

    <T> Optional<T> get(String key);

    MutableState set(String key, Object value);

    /** EmptyMutableState */
    class EmptyMutableState implements MutableState {
        public static final EmptyMutableState INSTANCE = new EmptyMutableState();

        private EmptyMutableState() {}

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
        public String toString() {
            return states.entrySet()
                    .stream()
                    .map(kv -> kv.getKey() + ": " + (kv.getValue() == null ? "null" : kv.getValue()))
                    .collect(Collectors.joining(", ", "[", "]"));
        }
    }
}
