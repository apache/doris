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

/** MutableState */
public interface MutableState {
    String KEY_GROUP = "group";
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
            return new SingleMutableState(key, value);
        }
    }

    /** SingleMutableState */
    class SingleMutableState implements MutableState {
        public final String key;
        public final Object value;

        public SingleMutableState(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public <T> Optional<T> get(String key) {
            if (this.key.equals(key)) {
                return (Optional<T>) Optional.ofNullable(value);
            }
            return Optional.empty();
        }

        @Override
        public MutableState set(String key, Object value) {
            if (this.key.equals(key)) {
                return new SingleMutableState(key, value);
            }
            MultiMutableState multiMutableState = new MultiMutableState();
            multiMutableState.set(this.key, this.value);
            multiMutableState.set(key, value);
            return multiMutableState;
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
    }
}
