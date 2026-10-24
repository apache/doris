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

package org.apache.doris.common.profile;

import org.apache.doris.common.profile.PlanningDiagnostics.Phase;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.annotations.JsonAdapter;

import java.lang.reflect.Type;
import java.util.Arrays;

/** Primitive counters on the planning path, preserving the existing object-shaped Profile JSON. */
@JsonAdapter(PlanningPhaseCounters.Adapter.class)
final class PlanningPhaseCounters {
    private final long[] values = new long[Phase.values().length];

    PlanningPhaseCounters() {
        Arrays.fill(values, -1);
    }

    void add(Phase phase, long amount) {
        int index = phase.ordinal();
        values[index] = values[index] < 0 ? amount : values[index] + amount;
    }

    JsonObject toJson() {
        JsonObject result = new JsonObject();
        for (Phase phase : Phase.values()) {
            long value = values[phase.ordinal()];
            if (value >= 0) {
                result.addProperty(phase.key(), value);
            }
        }
        return result;
    }

    static final class Adapter implements JsonSerializer<PlanningPhaseCounters>,
            JsonDeserializer<PlanningPhaseCounters> {
        @Override
        public JsonElement serialize(PlanningPhaseCounters value, Type type, JsonSerializationContext context) {
            return value.toJson();
        }

        @Override
        public PlanningPhaseCounters deserialize(JsonElement json, Type type, JsonDeserializationContext context) {
            PlanningPhaseCounters result = new PlanningPhaseCounters();
            JsonObject values = json.getAsJsonObject();
            for (Phase phase : Phase.values()) {
                if (values.has(phase.key())) {
                    result.values[phase.ordinal()] = values.get(phase.key()).getAsLong();
                }
            }
            return result;
        }
    }
}
