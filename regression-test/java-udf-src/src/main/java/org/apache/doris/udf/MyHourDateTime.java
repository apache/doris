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
package org.apache.doris.udf;
import org.apache.log4j.Logger;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;

public class MyHourDateTime {
    private static final Logger LOG = Logger.getLogger(MyHourDateTime.class);
    public static class State {
        public LocalDateTime counter = LocalDateTime.of(2022,01,01,00,00,00);
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, LocalDateTime val1) {
        if (val1 == null) return;
        state.counter = state.counter.plusHours(val1.getHour());
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        out.writeInt(state.counter.getYear());
        out.writeInt(state.counter.getMonthValue());
        out.writeInt(state.counter.getDayOfMonth());
        out.writeInt(state.counter.getHour());
        out.writeInt(state.counter.getMinute());
        out.writeInt(state.counter.getSecond());
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        state.counter = LocalDateTime.of(in.readInt(),in.readInt(),in.readInt(),in.readInt(),in.readInt(),in.readInt());
    }

    public void merge(State state, State rhs) {
        state.counter = state.counter.plusHours(rhs.counter.getHour());
    }

    public LocalDateTime getValue(State state) {
        return state.counter;
    }
}