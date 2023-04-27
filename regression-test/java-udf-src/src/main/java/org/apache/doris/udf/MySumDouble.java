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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MySumDouble {
    public static class State {
        public double counter = 0.0;
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, Double val1, Double val2) {
        if (val1 == null || val2 == null) return;
        state.counter += val1;
        state.counter += val2;
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        out.writeDouble(state.counter);
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        state.counter = in.readDouble();
    }

    public void merge(State state, State rhs) {
        state.counter += rhs.counter;
    }

    public Double getValue(State state) {
        return state.counter;
    }
}