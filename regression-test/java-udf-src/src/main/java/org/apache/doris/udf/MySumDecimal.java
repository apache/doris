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
import java.math.BigDecimal;

public class MySumDecimal {
    private static final Logger LOG = Logger.getLogger(MySumDecimal.class);
    public static class State {
        public BigDecimal counter = new BigDecimal(0);
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, BigDecimal val1) {
        if (val1 == null) return;
        state.counter = state.counter.add(val1);
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        String val = state.counter.toString();
        out.writeUTF(val);
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        String val = in.readUTF();
        state.counter = new BigDecimal(val);
    }

    public void merge(State state, State rhs) {
        state.counter = state.counter.add(rhs.counter);
    }

    public BigDecimal getValue(State state) {
        return state.counter;
    }
}