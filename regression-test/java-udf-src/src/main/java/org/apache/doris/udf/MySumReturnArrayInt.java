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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class MySumReturnArrayInt {
    private static final Logger LOG = Logger.getLogger(MySumReturnArrayInt.class);
    public static class State {
        public ArrayList<Integer> counter = new ArrayList<Integer>();
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, ArrayList<Integer> val) {
        if (val == null) {
            return;
        }
        for (int i = 0; i < val.size(); ++i) {
            Integer data = val.get(i);
            
            if (data != null) {
                state.counter.add(data);
            }
        }
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        int size = state.counter.size();
        out.writeInt(size);
        for (int i = 0; i < size; ++i) {
            out.writeInt(state.counter.get(i));
        }
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            state.counter.add(in.readInt());
        }
    }

    public void merge(State state, State rhs) {
        state.counter.addAll(rhs.counter);
    }

    public ArrayList<Integer> getValue(State state) {
        //sort for regression test
        state.counter.sort(Comparator.naturalOrder());
        return state.counter;
    }
}