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

public class MyReturnArrayString {
    private static final Logger LOG = Logger.getLogger(MyReturnArrayString.class);
    public static class State {
        public ArrayList<String> data = new ArrayList<String>();
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {

    }

    public void add(State state, ArrayList<String> val) {
        if (val == null) return;
        for (int i = 0; i < val.size(); ++i) {
            String s = val.get(i);
            if (s != null) {
                state.data.add(s);
            }
        }
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        int size = state.data.size();
        out.writeInt(size);
        for (int i = 0; i < size; ++i) {
            String val = state.data.get(i);
            out.writeInt(val.length());
            out.writeBytes(val);
        }
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.read(bytes);
            state.data.add(new String(bytes));
        }
    }

    public void merge(State state, State rhs) {
        state.data.addAll(rhs.data);
    }

    public ArrayList<String> getValue(State state) {
        //sort for regression test
        state.data.sort(Comparator.naturalOrder());
        return state.data;
    }
}