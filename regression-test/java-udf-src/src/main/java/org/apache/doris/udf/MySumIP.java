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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Collectors;

public class MySumIP {
    public static class State {
        public ArrayList<String> list = new ArrayList<>();
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, InetAddress val1) {
        if (val1 == null)
            return;
        state.list.add(val1.toString());
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        out.writeInt(state.list.size());

        for (String str : state.list) {
            out.writeUTF(str);
        }
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        int size = in.readInt();

        state.list = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            state.list.add(in.readUTF());
        }
    }

    public void merge(State state, State rhs) {
        for (String s : rhs.list) {
            state.list.add(s);
        }
    }

    public String getValue(State state) {
        state.list.sort(Comparator.nullsLast(String::compareTo));
        String result = state.list.stream()
                .filter(s -> s != null)
                .collect(Collectors.joining(", "));
        return result;
    }
}