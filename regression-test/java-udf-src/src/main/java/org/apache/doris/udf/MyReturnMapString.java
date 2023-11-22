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
import java.util.*;


public class MyReturnMapString {
    private static final Logger LOG = Logger.getLogger(MyReturnMapString.class);
    public static class State {
        public HashMap<Integer,Double> counter = new HashMap<>();
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, Integer k, Double v) {
        state.counter.put(k, v);
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        int size = state.counter.size();
        out.writeInt(size);
        for(Map.Entry<Integer,Double> it : state.counter.entrySet()){
            out.writeInt(it.getKey());
            out.writeDouble(it.getValue());
        }
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            Integer key = in.readInt();
            Double value = in.readDouble();
            state.counter.put(key, value);
        }
    }

    public void merge(State state, State rhs) {
        for(Map.Entry<Integer,Double> it : rhs.counter.entrySet()){
            state.counter.put(it.getKey(), it.getValue());
        }
    }

    public HashMap<String,String> getValue(State state) {
        //sort for regression test
        HashMap<String,String> map = new HashMap<>();
        for(Map.Entry<Integer,Double> it : state.counter.entrySet()){
            map.put(it.getKey() + "  114", it.getValue() + "  514");
        }
        return map;
    }
}