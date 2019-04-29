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

package org.apache.doris.optimizer.base;

import com.google.common.collect.Maps;
import org.apache.doris.catalog.Type;

import java.util.Map;

public class OptColumnRefFactory {
    // usd
    private int nextId = 1;
    private Map<Integer, OptColumnRef> columnRefMap = Maps.newHashMap();

    public OptColumnRef create(Type type) {
        int id = nextId++;
        String name = "ColRef_" + id;
        return create(id, name, type);
    }

    public OptColumnRef create(String name, Type type) {
        return create(nextId++, name, type);
    }

    private OptColumnRef create(int id, String name, Type type) {
        OptColumnRef columnRef = new OptColumnRef(id, type, name);
        columnRefMap.put(id, columnRef);
        return columnRef;
    }

    public Map<Integer, OptColumnRef> getColumnRefMap() { return columnRefMap; }
}
