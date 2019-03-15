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

import org.apache.doris.catalog.Type;

// Reference to one column
public class OptColumnRef {
    // id is unique in one process of an optimization
    // Used in bit set to accelerate operation
    private int id;
    private Type type;
    // used to debug
    private String name;

    public OptColumnRef(int id, Type type, String name) {
        this.id = id;
        this.type = type;
        this.name = name;
    }

    public int getId() { return id; }
    public Type getType() { return type; }
    public String getName() { return name; }

    @Override
    public String toString() {
        return name;
    }
}
