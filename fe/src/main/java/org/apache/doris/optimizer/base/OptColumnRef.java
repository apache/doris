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
import org.apache.doris.optimizer.OptUtils;

// Reference to one column
public class OptColumnRef {
    // id is unique in one process of an optimization
    // Used in bit set to accelerate operation
    private final int id;
    private final Type type;
    // used to debug
    private final String name;
    private final int hashCode;

    public OptColumnRef(int id, Type type, String name) {
        this.id = id;
        this.type = type;
        this.name = name;
        this.hashCode = generateHashCode();
    }

    // TODO ch
    private int generateHashCode() {
        int result = 16;
        result = 31 * result + (id ^ (id >>> 32));
        result = OptUtils.combineHash(result, type);
        return OptUtils.combineHash(result, name);
    }

    public int getId() { return id; }
    public Type getType() { return type; }
    public String getName() { return name; }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null ||
                !(obj instanceof OptColumnRef)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        final OptColumnRef column = (OptColumnRef) obj;
        return id == column.id && type == column.type && name == column.name;
    }

    @Override
    public String toString() {
        return name;
    }
}
