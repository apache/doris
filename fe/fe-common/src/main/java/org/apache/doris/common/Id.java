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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Id.java
// and modified by Doris

package org.apache.doris.common;

import java.util.ArrayList;

/**
 * Integer ids that cannot accidentally be compared with ints.
 */
public class Id<IdType extends Id<IdType>> implements Comparable<Id<IdType>> {
    protected final int id;

    public Id(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Id<?> id1 = (Id<?>) obj;
        return id == id1.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public int asInt() {
        return id;
    }

    public ArrayList<IdType> asList() {
        ArrayList<IdType> list = new ArrayList<>();
        list.add((IdType) this);
        return list;
    }

    public String toString() {
        return Integer.toString(id);
    }

    @Override
    public int compareTo(Id<IdType> idTypeId) {
        return id - idTypeId.id;
    }
}
