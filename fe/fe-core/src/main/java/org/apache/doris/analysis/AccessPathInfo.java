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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TupleDescriptor.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TColumnAccessPath;

import java.util.List;

/** AccessPathInfo */
public class AccessPathInfo {
    public static final String ACCESS_ALL = "*";
    public static final String ACCESS_MAP_KEYS = "KEYS";
    public static final String ACCESS_MAP_VALUES = "VALUES";

    private DataType prunedType;
    // allAccessPaths is used to record all access path include predicate access path and non-predicate access path,
    // and predicateAccessPaths only contains the predicate access path.
    // e.g. select element_at(s, 'name') from tbl where element_at(s, 'id') = 1
    //      the allAccessPaths is: ["s.name", "s.id"]
    //      the predicateAccessPaths is: ["s.id"]
    private List<TColumnAccessPath> allAccessPaths;
    private List<TColumnAccessPath> predicateAccessPaths;

    public AccessPathInfo(DataType prunedType, List<TColumnAccessPath> allAccessPaths,
            List<TColumnAccessPath> predicateAccessPaths) {
        this.prunedType = prunedType;
        this.allAccessPaths = allAccessPaths;
        this.predicateAccessPaths = predicateAccessPaths;
    }

    public DataType getPrunedType() {
        return prunedType;
    }

    public void setPrunedType(DataType prunedType) {
        this.prunedType = prunedType;
    }

    public List<TColumnAccessPath> getAllAccessPaths() {
        return allAccessPaths;
    }

    public void setAllAccessPaths(List<TColumnAccessPath> allAccessPaths) {
        this.allAccessPaths = allAccessPaths;
    }

    public List<TColumnAccessPath> getPredicateAccessPaths() {
        return predicateAccessPaths;
    }

    public void setPredicateAccessPaths(List<TColumnAccessPath> predicateAccessPaths) {
        this.predicateAccessPaths = predicateAccessPaths;
    }
}
