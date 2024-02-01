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

package org.apache.doris.nereids.trees.plans;

/**
 * Represents different phase of sort and map it to the
 * enum of sort phase definition of stale optimizer.
 */
public enum SortPhase {
    MERGE_SORT("MergeSort"),
    GATHER_SORT("GatherSort"),
    LOCAL_SORT("LocalSort");
    private final String name;

    SortPhase(String name) {
        this.name = name;
    }

    public boolean isLocal() {
        return this == LOCAL_SORT;
    }

    public boolean isMerge() {
        return this == MERGE_SORT;
    }

    public boolean isGather() {
        return this == GATHER_SORT;
    }
}
