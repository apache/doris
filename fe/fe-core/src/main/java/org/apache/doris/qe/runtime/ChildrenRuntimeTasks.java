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

package org.apache.doris.qe.runtime;

import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class ChildrenRuntimeTasks<Id, C extends AbstractRuntimeTask<?, ?>> {
    // LinkedHashMap: make sure the key set order is same as the input map
    //                so that we can initial the runtime filter merge backend first
    private final Map<Id, C> childrenTasks = Maps.newLinkedHashMap();

    public ChildrenRuntimeTasks(Map<Id, C> childrenTasks) {
        this.childrenTasks.putAll(childrenTasks);
    }

    public C get(Id id) {
        return childrenTasks.get(id);
    }

    public List<C> allTasks() {
        return Utils.fastToImmutableList(childrenTasks.values());
    }

    public Map<Id, C> allTaskMap() {
        return ImmutableMap.copyOf(childrenTasks);
    }
}
