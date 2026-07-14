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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/** Immutable rewrite decision for one candidate TopN. */
public final class TopNLazyAnalysis {
    private final List<Slot> materializedSlots;
    private final Map<Slot, MaterializeSource> lazyOutputs;

    public TopNLazyAnalysis(List<Slot> materializedSlots,
            Map<Slot, MaterializeSource> lazyOutputs) {
        this.materializedSlots = ImmutableList.copyOf(materializedSlots);
        this.lazyOutputs = ImmutableMap.copyOf(lazyOutputs);
    }

    public List<Slot> getMaterializedSlots() {
        return materializedSlots;
    }

    public Map<Slot, MaterializeSource> getLazyOutputs() {
        return lazyOutputs;
    }
}
