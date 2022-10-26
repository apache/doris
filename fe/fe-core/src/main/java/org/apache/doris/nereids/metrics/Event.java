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

package org.apache.doris.nereids.metrics;

import org.apache.doris.nereids.metrics.event.CostStateEvent;
import org.apache.doris.nereids.metrics.event.CounterEvent;
import org.apache.doris.nereids.metrics.event.EnforcerEvent;
import org.apache.doris.nereids.metrics.event.FunctionCallEvent;
import org.apache.doris.nereids.metrics.event.GroupMergeEvent;
import org.apache.doris.nereids.metrics.event.MemoTransformEvent;
import org.apache.doris.nereids.metrics.event.StatsStateEvent;
import org.apache.doris.nereids.metrics.event.TransformEvent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * event
 */
public abstract class Event implements Cloneable {
    public static final ImmutableMap EVENT_TYPE_SET = new Builder<String, Class<? extends Event>>()
            .put("costState", CostStateEvent.class)
            .put("counter", CounterEvent.class)
            .put("enforcer", EnforcerEvent.class)
            .put("functionCall", FunctionCallEvent.class)
            .put("groupMerge", GroupMergeEvent.class)
            .put("memoTransform", MemoTransformEvent.class)
            .put("statsState", StatsStateEvent.class)
            .put("transform", TransformEvent.class)
            .build();
    private final long stateId;

    protected Event() {
        this.stateId = -1;
    }

    protected Event(long stateId) {
        this.stateId = stateId;
    }

    public final String toJson() {
        return "[stateId=" + stateId + "]";
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public Event clone() throws CloneNotSupportedException {
        return (Event) super.clone();
    }
}
