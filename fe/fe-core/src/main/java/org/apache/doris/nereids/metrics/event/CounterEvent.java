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

package org.apache.doris.nereids.metrics.event;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.metrics.CounterType;
import org.apache.doris.nereids.metrics.Event;
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * counter event
 */
public class CounterEvent extends Event {
    private final long count;
    private final CounterType counterType;
    private final Group group;
    private final GroupExpression groupExpression;
    private final Plan plan;

    /**
     * counter event
     */
    public CounterEvent(long stateId, long count, CounterType counterType, Group group,
            GroupExpression groupExpression, Plan plan) {
        super(stateId);
        this.count = count;
        this.counterType = counterType;
        this.group = group;
        this.groupExpression = groupExpression;
        this.plan = plan;
    }

    @Override
    public String toString() {
        return "CounterEvent{"
                + "count=" + count
                + ", counterType=" + counterType
                + ", group=" + group
                + ", groupExpression=" + groupExpression
                + ", plan=" + plan
                + '}';
    }
}
